package tcp

import (
	"context"
	"errors"
	"sync"

	"github.com/rs/zerolog/log"
)

type server struct {
	Handler
	name   string
	weight int
}

// WRRLoadBalancer is a naive RoundRobin load balancer for TCP services.
type WRRLoadBalancer struct {
	servers    []server
	handlersMu sync.Mutex

	wantsHealthCheck bool

	currentWeight int
	index         int

	// status is a record of which child services of the Balancer are healthy, keyed
	// by name of child service. A service is initially added to the map when it is
	// created via Add, and it is later removed or added to the map as needed,
	// through the SetStatus method.
	status map[string]struct{}
	// updaters is the list of hooks that are run (to update the Balancer
	// parent(s)), whenever the Balancer status changes.
	updaters []func(bool)
}

// NewWRRLoadBalancer creates a new WRRLoadBalancer.
func NewWRRLoadBalancer(wantHealthCheck bool) *WRRLoadBalancer {
	return &WRRLoadBalancer{
		index:            -1,
		wantsHealthCheck: wantHealthCheck,
		status:           make(map[string]struct{}),
	}
}

// /traefik/pkg/server/service/loadbalancer/wrr.go

// SetStatus sets on the balancer that its given child is now of the given
// status. balancerName is only needed for logging purposes.
func (b *WRRLoadBalancer) SetStatus(ctx context.Context, childName string, up bool) {
	b.handlersMu.Lock()
	defer b.handlersMu.Unlock()

	upBefore := len(b.status) > 0

	status := "DOWN"
	if up {
		status = "UP"
	}

	log.Ctx(ctx).Debug().Msgf("Setting status of %s to %v", childName, status)

	if up {
		b.status[childName] = struct{}{}
	} else {
		delete(b.status, childName)
	}

	upAfter := len(b.status) > 0
	status = "DOWN"
	if upAfter {
		status = "UP"
	}

	// No Status Change
	if upBefore == upAfter {
		// We're still with the same status, no need to propagate
		log.Ctx(ctx).Debug().Msgf("Still %s, no need to propagate", status)
		return
	}

	// Status Change
	log.Ctx(ctx).Debug().Msgf("Propagating new %s status", status)
	for _, fn := range b.updaters {
		fn(upAfter)
	}
}

// RegisterStatusUpdater adds fn to the list of hooks that are run when the
// status of the Balancer changes.
// Not thread safe.
func (b *WRRLoadBalancer) RegisterStatusUpdater(fn func(up bool)) error {
	if !b.wantsHealthCheck {
		return errors.New("healthCheck not enabled in config for this weighted service")
	}
	b.updaters = append(b.updaters, fn)
	return nil
}

// ServeTCP forwards the connection to the right service.
func (b *WRRLoadBalancer) ServeTCP(conn WriteCloser) {
	next, err := b.next()
	if err != nil {
		log.Error().Err(err).Msg("Error during load balancing")
		conn.Close()
		return
	}

	next.ServeTCP(conn)
}

// AddServer appends a server to the existing list.
func (b *WRRLoadBalancer) AddServer(name string, serverHandler Handler) {
	w := 1
	b.AddWeightServer(name, serverHandler, &w)
}

// AddWeightServer appends a server to the existing list with a weight.
func (b *WRRLoadBalancer) AddWeightServer(name string, serverHandler Handler, weight *int) {
	b.handlersMu.Lock()
	defer b.handlersMu.Unlock()

	w := 1
	if weight != nil {
		w = *weight
	}

	b.servers = append(b.servers, server{
		Handler: serverHandler,
		name:    name,
		weight:  w,
	})
	b.status[name] = struct{}{}
}

func (b *WRRLoadBalancer) maxWeight() int {
	max := -1
	for _, s := range b.servers {
		if s.weight > max {
			max = s.weight
		}
	}
	return max
}

func (b *WRRLoadBalancer) weightGcd() int {
	divisor := -1
	for _, s := range b.servers {
		if divisor == -1 {
			divisor = s.weight
		} else {
			divisor = gcd(divisor, s.weight)
		}
	}
	return divisor
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func (b *WRRLoadBalancer) next() (Handler, error) {
	b.handlersMu.Lock()
	defer b.handlersMu.Unlock()

	if len(b.servers) == 0 || len(b.status) == 0 {
		return nil, errors.New("no servers in the pool")
	}

	// The algo below may look messy, but is actually very simple
	// it calculates the GCD  and subtracts it on every iteration, what interleaves servers
	// and allows us not to build an iterator every time we readjust weights

	// Maximum weight across all enabled servers
	max := b.maxWeight()
	if max == 0 {
		return nil, errors.New("all servers have 0 weight")
	}

	// GCD across all enabled servers
	gcd := b.weightGcd()

	var srv server
	for {
		b.index = (b.index + 1) % len(b.servers)
		if b.index == 0 {
			b.currentWeight -= gcd
			if b.currentWeight <= 0 {
				b.currentWeight = max
			}
		}
		srv = b.servers[b.index]
		if srv.weight >= b.currentWeight {
			if _, ok := b.status[srv.name]; ok {
				break
			}
		}
	}

	log.Debug().Msgf("Service selected by WRR: %s", srv.name)
	return srv, nil
}
