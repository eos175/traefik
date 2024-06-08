package tcp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net"
	"net/url"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/runtime"
	"github.com/traefik/traefik/v3/pkg/healthcheck"
	"github.com/traefik/traefik/v3/pkg/logs"
	"github.com/traefik/traefik/v3/pkg/server/middleware"
	"github.com/traefik/traefik/v3/pkg/server/provider"
	"github.com/traefik/traefik/v3/pkg/tcp"
	"golang.org/x/net/proxy"
)

// Manager is the TCPHandlers factory.
type Manager struct {
	dialerManager    *tcp.DialerManager
	observabilityMgr *middleware.ObservabilityMgr

	configs        map[string]*runtime.TCPServiceInfo
	healthCheckers map[string]*healthcheck.ServiceHealthChecker
	rand           *rand.Rand // For the initial shuffling of load-balancers.
}

// NewManager creates a new manager.
func NewManager(conf *runtime.Configuration, observabilityMgr *middleware.ObservabilityMgr, dialerManager *tcp.DialerManager) *Manager {
	return &Manager{
		dialerManager:    dialerManager,
		observabilityMgr: observabilityMgr,
		configs:          conf.TCPServices,
		healthCheckers:   make(map[string]*healthcheck.ServiceHealthChecker),
		rand:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// BuildTCP Creates a tcp.Handler for a service configuration.
func (m *Manager) BuildTCP(rootCtx context.Context, serviceName string) (tcp.Handler, error) {
	serviceQualifiedName := provider.GetQualifiedName(rootCtx, serviceName)

	logger := log.Ctx(rootCtx).With().Str(logs.ServiceName, serviceQualifiedName).Logger()
	ctx := provider.AddInContext(rootCtx, serviceQualifiedName)

	conf, ok := m.configs[serviceQualifiedName]
	if !ok {
		return nil, fmt.Errorf("the service %q does not exist", serviceQualifiedName)
	}

	if conf.LoadBalancer != nil && conf.Weighted != nil {
		err := errors.New("cannot create service: multi-types service not supported, consider declaring two different pieces of service instead")
		conf.AddError(err, true)
		return nil, err
	}

	switch {
	case conf.LoadBalancer != nil:
		service := conf.LoadBalancer

		loadBalancer := tcp.NewWRRLoadBalancer(service.HealthCheck != nil)
		healthCheckTargets := make(map[string]*url.URL)

		if conf.LoadBalancer.TerminationDelay != nil {
			log.Ctx(ctx).Warn().Msgf("Service %q load balancer uses `TerminationDelay`, but this option is deprecated, please use ServersTransport configuration instead.", serviceName)
		}

		if len(conf.LoadBalancer.ServersTransport) > 0 {
			conf.LoadBalancer.ServersTransport = provider.GetQualifiedName(ctx, conf.LoadBalancer.ServersTransport)
		}

		for index, server := range shuffle(conf.LoadBalancer.Servers, m.rand) {
			proxyName := makeProxyName(server.Address)
			target := &url.URL{
				Scheme: "tcp",
				Host:   server.Address,
			}

			srvLogger := logger.With().
				Int(logs.ServerIndex, index).
				Str("serverAddress", server.Address).Logger()

			if _, _, err := net.SplitHostPort(server.Address); err != nil {
				srvLogger.Error().Err(err).Msg("Failed to split host port")
				continue
			}

			dialer, err := m.dialerManager.Get(conf.LoadBalancer.ServersTransport, server.TLS)
			if err != nil {
				return nil, err
			}

			// Handle TerminationDelay deprecated option.
			if conf.LoadBalancer.ServersTransport == "" && conf.LoadBalancer.TerminationDelay != nil {
				dialer = &dialerWrapper{
					Dialer:           dialer,
					terminationDelay: time.Duration(*conf.LoadBalancer.TerminationDelay),
				}
			}

			handler, err := tcp.NewProxy(server.Address, conf.LoadBalancer.ProxyProtocol, dialer)
			if err != nil {
				srvLogger.Error().Err(err).Msg("Failed to create server")
				continue
			}

			loadBalancer.AddServer(proxyName, handler)

			// servers are considered UP by default.
			conf.UpdateServerStatus(target.String(), runtime.StatusUp)

			healthCheckTargets[proxyName] = target

			logger.Debug().Msg("Creating TCP server")
		}

		if service.HealthCheck != nil {
			m.healthCheckers[serviceName] = healthcheck.NewServiceHealthChecker(
				ctx,
				m.observabilityMgr.MetricsRegistry(),
				service.HealthCheck,
				loadBalancer,
				conf,
				nil,
				healthCheckTargets,
			)
		}

		return loadBalancer, nil

	case conf.Weighted != nil:

		loadBalancer := tcp.NewWRRLoadBalancer(conf.Weighted.HealthCheck != nil)

		for _, service := range shuffle(conf.Weighted.Services, m.rand) {
			handler, err := m.BuildTCP(ctx, service.Name)
			if err != nil {
				logger.Error().Err(err).Msg("Failed to build TCP handler")
				return nil, err
			}

			loadBalancer.AddWeightServer(service.Name, handler, service.Weight)
		}

		return loadBalancer, nil

	default:
		err := fmt.Errorf("the service %q does not have any type defined", serviceQualifiedName)
		conf.AddError(err, true)
		return nil, err
	}
}

// LaunchHealthCheck launches the health checks.
func (m *Manager) LaunchHealthCheck(ctx context.Context) {
	for serviceName, hc := range m.healthCheckers {
		logger := log.Ctx(ctx).With().Str(logs.ServiceName, serviceName).Logger()
		go hc.Launch(logger.WithContext(ctx))
	}
}

func makeProxyName(s string) string {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(s)) // this will never return an error.
	return hex.EncodeToString(hasher.Sum(nil))
}

func shuffle[T any](values []T, r *rand.Rand) []T {
	shuffled := make([]T, len(values))
	copy(shuffled, values)
	r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	return shuffled
}

// dialerWrapper is only used to handle TerminationDelay deprecated option on TCPServersLoadBalancer.
type dialerWrapper struct {
	proxy.Dialer
	terminationDelay time.Duration
}

func (d dialerWrapper) TerminationDelay() time.Duration {
	return d.terminationDelay
}
