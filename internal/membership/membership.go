package membership

import (
	"sync"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/routing"
)

// Provider is an interface for cluster membership discovery.
// Implementations can be static (config-based) or dynamic (gossip-based).
type Provider interface {
	// Nodes returns the current list of nodes in the cluster.
	Nodes() []routing.Node
	// OnChange registers a callback to be called when membership changes.
	OnChange(func([]routing.Node))
	// Start begins membership discovery (for dynamic providers).
	Start() error
	// Stop halts membership discovery.
	Stop()
}

// StaticProvider provides static cluster membership from configuration.
type StaticProvider struct {
	mu        sync.RWMutex
	nodes     []routing.Node
	callbacks []func([]routing.Node)
}

// NewStaticProvider creates a StaticProvider from a MembershipConfig.
func NewStaticProvider(cfg config.MembershipConfig) *StaticProvider {
	nodes := make([]routing.Node, 0, len(cfg.Nodes))
	for _, addr := range cfg.Nodes {
		nodes = append(nodes, routing.Node{
			ID:   addr,
			Addr: addr,
		})
	}
	return &StaticProvider{
		nodes: nodes,
	}
}

// NewStaticProviderFromNodes creates a StaticProvider from a list of nodes.
func NewStaticProviderFromNodes(nodes []routing.Node) *StaticProvider {
	nodeCopy := make([]routing.Node, len(nodes))
	copy(nodeCopy, nodes)
	return &StaticProvider{
		nodes: nodeCopy,
	}
}

// Nodes returns the static list of nodes.
func (p *StaticProvider) Nodes() []routing.Node {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]routing.Node, len(p.nodes))
	copy(result, p.nodes)
	return result
}

// OnChange registers a callback for membership changes.
// For static providers, this callback is only invoked if SetNodes is called.
func (p *StaticProvider) OnChange(cb func([]routing.Node)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callbacks = append(p.callbacks, cb)
}

// Start is a no-op for static providers.
func (p *StaticProvider) Start() error {
	return nil
}

// Stop is a no-op for static providers.
func (p *StaticProvider) Stop() {}

// SetNodes updates the static node list and notifies callbacks.
// This is useful for testing or runtime reconfiguration.
func (p *StaticProvider) SetNodes(nodes []routing.Node) {
	p.mu.Lock()
	p.nodes = make([]routing.Node, len(nodes))
	copy(p.nodes, nodes)
	callbacks := make([]func([]routing.Node), len(p.callbacks))
	copy(callbacks, p.callbacks)
	p.mu.Unlock()

	nodeCopy := make([]routing.Node, len(nodes))
	copy(nodeCopy, nodes)
	for _, cb := range callbacks {
		cb(nodeCopy)
	}
}

// Manager manages cluster membership and integrates with the routing layer.
type Manager struct {
	provider Provider
	router   *routing.Router
	stopCh   chan struct{}
}

// NewManager creates a new membership Manager.
func NewManager(provider Provider, router *routing.Router) *Manager {
	return &Manager{
		provider: provider,
		router:   router,
		stopCh:   make(chan struct{}),
	}
}

// Start initializes the membership manager and syncs nodes to the router.
func (m *Manager) Start() error {
	// Initial sync
	m.syncNodes()

	// Register for membership changes
	m.provider.OnChange(func(nodes []routing.Node) {
		m.router.SetNodes(nodes)
	})

	// Start the provider
	return m.provider.Start()
}

// Stop halts the membership manager.
func (m *Manager) Stop() {
	close(m.stopCh)
	m.provider.Stop()
}

// Nodes returns the current cluster nodes.
func (m *Manager) Nodes() []routing.Node {
	return m.provider.Nodes()
}

// syncNodes syncs the current membership to the router.
func (m *Manager) syncNodes() {
	nodes := m.provider.Nodes()
	m.router.SetNodes(nodes)
}

// NewFromConfig creates the appropriate Provider based on configuration.
func NewFromConfig(cfg config.MembershipConfig) Provider {
	switch cfg.Type {
	case "gossip":
		return NewGossipProvider(cfg)
	default:
		return NewStaticProvider(cfg)
	}
}
