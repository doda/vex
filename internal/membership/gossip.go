package membership

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/routing"
)

// GossipProvider implements Provider using HashiCorp memberlist for gossip-based
// membership discovery. It automatically discovers cluster nodes and updates
// routing when membership changes.
type GossipProvider struct {
	mu        sync.RWMutex
	list      *memberlist.Memberlist
	nodes     []routing.Node
	callbacks []func([]routing.Node)
	cfg       config.MembershipConfig
	selfAddr  string
	stopCh    chan struct{}
	updateCh  chan struct{}
	started   bool
	stopped   bool
}

// GossipDelegate implements memberlist.Delegate to handle gossip events.
type GossipDelegate struct {
	provider *GossipProvider
	meta     []byte
}

func (d *GossipDelegate) NodeMeta(limit int) []byte {
	if len(d.meta) > limit {
		return d.meta[:limit]
	}
	return d.meta
}

func (d *GossipDelegate) NotifyMsg([]byte)                           {}
func (d *GossipDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *GossipDelegate) LocalState(join bool) []byte                { return nil }
func (d *GossipDelegate) MergeRemoteState(buf []byte, join bool)     {}

// GossipEventDelegate handles membership change events.
type GossipEventDelegate struct {
	provider *GossipProvider
}

func (d *GossipEventDelegate) NotifyJoin(node *memberlist.Node) {
	d.provider.scheduleUpdateNodes()
}

func (d *GossipEventDelegate) NotifyLeave(node *memberlist.Node) {
	d.provider.scheduleUpdateNodes()
}

func (d *GossipEventDelegate) NotifyUpdate(node *memberlist.Node) {
	d.provider.scheduleUpdateNodes()
}

// NewGossipProvider creates a GossipProvider from a MembershipConfig.
func NewGossipProvider(cfg config.MembershipConfig) *GossipProvider {
	return &GossipProvider{
		cfg:      cfg,
		stopCh:   make(chan struct{}),
		updateCh: make(chan struct{}, 1),
	}
}

// Nodes returns the current list of nodes discovered via gossip.
func (p *GossipProvider) Nodes() []routing.Node {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]routing.Node, len(p.nodes))
	copy(result, p.nodes)
	return result
}

// OnChange registers a callback to be called when membership changes.
func (p *GossipProvider) OnChange(cb func([]routing.Node)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callbacks = append(p.callbacks, cb)
}

// Start begins gossip-based membership discovery.
func (p *GossipProvider) Start() error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return nil
	}
	p.started = true
	p.mu.Unlock()

	mlCfg := memberlist.DefaultLANConfig()

	// Configure bind address
	if p.cfg.Gossip.BindAddr != "" {
		mlCfg.BindAddr = p.cfg.Gossip.BindAddr
	} else {
		mlCfg.BindAddr = "0.0.0.0"
	}

	// Configure bind port
	if p.cfg.Gossip.BindPort > 0 {
		mlCfg.BindPort = p.cfg.Gossip.BindPort
	} else {
		mlCfg.BindPort = 7946
	}

	// Configure advertise address
	if p.cfg.Gossip.AdvertiseAddr != "" {
		mlCfg.AdvertiseAddr = p.cfg.Gossip.AdvertiseAddr
	}
	if p.cfg.Gossip.AdvertisePort > 0 {
		mlCfg.AdvertisePort = p.cfg.Gossip.AdvertisePort
	}

	// Determine self HTTP address - prefer explicit config, then advertise/bind addr.
	selfAPIAddr := resolveSelfAPIAddr(p.cfg, mlCfg)
	p.selfAddr = selfAPIAddr

	// Set up delegates
	delegate := &GossipDelegate{
		provider: p,
		meta:     []byte(selfAPIAddr),
	}
	mlCfg.Delegate = delegate
	mlCfg.Events = &GossipEventDelegate{provider: p}

	// Reduce log noise for production
	mlCfg.Logger = log.New(&discardWriter{}, "", 0)

	// Unique name for the node
	hostname, _ := getLocalIP()
	if hostname == "" {
		hostname = mlCfg.BindAddr
	}
	mlCfg.Name = fmt.Sprintf("%s:%d", hostname, mlCfg.BindPort)

	// Create memberlist
	list, err := memberlist.Create(mlCfg)
	if err != nil {
		return fmt.Errorf("failed to create memberlist: %w", err)
	}
	p.list = list

	// Start the update goroutine
	go p.updateLoop()

	// Join seed nodes if any
	seedNodes := p.cfg.Gossip.SeedNodes
	if len(seedNodes) > 0 {
		n, err := list.Join(seedNodes)
		if err != nil {
			list.Shutdown()
			return fmt.Errorf("failed to join gossip cluster: %w", err)
		}
		if n == 0 {
			// If we couldn't join any seed nodes, continue anyway (we might be the first)
			// This is not an error - we just become a single-node cluster initially
		}
	}

	// Initial node update
	p.scheduleUpdateNodes()

	return nil
}

// Stop halts gossip membership discovery.
func (p *GossipProvider) Stop() {
	p.mu.Lock()
	if !p.started || p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	list := p.list
	p.mu.Unlock()

	select {
	case <-p.stopCh:
		// Already closed
	default:
		close(p.stopCh)
	}

	if list != nil {
		list.Leave(time.Second)
		list.Shutdown()
	}
}

// scheduleUpdateNodes signals the update loop to refresh nodes.
// This is safe to call from memberlist callbacks.
func (p *GossipProvider) scheduleUpdateNodes() {
	select {
	case p.updateCh <- struct{}{}:
	default:
		// Update already pending
	}
}

// updateLoop processes update signals in a separate goroutine.
func (p *GossipProvider) updateLoop() {
	for {
		select {
		case <-p.stopCh:
			return
		case <-p.updateCh:
			p.updateNodes()
		}
	}
}

// updateNodes syncs the memberlist state to our node list.
func (p *GossipProvider) updateNodes() {
	p.mu.RLock()
	if p.list == nil || p.stopped {
		p.mu.RUnlock()
		return
	}
	list := p.list
	p.mu.RUnlock()

	members := list.Members()
	nodes := make([]routing.Node, 0, len(members))

	for _, member := range members {
		// Get API address from metadata
		apiAddr := string(member.Meta)
		if apiAddr == "" {
			// Fallback: derive from member address
			apiAddr = net.JoinHostPort(member.Addr.String(), "8080")
		}

		nodes = append(nodes, routing.Node{
			ID:   member.Name,
			Addr: apiAddr,
		})
	}

	p.mu.Lock()
	p.nodes = nodes
	callbacks := make([]func([]routing.Node), len(p.callbacks))
	copy(callbacks, p.callbacks)
	p.mu.Unlock()

	// Notify all callbacks
	nodeCopy := make([]routing.Node, len(nodes))
	copy(nodeCopy, nodes)
	for _, cb := range callbacks {
		cb(nodeCopy)
	}
}

// NumMembers returns the number of members in the gossip cluster.
func (p *GossipProvider) NumMembers() int {
	if p.list == nil {
		return 0
	}
	return p.list.NumMembers()
}

// Join adds new seed nodes to the gossip cluster.
func (p *GossipProvider) Join(addrs []string) (int, error) {
	if p.list == nil {
		return 0, fmt.Errorf("gossip not started")
	}
	return p.list.Join(addrs)
}

// SelfAddr returns the HTTP API address of this node.
func (p *GossipProvider) SelfAddr() string {
	return p.selfAddr
}

func resolveSelfAPIAddr(cfg config.MembershipConfig, mlCfg *memberlist.Config) string {
	if len(cfg.Nodes) > 0 && cfg.Nodes[0] != "" {
		return cfg.Nodes[0]
	}

	host := cfg.Gossip.AdvertiseAddr
	if host == "" {
		host = mlCfg.AdvertiseAddr
	}
	if host == "" {
		host = cfg.Gossip.BindAddr
	}
	if host == "" || host == "0.0.0.0" {
		if ip, err := getLocalIP(); err == nil && ip != "" {
			host = ip
		} else {
			host = "127.0.0.1"
		}
	}
	return fmt.Sprintf("%s:%d", host, 8080)
}

// discardWriter discards all writes (for silencing memberlist logs).
type discardWriter struct{}

func (d *discardWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

// getLocalIP returns the first non-loopback IP address.
func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", nil
}
