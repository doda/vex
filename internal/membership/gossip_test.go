package membership

import (
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/routing"
)

func TestGossipProviderImplementsInterface(t *testing.T) {
	var _ Provider = (*GossipProvider)(nil)
}

func TestGossipProviderBasic(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"localhost:8080"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17945, // Use fixed port
		},
	}

	provider := NewGossipProvider(cfg)
	if err := provider.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	defer provider.Stop()

	// Wait for async update to process
	time.Sleep(50 * time.Millisecond)

	// Should have at least one node (itself)
	nodes := provider.Nodes()
	if len(nodes) < 1 {
		t.Errorf("expected at least 1 node, got %d", len(nodes))
	}

	// NumMembers should match
	if provider.NumMembers() != len(nodes) {
		t.Errorf("NumMembers() = %d, expected %d", provider.NumMembers(), len(nodes))
	}
}

func TestGossipProviderTwoNodes(t *testing.T) {
	// Start first node
	cfg1 := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8081"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17946,
		},
	}

	provider1 := NewGossipProvider(cfg1)
	if err := provider1.Start(); err != nil {
		t.Fatalf("provider1 Start() returned error: %v", err)
	}
	defer provider1.Stop()

	// Start second node and join first
	cfg2 := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8082"},
		Gossip: config.GossipConfig{
			BindAddr:  "127.0.0.1",
			BindPort:  17947,
			SeedNodes: []string{"127.0.0.1:17946"},
		},
	}

	provider2 := NewGossipProvider(cfg2)
	if err := provider2.Start(); err != nil {
		t.Fatalf("provider2 Start() returned error: %v", err)
	}
	defer provider2.Stop()

	// Wait for gossip to propagate
	time.Sleep(100 * time.Millisecond)

	// Both nodes should see 2 members
	if provider1.NumMembers() != 2 {
		t.Errorf("provider1 NumMembers() = %d, expected 2", provider1.NumMembers())
	}
	if provider2.NumMembers() != 2 {
		t.Errorf("provider2 NumMembers() = %d, expected 2", provider2.NumMembers())
	}

	// Verify Nodes() returns both nodes
	nodes1 := provider1.Nodes()
	nodes2 := provider2.Nodes()
	if len(nodes1) != 2 {
		t.Errorf("provider1 Nodes() returned %d nodes, expected 2", len(nodes1))
	}
	if len(nodes2) != 2 {
		t.Errorf("provider2 Nodes() returned %d nodes, expected 2", len(nodes2))
	}
}

func TestGossipProviderOnChange(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8083"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17948,
		},
	}

	provider := NewGossipProvider(cfg)

	var callCount int
	var lastNodes []routing.Node

	provider.OnChange(func(nodes []routing.Node) {
		callCount++
		lastNodes = nodes
	})

	if err := provider.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	defer provider.Stop()

	// Wait for initial update
	time.Sleep(50 * time.Millisecond)

	// Should have received at least one callback
	if callCount < 1 {
		t.Errorf("expected at least 1 OnChange callback, got %d", callCount)
	}

	if len(lastNodes) < 1 {
		t.Errorf("expected at least 1 node in callback, got %d", len(lastNodes))
	}
}

func TestGossipProviderStartStop(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8084"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17949,
		},
	}

	provider := NewGossipProvider(cfg)

	// Start
	if err := provider.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}

	// Double start should be no-op
	if err := provider.Start(); err != nil {
		t.Fatalf("second Start() returned error: %v", err)
	}

	// Stop
	provider.Stop()

	// Double stop should be no-op
	provider.Stop()
}

func TestGossipProviderNodesIsolation(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8085"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17950,
		},
	}

	provider := NewGossipProvider(cfg)
	if err := provider.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	defer provider.Stop()

	// Wait for async update to process
	time.Sleep(50 * time.Millisecond)

	nodes1 := provider.Nodes()
	if len(nodes1) > 0 {
		nodes1[0].ID = "modified"
	}

	nodes2 := provider.Nodes()
	if len(nodes2) > 0 && nodes2[0].ID == "modified" {
		t.Error("Nodes() should return a copy, not the internal slice")
	}
}

func TestGossipProviderIntegrationWithRouter(t *testing.T) {
	// Test that gossip membership updates are propagated to the router
	cfg1 := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8086"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17951,
		},
	}

	provider1 := NewGossipProvider(cfg1)
	router := routing.New("127.0.0.1:8086")
	manager := NewManager(provider1, router)

	if err := manager.Start(); err != nil {
		t.Fatalf("manager Start() returned error: %v", err)
	}
	defer manager.Stop()

	// Wait for initial sync
	time.Sleep(50 * time.Millisecond)

	// Router should have at least 1 node
	routerNodes := router.Nodes()
	if len(routerNodes) < 1 {
		t.Errorf("expected router to have at least 1 node, got %d", len(routerNodes))
	}
}

func TestGossipProviderDynamicMembershipUpdatesRouting(t *testing.T) {
	// Test that dynamic membership changes update routing
	cfg1 := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8087"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17952,
		},
	}

	provider1 := NewGossipProvider(cfg1)
	router := routing.New("127.0.0.1:8087")
	manager := NewManager(provider1, router)

	if err := manager.Start(); err != nil {
		t.Fatalf("manager Start() returned error: %v", err)
	}
	defer manager.Stop()

	// Wait for initial sync
	time.Sleep(50 * time.Millisecond)

	initialNodes := len(router.Nodes())

	// Start second node and join
	cfg2 := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8088"},
		Gossip: config.GossipConfig{
			BindAddr:  "127.0.0.1",
			BindPort:  17953,
			SeedNodes: []string{"127.0.0.1:17952"},
		},
	}

	provider2 := NewGossipProvider(cfg2)
	if err := provider2.Start(); err != nil {
		t.Fatalf("provider2 Start() returned error: %v", err)
	}
	defer provider2.Stop()

	// Wait for gossip to propagate
	time.Sleep(200 * time.Millisecond)

	// Router should now have 2 nodes
	finalNodes := len(router.Nodes())
	if finalNodes <= initialNodes {
		t.Errorf("expected router to have more nodes after join, had %d, now %d",
			initialNodes, finalNodes)
	}

	// Verify routing works with the new membership
	home, ok := router.HomeNode("test-namespace")
	if !ok {
		t.Error("HomeNode should return ok=true with nodes")
	}

	// Home node should be valid
	if home.Addr == "" {
		t.Error("HomeNode returned empty address")
	}
}

func TestNewFromConfigGossip(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8089"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17954,
		},
	}

	provider := NewFromConfig(cfg)

	// Should be a GossipProvider
	_, ok := provider.(*GossipProvider)
	if !ok {
		t.Errorf("NewFromConfig with type=gossip should return *GossipProvider")
	}

	// Start and verify it works
	if err := provider.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	defer provider.Stop()

	// Wait for async update to process
	time.Sleep(50 * time.Millisecond)

	nodes := provider.Nodes()
	if len(nodes) < 1 {
		t.Errorf("expected at least 1 node, got %d", len(nodes))
	}
}

func TestGossipProviderJoinMethod(t *testing.T) {
	// Start first node
	cfg1 := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8090"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17955,
		},
	}

	provider1 := NewGossipProvider(cfg1)
	if err := provider1.Start(); err != nil {
		t.Fatalf("provider1 Start() returned error: %v", err)
	}
	defer provider1.Stop()

	// Start second node without seed nodes
	cfg2 := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8091"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17956,
		},
	}

	provider2 := NewGossipProvider(cfg2)
	if err := provider2.Start(); err != nil {
		t.Fatalf("provider2 Start() returned error: %v", err)
	}
	defer provider2.Stop()

	// Join programmatically
	n, err := provider2.Join([]string{"127.0.0.1:17955"})
	if err != nil {
		t.Fatalf("Join() returned error: %v", err)
	}
	if n == 0 {
		t.Error("Join() joined 0 nodes, expected at least 1")
	}

	// Wait for gossip to propagate
	time.Sleep(100 * time.Millisecond)

	if provider2.NumMembers() != 2 {
		t.Errorf("provider2 NumMembers() = %d, expected 2", provider2.NumMembers())
	}
}

func TestGossipProviderJoinBeforeStart(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8092"},
	}

	provider := NewGossipProvider(cfg)

	// Join before Start should fail
	_, err := provider.Join([]string{"127.0.0.1:17957"})
	if err == nil {
		t.Error("Join() before Start() should return error")
	}
}

func TestGossipProviderSelfAddr(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8093"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17958,
		},
	}

	provider := NewGossipProvider(cfg)
	if err := provider.Start(); err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	defer provider.Stop()

	selfAddr := provider.SelfAddr()
	if selfAddr != "127.0.0.1:8093" {
		t.Errorf("SelfAddr() = %s, expected 127.0.0.1:8093", selfAddr)
	}
}

func TestGossipProviderRoutingConsistency(t *testing.T) {
	// Verify that with gossip membership, routing is consistent across nodes
	cfg1 := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8094"},
		Gossip: config.GossipConfig{
			BindAddr: "127.0.0.1",
			BindPort: 17959,
		},
	}

	cfg2 := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"127.0.0.1:8095"},
		Gossip: config.GossipConfig{
			BindAddr:  "127.0.0.1",
			BindPort:  17960,
			SeedNodes: []string{"127.0.0.1:17959"},
		},
	}

	provider1 := NewGossipProvider(cfg1)
	router1 := routing.New("127.0.0.1:8094")
	manager1 := NewManager(provider1, router1)

	provider2 := NewGossipProvider(cfg2)
	router2 := routing.New("127.0.0.1:8095")
	manager2 := NewManager(provider2, router2)

	if err := manager1.Start(); err != nil {
		t.Fatalf("manager1 Start() returned error: %v", err)
	}
	defer manager1.Stop()

	if err := manager2.Start(); err != nil {
		t.Fatalf("manager2 Start() returned error: %v", err)
	}
	defer manager2.Stop()

	// Wait for gossip to converge
	time.Sleep(300 * time.Millisecond)

	// Both routers should have same node count
	nodes1 := router1.Nodes()
	nodes2 := router2.Nodes()
	if len(nodes1) != len(nodes2) {
		t.Errorf("routers have different node counts: %d vs %d", len(nodes1), len(nodes2))
	}

	// Both routers should agree on home node for namespaces
	namespaces := []string{"users", "products", "orders"}
	for _, ns := range namespaces {
		home1, ok1 := router1.HomeNode(ns)
		home2, ok2 := router2.HomeNode(ns)

		if !ok1 || !ok2 {
			t.Errorf("HomeNode(%s) returned not ok", ns)
			continue
		}

		if home1.Addr != home2.Addr {
			t.Errorf("namespace %s: routers disagree on home node: %s vs %s",
				ns, home1.Addr, home2.Addr)
		}
	}
}
