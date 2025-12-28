package membership

import (
	"sync"
	"testing"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/routing"
)

func TestStaticProviderFromConfig(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080", "node2:8080", "node3:8080"},
	}

	provider := NewStaticProvider(cfg)
	nodes := provider.Nodes()

	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}

	expected := []string{"node1:8080", "node2:8080", "node3:8080"}
	for i, node := range nodes {
		if node.Addr != expected[i] {
			t.Errorf("node %d: expected addr %s, got %s", i, expected[i], node.Addr)
		}
		if node.ID != expected[i] {
			t.Errorf("node %d: expected ID %s, got %s", i, expected[i], node.ID)
		}
	}
}

func TestStaticProviderFromNodes(t *testing.T) {
	inputNodes := []routing.Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	}

	provider := NewStaticProviderFromNodes(inputNodes)
	nodes := provider.Nodes()

	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}

	// Verify original slice is copied (not referenced)
	inputNodes[0].ID = "modified"
	if provider.Nodes()[0].ID == "modified" {
		t.Error("StaticProvider should copy nodes, not reference them")
	}
}

func TestStaticProviderNodesIsolation(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080"},
	}

	provider := NewStaticProvider(cfg)
	nodes := provider.Nodes()

	// Modify returned slice
	nodes[0].ID = "modified"

	// Verify internal state unchanged
	nodes2 := provider.Nodes()
	if nodes2[0].ID == "modified" {
		t.Error("Nodes() should return a copy, not the internal slice")
	}
}

func TestStaticProviderSetNodes(t *testing.T) {
	provider := NewStaticProvider(config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080"},
	})

	newNodes := []routing.Node{
		{ID: "nodeA", Addr: "nodeA:8080"},
		{ID: "nodeB", Addr: "nodeB:8080"},
		{ID: "nodeC", Addr: "nodeC:8080"},
	}

	provider.SetNodes(newNodes)
	nodes := provider.Nodes()

	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes after SetNodes, got %d", len(nodes))
	}

	if nodes[0].ID != "nodeA" {
		t.Errorf("expected first node ID nodeA, got %s", nodes[0].ID)
	}
}

func TestStaticProviderOnChange(t *testing.T) {
	provider := NewStaticProvider(config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080"},
	})

	var callbackNodes []routing.Node
	var callCount int
	var mu sync.Mutex

	provider.OnChange(func(nodes []routing.Node) {
		mu.Lock()
		defer mu.Unlock()
		callbackNodes = nodes
		callCount++
	})

	// Trigger a change
	newNodes := []routing.Node{
		{ID: "nodeX", Addr: "nodeX:8080"},
		{ID: "nodeY", Addr: "nodeY:8080"},
	}
	provider.SetNodes(newNodes)

	mu.Lock()
	if callCount != 1 {
		t.Errorf("expected callback to be called once, called %d times", callCount)
	}
	if len(callbackNodes) != 2 {
		t.Errorf("expected 2 nodes in callback, got %d", len(callbackNodes))
	}
	mu.Unlock()
}

func TestStaticProviderMultipleCallbacks(t *testing.T) {
	provider := NewStaticProvider(config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080"},
	})

	var count1, count2 int
	var mu sync.Mutex

	provider.OnChange(func(nodes []routing.Node) {
		mu.Lock()
		defer mu.Unlock()
		count1++
	})

	provider.OnChange(func(nodes []routing.Node) {
		mu.Lock()
		defer mu.Unlock()
		count2++
	})

	provider.SetNodes([]routing.Node{{ID: "x", Addr: "x:8080"}})

	mu.Lock()
	if count1 != 1 || count2 != 1 {
		t.Errorf("expected both callbacks to be called once, got %d and %d", count1, count2)
	}
	mu.Unlock()
}

func TestStaticProviderStartStop(t *testing.T) {
	provider := NewStaticProvider(config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080"},
	})

	// Start and Stop should be no-ops for static provider
	if err := provider.Start(); err != nil {
		t.Errorf("Start() returned error: %v", err)
	}

	provider.Stop()

	// Provider should still work after Stop
	nodes := provider.Nodes()
	if len(nodes) != 1 {
		t.Errorf("expected 1 node after Stop, got %d", len(nodes))
	}
}

func TestManagerIntegrationWithRouter(t *testing.T) {
	// Test that membership is used for routing calculations
	cfg := config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080", "node2:8080", "node3:8080"},
	}

	provider := NewStaticProvider(cfg)
	router := routing.New("node1:8080")
	manager := NewManager(provider, router)

	if err := manager.Start(); err != nil {
		t.Fatalf("Manager.Start() returned error: %v", err)
	}
	defer manager.Stop()

	// Verify router has the nodes
	routerNodes := router.Nodes()
	if len(routerNodes) != 3 {
		t.Errorf("expected router to have 3 nodes, got %d", len(routerNodes))
	}

	// Verify routing works using membership nodes
	home, ok := router.HomeNode("test-namespace")
	if !ok {
		t.Error("HomeNode should return ok=true with nodes")
	}

	// Verify the home node is one of the configured nodes
	found := false
	for _, addr := range cfg.Nodes {
		if home.Addr == addr {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("HomeNode returned %s which is not in configured nodes", home.Addr)
	}
}

func TestManagerMembershipChangeUpdatesRouter(t *testing.T) {
	// Verify membership changes propagate to router
	provider := NewStaticProvider(config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080"},
	})

	router := routing.New("node1:8080")
	manager := NewManager(provider, router)

	if err := manager.Start(); err != nil {
		t.Fatalf("Manager.Start() returned error: %v", err)
	}
	defer manager.Stop()

	// Initial state
	if len(router.Nodes()) != 1 {
		t.Errorf("expected 1 node initially, got %d", len(router.Nodes()))
	}

	// Update membership
	provider.SetNodes([]routing.Node{
		{ID: "nodeA:8080", Addr: "nodeA:8080"},
		{ID: "nodeB:8080", Addr: "nodeB:8080"},
	})

	// Verify router was updated
	nodes := router.Nodes()
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes after update, got %d", len(nodes))
	}
}

func TestManagerNodes(t *testing.T) {
	provider := NewStaticProvider(config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080", "node2:8080"},
	})

	router := routing.New("node1:8080")
	manager := NewManager(provider, router)

	nodes := manager.Nodes()
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes from Manager.Nodes(), got %d", len(nodes))
	}
}

func TestNewFromConfigStatic(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080", "node2:8080"},
	}

	provider := NewFromConfig(cfg)
	nodes := provider.Nodes()

	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestNewFromConfigDefaultToStatic(t *testing.T) {
	// Empty type should default to static
	cfg := config.MembershipConfig{
		Type:  "",
		Nodes: []string{"node1:8080"},
	}

	provider := NewFromConfig(cfg)
	nodes := provider.Nodes()

	if len(nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(nodes))
	}
}

func TestNewFromConfigGossipReturnsGossipProvider(t *testing.T) {
	// Gossip is now implemented, should return GossipProvider
	cfg := config.MembershipConfig{
		Type:  "gossip",
		Nodes: []string{"node1:8080", "node2:8080"},
	}

	provider := NewFromConfig(cfg)

	// Should be a GossipProvider
	_, ok := provider.(*GossipProvider)
	if !ok {
		t.Errorf("NewFromConfig with type=gossip should return *GossipProvider")
	}
}

func TestRouterIntegrationHomeNodeConsistency(t *testing.T) {
	// Verify that with static membership, routing is consistent
	cfg := config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080", "node2:8080", "node3:8080"},
	}

	// Create multiple managers with same config (simulating different nodes)
	routers := make([]*routing.Router, 3)
	managers := make([]*Manager, 3)

	for i := 0; i < 3; i++ {
		provider := NewStaticProvider(cfg)
		routers[i] = routing.New(cfg.Nodes[i])
		managers[i] = NewManager(provider, routers[i])
		if err := managers[i].Start(); err != nil {
			t.Fatalf("failed to start manager %d: %v", i, err)
		}
	}
	defer func() {
		for _, m := range managers {
			m.Stop()
		}
	}()

	// Verify all routers agree on home node for each namespace
	namespaces := []string{"users", "products", "orders", "analytics"}

	for _, ns := range namespaces {
		var expectedHome string
		for i, router := range routers {
			home, ok := router.HomeNode(ns)
			if !ok {
				t.Errorf("router %d: HomeNode(%s) returned not ok", i, ns)
				continue
			}
			if i == 0 {
				expectedHome = home.Addr
			} else if home.Addr != expectedHome {
				t.Errorf("namespace %s: router %d returned home %s, expected %s",
					ns, i, home.Addr, expectedHome)
			}
		}
	}
}

func TestRouterIntegrationDistribution(t *testing.T) {
	// Verify namespaces are distributed across static nodes
	cfg := config.MembershipConfig{
		Type:  "static",
		Nodes: []string{"node1:8080", "node2:8080", "node3:8080"},
	}

	provider := NewStaticProvider(cfg)
	router := routing.New("node1:8080")
	manager := NewManager(provider, router)
	if err := manager.Start(); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer manager.Stop()

	counts := make(map[string]int)
	for i := 0; i < 300; i++ {
		ns := "namespace-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		home, ok := router.HomeNode(ns)
		if !ok {
			t.Fatalf("HomeNode(%s) returned not ok", ns)
		}
		counts[home.Addr]++
	}

	// Each node should get some namespaces
	for _, addr := range cfg.Nodes {
		if counts[addr] < 50 {
			t.Errorf("node %s only got %d namespaces, expected at least 50", addr, counts[addr])
		}
	}
}

func TestEmptyStaticConfig(t *testing.T) {
	cfg := config.MembershipConfig{
		Type:  "static",
		Nodes: []string{},
	}

	provider := NewStaticProvider(cfg)
	nodes := provider.Nodes()

	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes for empty config, got %d", len(nodes))
	}
}

func TestProviderInterface(t *testing.T) {
	// Ensure StaticProvider implements Provider
	var _ Provider = (*StaticProvider)(nil)
}
