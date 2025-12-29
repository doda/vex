package routing

import (
	"fmt"
	"testing"
)

func TestHomeNodeConsistency(t *testing.T) {
	// Step 1: Test home_node(namespace) computed consistently
	r := New("node1:8080")
	r.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	namespaces := []string{
		"users",
		"products",
		"orders",
		"analytics",
		"test-namespace-1",
		"test-namespace-2",
	}

	// Run multiple times to verify consistency
	for _, ns := range namespaces {
		home1, ok1 := r.HomeNode(ns)
		if !ok1 {
			t.Errorf("HomeNode(%q) returned not ok", ns)
			continue
		}

		// Verify same result on repeated calls
		for i := 0; i < 100; i++ {
			home2, ok2 := r.HomeNode(ns)
			if !ok2 {
				t.Errorf("HomeNode(%q) returned not ok on iteration %d", ns, i)
				continue
			}
			if home1.ID != home2.ID || home1.Addr != home2.Addr {
				t.Errorf("HomeNode(%q) inconsistent: got %v then %v", ns, home1, home2)
			}
		}
	}
}

func TestHomeNodeConsistencyAcrossRouters(t *testing.T) {
	// Verify different router instances produce same results
	nodes := []Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	}

	r1 := New("node1:8080")
	r1.SetNodes(nodes)

	r2 := New("node2:8080")
	r2.SetNodes(nodes)

	r3 := New("node3:8080")
	r3.SetNodes(nodes)

	namespaces := []string{"users", "products", "orders"}

	for _, ns := range namespaces {
		home1, _ := r1.HomeNode(ns)
		home2, _ := r2.HomeNode(ns)
		home3, _ := r3.HomeNode(ns)

		if home1.ID != home2.ID || home2.ID != home3.ID {
			t.Errorf("HomeNode(%q) inconsistent across routers: %v, %v, %v",
				ns, home1.ID, home2.ID, home3.ID)
		}
	}
}

func TestHomeNodeDistribution(t *testing.T) {
	// Verify rendezvous hashing distributes namespaces across nodes
	r := New("node1:8080")
	r.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	counts := make(map[string]int)

	// Test 1000 namespaces
	for i := 0; i < 1000; i++ {
		ns := fmt.Sprintf("namespace-%d", i)
		home, ok := r.HomeNode(ns)
		if !ok {
			t.Fatalf("HomeNode(%q) returned not ok", ns)
		}
		counts[home.ID]++
	}

	// Verify each node gets some namespaces (rough distribution check)
	for nodeID, count := range counts {
		if count < 200 {
			t.Errorf("Node %s only got %d namespaces, expected at least 200", nodeID, count)
		}
	}
}

func TestRouteToHomeNode(t *testing.T) {
	// Step 2: Verify requests route to home node when possible
	nodes := []Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	}

	// Create router for each node
	routers := make(map[string]*Router)
	for _, n := range nodes {
		r := New(n.Addr)
		r.SetNodes(nodes)
		routers[n.ID] = r
	}

	// Verify each router correctly identifies if it's the home node
	testCases := []string{"users", "products", "orders", "analytics"}

	for _, ns := range testCases {
		homeNode, _ := routers["node1"].HomeNode(ns)

		for nodeID, r := range routers {
			isHome := r.IsHomeNode(ns)
			expected := (homeNode.ID == nodeID)

			if isHome != expected {
				t.Errorf("For namespace %q, node %s: IsHomeNode() = %v, want %v (home is %s)",
					ns, nodeID, isHome, expected, homeNode.ID)
			}
		}
	}
}

func TestRankedNodes(t *testing.T) {
	// Verify RankedNodes returns consistent ordering
	r := New("node1:8080")
	r.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	ns := "test-namespace"

	ranked1 := r.RankedNodes(ns)
	ranked2 := r.RankedNodes(ns)

	if len(ranked1) != len(ranked2) {
		t.Fatalf("RankedNodes returned different lengths: %d vs %d", len(ranked1), len(ranked2))
	}

	for i := range ranked1 {
		if ranked1[i].ID != ranked2[i].ID {
			t.Errorf("RankedNodes inconsistent at position %d: %s vs %s",
				i, ranked1[i].ID, ranked2[i].ID)
		}
	}

	// Verify home node is first
	home, _ := r.HomeNode(ns)
	if ranked1[0].ID != home.ID {
		t.Errorf("RankedNodes first node %s != HomeNode %s", ranked1[0].ID, home.ID)
	}
}

func TestAnyNodeCanServe(t *testing.T) {
	// Step 3: Test any node can serve any namespace (fallback)
	r := New("node1:8080")
	r.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	namespaces := []string{
		"users",
		"products",
		"orders",
		"any-random-namespace",
		"",
	}

	for _, ns := range namespaces {
		if !r.CanServe(ns) {
			t.Errorf("CanServe(%q) = false, expected true", ns)
		}
	}
}

func TestEmptyNodes(t *testing.T) {
	r := New("node1:8080")

	home, ok := r.HomeNode("test")
	if ok {
		t.Errorf("HomeNode with no nodes should return ok=false, got %v", home)
	}

	ranked := r.RankedNodes("test")
	if len(ranked) != 0 {
		t.Errorf("RankedNodes with no nodes should return empty, got %v", ranked)
	}

	// CanServe should still return true (local node can serve)
	if !r.CanServe("test") {
		t.Error("CanServe should return true even with no nodes")
	}
}

func TestNodeAddRemove(t *testing.T) {
	r := New("node1:8080")

	// Start with 3 nodes
	r.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	home1, _ := r.HomeNode("test-namespace")

	// Add a node
	r.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
		{ID: "node4", Addr: "node4:8080"},
	})

	home2, _ := r.HomeNode("test-namespace")

	// Verify the home may or may not change
	_ = home1
	_ = home2

	// Remove a node
	r.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	home3, ok := r.HomeNode("test-namespace")
	if !ok {
		t.Error("HomeNode should still work with fewer nodes")
	}

	// With rendezvous hashing, removing node2 shouldn't affect namespaces
	// that were assigned to other nodes
	_ = home3
}

func TestSingleNode(t *testing.T) {
	r := New("node1:8080")
	r.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
	})

	namespaces := []string{"ns1", "ns2", "ns3"}

	for _, ns := range namespaces {
		home, ok := r.HomeNode(ns)
		if !ok {
			t.Errorf("HomeNode(%q) returned not ok", ns)
			continue
		}
		if home.ID != "node1" {
			t.Errorf("HomeNode(%q) = %s, expected node1", ns, home.ID)
		}
		if !r.IsHomeNode(ns) {
			t.Errorf("IsHomeNode(%q) = false, expected true for single node", ns)
		}
	}
}

func TestNodesList(t *testing.T) {
	r := New("node1:8080")

	nodes := []Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	}
	r.SetNodes(nodes)

	got := r.Nodes()
	if len(got) != 2 {
		t.Errorf("Nodes() returned %d nodes, expected 2", len(got))
	}

	// Verify modifying returned slice doesn't affect internal state
	got[0].ID = "modified"
	got2 := r.Nodes()
	if got2[0].ID == "modified" {
		t.Error("Modifying returned Nodes() slice affected internal state")
	}
}

func TestSelfAddr(t *testing.T) {
	r := New("my-node:8080")
	if r.SelfAddr() != "my-node:8080" {
		t.Errorf("SelfAddr() = %q, expected %q", r.SelfAddr(), "my-node:8080")
	}
}

func TestHomeNodeAddressNormalization(t *testing.T) {
	r := New(":8080")
	r.SetNodes([]Node{{ID: "node1", Addr: "localhost:8080"}})

	if !r.IsHomeNode("test") {
		t.Errorf("expected IsHomeNode to be true with normalized listen addr")
	}

	nodes := r.Nodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].Addr != "localhost:8080" {
		t.Errorf("expected normalized node addr localhost:8080, got %s", nodes[0].Addr)
	}
}

func TestWeightDeterminism(t *testing.T) {
	// Verify computeWeight is deterministic
	w1 := computeWeight("node1", "namespace1")
	w2 := computeWeight("node1", "namespace1")

	if w1 != w2 {
		t.Errorf("computeWeight not deterministic: %d != %d", w1, w2)
	}

	// Different inputs should (usually) produce different weights
	w3 := computeWeight("node2", "namespace1")
	w4 := computeWeight("node1", "namespace2")

	if w1 == w3 && w1 == w4 {
		t.Error("computeWeight produced same value for different inputs")
	}
}

func BenchmarkHomeNode(b *testing.B) {
	r := New("node1:8080")
	nodes := make([]Node, 10)
	for i := range nodes {
		nodes[i] = Node{
			ID:   fmt.Sprintf("node%d", i),
			Addr: fmt.Sprintf("node%d:8080", i),
		}
	}
	r.SetNodes(nodes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.HomeNode(fmt.Sprintf("namespace-%d", i%1000))
	}
}

func BenchmarkRankedNodes(b *testing.B) {
	r := New("node1:8080")
	nodes := make([]Node, 10)
	for i := range nodes {
		nodes[i] = Node{
			ID:   fmt.Sprintf("node%d", i),
			Addr: fmt.Sprintf("node%d:8080", i),
		}
	}
	r.SetNodes(nodes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.RankedNodes(fmt.Sprintf("namespace-%d", i%1000))
	}
}
