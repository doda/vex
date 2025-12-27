package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestsServedByAnyNode(t *testing.T) {
	nodes := []Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	}

	for _, selfNode := range nodes {
		r := New(selfNode.Addr)
		r.SetNodes(nodes)

		namespaces := []string{"users", "products", "orders", "analytics"}
		for _, ns := range namespaces {
			if !r.CanServe(ns) {
				t.Errorf("Node %s cannot serve namespace %s - expected any node can serve any namespace",
					selfNode.ID, ns)
			}
		}
	}
}

func TestStrongConsistencyAfterRoutingChange(t *testing.T) {
	router := New("node1:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	})

	cm := NewChurnManager(router)
	ns := "test-namespace"

	snapshot1 := cm.CreateSnapshot(ConsistencyStrong)
	if !snapshot1.MustRefresh {
		t.Error("Strong consistency snapshot should require refresh")
	}
	if !snapshot1.NeedsRefresh() {
		t.Error("Strong consistency snapshot NeedsRefresh() should return true")
	}

	cm.UpdateNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	snapshot2 := cm.CreateSnapshot(ConsistencyStrong)
	if snapshot2.Version == snapshot1.Version {
		t.Error("Snapshot version should increment after churn")
	}

	if cm.ValidateSnapshot(snapshot1) {
		t.Error("Old strong snapshot should be invalidated after churn")
	}

	home, ok := router.HomeNode(ns)
	if !ok {
		t.Fatal("HomeNode should return a valid node")
	}
	_ = home

	if !router.CanServe(ns) {
		t.Error("Node should be able to serve after routing change")
	}
}

func TestEventualConsistencyBrieflyStale(t *testing.T) {
	router := New("node1:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	})

	cm := NewChurnManager(router)

	snapshot := cm.CreateSnapshot(ConsistencyEventual)
	if snapshot.MustRefresh {
		t.Error("Eventual consistency snapshot should not require immediate refresh")
	}
	if snapshot.NeedsRefresh() {
		t.Error("Fresh eventual snapshot NeedsRefresh() should return false")
	}
	if snapshot.MaxTailBytes != EventualMaxTailBytes {
		t.Errorf("Expected max tail bytes %d, got %d", EventualMaxTailBytes, snapshot.MaxTailBytes)
	}

	if !cm.ValidateSnapshot(snapshot) {
		t.Error("Fresh eventual snapshot should be valid")
	}

	cm.UpdateNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	if !cm.ValidateSnapshot(snapshot) {
		t.Error("Eventual snapshot should remain valid after churn (briefly stale is OK)")
	}
}

func TestNodeChurnDetection(t *testing.T) {
	router := New("node1:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
	})

	cm := NewChurnManager(router)
	initialVersion := cm.SnapshotVersion()

	changed := cm.UpdateNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	})

	if !changed {
		t.Error("Adding a node should trigger churn detection")
	}

	if cm.SnapshotVersion() == initialVersion {
		t.Error("Snapshot version should increment after churn")
	}

	if !cm.RecentChurn(1 * time.Second) {
		t.Error("RecentChurn should return true immediately after churn")
	}

	changed2 := cm.UpdateNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
	})

	if !changed2 {
		t.Error("Removing a node should trigger churn detection")
	}

	churns := cm.ChurnsSince(time.Now().Add(-1 * time.Hour))
	if len(churns) != 2 {
		t.Errorf("Expected 2 churn events, got %d", len(churns))
	}
}

func TestNoChurnForSameNodes(t *testing.T) {
	router := New("node1:8080")
	nodes := []Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	}
	router.SetNodes(nodes)

	cm := NewChurnManager(router)
	initialVersion := cm.SnapshotVersion()

	changed := cm.UpdateNodes(nodes)
	if changed {
		t.Error("Setting same nodes should not trigger churn")
	}

	if cm.SnapshotVersion() != initialVersion {
		t.Error("Snapshot version should not change when nodes are the same")
	}
}

func TestProxyFallbackAfterNodeChurn(t *testing.T) {
	var requestCount int32

	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.Header().Set("X-Served-By", "home-node")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer homeServer.Close()

	router := New("other-node:8080")
	router.SetNodes([]Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	cm := NewChurnManager(router)
	proxy := NewProxy(router, ProxyConfig{Timeout: 5 * time.Second})

	ns := findNamespaceForNode(router, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	req := httptest.NewRequest("GET", "/v1/namespaces/"+ns+"/metadata", nil)
	resp, err := proxy.ProxyRequest(context.Background(), ns, req)
	if err != nil {
		t.Fatalf("Initial proxy request failed: %v", err)
	}
	resp.Body.Close()

	if atomic.LoadInt32(&requestCount) != 1 {
		t.Error("Request should have been proxied to home node")
	}

	cm.UpdateNodes([]Node{
		{ID: "other-node", Addr: "other-node:8080"},
		{ID: "new-node", Addr: "new-node:8080"},
	})

	req2 := httptest.NewRequest("GET", "/v1/namespaces/"+ns+"/metadata", nil)

	localHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Served-By", "local-fallback")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "served_by": "local"})
	})

	handler := proxy.ProxyHandler(ns, localHandler)
	rec := httptest.NewRecorder()
	handler(rec, req2)

	if rec.Header().Get("X-Served-By") != "local-fallback" {
		t.Errorf("Expected local fallback after node removal, got %s", rec.Header().Get("X-Served-By"))
	}
}

func TestConcurrentRequestsDuringChurn(t *testing.T) {
	router := New("node1:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	})

	cm := NewChurnManager(router)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				mode := ConsistencyStrong
				if j%2 == 0 {
					mode = ConsistencyEventual
				}
				snapshot := cm.CreateSnapshot(mode)
				if snapshot.Mode != mode {
					errors <- fmt.Errorf("snapshot mode mismatch: got %s want %s", snapshot.Mode, mode)
				}
			}
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				cm.UpdateNodes([]Node{
					{ID: "node1", Addr: "node1:8080"},
					{ID: "node" + string(rune('2'+j)), Addr: "node" + string(rune('2'+j)) + ":8080"},
				})
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestStrongConsistencyRequiresRefreshAfterAnyChurn(t *testing.T) {
	router := New("node1:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	})

	cm := NewChurnManager(router)
	strongSnapshot := cm.CreateSnapshot(ConsistencyStrong)

	testCases := []struct {
		name     string
		newNodes []Node
	}{
		{
			name: "add node",
			newNodes: []Node{
				{ID: "node1", Addr: "node1:8080"},
				{ID: "node2", Addr: "node2:8080"},
				{ID: "node3", Addr: "node3:8080"},
			},
		},
		{
			name: "remove node",
			newNodes: []Node{
				{ID: "node1", Addr: "node1:8080"},
			},
		},
		{
			name: "replace node",
			newNodes: []Node{
				{ID: "node1", Addr: "node1:8080"},
				{ID: "node4", Addr: "node4:8080"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			router.SetNodes([]Node{
				{ID: "node1", Addr: "node1:8080"},
				{ID: "node2", Addr: "node2:8080"},
			})
			cm2 := NewChurnManager(router)
			snapshot := cm2.CreateSnapshot(ConsistencyStrong)

			cm2.UpdateNodes(tc.newNodes)

			if cm2.ValidateSnapshot(snapshot) {
				t.Errorf("%s: strong snapshot should be invalidated after churn", tc.name)
			}
		})
	}

	_ = strongSnapshot
}

func TestEventualConsistencyAllowsStaleness(t *testing.T) {
	router := New("node1:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	})

	cm := NewChurnManager(router)
	snapshot := cm.CreateSnapshot(ConsistencyEventual)

	cm.UpdateNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	if !cm.ValidateSnapshot(snapshot) {
		t.Error("Eventual snapshot should remain valid immediately after churn")
	}

	cm.UpdateNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node4", Addr: "node4:8080"},
	})

	if !cm.ValidateSnapshot(snapshot) {
		t.Error("Eventual snapshot should remain valid through multiple churns within staleness window")
	}
}

func TestRoutingChangeDoesNotBreakExistingConnections(t *testing.T) {
	router := New("node1:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	})

	ns := "test-namespace"
	homeBeforeChurn, _ := router.HomeNode(ns)

	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node4", Addr: "node4:8080"},
		{ID: "node5", Addr: "node5:8080"},
	})

	homeAfterChurn, ok := router.HomeNode(ns)
	if !ok {
		t.Fatal("HomeNode should return a valid node after churn")
	}

	for _, n := range router.Nodes() {
		nodeRouter := New(n.Addr)
		nodeRouter.SetNodes(router.Nodes())
		if !nodeRouter.CanServe(ns) {
			t.Errorf("Node %s should be able to serve namespace %s after churn", n.ID, ns)
		}
	}

	_ = homeBeforeChurn
	_ = homeAfterChurn
}

func TestChurnHistoryMaintained(t *testing.T) {
	router := New("node1:8080")
	router.SetNodes([]Node{{ID: "node1", Addr: "node1:8080"}})

	cm := NewChurnManager(router)
	startTime := time.Now()

	for i := 0; i < 5; i++ {
		cm.UpdateNodes([]Node{
			{ID: "node1", Addr: "node1:8080"},
			{ID: "node" + string(rune('2'+i)), Addr: "node:8080"},
		})
	}

	churns := cm.ChurnsSince(startTime)
	if len(churns) != 5 {
		t.Errorf("Expected 5 churn events, got %d", len(churns))
	}

	for i, churn := range churns {
		if len(churn.AddedNodes) != 1 && i > 0 {
			t.Errorf("Churn %d should have 1 added node", i)
		}
	}
}

func TestDiffNodes(t *testing.T) {
	old := []Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	}
	new := []Node{
		{ID: "node2", Addr: "node2:8080"},
		{ID: "node3", Addr: "node3:8080"},
	}

	added, removed := diffNodes(old, new)

	if len(added) != 1 || added[0].ID != "node3" {
		t.Errorf("Expected node3 to be added, got %v", added)
	}
	if len(removed) != 1 || removed[0].ID != "node1" {
		t.Errorf("Expected node1 to be removed, got %v", removed)
	}
}

func TestNodesEqual(t *testing.T) {
	testCases := []struct {
		name     string
		a        []Node
		b        []Node
		expected bool
	}{
		{
			name:     "equal same order",
			a:        []Node{{ID: "n1", Addr: "a1"}, {ID: "n2", Addr: "a2"}},
			b:        []Node{{ID: "n1", Addr: "a1"}, {ID: "n2", Addr: "a2"}},
			expected: true,
		},
		{
			name:     "equal different order",
			a:        []Node{{ID: "n1", Addr: "a1"}, {ID: "n2", Addr: "a2"}},
			b:        []Node{{ID: "n2", Addr: "a2"}, {ID: "n1", Addr: "a1"}},
			expected: true,
		},
		{
			name:     "different length",
			a:        []Node{{ID: "n1", Addr: "a1"}},
			b:        []Node{{ID: "n1", Addr: "a1"}, {ID: "n2", Addr: "a2"}},
			expected: false,
		},
		{
			name:     "different nodes",
			a:        []Node{{ID: "n1", Addr: "a1"}},
			b:        []Node{{ID: "n2", Addr: "a2"}},
			expected: false,
		},
		{
			name:     "same id different addr",
			a:        []Node{{ID: "n1", Addr: "a1"}},
			b:        []Node{{ID: "n1", Addr: "a2"}},
			expected: false,
		},
		{
			name:     "both empty",
			a:        []Node{},
			b:        []Node{},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := nodesEqual(tc.a, tc.b)
			if result != tc.expected {
				t.Errorf("nodesEqual() = %v, expected %v", result, tc.expected)
			}
		})
	}
}
