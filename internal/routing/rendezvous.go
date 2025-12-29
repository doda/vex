package routing

import (
	"crypto/sha256"
	"encoding/binary"
	"net"
	"sort"
	"strings"
	"sync"
)

// Node represents a node in the cluster that can serve requests.
type Node struct {
	ID   string
	Addr string
}

// Router uses rendezvous hashing to route requests to nodes based on namespace.
// It ensures cache locality by consistently mapping namespaces to home nodes.
type Router struct {
	mu    sync.RWMutex
	nodes []Node
	self  string
}

// New creates a new Router with the given self address.
func New(selfAddr string) *Router {
	return &Router{
		self:  normalizeAddr(selfAddr),
		nodes: []Node{},
	}
}

// SetNodes updates the list of nodes in the cluster.
func (r *Router) SetNodes(nodes []Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes = make([]Node, len(nodes))
	for i, node := range nodes {
		node.Addr = normalizeAddr(node.Addr)
		r.nodes[i] = node
	}
}

// Nodes returns a copy of the current node list.
func (r *Router) Nodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Node, len(r.nodes))
	copy(result, r.nodes)
	return result
}

// HomeNode returns the node that should own the given namespace for cache locality.
// Uses rendezvous hashing to compute a consistent mapping.
func (r *Router) HomeNode(namespace string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return Node{}, false
	}

	var homeNode Node
	var maxWeight uint64

	for _, node := range r.nodes {
		weight := computeWeight(node.ID, namespace)
		if weight > maxWeight {
			maxWeight = weight
			homeNode = node
		}
	}

	return homeNode, true
}

// RankedNodes returns all nodes ranked by their weight for the given namespace.
// The first node is the "home" node, followed by fallback nodes in order.
func (r *Router) RankedNodes(namespace string) []Node {
	r.mu.RLock()
	nodes := make([]Node, len(r.nodes))
	copy(nodes, r.nodes)
	r.mu.RUnlock()

	if len(nodes) == 0 {
		return nil
	}

	type nodeWeight struct {
		node   Node
		weight uint64
	}

	weights := make([]nodeWeight, len(nodes))
	for i, node := range nodes {
		weights[i] = nodeWeight{
			node:   node,
			weight: computeWeight(node.ID, namespace),
		}
	}

	sort.Slice(weights, func(i, j int) bool {
		if weights[i].weight != weights[j].weight {
			return weights[i].weight > weights[j].weight
		}
		// Tiebreak by node ID for deterministic ordering
		return weights[i].node.ID < weights[j].node.ID
	})

	result := make([]Node, len(weights))
	for i, nw := range weights {
		result[i] = nw.node
	}
	return result
}

// IsHomeNode checks if this router's node is the home node for the given namespace.
func (r *Router) IsHomeNode(namespace string) bool {
	home, ok := r.HomeNode(namespace)
	if !ok {
		return false
	}
	return home.Addr == r.self
}

// SelfAddr returns this router's self address.
func (r *Router) SelfAddr() string {
	return r.self
}

// CanServe always returns true because any node can serve any namespace.
// This is the fallback behavior when the home node is unavailable.
func (r *Router) CanServe(namespace string) bool {
	return true
}

// computeWeight calculates the rendezvous hash weight for a node-namespace pair.
// Higher weight means the node is preferred for that namespace.
func computeWeight(nodeID, namespace string) uint64 {
	h := sha256.New()
	h.Write([]byte(nodeID))
	h.Write([]byte{0}) // separator
	h.Write([]byte(namespace))
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

func normalizeAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return addr
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	switch host {
	case "", "0.0.0.0", "::", "127.0.0.1", "::1", "localhost":
		host = "localhost"
	}

	return net.JoinHostPort(host, port)
}
