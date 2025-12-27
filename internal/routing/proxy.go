package routing

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	// DefaultProxyTimeout is the maximum time to wait for a proxied request.
	DefaultProxyTimeout = 5 * time.Second

	// ProxiedHeader is set to "true" when a request was proxied from another node.
	// This prevents infinite proxy loops.
	ProxiedHeader = "X-Vex-Proxied"
)

// ProxyConfig holds configuration for request proxying.
type ProxyConfig struct {
	// Timeout for proxied requests. Zero means use DefaultProxyTimeout.
	Timeout time.Duration
	// HTTPClient to use for proxied requests. If nil, uses http.DefaultClient.
	HTTPClient *http.Client
}

// Proxy handles proxying requests to home nodes with fallback to local serving.
type Proxy struct {
	router     *Router
	config     ProxyConfig
	httpClient *http.Client
}

// NewProxy creates a new Proxy with the given router and configuration.
func NewProxy(router *Router, config ProxyConfig) *Proxy {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = DefaultProxyTimeout
	}

	client := config.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: timeout,
		}
	}

	return &Proxy{
		router:     router,
		config:     config,
		httpClient: client,
	}
}

// ShouldProxy determines if a request for the given namespace should be proxied
// to the home node. Returns true if this node is not the home node and the
// request was not already proxied.
func (p *Proxy) ShouldProxy(namespace string, req *http.Request) bool {
	if req.Header.Get(ProxiedHeader) == "true" {
		return false
	}
	return !p.router.IsHomeNode(namespace)
}

// ProxyRequest proxies an HTTP request to the home node for the given namespace.
// Returns the response if successful, or an error if the proxy failed.
// If proxy fails, the caller should fall back to local serving.
func (p *Proxy) ProxyRequest(ctx context.Context, namespace string, req *http.Request) (*http.Response, error) {
	home, ok := p.router.HomeNode(namespace)
	if !ok {
		return nil, ErrNoHomeNode
	}

	timeout := p.config.Timeout
	if timeout == 0 {
		timeout = DefaultProxyTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	proxyReq, err := p.buildProxyRequest(ctx, home, req)
	if err != nil {
		return nil, err
	}

	return p.httpClient.Do(proxyReq)
}

// buildProxyRequest creates an HTTP request to proxy to the target node.
func (p *Proxy) buildProxyRequest(ctx context.Context, target Node, orig *http.Request) (*http.Request, error) {
	targetURL := *orig.URL
	targetURL.Scheme = "http"
	targetURL.Host = target.Addr

	var body io.Reader
	if orig.Body != nil {
		bodyBytes, err := io.ReadAll(orig.Body)
		if err != nil {
			return nil, err
		}
		orig.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		body = bytes.NewReader(bodyBytes)
	}

	proxyReq, err := http.NewRequestWithContext(ctx, orig.Method, targetURL.String(), body)
	if err != nil {
		return nil, err
	}

	for key, values := range orig.Header {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	proxyReq.Header.Set(ProxiedHeader, "true")

	return proxyReq, nil
}

// ProxyMiddleware returns an HTTP middleware that proxies requests to the home node.
// If the proxy fails or times out, the request is handled locally.
func (p *Proxy) ProxyMiddleware(extractNamespace func(*http.Request) string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		namespace := extractNamespace(req)
		if namespace == "" {
			next.ServeHTTP(w, req)
			return
		}

		if !p.ShouldProxy(namespace, req) {
			next.ServeHTTP(w, req)
			return
		}

		resp, err := p.ProxyRequest(req.Context(), namespace, req)
		if err != nil {
			next.ServeHTTP(w, req)
			return
		}
		defer resp.Body.Close()

		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	})
}

// ProxyHandler returns an http.HandlerFunc that proxies the request or falls back to local.
func (p *Proxy) ProxyHandler(namespace string, localHandler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if !p.ShouldProxy(namespace, req) {
			localHandler(w, req)
			return
		}

		resp, err := p.ProxyRequest(req.Context(), namespace, req)
		if err != nil {
			localHandler(w, req)
			return
		}
		defer resp.Body.Close()

		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}
}

// buildTargetURL creates the URL for the proxied request.
func buildTargetURL(targetAddr string, originalURL *url.URL) (*url.URL, error) {
	target := &url.URL{
		Scheme:   "http",
		Host:     targetAddr,
		Path:     originalURL.Path,
		RawQuery: originalURL.RawQuery,
	}
	return target, nil
}

// ErrNoHomeNode is returned when no home node can be found.
type ErrNoHomeNodeType struct{}

func (e ErrNoHomeNodeType) Error() string {
	return "no home node available"
}

var ErrNoHomeNode = ErrNoHomeNodeType{}
