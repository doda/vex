package api

import (
	"net/http"

	"github.com/vexsearch/vex/internal/config"
)

const testAuthToken = "test-token"

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.AuthToken = testAuthToken
	return cfg
}

func addAuth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+testAuthToken)
}

func (r *Router) ServeAuthed(w http.ResponseWriter, req *http.Request) {
	if req.Header.Get("Authorization") == "" {
		addAuth(req)
	}
	r.ServeHTTP(w, req)
}
