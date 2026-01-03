package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestTestStateEndpoint_RequiresAdminAuth(t *testing.T) {
	cfg := testConfig()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := bytes.NewBufferString(`{}`)

	req := httptest.NewRequest("POST", "/_test/state", body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 without auth header, got %d", w.Result().StatusCode)
	}

	req = httptest.NewRequest("POST", "/_test/state", bytes.NewBufferString(`{}`))
	req.Header.Set("Authorization", "Bearer "+testAuthToken)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 with regular token, got %d", w.Result().StatusCode)
	}

	req = httptest.NewRequest("POST", "/_test/state", bytes.NewBufferString(`{}`))
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("expected 200 with admin token, got %d", w.Result().StatusCode)
	}
}
