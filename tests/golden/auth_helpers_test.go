package golden

import (
	"net/http"
	"os"

	"github.com/vexsearch/vex/internal/config"
)

const defaultTestAuthToken = "test-token"

func testAuthToken() string {
	if token := os.Getenv("VEX_TEST_AUTH_TOKEN"); token != "" {
		return token
	}
	if token := os.Getenv("VEX_AUTH_TOKEN"); token != "" {
		return token
	}
	return defaultTestAuthToken
}

func newTestConfig() *config.Config {
	cfg := config.Default()
	cfg.AuthToken = testAuthToken()
	return cfg
}

func addAuthHeader(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+testAuthToken())
}
