package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"habitat/internal/config"
)

const integrationUsername = "fixture-operator"
const integrationPassword = "fixture-password-long-enough"

// Use the entrypoint's actual parser and constructor, not a manually composed
// middleware chain. The scripted SQL driver never opens a network connection.
func newAuthIntegrationServer(t *testing.T, basePath string, authenticated bool) *Server {
	t.Helper()
	t.Setenv("HABITAT_LISTEN", "127.0.0.1:7890")
	t.Setenv("HABITAT_BASE_PATH", basePath)
	t.Setenv("HABITAT_DB_URL", "")
	t.Setenv("HABITAT_DB_HOST", "fixture.invalid")
	t.Setenv("HABITAT_DB_NAME", "fixture")
	t.Setenv("HABITAT_AUTH_USERNAME", "")
	t.Setenv("HABITAT_AUTH_PASSWORD", "")
	if authenticated {
		t.Setenv("HABITAT_AUTH_USERNAME", integrationUsername)
		t.Setenv("HABITAT_AUTH_PASSWORD", integrationPassword)
	}
	cfg, err := config.FromArgs(nil)
	if err != nil {
		t.Fatalf("parse server config: %v", err)
	}
	srv, err := New(cfg, newScriptedDB(t, nil))
	if err != nil {
		t.Fatalf("construct real server: %v", err)
	}
	return srv
}

func TestAuthIntegrationProtectsRealMux(t *testing.T) {
	for _, basePath := range []string{"", "/habitat"} {
		t.Run("base="+basePath, func(t *testing.T) {
			srv := newAuthIntegrationServer(t, basePath, true)
			for _, path := range []string{
				"/", "/tasks", "/_static/", "/_static/assets/app.js",
				"/api/config", "/api/metrics", "/api/tasks", "/api/tasks/id",
				"/api/queues", "/api/queues/default", "/api/events",
				"/_healthz/", "/_healthz/../api/config", "//api/config",
			} {
				t.Run(path, func(t *testing.T) {
					resp := httptest.NewRecorder()
					srv.mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, basePath+path, nil))
					if resp.Code != http.StatusUnauthorized {
						t.Fatalf("real mux %s: status = %d, want 401", path, resp.Code)
					}
					if resp.Header().Get("WWW-Authenticate") != `Basic realm="Habitat", charset="UTF-8"` || resp.Header().Get("Cache-Control") != "no-store" {
						t.Fatalf("missing authentication response headers: %v", resp.Header())
					}
				})
			}
		})
	}
}

func TestAuthIntegrationCredentialAndAssetRouting(t *testing.T) {
	for _, basePath := range []string{"", "/habitat"} {
		t.Run("base="+basePath, func(t *testing.T) {
			srv := newAuthIntegrationServer(t, basePath, true)
			for _, credentials := range []struct{ username, password string }{
				{"wrong", integrationPassword}, {integrationUsername, "wrong"}, {"", ""},
			} {
				req := httptest.NewRequest(http.MethodGet, basePath+"/api/config", nil)
				req.SetBasicAuth(credentials.username, credentials.password)
				resp := httptest.NewRecorder()
				srv.mux.ServeHTTP(resp, req)
				if resp.Code != http.StatusUnauthorized {
					t.Fatalf("incorrect credentials reached config handler: %d", resp.Code)
				}
			}
			for _, path := range []string{"/api/config", "/", "/_static/"} {
				req := httptest.NewRequest(http.MethodGet, basePath+path, nil)
				req.SetBasicAuth(integrationUsername, integrationPassword)
				resp := httptest.NewRecorder()
				srv.mux.ServeHTTP(resp, req)
				if resp.Code != http.StatusOK {
					t.Fatalf("correct credentials failed real %s handler: %d: %s", path, resp.Code, resp.Body.String())
				}
				if path == "/api/config" {
					var cfg uiRuntimeConfig
					if err := json.Unmarshal(resp.Body.Bytes(), &cfg); err != nil || cfg.APIBasePath != basePath+"/api" {
						t.Fatalf("real config handler returned incorrect prefix: %s (%v)", resp.Body.String(), err)
					}
				} else if !strings.Contains(resp.Body.String(), "<html") {
					t.Fatalf("real asset handler did not serve the embedded frontend: %s", path)
				}
			}
		})
	}
}

func TestAuthIntegrationHealthAndBasePath(t *testing.T) {
	srv := newAuthIntegrationServer(t, "/habitat", true)
	for _, tc := range []struct {
		path string
		want int
	}{
		{"/habitat/_healthz", http.StatusOK},
		{"/_healthz", http.StatusNotFound},
		{"/api/config", http.StatusNotFound},
		{"/habitat", http.StatusPermanentRedirect},
	} {
		resp := httptest.NewRecorder()
		srv.mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if resp.Code != tc.want {
			t.Fatalf("real mux %s: status = %d, want %d", tc.path, resp.Code, tc.want)
		}
		if tc.want == http.StatusOK && resp.Body.String() != "ok" {
			t.Fatalf("health exposed unexpected body: %q", resp.Body.String())
		}
		if tc.want == http.StatusPermanentRedirect && resp.Header().Get("Location") != "/habitat/" {
			t.Fatalf("base-path redirect = %q", resp.Header().Get("Location"))
		}
	}
	if err := srv.db.Close(); err != nil {
		t.Fatal(err)
	}
	resp := httptest.NewRecorder()
	srv.mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/habitat/_healthz", nil))
	if resp.Code != http.StatusServiceUnavailable || resp.Body.String() != "database unavailable\n" {
		t.Fatalf("unhealthy database probe: %d %q", resp.Code, resp.Body.String())
	}
}

func TestAuthIntegrationLoopbackWithoutCredentials(t *testing.T) {
	srv := newAuthIntegrationServer(t, "", false)
	resp := httptest.NewRecorder()
	srv.mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("local development config handler unavailable: %d", resp.Code)
	}
}
