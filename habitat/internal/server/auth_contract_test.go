package server

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"habitat/internal/config"
)

// The real mux uses an inert SQL driver; unexpected database work is observable
// and no test can contact a database or external service.
type authDriver struct{ queries atomic.Int32 }
type authConn struct{ owner *authDriver }
type authRows struct{}

func (d *authDriver) Open(string) (driver.Conn, error)  { return &authConn{d}, nil }
func (c *authConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (c *authConn) Close() error                        { return nil }
func (c *authConn) Begin() (driver.Tx, error)           { return nil, errors.New("unexpected transaction") }
func (c *authConn) Ping(context.Context) error          { return nil }
func (c *authConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	c.owner.queries.Add(1)
	return authRows{}, nil
}
func (authRows) Columns() []string         { return []string{"queue_name", "created_at"} }
func (authRows) Close() error              { return nil }
func (authRows) Next([]driver.Value) error { return io.EOF }

var authDBDriver = &authDriver{}

func init() { sql.Register("habitat-auth-test", authDBDriver) }

func authServer(t *testing.T, enabled bool) *Server {
	t.Helper()
	t.Setenv("HABITAT_LISTEN", "127.0.0.1:7890")
	t.Setenv("HABITAT_AUTH_USERNAME", "")
	t.Setenv("HABITAT_AUTH_PASSWORD", "")
	t.Setenv("HABITAT_DB_URL", "")
	t.Setenv("HABITAT_DB_HOST", "fixture.invalid")
	t.Setenv("HABITAT_DB_NAME", "fixture")
	if enabled {
		t.Setenv("HABITAT_AUTH_USERNAME", "operator")
		t.Setenv("HABITAT_AUTH_PASSWORD", "fixture-password-long-enough")
	}
	cfg, err := config.FromArgs(nil)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("habitat-auth-test", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	srv, err := New(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestAuthContractRealMux(t *testing.T) {
	srv := authServer(t, true)
	for _, path := range []string{
		"/", "/tasks", "/_static/", "/api/config", "/api/metrics", "/api/tasks",
		"/api/tasks/run-id", "/api/queues", "/api/queues/default/tasks", "/api/queues/default/events", "/api/events",
		"/_healthz/", "/_healthz/../api/tasks", "//api/tasks", "/%61pi/tasks", "/_healthz%2f..%2fapi/tasks",
	} {
		t.Run(path, func(t *testing.T) {
			before := authDBDriver.queries.Load()
			resp := httptest.NewRecorder()
			srv.mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, path, nil))
			if resp.Code != http.StatusUnauthorized {
				t.Fatalf("%s returned %d; want 401", path, resp.Code)
			}
			if authDBDriver.queries.Load() != before {
				t.Fatal("unauthenticated request reached database")
			}
			if resp.Header().Get("WWW-Authenticate") != `Basic realm="Habitat", charset="UTF-8"` || resp.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("missing authentication headers: %v", resp.Header())
			}
		})
	}
}

func TestAuthContractCredentialsAndControls(t *testing.T) {
	srv := authServer(t, true)
	for _, header := range []string{"", "Bearer fixture-password-long-enough", "Basic !!!", "Basic b3BlcmF0b3I6d3Jvbmc="} {
		req := httptest.NewRequest(http.MethodGet, "/api/queues", nil)
		req.Header.Set("Authorization", header)
		req.Header.Add("Authorization", "Basic b3BlcmF0b3I6Zml4dHVyZS1wYXNzd29yZC1sb25nLWVub3VnaA==")
		resp := httptest.NewRecorder()
		srv.mux.ServeHTTP(resp, req)
		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("malformed/duplicate header reached handler: %d", resp.Code)
		}
	}
	for _, credentials := range [][2]string{{"wrong", "fixture-password-long-enough"}, {"operator", "wrong"}} {
		req := httptest.NewRequest(http.MethodGet, "/api/queues", nil)
		req.SetBasicAuth(credentials[0], credentials[1])
		resp := httptest.NewRecorder()
		srv.mux.ServeHTTP(resp, req)
		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("incorrect credential reached handler: %d", resp.Code)
		}
	}
	for _, path := range []string{"/api/queues", "/api/config", "/", "/_static/"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.SetBasicAuth("operator", "fixture-password-long-enough")
		resp := httptest.NewRecorder()
		srv.mux.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("authenticated %s returned %d: %s", path, resp.Code, resp.Body.String())
		}
		if path == "/api/queues" && authDBDriver.queries.Load() == 0 {
			t.Fatal("authenticated API never queried the inert database")
		}
		if path == "/" && !strings.Contains(resp.Body.String(), "<html") {
			t.Fatal("real embedded frontend missing")
		}
	}
	resp := httptest.NewRecorder()
	srv.mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/_healthz", nil))
	if resp.Code != http.StatusOK || resp.Body.String() != "ok" {
		t.Fatalf("health probe changed: %d %q", resp.Code, resp.Body.String())
	}
	local := authServer(t, false)
	resp = httptest.NewRecorder()
	local.mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("uncredentialed loopback control failed: %d", resp.Code)
	}
}
