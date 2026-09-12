package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"habitat/internal/config"
)

func TestWithAuthenticationProtectsOperationalRoutes(t *testing.T) {
	srv := &Server{cfg: config.Config{Auth: config.AuthConfig{
		Username: "operator",
		Password: "correct horse battery staple",
	}}}

	called := false
	handler := srv.withAuthentication(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	for _, path := range []string{
		"/",
		"/_static/app.js",
		"/api/config",
		"/api/metrics",
		"/api/tasks",
		"/api/tasks/run-id",
		"/api/queues",
		"/api/queues/work/tasks",
		"/api/queues/work/events",
		"/api/events",
	} {
		t.Run(path, func(t *testing.T) {
			called = false
			req := httptest.NewRequest(http.MethodGet, path, nil)
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)

			if resp.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", resp.Code, http.StatusUnauthorized)
			}
			if called {
				t.Fatal("protected handler was called without credentials")
			}
			if got := resp.Header().Get("WWW-Authenticate"); got == "" {
				t.Fatal("missing WWW-Authenticate challenge")
			}
		})
	}
}

func TestWithAuthenticationAcceptsExactCredentials(t *testing.T) {
	srv := &Server{cfg: config.Config{Auth: config.AuthConfig{
		Username: "operator",
		Password: "correct horse battery staple",
	}}}
	handler := srv.withAuthentication(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name     string
		username string
		password string
		want     int
	}{
		{name: "correct", username: "operator", password: "correct horse battery staple", want: http.StatusNoContent},
		{name: "wrong username", username: "other", password: "correct horse battery staple", want: http.StatusUnauthorized},
		{name: "wrong password", username: "operator", password: "wrong", want: http.StatusUnauthorized},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
			req.SetBasicAuth(tc.username, tc.password)
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)

			if resp.Code != tc.want {
				t.Fatalf("status = %d, want %d", resp.Code, tc.want)
			}
		})
	}
}

func TestWithAuthenticationLeavesHealthProbeOpen(t *testing.T) {
	srv := &Server{cfg: config.Config{Auth: config.AuthConfig{
		Username: "operator",
		Password: "correct horse battery staple",
	}}}
	handler := srv.withAuthentication(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/_healthz", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusNoContent)
	}
}

func TestWithAuthenticationIsInertWhenDisabled(t *testing.T) {
	srv := &Server{}
	handler := srv.withAuthentication(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusNoContent)
	}
}

func TestAuthenticationAppliesBehindBasePath(t *testing.T) {
	srv := &Server{cfg: config.Config{
		BasePath: "/habitat",
		Auth: config.AuthConfig{
			Username: "operator",
			Password: "correct horse battery staple",
		},
	}}

	handler := srv.withBasePath(srv.withAuthentication(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tasks" {
			t.Fatalf("forwarded path = %q, want %q", r.URL.Path, "/api/tasks")
		}
		w.WriteHeader(http.StatusNoContent)
	})))

	unauthorized := httptest.NewRequest(http.MethodGet, "/habitat/api/tasks", nil)
	unauthorizedResp := httptest.NewRecorder()
	handler.ServeHTTP(unauthorizedResp, unauthorized)
	if unauthorizedResp.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want %d", unauthorizedResp.Code, http.StatusUnauthorized)
	}

	authorized := httptest.NewRequest(http.MethodGet, "/habitat/api/tasks", nil)
	authorized.SetBasicAuth("operator", "correct horse battery staple")
	authorizedResp := httptest.NewRecorder()
	handler.ServeHTTP(authorizedResp, authorized)
	if authorizedResp.Code != http.StatusNoContent {
		t.Fatalf("authorized status = %d, want %d", authorizedResp.Code, http.StatusNoContent)
	}
}
