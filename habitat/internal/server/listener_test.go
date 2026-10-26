package server

import (
	"context"
	"strings"
	"testing"

	"habitat/internal/config"
)

func TestListenerEnforcesActualLoopbackBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, address string
		authenticated bool
		wantRefusal   bool
	}{
		{name: "numeric loopback", address: "127.0.0.1:0"},
		{name: "localhost", address: "localhost:0"},
		{name: "IPv6 loopback", address: "[::1]:0"},
		{name: "wildcard without auth", address: ":0", wantRefusal: true},
		{name: "wildcard with auth", address: ":0", authenticated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Exercise the shared start boundary independently of the CLI parser:
			// a hostname's resolved address cannot be trusted until it is bound.
			cfg := config.Config{ListenAddress: tc.address}
			if tc.authenticated {
				cfg.Auth = config.AuthConfig{Username: "operator", Password: "fixture-password-long-enough"}
			}
			srv, err := New(cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err = srv.Run(ctx)
			if tc.wantRefusal {
				if err == nil || !strings.Contains(err.Error(), "refusing unauthenticated non-loopback listener") {
					t.Fatalf("unsafe actual listener accepted: %v", err)
				}
			} else if err != nil {
				t.Fatalf("legitimate listener refused: %v", err)
			}
		})
	}
}
