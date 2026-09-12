package config

import (
	"strings"
	"testing"
)

// This contract test also compiles on the pre-authentication baseline. It pins
// observable entrypoint behavior independently of the implementation defaults.
func TestAuthContractDefaultAndListener(t *testing.T) {
	for _, tc := range []struct {
		name, listen, username, password, wantError string
	}{
		{name: "default"},
		{name: "IPv4 loopback", listen: "127.0.0.1:7890"},
		{name: "IPv6 loopback", listen: "[::1]:7890"},
		{name: "localhost", listen: "localhost:7890"},
		{name: "wildcard", listen: ":7890", wantError: "refusing unauthenticated non-loopback listener"},
		{name: "IPv4 wildcard", listen: "0.0.0.0:7890", wantError: "refusing unauthenticated non-loopback listener"},
		{name: "IPv6 wildcard", listen: "[::]:7890", wantError: "refusing unauthenticated non-loopback listener"},
		{name: "hostname", listen: "habitat.example:7890", wantError: "refusing unauthenticated non-loopback listener"},
		{name: "authenticated wildcard", listen: ":7890", username: "fixture", password: "fixture-long-password"},
		{name: "colon username", username: "fixture:operator", password: "fixture-long-password", wantError: "must not contain a colon"},
		{name: "username only", username: "fixture", wantError: "must be configured together"},
		{name: "password only", password: "fixture-long-password", wantError: "must be configured together"},
		{name: "short password", username: "fixture", password: "short", wantError: "at least 16 bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HABITAT_LISTEN", tc.listen)
			t.Setenv("HABITAT_BASE_PATH", "")
			t.Setenv("HABITAT_DB_URL", "")
			t.Setenv("HABITAT_DB_HOST", "fixture.invalid")
			t.Setenv("HABITAT_DB_NAME", "fixture")
			t.Setenv("HABITAT_AUTH_USERNAME", tc.username)
			t.Setenv("HABITAT_AUTH_PASSWORD", tc.password)
			cfg, err := FromArgs(nil)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("config accepted unsafe listener/credentials: error = %v, want %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "default" && cfg.ListenAddress != "127.0.0.1:7890" {
				t.Fatalf("default listener = %q, want loopback 127.0.0.1:7890", cfg.ListenAddress)
			}
		})
	}
}
