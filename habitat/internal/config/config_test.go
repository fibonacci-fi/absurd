package config

import (
	"strings"
	"testing"
)

func TestNormalizeBasePath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "empty", input: "", want: ""},
		{name: "root", input: "/", want: ""},
		{name: "adds leading slash", input: "habitat", want: "/habitat"},
		{name: "trims trailing slash", input: "/habitat/", want: "/habitat"},
		{name: "rejects query", input: "/habitat?q=1", wantErr: true},
		{name: "rejects network-path style", input: "//habitat", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeBasePath(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("normalizeBasePath(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestLoopbackListenAddress(t *testing.T) {
	tests := []struct {
		address string
		want    bool
	}{
		{address: "127.0.0.1:7890", want: true},
		{address: "localhost:7890", want: true},
		{address: "[::1]:7890", want: true},
		{address: ":7890", want: false},
		{address: "0.0.0.0:7890", want: false},
		{address: "[::]:7890", want: false},
		{address: "invalid", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.address, func(t *testing.T) {
			if got := isLoopbackListenAddress(tc.address); got != tc.want {
				t.Fatalf("isLoopbackListenAddress(%q) = %v, want %v", tc.address, got, tc.want)
			}
		})
	}
}

func TestFromArgsAuthenticationBoundary(t *testing.T) {
	tests := []struct {
		name     string
		listen   string
		username string
		password string
		wantErr  string
	}{
		{name: "default loopback without auth"},
		{name: "explicit loopback without auth", listen: "127.0.0.1:9000"},
		{name: "wildcard with auth", listen: ":9000", username: "operator", password: "a-long-test-secret"},
		{name: "wildcard without auth", listen: ":9000", wantErr: "refusing unauthenticated non-loopback listener"},
		{name: "username only", username: "operator", wantErr: "must be configured together"},
		{name: "password only", password: "secret", wantErr: "must be configured together"},
		{name: "short password", username: "operator", password: "too-short", wantErr: "at least 16 bytes"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HABITAT_LISTEN", "")
			t.Setenv("HABITAT_BASE_PATH", "")
			t.Setenv("HABITAT_AUTH_USERNAME", tc.username)
			t.Setenv("HABITAT_AUTH_PASSWORD", tc.password)

			args := []string{}
			if tc.listen != "" {
				args = append(args, "-listen", tc.listen)
			}

			cfg, err := FromArgs(args)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("FromArgs() error = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FromArgs() unexpected error: %v", err)
			}
			if tc.listen == "" && cfg.ListenAddress != defaultListenAddress {
				t.Fatalf("default listen address = %q, want %q", cfg.ListenAddress, defaultListenAddress)
			}
		})
	}
}
