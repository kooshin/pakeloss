package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResolveCLIConfigUsesControllerConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "controller.toml")
	if err := os.WriteFile(path, []byte(`
[server]
http_addr = "127.0.0.1:18080"

[auth]
token = "config-token"

[tui]
graph_mode = "ascii"
refresh_interval = "1500ms"
`), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := resolveCLIConfig(path, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.api != "127.0.0.1:18080" {
		t.Fatalf("api = %q, want 127.0.0.1:18080", got.api)
	}
	if got.token != "config-token" {
		t.Fatalf("token = %q, want config-token", got.token)
	}
	if got.graphMode != "ascii" {
		t.Fatalf("graph mode = %q, want ascii", got.graphMode)
	}
	if got.refreshInterval != 1500*time.Millisecond {
		t.Fatalf("refresh interval = %s, want 1.5s", got.refreshInterval)
	}
}

func TestResolveCLIConfigCLIOverridesConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "controller.toml")
	if err := os.WriteFile(path, []byte(`
[server]
http_addr = "127.0.0.1:18080"

[auth]
token = "config-token"

[tui]
graph_mode = "ascii"
refresh_interval = "1500ms"
`), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := resolveCLIConfig(path, "127.0.0.1:28080", "cli-token", "unicode")
	if err != nil {
		t.Fatal(err)
	}
	if got.api != "127.0.0.1:28080" {
		t.Fatalf("api = %q, want 127.0.0.1:28080", got.api)
	}
	if got.token != "cli-token" {
		t.Fatalf("token = %q, want cli-token", got.token)
	}
	if got.graphMode != "unicode" {
		t.Fatalf("graph mode = %q, want unicode", got.graphMode)
	}
	if got.refreshInterval != 1500*time.Millisecond {
		t.Fatalf("refresh interval = %s, want 1.5s", got.refreshInterval)
	}
}

func TestResolveCLIConfigRequiresToken(t *testing.T) {
	if _, err := resolveCLIConfig("", "", "", ""); err == nil {
		t.Fatal("expected missing token error")
	}
}

func TestResolveCLIConfigAcceptsTokenWithoutConfig(t *testing.T) {
	got, err := resolveCLIConfig("", "", "cli-token", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.api != "127.0.0.1:8080" || got.token != "cli-token" || got.graphMode != "unicode" || got.refreshInterval != time.Second {
		t.Fatalf("unexpected defaults: %+v", got)
	}
}

func TestResolveCLIConfigRejectsInvalidRefreshInterval(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "controller.toml")
	if err := os.WriteFile(path, []byte(`
[tui]
refresh_interval = "not-a-duration"

[auth]
token = "config-token"
`), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := resolveCLIConfig(path, "", "", ""); err == nil {
		t.Fatal("expected invalid refresh interval error")
	}
}
