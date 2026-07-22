package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigKeepsVRFFieldsAndDefaultsDisconnect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.toml")
	if err := os.WriteFile(path, []byte(`
[agent]
id = "node-a"

[controller]
addr = "127.0.0.1:8443"
vrf = "mgmt-vrf"
token = "dev-token"

[udp]
listen_addr = "0.0.0.0:40001"
advertise_addr = "192.0.2.10:40001"
listen_vrf = "probe-vrf"
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ControllerVRF != "mgmt-vrf" {
		t.Fatalf("controller vrf = %q, want %q", cfg.ControllerVRF, "mgmt-vrf")
	}
	if cfg.ListenVRF != "probe-vrf" {
		t.Fatalf("listen vrf = %q, want %q", cfg.ListenVRF, "probe-vrf")
	}
	if cfg.AdvertiseAddr != "192.0.2.10:40001" {
		t.Fatalf("advertise addr = %q, want %q", cfg.AdvertiseAddr, "192.0.2.10:40001")
	}
	if cfg.OnControllerDisconnect != "continue" {
		t.Fatalf("disconnect mode = %q, want continue", cfg.OnControllerDisconnect)
	}
}

func TestLoadConfigRejectsLegacyKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.toml")
	if err := os.WriteFile(path, []byte(`
[node]
id = "node-a"

[probe]
listen_udp = "0.0.0.0:40001"
`), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected legacy config error")
	}
}
