package controller

import (
	"os"
	"path/filepath"
	"testing"

	"pakeloss/internal/model"
)

func TestFinalizeControllerConfigSetsResultFlushIntervalDefault(t *testing.T) {
	cfg := model.ControllerConfig{}

	FinalizeControllerConfig(&cfg)

	if cfg.ResultFlushInterval != "10s" {
		t.Fatalf("result flush interval = %q, want 10s", cfg.ResultFlushInterval)
	}
	if cfg.ResultCSV != "" {
		t.Fatalf("result csv = %q, want empty", cfg.ResultCSV)
	}
	if cfg.ResultJSONL != "" {
		t.Fatalf("result jsonl = %q, want empty", cfg.ResultJSONL)
	}
	if cfg.ResultSummaryCSV != "" {
		t.Fatalf("result summary csv = %q, want empty", cfg.ResultSummaryCSV)
	}
	if cfg.ResultSummaryJSONL != "" {
		t.Fatalf("result summary jsonl = %q, want empty", cfg.ResultSummaryJSONL)
	}
	if cfg.FlowDefaults.LossConfirmWindowMs != 2000 {
		t.Fatalf("loss confirm window = %d, want 2000", cfg.FlowDefaults.LossConfirmWindowMs)
	}
	if cfg.ReportBucketFactor != 10 {
		t.Fatalf("report bucket factor = %d, want 10", cfg.ReportBucketFactor)
	}
	if cfg.OutageThresholdMs != 100 {
		t.Fatalf("outage threshold = %d, want 100", cfg.OutageThresholdMs)
	}
	if cfg.Token != "" {
		t.Fatalf("token = %q, want empty", cfg.Token)
	}
}

func TestLoadControllerConfigRejectsRemovedDSCP(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "controller.toml")
	if err := os.WriteFile(path, []byte("[flow_defaults]\ndscp = 8\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadControllerConfig(path); err == nil {
		t.Fatal("expected removed dscp key to be rejected")
	}
}

func TestLoadControllerConfigWithFlowDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "controller.toml")
	if err := os.WriteFile(path, []byte(`
[server]
grpc_addr = "127.0.0.1:8443"
http_addr = "127.0.0.1:8080"

[result_log]
summary_csv = "logs/session_summaries.csv"
summary_jsonl = "logs/session_summaries.jsonl"
outage_event_csv = "logs/outage_events.csv"
outage_event_jsonl = "logs/outage_events.jsonl"

[measurement]
report_bucket_factor = 5
outage_threshold_ms = 250

[flow_defaults]
interval_ms = 10
packet_size = 96
source_port_count = 8
loss_confirm_window_ms = 3500
state = "stopped"
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadControllerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCAddr != "127.0.0.1:8443" {
		t.Fatalf("grpc addr = %q", cfg.GRPCAddr)
	}
	if cfg.ResultSummaryCSV != "logs/session_summaries.csv" {
		t.Fatalf("result summary csv = %q", cfg.ResultSummaryCSV)
	}
	if cfg.ResultSummaryJSONL != "logs/session_summaries.jsonl" {
		t.Fatalf("result summary jsonl = %q", cfg.ResultSummaryJSONL)
	}
	if cfg.OutageEventCSV != "logs/outage_events.csv" || cfg.OutageEventJSONL != "logs/outage_events.jsonl" {
		t.Fatalf("outage event paths = %q/%q", cfg.OutageEventCSV, cfg.OutageEventJSONL)
	}
	if cfg.FlowDefaults.LossConfirmWindowMs != 3500 {
		t.Fatalf("loss confirm window = %d, want 3500", cfg.FlowDefaults.LossConfirmWindowMs)
	}
	if cfg.ReportBucketFactor != 5 {
		t.Fatalf("report bucket factor = %d, want 5", cfg.ReportBucketFactor)
	}
	if cfg.OutageThresholdMs != 250 {
		t.Fatalf("outage threshold = %d, want 250", cfg.OutageThresholdMs)
	}
}

func TestLoadControllerConfigRejectsLegacyKeys(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "mesh",
			body: `
[mesh]
config_version = 1
`,
		},
		{
			name: "defaults",
			body: `
[defaults]
packet_size = 96
`,
		},
		{
			name: "auto_flow",
			body: `
[auto_flow]
packet_size = 96
`,
		},
		{
			name: "nodes",
			body: `
[[nodes]]
id = "node-a"
udp_addr = "127.0.0.1:40001"
`,
		},
		{
			name: "flows",
			body: `
[[flows]]
id = "node-a<=>node-b"
src = "node-a"
dst = "node-b"
`,
		},
		{
			name: "discovery_mode",
			body: `
discovery_mode = "auto"
`,
		},
		{
			name: "config_version",
			body: `
[flow_defaults]
config_version = 2
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "controller.toml")
			if err := os.WriteFile(path, []byte(tt.body), 0o600); err != nil {
				t.Fatal(err)
			}

			if _, err := LoadControllerConfig(path); err == nil {
				t.Fatal("expected static mesh validation error")
			}
		})
	}
}
