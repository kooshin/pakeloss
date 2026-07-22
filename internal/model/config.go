package model

import "time"

type ControllerConfig struct {
	GRPCAddr            string
	HTTPAddr            string
	ResultCSV           string
	ResultJSONL         string
	ResultDebugJSONL    string
	ResultSummaryCSV    string
	ResultSummaryJSONL  string
	OutageEventCSV      string
	OutageEventJSONL    string
	ResultFlushInterval string
	ReportFinalizeDelay string
	ReportBucketFactor  uint32
	OutageThresholdMs   uint32
	LogTimezone         string
	Token               string
	TUI                 TUIConfig
	FlowDefaults        AutoFlowConfig
}

func (c ControllerConfig) ReportFinalizeDelayDuration() time.Duration {
	d, err := time.ParseDuration(c.ReportFinalizeDelay)
	if err != nil || d <= 0 {
		return 2 * time.Second
	}
	return d
}

type TUIConfig struct {
	GraphMode       string `toml:"graph_mode"`
	RefreshInterval string `toml:"refresh_interval"`
}

type AgentConfig struct {
	AgentID                string
	ControllerAddr         string
	ControllerVRF          string
	Token                  string
	ListenAddr             string
	AdvertiseAddr          string
	ListenVRF              string
	OnControllerDisconnect string
}

type MeshConfig struct {
	ConfigVersion      uint64
	DiscoveryMode      string
	ReportBucketFactor uint32
	OutageThresholdMs  uint32
	Nodes              []NodeConfig
	AutoFlow           AutoFlowConfig
	Flows              []MeshFlowConfig
}

type NodeConfig struct {
	ID      string
	UDPAddr string
	Enabled bool
}

type AutoFlowConfig struct {
	IntervalMs          uint32 `toml:"interval_ms"`
	PacketSize          uint32 `toml:"packet_size"`
	SourcePortCount     uint32 `toml:"source_port_count"`
	LossConfirmWindowMs uint32 `toml:"loss_confirm_window_ms"`
	State               string `toml:"state"`
}

type MeshFlowConfig struct {
	ID                  string `json:"id"`
	Src                 string `json:"src"`
	Dst                 string `json:"dst"`
	IntervalMs          uint32 `json:"interval_ms"`
	PacketSize          uint32 `json:"packet_size"`
	SourcePortCount     uint32 `json:"source_port_count"`
	LossConfirmWindowMs uint32 `json:"loss_confirm_window_ms"`
	State               string `json:"state"`
}
