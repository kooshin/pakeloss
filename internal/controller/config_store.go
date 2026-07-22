package controller

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/BurntSushi/toml"

	"pakeloss/internal/model"
)

type ConfigStore struct {
	mu               sync.RWMutex
	mesh             model.MeshConfig
	discoveredNodes  map[string]model.NodeConfig
	discoveredStates map[string]string
}

type controllerConfigFile struct {
	Server       controllerServerConfig      `toml:"server"`
	ResultLog    controllerResultLogConfig   `toml:"result_log"`
	Auth         controllerAuthConfig        `toml:"auth"`
	TUI          model.TUIConfig             `toml:"tui"`
	Measurement  controllerMeasurementConfig `toml:"measurement"`
	FlowDefaults model.AutoFlowConfig        `toml:"flow_defaults"`
}

type controllerServerConfig struct {
	GRPCAddr string `toml:"grpc_addr"`
	HTTPAddr string `toml:"http_addr"`
}

type controllerResultLogConfig struct {
	CSV              string `toml:"csv"`
	JSONL            string `toml:"jsonl"`
	DebugJSONL       string `toml:"debug_jsonl"`
	SummaryCSV       string `toml:"summary_csv"`
	SummaryJSONL     string `toml:"summary_jsonl"`
	OutageEventCSV   string `toml:"outage_event_csv"`
	OutageEventJSONL string `toml:"outage_event_jsonl"`
	FlushInterval    string `toml:"flush_interval"`
	Timezone         string `toml:"timezone"`
}

type controllerAuthConfig struct {
	Token string `toml:"token"`
}

type controllerMeasurementConfig struct {
	ReportFinalizeDelay string `toml:"report_finalize_delay"`
	ReportBucketFactor  uint32 `toml:"report_bucket_factor"`
	OutageThresholdMs   uint32 `toml:"outage_threshold_ms"`
}

var (
	ErrAgentNotFound         = fmt.Errorf("agent not found")
	ErrAutoDiscoveryOnly     = fmt.Errorf("agent enable/disable is supported only in discovery_mode auto")
	ErrAllFlowsMustBeStopped = fmt.Errorf("all flows must be stopped")
)

func LoadControllerConfig(path string) (model.ControllerConfig, error) {
	var raw controllerConfigFile
	b, err := os.ReadFile(path)
	if err != nil {
		return model.ControllerConfig{}, err
	}
	md, err := toml.Decode(string(b), &raw)
	if err != nil {
		return model.ControllerConfig{}, err
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		parts := make([]string, 0, len(undecoded))
		for _, key := range undecoded {
			parts = append(parts, key.String())
		}
		return model.ControllerConfig{}, fmt.Errorf("unsupported controller config keys: %s", strings.Join(parts, ", "))
	}
	cfg := model.ControllerConfig{
		GRPCAddr:            raw.Server.GRPCAddr,
		HTTPAddr:            raw.Server.HTTPAddr,
		ResultCSV:           raw.ResultLog.CSV,
		ResultJSONL:         raw.ResultLog.JSONL,
		ResultDebugJSONL:    raw.ResultLog.DebugJSONL,
		ResultSummaryCSV:    raw.ResultLog.SummaryCSV,
		ResultSummaryJSONL:  raw.ResultLog.SummaryJSONL,
		OutageEventCSV:      raw.ResultLog.OutageEventCSV,
		OutageEventJSONL:    raw.ResultLog.OutageEventJSONL,
		ResultFlushInterval: raw.ResultLog.FlushInterval,
		ReportFinalizeDelay: raw.Measurement.ReportFinalizeDelay,
		ReportBucketFactor:  raw.Measurement.ReportBucketFactor,
		OutageThresholdMs:   raw.Measurement.OutageThresholdMs,
		LogTimezone:         raw.ResultLog.Timezone,
		Token:               raw.Auth.Token,
		TUI:                 raw.TUI,
		FlowDefaults:        raw.FlowDefaults,
	}
	FinalizeControllerConfig(&cfg)
	return cfg, nil
}

func FinalizeControllerConfig(cfg *model.ControllerConfig) {
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = "127.0.0.1:8443"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = "127.0.0.1:8080"
	}
	if cfg.ResultFlushInterval == "" {
		cfg.ResultFlushInterval = "10s"
	}
	if cfg.ReportFinalizeDelay == "" {
		cfg.ReportFinalizeDelay = "2s"
	}
	if cfg.ReportBucketFactor == 0 {
		cfg.ReportBucketFactor = 10
	}
	if cfg.OutageThresholdMs == 0 {
		cfg.OutageThresholdMs = 100
	}
	if cfg.LogTimezone == "" {
		cfg.LogTimezone = "Asia/Tokyo"
	}
	applyAutoFlowDefaults(&cfg.FlowDefaults)
}

func NewRuntimeMesh(flowDefaults model.AutoFlowConfig) model.MeshConfig {
	applyAutoFlowDefaults(&flowDefaults)
	return model.MeshConfig{
		ConfigVersion:      1,
		DiscoveryMode:      "auto",
		ReportBucketFactor: 10,
		OutageThresholdMs:  100,
		AutoFlow:           flowDefaults,
	}
}

func NewConfigStore(mesh model.MeshConfig) *ConfigStore {
	applyMeshDefaults(&mesh)
	return &ConfigStore{
		mesh:             mesh,
		discoveredNodes:  map[string]model.NodeConfig{},
		discoveredStates: map[string]string{},
	}
}

func FinalizeMeshConfig(cfg *model.MeshConfig) error {
	applyMeshDefaults(cfg)
	return validateMeshConfig(*cfg)
}

func applyAutoFlowDefaults(cfg *model.AutoFlowConfig) {
	if cfg.IntervalMs == 0 {
		cfg.IntervalMs = 10
	}
	if cfg.PacketSize == 0 {
		cfg.PacketSize = 256
	}
	if cfg.SourcePortCount == 0 {
		cfg.SourcePortCount = 8
	}
	if cfg.LossConfirmWindowMs == 0 {
		cfg.LossConfirmWindowMs = 2000
	}
	if cfg.State == "" {
		cfg.State = "stopped"
	}
}

func applyMeshDefaults(cfg *model.MeshConfig) {
	if cfg.ConfigVersion == 0 {
		cfg.ConfigVersion = 1
	}
	if cfg.DiscoveryMode == "" {
		cfg.DiscoveryMode = "static"
	}
	if cfg.OutageThresholdMs == 0 {
		cfg.OutageThresholdMs = 100
	}
	applyAutoFlowDefaults(&cfg.AutoFlow)
	if cfg.DiscoveryMode != "auto" && cfg.Flows == nil {
		cfg.Flows = fullMeshFlows(cfg.Nodes)
	}
	for i := range cfg.Flows {
		if cfg.Flows[i].IntervalMs == 0 {
			cfg.Flows[i].IntervalMs = cfg.AutoFlow.IntervalMs
		}
		if cfg.Flows[i].PacketSize == 0 {
			cfg.Flows[i].PacketSize = cfg.AutoFlow.PacketSize
		}
		if cfg.Flows[i].SourcePortCount == 0 {
			cfg.Flows[i].SourcePortCount = cfg.AutoFlow.SourcePortCount
		}
		if cfg.Flows[i].LossConfirmWindowMs == 0 {
			cfg.Flows[i].LossConfirmWindowMs = cfg.AutoFlow.LossConfirmWindowMs
		}
		if cfg.Flows[i].State == "" {
			cfg.Flows[i].State = cfg.AutoFlow.State
		}
	}
}

func applyDefaults(cfg *model.MeshConfig) {
	applyMeshDefaults(cfg)
}

func validateMeshConfig(cfg model.MeshConfig) error {
	switch cfg.DiscoveryMode {
	case "static", "auto":
	default:
		return fmt.Errorf("unsupported discovery_mode: %s", cfg.DiscoveryMode)
	}
	if cfg.DiscoveryMode == "auto" {
		if len(cfg.Nodes) > 0 {
			return fmt.Errorf("discovery_mode auto requires nodes to be empty or omitted")
		}
		if len(cfg.Flows) > 0 {
			return fmt.Errorf("discovery_mode auto requires flows to be empty or omitted")
		}
	}
	return nil
}

func fullMeshFlows(nodes []model.NodeConfig) []model.MeshFlowConfig {
	sortedNodes := append([]model.NodeConfig(nil), nodes...)
	sort.Slice(sortedNodes, func(i, j int) bool { return sortedNodes[i].ID < sortedNodes[j].ID })
	flows := make([]model.MeshFlowConfig, 0, len(sortedNodes)*(len(sortedNodes)-1))
	for i := 0; i < len(sortedNodes); i++ {
		for j := i + 1; j < len(sortedNodes); j++ {
			src := sortedNodes[i]
			dst := sortedNodes[j]
			flows = append(flows, model.MeshFlowConfig{
				ID:  src.ID + "->" + dst.ID,
				Src: src.ID,
				Dst: dst.ID,
			})
			flows = append(flows, model.MeshFlowConfig{
				ID:  dst.ID + "->" + src.ID,
				Src: dst.ID,
				Dst: src.ID,
			})
		}
	}
	return flows
}

func (s *ConfigStore) Mesh() model.MeshConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.meshLocked()
}

func (s *ConfigStore) UpsertDiscoveredNode(agentID, udpAddr string) (model.MeshConfig, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mesh.DiscoveryMode != "auto" {
		return s.meshLocked(), false, nil
	}
	if agentID == "" {
		return s.meshLocked(), false, fmt.Errorf("discovered node requires agent id")
	}
	if udpAddr == "" {
		return s.meshLocked(), false, fmt.Errorf("discovered node %q requires udp address", agentID)
	}

	current, ok := s.discoveredNodes[agentID]
	if ok && current.UDPAddr == udpAddr {
		return s.meshLocked(), false, nil
	}

	node := model.NodeConfig{ID: agentID, UDPAddr: udpAddr, Enabled: true}
	if ok {
		node.Enabled = current.Enabled
	}
	s.discoveredNodes[agentID] = node
	s.mesh.ConfigVersion++
	return s.meshLocked(), true, nil
}

func (s *ConfigStore) SetAgentEnabled(agentID string, enabled bool) (model.MeshConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mesh.DiscoveryMode != "auto" {
		return s.meshLocked(), ErrAutoDiscoveryOnly
	}
	if !s.canChangeAgentEnabledLocked() {
		return s.meshLocked(), ErrAllFlowsMustBeStopped
	}
	node, ok := s.discoveredNodes[agentID]
	if !ok {
		return s.meshLocked(), ErrAgentNotFound
	}
	if node.Enabled == enabled {
		return s.meshLocked(), nil
	}
	node.Enabled = enabled
	s.discoveredNodes[agentID] = node
	s.mesh.ConfigVersion++
	return s.meshLocked(), nil
}

func (s *ConfigStore) SetFlowState(flowID, state string) (model.MeshConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mesh.DiscoveryMode == "auto" {
		for _, f := range s.autoFlowsLocked() {
			if f.ID == flowID {
				s.discoveredStates[flowID] = state
				s.mesh.ConfigVersion++
				return s.meshLocked(), nil
			}
		}
		return s.meshLocked(), ErrFlowNotFound
	}
	for i := range s.mesh.Flows {
		if s.mesh.Flows[i].ID == flowID {
			s.mesh.Flows[i].State = state
			s.mesh.ConfigVersion++
			return s.meshLocked(), nil
		}
	}
	return s.meshLocked(), ErrFlowNotFound
}

func (s *ConfigStore) SetAllFlowStates(state string) model.MeshConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mesh.DiscoveryMode == "auto" {
		for _, f := range s.autoFlowsLocked() {
			s.discoveredStates[f.ID] = state
		}
		s.mesh.ConfigVersion++
		return s.meshLocked()
	}
	for i := range s.mesh.Flows {
		s.mesh.Flows[i].State = state
	}
	s.mesh.ConfigVersion++
	return s.meshLocked()
}

func (s *ConfigStore) meshLocked() model.MeshConfig {
	mesh := s.mesh
	mesh.Nodes = append([]model.NodeConfig(nil), mesh.Nodes...)
	mesh.Flows = append([]model.MeshFlowConfig(nil), mesh.Flows...)
	if mesh.DiscoveryMode != "auto" {
		return mesh
	}
	mesh.Nodes = s.discoveredNodesLocked()
	mesh.Flows = s.autoFlowsLocked()
	return mesh
}

func (s *ConfigStore) discoveredNodesLocked() []model.NodeConfig {
	nodes := make([]model.NodeConfig, 0, len(s.discoveredNodes))
	for _, node := range s.discoveredNodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

func (s *ConfigStore) autoFlowsLocked() []model.MeshFlowConfig {
	nodes := s.enabledDiscoveredNodesLocked()
	flows := make([]model.MeshFlowConfig, 0, len(nodes)*(len(nodes)-1))
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			src := nodes[i]
			dst := nodes[j]
			flows = append(flows,
				s.autoFlowLocked(src.ID, dst.ID),
				s.autoFlowLocked(dst.ID, src.ID),
			)
		}
	}
	return flows
}

func (s *ConfigStore) autoFlowLocked(srcID, dstID string) model.MeshFlowConfig {
	id := srcID + "->" + dstID
	state := s.discoveredStates[id]
	if state == "" {
		state = "stopped"
	}
	return model.MeshFlowConfig{
		ID:                  id,
		Src:                 srcID,
		Dst:                 dstID,
		IntervalMs:          s.mesh.AutoFlow.IntervalMs,
		PacketSize:          s.mesh.AutoFlow.PacketSize,
		SourcePortCount:     s.mesh.AutoFlow.SourcePortCount,
		LossConfirmWindowMs: s.mesh.AutoFlow.LossConfirmWindowMs,
		State:               state,
	}
}

func (s *ConfigStore) enabledDiscoveredNodesLocked() []model.NodeConfig {
	nodes := make([]model.NodeConfig, 0, len(s.discoveredNodes))
	for _, node := range s.discoveredNodes {
		if !node.Enabled {
			continue
		}
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

func (s *ConfigStore) canChangeAgentEnabledLocked() bool {
	for _, flow := range s.autoFlowsLocked() {
		if flow.State == "running" {
			return false
		}
	}
	return true
}
