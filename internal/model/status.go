package model

import "time"

const (
	flowSnapshotHistory60sLength  = 60
	flowSnapshotHistory240sLength = 240
)

type FlowRuntimeStatus struct {
	FlowID                string    `json:"flow_id"`
	Src                   string    `json:"src"`
	Dst                   string    `json:"dst"`
	ReceiverAgentID       string    `json:"receiver_agent_id"`
	DesiredState          string    `json:"desired_state"`
	ActualState           string    `json:"actual_state"`
	IntervalMs            uint32    `json:"interval_ms"`
	ReportWindowMs        uint32    `json:"report_window_ms"`
	PacketSize            uint32    `json:"packet_size"`
	SourcePortCount       uint32    `json:"source_port_count"`
	LastSeen              time.Time `json:"last_seen"`
	LastReportedAt        time.Time `json:"last_reported_at"`
	LastError             string    `json:"last_error"`
	Tx1s                  uint64    `json:"tx_1s"`
	Rx1s                  uint64    `json:"rx_1s"`
	TxTotal               uint64    `json:"tx_total"`
	RxTotal               uint64    `json:"rx_total"`
	Lost1s                uint64    `json:"lost_1s"`
	LostTotal             uint64    `json:"lost_total"`
	LossTime1sMs          uint64    `json:"loss_time_1s_ms"`
	LossTimeTotalMs       uint64    `json:"loss_time_total_ms"`
	DuplicateTotal        uint64    `json:"duplicate_total"`
	ReorderTotal          uint64    `json:"reorder_total"`
	Lost60s               uint64    `json:"lost_60s"`
	LossRatio1s           float64   `json:"loss_ratio_1s"`
	LossRatio10s          float64   `json:"loss_ratio_10s"`
	LossRatio60s          float64   `json:"loss_ratio_60s"`
	Duplicate1s           uint64    `json:"duplicate_1s"`
	Duplicate60s          uint64    `json:"duplicate_60s"`
	Reorder1s             uint64    `json:"reorder_1s"`
	Reorder60s            uint64    `json:"reorder_60s"`
	LossHistory10s        []float64 `json:"loss_history_10s"`
	LossHistory20s        []float64 `json:"loss_history_20s"`
	LossHistory60s        []float64 `json:"loss_history_60s"`
	LossHistory240s       []float64 `json:"loss_history_240s"`
	IsolatedLossEvents    uint64    `json:"isolated_loss_events"`
	OutageCount           uint64    `json:"outage_count"`
	OutageActive          bool      `json:"outage_active"`
	CurrentOutageMs       uint64    `json:"current_outage_ms"`
	LastOutageMs          uint64    `json:"last_outage_ms"`
	MaxOutageMs           uint64    `json:"max_outage_ms"`
	OutageTotalMs         uint64    `json:"outage_total_ms"`
	OutageThresholdMs     uint32    `json:"outage_threshold_ms"`
	UnmeasurableActive    bool      `json:"unmeasurable_active"`
	CurrentUnmeasurableMs uint64    `json:"current_unmeasurable_ms"`
	UnmeasurableCount     uint64    `json:"unmeasurable_count"`
	UnmeasurableTotalMs   uint64    `json:"unmeasurable_total_ms"`
	MaxUnmeasurableMs     uint64    `json:"max_unmeasurable_ms"`
	HistoryClearedAt      time.Time `json:"history_cleared_at"`
}

func (f *FlowRuntimeStatus) ClearHistory(now time.Time) {
	f.LossRatio1s = 0
	f.LossRatio10s = 0
	f.LossRatio60s = 0
	f.Tx1s = 0
	f.Rx1s = 0
	f.TxTotal = 0
	f.RxTotal = 0
	f.Lost1s = 0
	f.LostTotal = 0
	f.LossTime1sMs = 0
	f.LossTimeTotalMs = 0
	f.DuplicateTotal = 0
	f.ReorderTotal = 0
	f.Lost60s = 0
	f.Duplicate1s = 0
	f.Duplicate60s = 0
	f.Reorder1s = 0
	f.Reorder60s = 0
	f.IsolatedLossEvents = 0
	f.OutageCount = 0
	f.OutageActive = false
	f.CurrentOutageMs = 0
	f.LastOutageMs = 0
	f.MaxOutageMs = 0
	f.OutageTotalMs = 0
	f.UnmeasurableActive = false
	f.CurrentUnmeasurableMs = 0
	f.UnmeasurableCount = 0
	f.UnmeasurableTotalMs = 0
	f.MaxUnmeasurableMs = 0
	f.LossHistory10s = nil
	f.LossHistory20s = nil
	f.LossHistory60s = nil
	f.LossHistory240s = nil
	f.HistoryClearedAt = now
}

type AgentRuntimeStatus struct {
	AgentID              string    `json:"agent_id"`
	Status               string    `json:"status"`
	UDPAddr              string    `json:"udp_addr"`
	Enabled              bool      `json:"enabled"`
	ActiveConfigVersion  uint64    `json:"active_config_version"`
	DesiredConfigVersion uint64    `json:"desired_config_version"`
	ActiveFlows          uint32    `json:"active_flows"`
	LastHeartbeat        time.Time `json:"last_heartbeat"`
	LastResult           time.Time `json:"last_result"`
}

type FlowSnapshot struct {
	FlowID                string    `json:"flow_id"`
	Src                   string    `json:"src"`
	Dst                   string    `json:"dst"`
	DesiredState          string    `json:"desired_state"`
	ActualState           string    `json:"actual_state"`
	IntervalMs            uint32    `json:"interval_ms"`
	ReportWindowMs        uint32    `json:"report_window_ms"`
	PacketSize            uint32    `json:"packet_size"`
	LastSeen              time.Time `json:"last_seen"`
	LastError             string    `json:"last_error"`
	Tx1s                  uint64    `json:"tx_1s"`
	Rx1s                  uint64    `json:"rx_1s"`
	TxTotal               uint64    `json:"tx_total"`
	RxTotal               uint64    `json:"rx_total"`
	Lost1s                uint64    `json:"lost_1s"`
	LostTotal             uint64    `json:"lost_total"`
	LossTime1sMs          uint64    `json:"loss_time_1s_ms"`
	LossTimeTotalMs       uint64    `json:"loss_time_total_ms"`
	DuplicateTotal        uint64    `json:"duplicate_total"`
	ReorderTotal          uint64    `json:"reorder_total"`
	IsolatedLossEvents    uint64    `json:"isolated_loss_events"`
	OutageCount           uint64    `json:"outage_count"`
	OutageActive          bool      `json:"outage_active"`
	CurrentOutageMs       uint64    `json:"current_outage_ms"`
	LastOutageMs          uint64    `json:"last_outage_ms"`
	MaxOutageMs           uint64    `json:"max_outage_ms"`
	OutageTotalMs         uint64    `json:"outage_total_ms"`
	OutageThresholdMs     uint32    `json:"outage_threshold_ms"`
	UnmeasurableActive    bool      `json:"unmeasurable_active"`
	CurrentUnmeasurableMs uint64    `json:"current_unmeasurable_ms"`
	UnmeasurableCount     uint64    `json:"unmeasurable_count"`
	UnmeasurableTotalMs   uint64    `json:"unmeasurable_total_ms"`
	MaxUnmeasurableMs     uint64    `json:"max_unmeasurable_ms"`
	LossRatio1s           float64   `json:"loss_ratio_1s"`
	LossRatio10s          float64   `json:"loss_ratio_10s"`
	LossRatio60s          float64   `json:"loss_ratio_60s"`
	Duplicate1s           uint64    `json:"duplicate_1s"`
	Reorder1s             uint64    `json:"reorder_1s"`
	LossHistory60s        []float64 `json:"loss_history_60s"`
	LossHistory240s       []float64 `json:"loss_history_240s"`
}

type AgentSnapshot struct {
	AgentID              string    `json:"agent_id"`
	Status               string    `json:"status"`
	UDPAddr              string    `json:"udp_addr"`
	Enabled              bool      `json:"enabled"`
	ActiveConfigVersion  uint64    `json:"active_config_version"`
	DesiredConfigVersion uint64    `json:"desired_config_version"`
	ActiveFlows          uint32    `json:"active_flows"`
	LastHeartbeat        time.Time `json:"last_heartbeat"`
	LastResult           time.Time `json:"last_result"`
}

type StatusSnapshot struct {
	MeasurementSessionID string `json:"measurement_session_id"`
}

func NewFlowSnapshot(f FlowRuntimeStatus) FlowSnapshot {
	return FlowSnapshot{
		FlowID:                f.FlowID,
		Src:                   f.Src,
		Dst:                   f.Dst,
		DesiredState:          f.DesiredState,
		ActualState:           f.ActualState,
		IntervalMs:            f.IntervalMs,
		ReportWindowMs:        f.ReportWindowMs,
		PacketSize:            f.PacketSize,
		LastSeen:              f.LastReportedAt,
		LastError:             f.LastError,
		Tx1s:                  f.Tx1s,
		Rx1s:                  f.Rx1s,
		TxTotal:               f.TxTotal,
		RxTotal:               f.RxTotal,
		Lost1s:                f.Lost1s,
		LostTotal:             f.LostTotal,
		LossTime1sMs:          f.LossTime1sMs,
		LossTimeTotalMs:       f.LossTimeTotalMs,
		DuplicateTotal:        f.DuplicateTotal,
		ReorderTotal:          f.ReorderTotal,
		IsolatedLossEvents:    f.IsolatedLossEvents,
		OutageCount:           f.OutageCount,
		OutageActive:          f.OutageActive,
		CurrentOutageMs:       f.CurrentOutageMs,
		LastOutageMs:          f.LastOutageMs,
		MaxOutageMs:           f.MaxOutageMs,
		OutageTotalMs:         f.OutageTotalMs,
		OutageThresholdMs:     f.OutageThresholdMs,
		UnmeasurableActive:    f.UnmeasurableActive,
		CurrentUnmeasurableMs: f.CurrentUnmeasurableMs,
		UnmeasurableCount:     f.UnmeasurableCount,
		UnmeasurableTotalMs:   f.UnmeasurableTotalMs,
		MaxUnmeasurableMs:     f.MaxUnmeasurableMs,
		LossRatio1s:           f.LossRatio1s,
		LossRatio10s:          f.LossRatio10s,
		LossRatio60s:          f.LossRatio60s,
		Duplicate1s:           f.Duplicate1s,
		Reorder1s:             f.Reorder1s,
		LossHistory60s:        fixedLossHistory(f.LossHistory60s, flowSnapshotHistory60sLength),
		LossHistory240s:       fixedLossHistory(f.LossHistory240s, flowSnapshotHistory240sLength),
	}
}

func NewAgentSnapshot(a AgentRuntimeStatus) AgentSnapshot {
	return AgentSnapshot{
		AgentID:              a.AgentID,
		Status:               a.Status,
		UDPAddr:              a.UDPAddr,
		Enabled:              a.Enabled,
		ActiveConfigVersion:  a.ActiveConfigVersion,
		DesiredConfigVersion: a.DesiredConfigVersion,
		ActiveFlows:          a.ActiveFlows,
		LastHeartbeat:        a.LastHeartbeat,
		LastResult:           a.LastResult,
	}
}

func fixedLossHistory(history []float64, size int) []float64 {
	if size <= 0 {
		return nil
	}
	if len(history) >= size {
		return append([]float64(nil), history[len(history)-size:]...)
	}
	out := make([]float64, size)
	copy(out[size-len(history):], history)
	return out
}
