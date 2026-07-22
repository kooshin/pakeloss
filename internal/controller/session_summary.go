package controller

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"pakeloss/internal/model"
)

type sessionSummaryRecord struct {
	SessionID          string                           `json:"session_id"`
	StartedAt          string                           `json:"started_at"`
	EndedAt            string                           `json:"ended_at"`
	DurationSec        string                           `json:"duration_sec"`
	FlowCount          int                              `json:"flow_count"`
	MaxOutageSec       string                           `json:"max_outage_sec"`
	PeakFlowID         string                           `json:"peak_flow_id"`
	ResultCSVPath      string                           `json:"result_csv_path,omitempty"`
	ResultJSONL        string                           `json:"result_jsonl_path,omitempty"`
	Flows              []sessionFlowSummaryRecord       `json:"flows"`
	UnmeasurableEvents []sessionUnmeasurableEventRecord `json:"unmeasurable_events,omitempty"`
}

type sessionFlowSummaryRecord struct {
	FlowID               string  `json:"flow_id"`
	Src                  string  `json:"src"`
	Dst                  string  `json:"dst"`
	Tx                   uint64  `json:"tx"`
	Rx                   uint64  `json:"rx"`
	Lost                 uint64  `json:"lost"`
	LossRatio            float64 `json:"loss_ratio"`
	LossTimeTotalSec     string  `json:"loss_time_total_sec"`
	IsolatedLossEvents   uint64  `json:"isolated_loss_events"`
	OutageCount          uint64  `json:"outage_count"`
	LastOutageSec        string  `json:"last_outage_sec"`
	MaxOutageSec         string  `json:"max_outage_sec"`
	OutageTotalSec       string  `json:"outage_total_sec"`
	UnmeasurableCount    uint64  `json:"unmeasurable_count"`
	UnmeasurableTotalSec string  `json:"unmeasurable_total_sec"`
	MaxUnmeasurableSec   string  `json:"max_unmeasurable_sec"`
}

type sessionUnmeasurableEventRecord struct {
	FlowID      string `json:"flow_id"`
	Src         string `json:"src"`
	Dst         string `json:"dst"`
	StartedAt   string `json:"started_at"`
	EndedAt     string `json:"ended_at"`
	DurationSec string `json:"duration_sec"`
}

func buildSessionSummaryRecord(sessionID string, startedAt, endedAt time.Time, resultCSVPath, resultJSONLPath string, flows []model.FlowRuntimeStatus, optional ...[]unmeasurableEvent) sessionSummaryRecord {
	var unmeasurable []unmeasurableEvent
	if len(optional) > 0 {
		unmeasurable = optional[0]
	}
	record := sessionSummaryRecord{
		SessionID:          sessionID,
		StartedAt:          startedAt.UTC().Format(time.RFC3339Nano),
		EndedAt:            endedAt.UTC().Format(time.RFC3339Nano),
		DurationSec:        formatSeconds(endedAt.Sub(startedAt).Seconds()),
		FlowCount:          len(flows),
		ResultCSVPath:      resultCSVPath,
		ResultJSONL:        resultJSONLPath,
		Flows:              make([]sessionFlowSummaryRecord, 0, len(flows)),
		UnmeasurableEvents: make([]sessionUnmeasurableEventRecord, 0, len(unmeasurable)),
	}
	for _, flow := range flows {
		lossRatio := 0.0
		if flow.TxTotal > 0 {
			lossRatio = float64(flow.LostTotal) / float64(flow.TxTotal)
		}
		maxOutageSec := float64(flow.MaxOutageMs) / 1000.0
		if maxOutageSec > parseFormattedSeconds(record.MaxOutageSec) {
			record.MaxOutageSec = formatSeconds(maxOutageSec)
			record.PeakFlowID = flow.FlowID
		}
		record.Flows = append(record.Flows, sessionFlowSummaryRecord{
			FlowID:               flow.FlowID,
			Src:                  flow.Src,
			Dst:                  flow.Dst,
			Tx:                   flow.TxTotal,
			Rx:                   flow.RxTotal,
			Lost:                 flow.LostTotal,
			LossRatio:            lossRatio,
			LossTimeTotalSec:     formatSeconds(float64(flow.LossTimeTotalMs) / 1000.0),
			IsolatedLossEvents:   flow.IsolatedLossEvents,
			OutageCount:          flow.OutageCount,
			LastOutageSec:        formatSeconds(float64(flow.LastOutageMs) / 1000.0),
			MaxOutageSec:         formatSeconds(maxOutageSec),
			OutageTotalSec:       formatSeconds(float64(flow.OutageTotalMs) / 1000.0),
			UnmeasurableCount:    flow.UnmeasurableCount,
			UnmeasurableTotalSec: formatSeconds(float64(flow.UnmeasurableTotalMs) / 1000.0),
			MaxUnmeasurableSec:   formatSeconds(float64(maxUint64(flow.MaxUnmeasurableMs, flow.CurrentUnmeasurableMs)) / 1000.0),
		})
	}
	for _, event := range unmeasurable {
		record.UnmeasurableEvents = append(record.UnmeasurableEvents, sessionUnmeasurableEventRecord{FlowID: event.FlowID, Src: event.Src, Dst: event.Dst, StartedAt: event.StartedAt.UTC().Format(time.RFC3339Nano), EndedAt: event.EndedAt.UTC().Format(time.RFC3339Nano), DurationSec: formatSeconds(float64(event.DurationMs) / 1000.0)})
	}
	sort.Slice(record.Flows, func(i, j int) bool {
		return record.Flows[i].FlowID < record.Flows[j].FlowID
	})
	peak := selectPeakFlow(record.Flows)
	if peak != nil {
		record.PeakFlowID = peak.FlowID
		record.MaxOutageSec = peak.MaxOutageSec
	}
	return record
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func selectPeakFlow(flows []sessionFlowSummaryRecord) *sessionFlowSummaryRecord {
	if len(flows) == 0 {
		return nil
	}
	peak := &flows[0]
	for i := 1; i < len(flows); i++ {
		cur := &flows[i]
		switch comparePeakFlow(*cur, *peak) {
		case 1:
			peak = cur
		}
	}
	return peak
}

func comparePeakFlow(a, b sessionFlowSummaryRecord) int {
	aOutage := parseFormattedSeconds(a.MaxOutageSec)
	bOutage := parseFormattedSeconds(b.MaxOutageSec)
	switch {
	case aOutage > bOutage:
		return 1
	case aOutage < bOutage:
		return -1
	case a.LossRatio > b.LossRatio:
		return 1
	case a.LossRatio < b.LossRatio:
		return -1
	case a.FlowID < b.FlowID:
		return 1
	case a.FlowID > b.FlowID:
		return -1
	default:
		return 0
	}
}

func writeSessionSummaryJSONL(path string, record sessionSummaryRecord) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if err := enc.Encode(sessionSummaryJSONRow("session_summary", record, sessionFlowSummaryRecord{})); err != nil {
		return err
	}
	for _, flow := range record.Flows {
		if err := enc.Encode(sessionSummaryJSONRow("flow", record, flow)); err != nil {
			return err
		}
	}
	if peak := selectPeakFlow(record.Flows); peak != nil {
		if err := enc.Encode(sessionSummaryJSONRow("session_peak_flow", record, *peak)); err != nil {
			return err
		}
	}
	for _, event := range record.UnmeasurableEvents {
		row := map[string]any{"row_type": "unmeasurable_event", "session_id": record.SessionID, "flow_id": event.FlowID, "src": event.Src, "dst": event.Dst, "started_at": event.StartedAt, "ended_at": event.EndedAt, "duration_sec": event.DurationSec}
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	return f.Sync()
}

func writeSessionSummaryCSV(path string, record sessionSummaryRecord, location *time.Location) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	newFile := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		newFile = true
	} else if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if newFile {
		if err := w.Write(sessionSummaryCSVHeader()); err != nil {
			return err
		}
	}
	for _, flow := range record.Flows {
		if err := w.Write(sessionSummaryCSVRow("flow", record, flow, location)); err != nil {
			return err
		}
	}
	if peak := selectPeakFlow(record.Flows); peak != nil {
		if err := w.Write(sessionSummaryCSVRow("session_peak_flow", record, *peak, location)); err != nil {
			return err
		}
	}
	for _, event := range record.UnmeasurableEvents {
		if err := w.Write([]string{"unmeasurable_event", record.SessionID, timestampForCSV(event.StartedAt, location), timestampForCSV(event.EndedAt, location), event.DurationSec, strconv.Itoa(record.FlowCount), event.FlowID, event.Src, event.Dst, "", "", "", "", "", "", "", "", "", ""}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return f.Sync()
}

func sessionSummaryCSVHeader() []string {
	return []string{
		"row_type", "session_id", "started_at", "ended_at", "duration_sec", "flow_count",
		"flow_id", "src", "dst", "tx", "rx", "lost", "loss_ratio", "loss_time_total_sec", "isolated_loss_events", "outage_count", "last_outage_sec", "max_outage_sec", "outage_total_sec",
	}
}

func sessionSummaryCSVRow(rowType string, session sessionSummaryRecord, flow sessionFlowSummaryRecord, location *time.Location) []string {
	return []string{
		rowType,
		session.SessionID,
		timestampForCSV(session.StartedAt, location),
		timestampForCSV(session.EndedAt, location),
		session.DurationSec,
		strconv.Itoa(session.FlowCount),
		flow.FlowID,
		flow.Src,
		flow.Dst,
		strconv.FormatUint(flow.Tx, 10),
		strconv.FormatUint(flow.Rx, 10),
		strconv.FormatUint(flow.Lost, 10),
		strconv.FormatFloat(flow.LossRatio, 'f', -1, 64),
		flow.LossTimeTotalSec,
		strconv.FormatUint(flow.IsolatedLossEvents, 10),
		strconv.FormatUint(flow.OutageCount, 10),
		flow.LastOutageSec,
		flow.MaxOutageSec,
		flow.OutageTotalSec,
	}
}

func sessionSummaryJSONRow(rowType string, session sessionSummaryRecord, flow sessionFlowSummaryRecord) map[string]any {
	row := map[string]any{
		"row_type":       rowType,
		"session_id":     session.SessionID,
		"started_at":     session.StartedAt,
		"ended_at":       session.EndedAt,
		"duration_sec":   session.DurationSec,
		"flow_count":     session.FlowCount,
		"peak_flow_id":   session.PeakFlowID,
		"max_outage_sec": session.MaxOutageSec,
	}
	switch rowType {
	case "flow", "session_peak_flow":
		row["flow_id"] = flow.FlowID
		row["src"] = flow.Src
		row["dst"] = flow.Dst
		row["tx"] = flow.Tx
		row["rx"] = flow.Rx
		row["lost"] = flow.Lost
		row["loss_ratio"] = flow.LossRatio
		row["loss_time_total_sec"] = flow.LossTimeTotalSec
		row["isolated_loss_events"] = flow.IsolatedLossEvents
		row["outage_count"] = flow.OutageCount
		row["last_outage_sec"] = flow.LastOutageSec
		row["max_outage_sec"] = flow.MaxOutageSec
		row["outage_total_sec"] = flow.OutageTotalSec
		row["unmeasurable_count"] = flow.UnmeasurableCount
		row["unmeasurable_total_sec"] = flow.UnmeasurableTotalSec
		row["max_unmeasurable_sec"] = flow.MaxUnmeasurableSec
	}
	return row
}

func formatSeconds(v float64) string {
	if v < 0 {
		v = 0
	}
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func parseFormattedSeconds(v string) float64 {
	if v == "" {
		return 0
	}
	out, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return out
}
