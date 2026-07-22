package controller

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"pakeloss/internal/model"
	"pakeloss/internal/pb"
)

type ResultStore struct {
	mu              sync.Mutex
	csvPath         string
	jsonlPath       string
	debugJSONLPath  string
	outageCSVPath   string
	outageJSONLPath string
	summaryCSV      string
	summaryJSONL    string
	csvFile         *os.File
	jsonlFile       *os.File
	debugJSONLFile  *os.File
	outageCSVFile   *os.File
	outageJSONLFile *os.File
	csvWriter       *csv.Writer
	outageCSVWriter *csv.Writer
	outageRows      map[string]outageEventLogRow
	outageOrder     []string
	outageAliases   map[string]string
	outageSeen      map[string]struct{}
	outageDurations map[string]uint64
	outageLastFlow  map[string]string
	active          bool
	debugDirty      bool
	location        *time.Location
	now             func() time.Time
	flushInterval   time.Duration
	finalizeDelay   time.Duration
	pending         map[string]*resultAggregate
	sessionCSV      string
	sessionJS       string
	sessionID       string
	sessionStart    time.Time
}

func NewResultStore(csvPath, jsonlPath, summaryCSV, summaryJSONL, timezone, flushInterval string) (*ResultStore, error) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, err
	}
	if flushInterval == "" {
		flushInterval = "10s"
	}
	interval, err := time.ParseDuration(flushInterval)
	if err != nil {
		return nil, fmt.Errorf("parse result flush interval: %w", err)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("result flush interval must be positive: %s", flushInterval)
	}

	s := &ResultStore{
		csvPath:       csvPath,
		jsonlPath:     jsonlPath,
		summaryCSV:    summaryCSV,
		summaryJSONL:  summaryJSONL,
		location:      loc,
		now:           time.Now,
		flushInterval: interval,
		finalizeDelay: 2 * time.Second,
		pending:       map[string]*resultAggregate{},
	}
	return s, nil
}

func (s *ResultStore) SetDebugJSONLPath(path string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.debugJSONLPath = path
}

func (s *ResultStore) SetOutageEventPaths(csvPath, jsonlPath string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outageCSVPath = csvPath
	s.outageJSONLPath = jsonlPath
}

func (s *ResultStore) SetReportFinalizeDelay(delay time.Duration) {
	if s == nil {
		return
	}
	if delay <= 0 {
		delay = 2 * time.Second
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalizeDelay = delay
}

func (s *ResultStore) Run(ctx context.Context) {
	if s == nil {
		return
	}
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.flushTick(); err != nil {
				log.Printf("result log flush failed: %v", err)
			}
		}
	}
}

func (s *ResultStore) Close() error {
	if s == nil {
		return nil
	}
	return s.StopSession()
}

func (s *ResultStore) StartSession(sessionID string) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active || !s.hasSinksLocked() {
		return nil
	}
	start := s.now()
	if err := s.openSessionLocked(start, sessionID); err != nil {
		return err
	}
	s.active = true
	s.sessionID = sessionID
	s.sessionStart = start
	s.pending = map[string]*resultAggregate{}
	return nil
}

func (s *ResultStore) StopSession() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		s.pending = map[string]*resultAggregate{}
		return s.closeLocked()
	}
	cutoff := s.now()
	if err := s.flushWindowLocked(cutoff, true); err != nil {
		return err
	}
	if err := s.syncDebugLocked(); err != nil {
		return err
	}
	s.active = false
	s.pending = map[string]*resultAggregate{}
	return s.closeLocked()
}

func (s *ResultStore) StopSessionWithSummary(flows []model.FlowRuntimeStatus, unmeasurable ...[]unmeasurableEvent) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		s.pending = map[string]*resultAggregate{}
		return s.closeLocked()
	}
	cutoff := s.now()
	var unavailable []unmeasurableEvent
	if len(unmeasurable) > 0 {
		unavailable = unmeasurable[0]
	}
	record := buildSessionSummaryRecord(s.sessionID, s.sessionStart, cutoff, s.sessionCSV, s.sessionJS, flows, unavailable)
	if err := s.flushWindowLocked(cutoff, true); err != nil {
		return err
	}
	if err := s.syncDebugLocked(); err != nil {
		return err
	}
	s.active = false
	s.pending = map[string]*resultAggregate{}
	if err := s.closeLocked(); err != nil {
		return err
	}
	if err := writeSessionSummaryCSV(s.summaryCSV, record, s.location); err != nil {
		return err
	}
	return writeSessionSummaryJSONL(s.summaryJSONL, record)
}

func (s *ResultStore) Rotate() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active || !s.hasSinksLocked() {
		return nil
	}

	cutoff := s.now()
	if err := s.flushWindowLocked(cutoff, true); err != nil {
		return err
	}
	if err := s.syncDebugLocked(); err != nil {
		return err
	}
	if err := s.openSessionLocked(cutoff, s.sessionID); err != nil {
		return err
	}
	s.active = true
	s.pending = map[string]*resultAggregate{}
	return nil
}

func (s *ResultStore) Flush() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flushWindowLocked(s.now(), true); err != nil {
		return err
	}
	return s.syncDebugLocked()
}

func (s *ResultStore) Write(res *pb.ResultSummary) error {
	if s == nil || res == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return nil
	}

	windowStart, windowEnd := s.resultWindowForSummaryLocked(res)
	aggKey := resultAggregateKey(res.FlowId, windowStart)
	agg := s.pending[aggKey]
	if agg == nil {
		agg = &resultAggregate{
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			Src:         res.Src,
			Dst:         res.Dst,
			FlowID:      res.FlowId,
			IntervalMs:  res.IntervalMs,
		}
		s.pending[aggKey] = agg
	}
	agg.WindowStart = windowStart
	agg.WindowEnd = windowEnd
	agg.Src = res.Src
	agg.Dst = res.Dst
	agg.FlowID = res.FlowId
	agg.IntervalMs = res.IntervalMs
	agg.Tx += res.Tx
	agg.Rx += res.Rx
	agg.Lost += res.Lost
	agg.Duplicate += res.Duplicate
	agg.Reorder += res.Reorder
	agg.OutageMs += res.OutageMs
	return nil
}

func (s *ResultStore) WriteDebug(record SeqDebugRecord) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active || s.debugJSONLFile == nil {
		return nil
	}
	if err := json.NewEncoder(s.debugJSONLFile).Encode(record); err != nil {
		return err
	}
	s.debugDirty = true
	return nil
}

func (s *ResultStore) WriteOutageEvent(record OutageEventLogRecord) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return nil
	}
	seenKey := record.EventID + "|" + record.State
	if _, ok := s.outageSeen[seenKey]; ok {
		return nil
	}
	s.outageSeen[seenKey] = struct{}{}
	row := outageEventLogRow{
		SessionID:         s.sessionID,
		EventID:           record.EventID,
		State:             record.State,
		Ts:                record.Ts.UTC().Format(time.RFC3339Nano),
		FlowID:            record.FlowID,
		Src:               record.Src,
		Dst:               record.Dst,
		StartedAt:         record.StartedAt.UTC().Format(time.RFC3339Nano),
		OutageThresholdMs: record.OutageThresholdMs,
	}
	if record.State == "ended" {
		row.EndedAt = record.EndedAt.UTC().Format(time.RFC3339Nano)
		row.DurationMs = record.DurationMs
		row.DurationSec = float64(record.DurationMs) / 1000
		row.EndReason = record.EndReason
	}
	if s.outageRows == nil {
		s.outageRows = map[string]outageEventLogRow{}
	}
	canonicalID := row.EventID
	if row.State == "started" {
		if previousID := s.outageLastFlow[row.FlowID]; previousID != "" {
			previous := s.outageRows[previousID]
			if outageEventsShouldMerge(previous, row) {
				canonicalID = previousID
				s.outageAliases[row.EventID] = canonicalID
				row.EventID = canonicalID
				row.StartedAt = previous.StartedAt
			}
		}
	} else if alias := s.outageAliases[row.EventID]; alias != "" {
		canonicalID = alias
		row.EventID = canonicalID
	}
	if existing, ok := s.outageRows[canonicalID]; ok {
		row.StartedAt = existing.StartedAt
	} else {
		s.outageOrder = append(s.outageOrder, canonicalID)
	}
	if row.State == "ended" {
		row.DurationMs += s.outageDurations[canonicalID]
		row.DurationSec = float64(row.DurationMs) / 1000
		s.outageDurations[canonicalID] = row.DurationMs
	}
	s.outageRows[canonicalID] = row
	s.outageLastFlow[row.FlowID] = canonicalID
	return s.rewriteOutageEventsLocked()
}

func outageEventsShouldMerge(previous, current outageEventLogRow) bool {
	if previous.State != "ended" || previous.EndReason != "recovered" || current.State != "started" || previous.FlowID != current.FlowID {
		return false
	}
	endedAt, err := time.Parse(time.RFC3339Nano, previous.EndedAt)
	if err != nil {
		return false
	}
	startedAt, err := time.Parse(time.RFC3339Nano, current.StartedAt)
	if err != nil || startedAt.Before(endedAt) {
		return false
	}
	thresholdMs := previous.OutageThresholdMs
	if current.OutageThresholdMs > thresholdMs {
		thresholdMs = current.OutageThresholdMs
	}
	return !startedAt.After(endedAt.Add(time.Duration(thresholdMs) * time.Millisecond))
}

func (s *ResultStore) rewriteOutageEventsLocked() error {
	if s.outageCSVFile != nil {
		if err := s.outageCSVFile.Truncate(0); err != nil {
			return err
		}
		if _, err := s.outageCSVFile.Seek(0, 0); err != nil {
			return err
		}
		writer := csv.NewWriter(s.outageCSVFile)
		if err := writer.Write(outageEventCSVHeader()); err != nil {
			return err
		}
		for _, eventID := range s.outageOrder {
			if err := writer.Write(outageEventCSVRow(s.outageRows[eventID], s.location)); err != nil {
				return err
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return err
		}
		s.outageCSVWriter = writer
		if err := s.outageCSVFile.Sync(); err != nil {
			return err
		}
	}
	if s.outageJSONLFile != nil {
		if err := s.outageJSONLFile.Truncate(0); err != nil {
			return err
		}
		if _, err := s.outageJSONLFile.Seek(0, 0); err != nil {
			return err
		}
		encoder := json.NewEncoder(s.outageJSONLFile)
		for _, eventID := range s.outageOrder {
			if err := encoder.Encode(s.outageRows[eventID]); err != nil {
				return err
			}
		}
		if err := s.outageJSONLFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

type outageEventLogRow struct {
	SessionID         string  `json:"session_id"`
	EventID           string  `json:"event_id"`
	State             string  `json:"state"`
	Ts                string  `json:"ts"`
	FlowID            string  `json:"flow_id"`
	Src               string  `json:"src"`
	Dst               string  `json:"dst"`
	StartedAt         string  `json:"started_at"`
	EndedAt           string  `json:"ended_at,omitempty"`
	DurationMs        uint64  `json:"duration_ms,omitempty"`
	DurationSec       float64 `json:"duration_sec,omitempty"`
	OutageThresholdMs uint32  `json:"outage_threshold_ms"`
	EndReason         string  `json:"end_reason,omitempty"`
}

func outageEventCSVHeader() []string {
	return []string{"session_id", "event_id", "state", "ts", "flow_id", "src", "dst", "started_at", "ended_at", "duration_ms", "duration_sec", "outage_threshold_ms", "end_reason"}
}

func outageEventCSVRow(row outageEventLogRow, location *time.Location) []string {
	durationMs, durationSec := "", ""
	if row.State == "ended" {
		durationMs = strconv.FormatUint(row.DurationMs, 10)
		durationSec = strconv.FormatFloat(row.DurationSec, 'f', -1, 64)
	}
	return []string{row.SessionID, row.EventID, row.State, timestampForCSV(row.Ts, location), row.FlowID, row.Src, row.Dst, timestampForCSV(row.StartedAt, location), timestampForCSV(row.EndedAt, location), durationMs, durationSec, strconv.FormatUint(uint64(row.OutageThresholdMs), 10), row.EndReason}
}

func (s *ResultStore) flushTick() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return nil
	}
	if err := s.flushWindowLocked(s.now(), false); err != nil {
		return err
	}
	return s.syncDebugLocked()
}

func (s *ResultStore) flushWindowLocked(windowEnd time.Time, flushPartial bool) error {
	if len(s.pending) == 0 {
		return nil
	}
	type pendingRecord struct {
		key    string
		record resultLogRecord
	}
	records := make([]pendingRecord, 0, len(s.pending))
	keys := make([]string, 0, len(s.pending))
	for key := range s.pending {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left := s.pending[keys[i]]
		right := s.pending[keys[j]]
		if left == nil || right == nil {
			return keys[i] < keys[j]
		}
		if !left.WindowEnd.Equal(right.WindowEnd) {
			return left.WindowEnd.Before(right.WindowEnd)
		}
		if left.FlowID != right.FlowID {
			return left.FlowID < right.FlowID
		}
		return keys[i] < keys[j]
	})
	for _, key := range keys {
		agg := s.pending[key]
		if agg == nil {
			continue
		}
		recordWindowEnd := agg.WindowEnd
		if flushPartial {
			if !agg.WindowStart.Before(windowEnd) {
				continue
			}
			if recordWindowEnd.After(windowEnd) {
				recordWindowEnd = windowEnd
			}
		} else if agg.WindowEnd.Add(s.finalizeDelay).After(windowEnd) {
			continue
		}
		lost := agg.Lost
		lossRatio := 0.0
		if agg.Tx > 0 {
			lossRatio = float64(lost) / float64(agg.Tx)
		}
		lossTimeMs := lost * uint64(agg.IntervalMs)
		records = append(records, pendingRecord{
			key: key,
			record: resultLogRecord{
				SessionID:   s.sessionID,
				Ts:          recordWindowEnd.UTC().Format(time.RFC3339Nano),
				WindowStart: agg.WindowStart.UTC().Format(time.RFC3339Nano),
				WindowEnd:   recordWindowEnd.UTC().Format(time.RFC3339Nano),
				Src:         agg.Src,
				Dst:         agg.Dst,
				FlowID:      agg.FlowID,
				IntervalMs:  agg.IntervalMs,
				Tx:          agg.Tx,
				Rx:          agg.Rx,
				Lost:        lost,
				LossRatio:   lossRatio,
				LossTimeMs:  lossTimeMs,
				LossTimeSec: float64(lossTimeMs) / 1000.0,
				Duplicate:   agg.Duplicate,
				Reorder:     agg.Reorder,
				OutageMs:    agg.OutageMs,
				OutageSec:   float64(agg.OutageMs) / 1000.0,
			},
		})
	}
	for _, item := range records {
		if err := s.writeRecordLocked(item.record); err != nil {
			return err
		}
		delete(s.pending, item.key)
	}
	return nil
}

func (s *ResultStore) writeRecordLocked(record resultLogRecord) error {
	if s.csvWriter != nil {
		if err := s.csvWriter.Write([]string{
			record.SessionID,
			timestampForCSV(record.Ts, s.location),
			timestampForCSV(record.WindowStart, s.location),
			timestampForCSV(record.WindowEnd, s.location),
			record.Src,
			record.Dst,
			record.FlowID,
			strconv.FormatUint(uint64(record.IntervalMs), 10),
			strconv.FormatUint(record.Tx, 10),
			strconv.FormatUint(record.Rx, 10),
			strconv.FormatUint(record.Lost, 10),
			strconv.FormatFloat(record.LossRatio, 'f', -1, 64),
			strconv.FormatUint(record.LossTimeMs, 10),
			strconv.FormatFloat(record.LossTimeSec, 'f', -1, 64),
			strconv.FormatUint(record.Duplicate, 10),
			strconv.FormatUint(record.Reorder, 10),
			strconv.FormatUint(record.OutageMs, 10),
			strconv.FormatFloat(record.OutageSec, 'f', -1, 64),
		}); err != nil {
			return err
		}
		s.csvWriter.Flush()
		if err := s.csvWriter.Error(); err != nil {
			return err
		}
	}
	if s.jsonlFile != nil {
		if err := json.NewEncoder(s.jsonlFile).Encode(record); err != nil {
			return err
		}
		return s.jsonlFile.Sync()
	}
	return nil
}

func (s *ResultStore) openSessionLocked(now time.Time, sessionID string) error {
	var (
		csvFile         *os.File
		jsonlFile       *os.File
		debugFile       *os.File
		outageCSVFile   *os.File
		outageJSONLFile *os.File
		csvWriter       *csv.Writer
		outageCSVWriter *csv.Writer
		csvSession      string
		jsSession       string
		debugSession    string
		err             error
	)
	if s.csvPath != "" {
		csvFile, csvSession, err = openTimestampedLogFile(s.csvPath, now, sessionID)
		if err != nil {
			return err
		}
		csvWriter = csv.NewWriter(csvFile)
		if err := csvWriter.Write(resultCSVHeader()); err != nil {
			_ = csvFile.Close()
			return err
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			_ = csvFile.Close()
			return err
		}
	}
	if s.jsonlPath != "" {
		jsonlFile, jsSession, err = openTimestampedLogFile(s.jsonlPath, now, sessionID)
		if err != nil {
			if csvFile != nil {
				_ = csvFile.Close()
			}
			return err
		}
	}
	if s.debugJSONLPath != "" {
		debugFile, debugSession, err = openTimestampedLogFile(s.debugJSONLPath, now, sessionID)
		if err != nil {
			if csvFile != nil {
				_ = csvFile.Close()
			}
			if jsonlFile != nil {
				_ = jsonlFile.Close()
			}
			return err
		}
		_ = debugSession
	}
	if s.outageCSVPath != "" {
		outageCSVFile, _, err = openTimestampedLogFile(s.outageCSVPath, now, sessionID)
		if err != nil {
			closeFiles(csvFile, jsonlFile, debugFile)
			return err
		}
		outageCSVWriter = csv.NewWriter(outageCSVFile)
		if err := outageCSVWriter.Write(outageEventCSVHeader()); err != nil {
			closeFiles(csvFile, jsonlFile, debugFile, outageCSVFile)
			return err
		}
		outageCSVWriter.Flush()
		if err := outageCSVWriter.Error(); err != nil {
			closeFiles(csvFile, jsonlFile, debugFile, outageCSVFile)
			return err
		}
	}
	if s.outageJSONLPath != "" {
		outageJSONLFile, _, err = openTimestampedLogFile(s.outageJSONLPath, now, sessionID)
		if err != nil {
			closeFiles(csvFile, jsonlFile, debugFile, outageCSVFile)
			return err
		}
	}

	closeErr := s.closeLocked()
	s.csvFile = csvFile
	s.jsonlFile = jsonlFile
	s.debugJSONLFile = debugFile
	s.outageCSVFile = outageCSVFile
	s.outageJSONLFile = outageJSONLFile
	s.debugDirty = false
	s.csvWriter = csvWriter
	s.outageCSVWriter = outageCSVWriter
	s.outageRows = map[string]outageEventLogRow{}
	s.outageOrder = nil
	s.outageAliases = map[string]string{}
	s.outageSeen = map[string]struct{}{}
	s.outageDurations = map[string]uint64{}
	s.outageLastFlow = map[string]string{}
	s.sessionCSV = csvSession
	s.sessionJS = jsSession
	s.sessionID = sessionID
	return closeErr
}

func (s *ResultStore) closeLocked() error {
	var err error
	if s.csvWriter != nil {
		s.csvWriter.Flush()
		err = s.csvWriter.Error()
	}
	if s.csvFile != nil {
		if closeErr := s.csvFile.Close(); err == nil {
			err = closeErr
		}
	}
	if s.jsonlFile != nil {
		if closeErr := s.jsonlFile.Close(); err == nil {
			err = closeErr
		}
	}
	if s.debugJSONLFile != nil {
		if syncErr := s.syncDebugLocked(); err == nil {
			err = syncErr
		}
		if closeErr := s.debugJSONLFile.Close(); err == nil {
			err = closeErr
		}
	}
	if s.outageCSVWriter != nil {
		s.outageCSVWriter.Flush()
		if writerErr := s.outageCSVWriter.Error(); err == nil {
			err = writerErr
		}
	}
	if s.outageCSVFile != nil {
		if closeErr := s.outageCSVFile.Close(); err == nil {
			err = closeErr
		}
	}
	if s.outageJSONLFile != nil {
		if closeErr := s.outageJSONLFile.Close(); err == nil {
			err = closeErr
		}
	}
	s.csvFile = nil
	s.jsonlFile = nil
	s.debugJSONLFile = nil
	s.outageCSVFile = nil
	s.outageJSONLFile = nil
	s.debugDirty = false
	s.csvWriter = nil
	s.outageCSVWriter = nil
	s.outageRows = nil
	s.outageOrder = nil
	s.outageAliases = nil
	s.outageSeen = nil
	s.outageDurations = nil
	s.outageLastFlow = nil
	s.sessionCSV = ""
	s.sessionJS = ""
	s.sessionID = ""
	s.sessionStart = time.Time{}
	return err
}

func (s *ResultStore) syncDebugLocked() error {
	if s == nil || s.debugJSONLFile == nil || !s.debugDirty {
		return nil
	}
	if err := s.debugJSONLFile.Sync(); err != nil {
		return err
	}
	s.debugDirty = false
	return nil
}

func (s *ResultStore) resultWindowForSummaryLocked(res *pb.ResultSummary) (time.Time, time.Time) {
	if res != nil && res.Ts != "" {
		if ts, err := time.Parse(time.RFC3339Nano, res.Ts); err == nil {
			windowStart := ts.UTC().Add(-time.Nanosecond).Truncate(time.Second)
			return windowStart, windowStart.Add(time.Second)
		}
	}
	windowStart := s.now().UTC().Add(-time.Nanosecond).Truncate(time.Second)
	return windowStart, windowStart.Add(time.Second)
}

func resultAggregateKey(flowID string, windowStart time.Time) string {
	return flowID + "|" + windowStart.UTC().Format(time.RFC3339Nano)
}

func (s *ResultStore) hasSinksLocked() bool {
	return s.csvPath != "" || s.jsonlPath != "" || s.debugJSONLPath != "" || s.outageCSVPath != "" || s.outageJSONLPath != "" || s.summaryCSV != "" || s.summaryJSONL != ""
}

func closeFiles(files ...*os.File) {
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
}

func openTimestampedLogFile(basePath string, now time.Time, sessionID string) (*os.File, string, error) {
	if err := os.MkdirAll(filepath.Dir(basePath), 0o755); err != nil {
		return nil, "", err
	}
	for i := 0; ; i++ {
		path := timestampedLogPath(basePath, now, sessionID, i)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			return f, path, nil
		}
		if !os.IsExist(err) {
			return nil, "", err
		}
	}
}

func timestampedLogPath(basePath string, now time.Time, sessionID string, suffix int) string {
	ext := filepath.Ext(basePath)
	dir := filepath.Dir(basePath)
	base := strings.TrimSuffix(filepath.Base(basePath), ext)
	stamp := now.Format("20060102-150405")
	if suffix > 0 {
		stamp += "-" + strconv.Itoa(suffix+1)
	}
	return filepath.Join(dir, fmt.Sprintf("%s_%s_%s%s", base, stamp, sessionID, ext))
}

func resultCSVHeader() []string {
	return []string{"session_id", "ts", "window_start", "window_end", "src", "dst", "flow_id", "interval_ms", "tx", "rx", "lost", "loss_ratio", "loss_time_ms", "loss_time_sec", "duplicate", "reorder", "outage_ms", "outage_sec"}
}

func timestampForCSV(value string, loc *time.Location) string {
	if value == "" {
		return ""
	}
	ts, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return value
	}
	return ts.In(loc).Format("2006-01-02 15:04:05.999999999")
}

type resultAggregate struct {
	WindowStart time.Time
	WindowEnd   time.Time
	Src         string
	Dst         string
	FlowID      string
	IntervalMs  uint32
	Tx          uint64
	Rx          uint64
	Lost        uint64
	Duplicate   uint64
	Reorder     uint64
	OutageMs    uint64
}

type resultLogRecord struct {
	SessionID   string  `json:"session_id"`
	Ts          string  `json:"ts"`
	WindowStart string  `json:"window_start"`
	WindowEnd   string  `json:"window_end"`
	Src         string  `json:"src"`
	Dst         string  `json:"dst"`
	FlowID      string  `json:"flow_id"`
	IntervalMs  uint32  `json:"interval_ms"`
	Tx          uint64  `json:"tx"`
	Rx          uint64  `json:"rx"`
	Lost        uint64  `json:"lost"`
	LossRatio   float64 `json:"loss_ratio"`
	LossTimeMs  uint64  `json:"loss_time_ms"`
	LossTimeSec float64 `json:"loss_time_sec"`
	Duplicate   uint64  `json:"duplicate"`
	Reorder     uint64  `json:"reorder"`
	OutageMs    uint64  `json:"outage_ms"`
	OutageSec   float64 `json:"outage_sec"`
}
