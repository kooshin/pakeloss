package controller

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"pakeloss/internal/model"
	"pakeloss/internal/pb"
	"pakeloss/internal/protocol"
)

var ErrFlowNotFound = errors.New("flow not found")

const defaultAgentStaleAfter = 3 * time.Second

type RuntimeStore struct {
	mu            sync.RWMutex
	flows         map[string]*flowRuntime
	flowKeys      map[uint32]*flowRuntime
	agents        map[string]*model.AgentRuntimeStatus
	finalizeDelay time.Duration
	debugRecords  []SeqDebugRecord
	outageRecords []OutageEventLogRecord
}

type flowRuntime struct {
	flowKey               uint32
	status                *model.FlowRuntimeStatus
	samples               []sample
	reports               map[time.Time]*reportBucket
	seqs                  map[string]*seqLedgerEntry
	anchors               map[uint32]reportTimeAnchor
	warmupUntil           time.Time
	unmeasurableStarted   time.Time
	unmeasurableAccounted time.Time
	unmeasurableEvents    []unmeasurableEvent
	lossRunStart          time.Time
	lossRunMs             uint64
	lossRunQualified      bool
	activeOutageID        string
}

type sample struct {
	windowStart time.Time
	windowMs    uint32
	intervalMs  uint32
	tx          uint64
	rx          uint64
	lost        uint64
	duplicate   uint64
	reorder     uint64
	missing     bool
}

type reportBucket struct {
	windowStart time.Time
	windowMs    uint32
	intervalMs  uint32
	senderAgent string
	receiver    string
	tx          uint64
	rx          uint64
	lost        uint64
	duplicate   uint64
	reorder     uint64
	pendingTx   uint64
	outcomes    map[string]*packetOutcome
}

type packetOutcome struct {
	sessionID uint32
	seq       uint64
	received  bool
}

type reportRole string

const (
	reportRoleSender   reportRole = "sender"
	reportRoleReceiver reportRole = "receiver"
)

type reportTimeAnchor struct {
	reportWindowStart     time.Time
	controllerWindowStart time.Time
}

type seqLedgerEntry struct {
	sessionID        uint32
	seq              uint64
	senderSeen       bool
	receiverSeen     bool
	matchedCounted   bool
	senderBucket     time.Time
	receiverBucket   time.Time
	senderSeenAt     time.Time
	receiverSeenAt   time.Time
	senderReportTs   string
	receiverReportTs string
	senderAgent      string
	receiverAgent    string
}

type SeqDebugRecord struct {
	ControllerTs     string `json:"controller_ts"`
	FlowID           string `json:"flow_id"`
	SessionID        uint32 `json:"session_id"`
	Seq              uint64 `json:"seq"`
	SenderSeen       bool   `json:"sender_seen"`
	ReceiverSeen     bool   `json:"receiver_seen"`
	SenderReportTs   string `json:"sender_report_ts,omitempty"`
	ReceiverReportTs string `json:"receiver_report_ts,omitempty"`
	FinalState       string `json:"final_state"`
	FinalizedReason  string `json:"finalized_reason"`
}

type OutageEventLogRecord struct {
	EventID           string
	State             string
	Ts                time.Time
	FlowID            string
	Src               string
	Dst               string
	StartedAt         time.Time
	EndedAt           time.Time
	DurationMs        uint64
	OutageThresholdMs uint32
	EndReason         string
}

type unmeasurableEvent struct {
	FlowID     string
	Src        string
	Dst        string
	StartedAt  time.Time
	EndedAt    time.Time
	DurationMs uint64
}

func NewRuntimeStore(mesh model.MeshConfig) *RuntimeStore {
	threshold := mesh.OutageThresholdMs
	if threshold == 0 {
		threshold = 100
	}
	s := &RuntimeStore{
		flows:         map[string]*flowRuntime{},
		flowKeys:      map[uint32]*flowRuntime{},
		agents:        map[string]*model.AgentRuntimeStatus{},
		finalizeDelay: 2 * time.Second,
	}
	for _, f := range mesh.Flows {
		factor := mesh.ReportBucketFactor
		if factor == 0 {
			factor = 10
		}
		interval := effectiveReportIntervalMs(f.IntervalMs)
		key := protocol.ComputeFlowKey(f.Src, f.Dst, f.ID)
		r := &flowRuntime{flowKey: key, reports: map[time.Time]*reportBucket{}, status: &model.FlowRuntimeStatus{
			FlowID:            f.ID,
			Src:               f.Src,
			Dst:               f.Dst,
			ReceiverAgentID:   f.Dst,
			DesiredState:      f.State,
			ActualState:       "unknown",
			IntervalMs:        f.IntervalMs,
			ReportWindowMs:    interval * factor,
			PacketSize:        f.PacketSize,
			SourcePortCount:   f.SourcePortCount,
			OutageThresholdMs: threshold,
		}}
		s.flows[f.ID] = r
		s.flowKeys[key] = r
	}
	for _, n := range mesh.Nodes {
		enabled := true
		if mesh.DiscoveryMode == "auto" {
			enabled = n.Enabled
		}
		s.agents[n.ID] = &model.AgentRuntimeStatus{AgentID: n.ID, Status: "offline", UDPAddr: n.UDPAddr, Enabled: enabled, DesiredConfigVersion: mesh.ConfigVersion}
	}
	return s
}

func (s *RuntimeStore) SetReportFinalizeDelay(delay time.Duration) {
	if delay <= 0 {
		delay = 2 * time.Second
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalizeDelay = delay
}

func (s *RuntimeStore) SetDesiredConfigVersion(mesh model.MeshConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	activeAgents := make(map[string]struct{}, len(mesh.Nodes))
	for _, n := range mesh.Nodes {
		a := s.agents[n.ID]
		if a == nil {
			a = &model.AgentRuntimeStatus{AgentID: n.ID, UDPAddr: n.UDPAddr}
			s.agents[n.ID] = a
		}
		a.UDPAddr = n.UDPAddr
		if mesh.DiscoveryMode == "auto" {
			a.Enabled = n.Enabled
		} else {
			a.Enabled = true
		}
		a.DesiredConfigVersion = mesh.ConfigVersion
		activeAgents[n.ID] = struct{}{}
	}
	if mesh.DiscoveryMode == "auto" {
		for _, a := range s.agents {
			if _, ok := activeAgents[a.AgentID]; !ok {
				a.Enabled = false
			}
			a.DesiredConfigVersion = mesh.ConfigVersion
		}
	}
	activeFlows := make(map[string]struct{}, len(mesh.Flows))
	activeFlowKeys := make(map[uint32]struct{}, len(mesh.Flows))
	for _, f := range mesh.Flows {
		factor := mesh.ReportBucketFactor
		if factor == 0 {
			factor = 10
		}
		r := s.flows[f.ID]
		key := protocol.ComputeFlowKey(f.Src, f.Dst, f.ID)
		if r == nil {
			r = &flowRuntime{flowKey: key, status: &model.FlowRuntimeStatus{FlowID: f.ID}}
			s.flows[f.ID] = r
		}
		r.flowKey = key
		s.flowKeys[key] = r
		wasRunning := r.status.DesiredState == "running"
		if wasRunning && f.State != "running" {
			s.finishLossRunLocked(r, now, "measurement_stopped", now)
		}
		r.status.Src = f.Src
		r.status.Dst = f.Dst
		r.status.ReceiverAgentID = f.Dst
		r.status.DesiredState = f.State
		r.status.IntervalMs = f.IntervalMs
		r.status.ReportWindowMs = effectiveReportIntervalMs(f.IntervalMs) * factor
		r.status.PacketSize = f.PacketSize
		r.status.SourcePortCount = f.SourcePortCount
		r.status.OutageThresholdMs = mesh.OutageThresholdMs
		if r.status.OutageThresholdMs == 0 {
			r.status.OutageThresholdMs = 100
		}
		if f.State == "running" && !wasRunning {
			r.resetForStartLocked(now)
			r.warmupUntil = now.Add(defaultAgentStaleAfter)
			r.status.LastError = ""
		}
		if f.State != "running" {
			r.warmupUntil = time.Time{}
		}
		activeFlows[f.ID] = struct{}{}
		activeFlowKeys[key] = struct{}{}
	}
	for flowID := range s.flows {
		if _, ok := activeFlows[flowID]; !ok {
			delete(s.flows, flowID)
		}
	}
	for key := range s.flowKeys {
		if _, ok := activeFlowKeys[key]; !ok {
			delete(s.flowKeys, key)
		}
	}
}

func (s *RuntimeStore) AgentOnline(agentID, udpAddr string, desiredVersion uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	a := s.ensureAgent(agentID)
	a.Status = "online"
	a.UDPAddr = udpAddr
	a.DesiredConfigVersion = desiredVersion
	a.LastHeartbeat = now
	s.rearmWarmupForAgentLocked(agentID, now, true)
}

func (s *RuntimeStore) AgentOffline(agentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.ensureAgent(agentID)
	a.Status = "offline"
}

func (s *RuntimeStore) Heartbeat(h *pb.Heartbeat) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.ensureAgent(h.AgentId)
	a.Status = "online"
	a.ActiveConfigVersion = h.ActiveConfigVersion
	a.ActiveFlows = h.ActiveFlows
	a.LastHeartbeat = time.Now()
}

func (s *RuntimeStore) ConfigAck(ack *pb.ConfigAck) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.ensureAgent(ack.AgentId)
	a.ActiveConfigVersion = ack.ConfigVersion
	now := time.Now()
	for _, r := range s.flows {
		if r.status.Src == ack.AgentId {
			r.status.ActualState = r.status.DesiredState
			if r.status.DesiredState == "running" {
				s.rearmWarmupForFlowLocked(r, now, false)
			}
		}
	}
}

func (s *RuntimeStore) ConfigError(e *pb.ConfigError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.flows {
		if r.status.Src == e.AgentId {
			r.status.LastError = e.Error
		}
	}
}

func (s *RuntimeStore) IngestResult(res *pb.ResultSummary) *pb.ResultSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	applied := s.ingestResultLocked(res, now)
	a := s.ensureAgent(res.AgentId)
	a.LastResult = now
	return applied
}

func (s *RuntimeStore) IngestReport(report *pb.ResultReport) []*pb.ResultSummary {
	if report == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	out := s.finalizeDueReportsLocked(now)
	r := s.ingestReportLocked(report, now)
	a := s.ensureAgent(report.AgentId)
	a.LastResult = now
	if r == nil {
		if len(out) == 0 {
			return nil
		}
		return out
	}
	out = append(out, s.finalizeDueReportsLocked(now)...)
	if len(out) == 0 {
		return nil
	}
	return out
}

func (s *RuntimeStore) DrainDebugRecords() []SeqDebugRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.debugRecords) == 0 {
		return nil
	}
	out := append([]SeqDebugRecord(nil), s.debugRecords...)
	s.debugRecords = s.debugRecords[:0]
	return out
}

func (s *RuntimeStore) FinalizeDueReports(now time.Time) []*pb.ResultSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finalizeDueReportsLocked(now)
}

func (s *RuntimeStore) ingestReportLocked(report *pb.ResultReport, now time.Time) *flowRuntime {
	if report == nil {
		return nil
	}
	r := s.resolveFlowRuntimeByReportLocked(report)
	if r == nil {
		flowID := report.FlowId
		if flowID == "" {
			flowID = fmt.Sprintf("flow_key_%08x", report.FlowKey)
		}
		r = &flowRuntime{flowKey: report.FlowKey, reports: map[time.Time]*reportBucket{}, status: &model.FlowRuntimeStatus{FlowID: flowID, Src: report.Src, Dst: report.Dst, ReceiverAgentID: report.Dst}}
		s.flows[flowID] = r
		if report.FlowKey != 0 {
			s.flowKeys[report.FlowKey] = r
		}
	}
	st := r.status
	if report.FlowKey == 0 {
		report.FlowKey = r.flowKey
	}
	if report.FlowId == "" {
		report.FlowId = st.FlowID
	}
	if report.Src == "" {
		report.Src = st.Src
	}
	if report.Dst == "" {
		report.Dst = st.Dst
	}
	st.Src = report.Src
	st.Dst = report.Dst
	if report.Role == "receiver" || st.ReceiverAgentID == "" {
		st.ReceiverAgentID = report.AgentId
	}
	st.IntervalMs = effectiveReportIntervalMs(report.IntervalMs)
	if st.ReportWindowMs == 0 {
		st.ReportWindowMs = st.IntervalMs * 10
	}
	if report.WindowMs != 0 && report.WindowMs != st.ReportWindowMs {
		st.LastError = fmt.Sprintf("report window mismatch: got %dms want %dms", report.WindowMs, st.ReportWindowMs)
		return nil
	}
	st.LastReportedAt = now
	if !agentReportedOfflineAtIngest(s.agents[st.Src], now, defaultAgentStaleAfter) {
		st.LastError = ""
	}
	windowMs := effectiveReportWindowMs(report.WindowMs)
	windowStart := controllerReportWindowStart(now, windowMs)
	if r.reports == nil {
		r.reports = map[time.Time]*reportBucket{}
	}
	if r.seqs == nil {
		r.seqs = map[string]*seqLedgerEntry{}
	}
	switch report.Role {
	case "sender":
		windowStart = senderReportWindowStart(r, report, now, windowMs)
		b := ensureReportBucket(r, windowStart, windowMs, st.IntervalMs)
		b.senderAgent = report.AgentId
		if report.Duplicate > b.duplicate {
			b.duplicate = report.Duplicate
		}
		if report.Reorder > b.reorder {
			b.reorder = report.Reorder
		}
		for _, seq := range expandSeqRanges(report.SeqRanges) {
			s.ingestSenderSeqLocked(r, b, report, now, seq)
		}
	case "receiver":
		for _, seq := range expandSeqRanges(report.SeqRanges) {
			s.ingestReceiverSeqLocked(r, report, now, windowStart, windowMs, seq)
		}
	}
	return r
}

func controllerReportWindowStart(now time.Time, windowMs uint32) time.Time {
	window := time.Duration(effectiveReportWindowMs(windowMs)) * time.Millisecond
	return now.UTC().Truncate(window)
}

func senderReportWindowStart(r *flowRuntime, report *pb.ResultReport, now time.Time, windowMs uint32) time.Time {
	if r == nil || report == nil {
		return controllerReportWindowStart(now, windowMs)
	}
	reportWindowStart, ok := parsedReportWindowStart(report.Ts, windowMs)
	if !ok {
		return controllerReportWindowStart(now, windowMs)
	}
	if r.anchors == nil {
		r.anchors = map[uint32]reportTimeAnchor{}
	}
	anchor, exists := r.anchors[report.SessionId]
	if !exists {
		anchor = reportTimeAnchor{
			reportWindowStart:     reportWindowStart,
			controllerWindowStart: controllerReportWindowStart(now, windowMs),
		}
		r.anchors[report.SessionId] = anchor
	}
	return anchor.controllerWindowStart.Add(reportWindowStart.Sub(anchor.reportWindowStart))
}

func parsedReportWindowStart(ts string, windowMs uint32) (time.Time, bool) {
	if ts == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, false
	}
	window := time.Duration(effectiveReportWindowMs(windowMs)) * time.Millisecond
	return parsed.UTC().Add(-window), true
}

func (s *RuntimeStore) resolveFlowRuntimeByReportLocked(report *pb.ResultReport) *flowRuntime {
	if report == nil {
		return nil
	}
	if report.FlowId != "" {
		if r := s.flows[report.FlowId]; r != nil {
			return r
		}
	}
	if report.FlowKey != 0 {
		return s.flowKeys[report.FlowKey]
	}
	return nil
}

func (s *RuntimeStore) finalizeDueReportsLocked(now time.Time) []*pb.ResultSummary {
	var out []*pb.ResultSummary
	for _, r := range s.flows {
		if r == nil {
			continue
		}
		s.finalizeSeqLedgersLocked(r, now)
		if len(r.reports) == 0 {
			continue
		}
		keys := make([]time.Time, 0, len(r.reports))
		for key := range r.reports {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].Before(keys[j]) })
		for _, key := range keys {
			b := r.reports[key]
			if b == nil {
				delete(r.reports, key)
				continue
			}
			bucketEnd := b.windowStart.Add(time.Duration(effectiveReportWindowMs(b.windowMs)) * time.Millisecond)
			if b.pendingTx > 0 && now.Before(bucketEnd.Add(s.finalizeDelay)) {
				continue
			}
			sampleWindow := resultWindowStart(bucketEnd, effectiveReportWindowMs(b.windowMs))
			if sampleIndex(r.samples, sampleWindow) >= 0 {
				delete(r.reports, key)
				continue
			}
			out = append(out, s.finalizeReportBucketLocked(r, b, now))
			delete(r.reports, key)
		}
	}
	return out
}

func (s *RuntimeStore) finalizeReportBucketLocked(r *flowRuntime, b *reportBucket, now time.Time) *pb.ResultSummary {
	st := r.status
	tx := b.tx
	rx := b.rx
	lost := b.lost
	lossRatio := ratio(lost, tx)
	sampleAt := b.windowStart.Add(time.Duration(effectiveReportWindowMs(b.windowMs)) * time.Millisecond)
	outageMs := s.applyPacketOutcomesLocked(r, b, now)
	s.applyFlowSample(r, tx, rx, lost, lossRatio, b.duplicate, b.reorder, effectiveReportWindowMs(b.windowMs), false, now, sampleAt)
	agentID := b.senderAgent
	if agentID == "" {
		agentID = st.ReceiverAgentID
	}
	if agentID == "" {
		agentID = b.receiver
	}
	return &pb.ResultSummary{
		Ts:         sampleAt.UTC().Format(time.RFC3339Nano),
		AgentId:    agentID,
		FlowKey:    r.flowKey,
		Src:        st.Src,
		Dst:        st.Dst,
		FlowId:     st.FlowID,
		IntervalMs: st.IntervalMs,
		Tx:         tx,
		Rx:         rx,
		Lost:       lost,
		LossRatio:  lossRatio,
		Duplicate:  b.duplicate,
		Reorder:    b.reorder,
		OutageMs:   outageMs,
	}
}

func (s *RuntimeStore) applyPacketOutcomesLocked(r *flowRuntime, b *reportBucket, now time.Time) uint64 {
	if r == nil || b == nil || len(b.outcomes) == 0 {
		return 0
	}
	outcomes := make([]packetOutcome, 0, len(b.outcomes))
	for _, outcome := range b.outcomes {
		if outcome != nil {
			outcomes = append(outcomes, *outcome)
		}
	}
	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].sessionID != outcomes[j].sessionID {
			return outcomes[i].sessionID < outcomes[j].sessionID
		}
		return outcomes[i].seq < outcomes[j].seq
	})
	intervalMs := uint64(effectiveReportIntervalMs(b.intervalMs))
	var attributed uint64
	for i, outcome := range outcomes {
		at := b.windowStart.Add(time.Duration(i) * time.Duration(intervalMs) * time.Millisecond)
		if !r.lossRunStart.IsZero() {
			at = r.lossRunStart.Add(time.Duration(r.lossRunMs) * time.Millisecond)
		}
		if outcome.received {
			s.finishLossRunLocked(r, at, "recovered", now)
		} else {
			attributed += s.extendLossRunLocked(r, at, intervalMs, now)
		}
	}
	return attributed
}

func (s *RuntimeStore) extendLossRunLocked(r *flowRuntime, at time.Time, intervalMs uint64, now time.Time) uint64 {
	if r.lossRunStart.IsZero() {
		r.lossRunStart = at
		r.lossRunMs = 0
	}
	r.lossRunMs += intervalMs
	threshold := uint64(r.status.OutageThresholdMs)
	if threshold == 0 {
		threshold = 100
	}
	if !r.lossRunQualified && r.lossRunMs >= threshold {
		r.lossRunQualified = true
		r.activeOutageID = fmt.Sprintf("%s:%d", r.status.FlowID, r.lossRunStart.UnixNano())
		r.status.OutageCount++
		r.status.OutageActive = true
		r.status.CurrentOutageMs = r.lossRunMs
		r.status.OutageTotalMs += r.lossRunMs
		if r.lossRunMs > r.status.MaxOutageMs {
			r.status.MaxOutageMs = r.lossRunMs
		}
		s.outageRecords = append(s.outageRecords, OutageEventLogRecord{EventID: r.activeOutageID, State: "started", Ts: now, FlowID: r.status.FlowID, Src: r.status.Src, Dst: r.status.Dst, StartedAt: r.lossRunStart, OutageThresholdMs: r.status.OutageThresholdMs})
		return r.lossRunMs
	}
	if r.lossRunQualified {
		r.status.CurrentOutageMs = r.lossRunMs
		r.status.OutageTotalMs += intervalMs
		if r.lossRunMs > r.status.MaxOutageMs {
			r.status.MaxOutageMs = r.lossRunMs
		}
		return intervalMs
	}
	return 0
}

func (s *RuntimeStore) finishLossRunLocked(r *flowRuntime, end time.Time, reason string, now time.Time) {
	if r == nil || r.lossRunStart.IsZero() {
		return
	}
	if r.lossRunQualified {
		duration := r.lossRunMs
		if reason != "recovered" {
			end = r.lossRunStart.Add(time.Duration(duration) * time.Millisecond)
		}
		r.status.OutageActive = false
		r.status.CurrentOutageMs = 0
		r.status.LastOutageMs = duration
		s.outageRecords = append(s.outageRecords, OutageEventLogRecord{EventID: r.activeOutageID, State: "ended", Ts: now, FlowID: r.status.FlowID, Src: r.status.Src, Dst: r.status.Dst, StartedAt: r.lossRunStart, EndedAt: end, DurationMs: duration, OutageThresholdMs: r.status.OutageThresholdMs, EndReason: reason})
	} else if r.lossRunMs > 0 {
		r.status.IsolatedLossEvents++
	}
	r.lossRunStart = time.Time{}
	r.lossRunMs = 0
	r.lossRunQualified = false
	r.activeOutageID = ""
}

func ensureReportBucket(r *flowRuntime, windowStart time.Time, windowMs, intervalMs uint32) *reportBucket {
	if r.reports == nil {
		r.reports = map[time.Time]*reportBucket{}
	}
	b := r.reports[windowStart]
	if b == nil {
		b = &reportBucket{
			windowStart: windowStart,
			windowMs:    windowMs,
			intervalMs:  intervalMs,
			outcomes:    map[string]*packetOutcome{},
		}
		r.reports[windowStart] = b
	}
	b.windowMs = windowMs
	b.intervalMs = intervalMs
	return b
}

func expandSeqRanges(ranges []*pb.SeqRange) []uint64 {
	if len(ranges) == 0 {
		return nil
	}
	out := make([]uint64, 0, len(ranges)*2)
	for _, r := range ranges {
		if r == nil || r.End < r.Start {
			continue
		}
		for seq := r.Start; ; seq++ {
			out = append(out, seq)
			if seq == r.End {
				break
			}
		}
	}
	return out
}

func seqLedgerMapKey(sessionID uint32, seq uint64) string {
	return strconv.FormatUint(uint64(sessionID), 10) + ":" + strconv.FormatUint(seq, 10)
}

func (s *RuntimeStore) ingestSenderSeqLocked(r *flowRuntime, b *reportBucket, report *pb.ResultReport, now time.Time, seq uint64) {
	key := seqLedgerMapKey(report.SessionId, seq)
	entry := r.seqs[key]
	if entry == nil {
		entry = &seqLedgerEntry{sessionID: report.SessionId, seq: seq}
		r.seqs[key] = entry
	}
	if entry.senderSeen {
		return
	}
	entry.senderSeen = true
	entry.senderBucket = b.windowStart
	entry.senderSeenAt = now
	entry.senderReportTs = report.Ts
	entry.senderAgent = report.AgentId
	if b.outcomes == nil {
		b.outcomes = map[string]*packetOutcome{}
	}
	b.outcomes[key] = &packetOutcome{sessionID: report.SessionId, seq: seq, received: entry.receiverSeen}
	b.tx++
	b.pendingTx++
	b.senderAgent = report.AgentId
	if entry.receiverSeen && !entry.matchedCounted {
		b.rx++
		if b.pendingTx > 0 {
			b.pendingTx--
		}
		entry.matchedCounted = true
		s.appendDebugRecordLocked(r.status.FlowID, entry, now, "matched", "matched_on_sender_report")
		delete(r.seqs, key)
	}
}

func (s *RuntimeStore) ingestReceiverSeqLocked(r *flowRuntime, report *pb.ResultReport, now, windowStart time.Time, windowMs uint32, seq uint64) {
	key := seqLedgerMapKey(report.SessionId, seq)
	entry := r.seqs[key]
	if entry == nil {
		entry = &seqLedgerEntry{sessionID: report.SessionId, seq: seq}
		r.seqs[key] = entry
	}
	if entry.receiverSeen {
		return
	}
	entry.receiverSeen = true
	entry.receiverBucket = windowStart
	entry.receiverSeenAt = now
	entry.receiverReportTs = report.Ts
	entry.receiverAgent = report.AgentId
	if entry.senderSeen && !entry.matchedCounted {
		b := ensureReportBucket(r, entry.senderBucket, windowMs, r.status.IntervalMs)
		if outcome := b.outcomes[key]; outcome != nil {
			outcome.received = true
		}
		b.receiver = report.AgentId
		b.rx++
		if b.pendingTx > 0 {
			b.pendingTx--
		}
		entry.matchedCounted = true
		s.appendDebugRecordLocked(r.status.FlowID, entry, now, "matched", "matched_on_receiver_report")
		delete(r.seqs, key)
	}
}

func (s *RuntimeStore) finalizeSeqLedgersLocked(r *flowRuntime, now time.Time) {
	if r == nil || len(r.seqs) == 0 {
		return
	}
	keys := make([]string, 0, len(r.seqs))
	for key := range r.seqs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		entry := r.seqs[key]
		if entry == nil {
			delete(r.seqs, key)
			continue
		}
		switch {
		case entry.senderSeen && !entry.receiverSeen:
			b := r.reports[entry.senderBucket]
			if b == nil {
				delete(r.seqs, key)
				continue
			}
			bucketEnd := b.windowStart.Add(time.Duration(effectiveReportWindowMs(b.windowMs)) * time.Millisecond)
			if now.Before(bucketEnd.Add(s.finalizeDelay)) {
				continue
			}
			b.lost++
			if b.pendingTx > 0 {
				b.pendingTx--
			}
			s.appendDebugRecordLocked(r.status.FlowID, entry, now, "lost", "sender_only_expired")
			delete(r.seqs, key)
		case entry.receiverSeen && !entry.senderSeen:
			bucketEnd := entry.receiverBucket.Add(100 * time.Millisecond)
			if now.Before(bucketEnd.Add(s.finalizeDelay)) {
				continue
			}
			s.appendDebugRecordLocked(r.status.FlowID, entry, now, "receiver_only", "receiver_only_expired")
			delete(r.seqs, key)
		}
	}
}

func (s *RuntimeStore) appendDebugRecordLocked(flowID string, entry *seqLedgerEntry, now time.Time, finalState, finalizedReason string) {
	if entry == nil {
		return
	}
	s.debugRecords = append(s.debugRecords, SeqDebugRecord{
		ControllerTs:     now.UTC().Format(time.RFC3339Nano),
		FlowID:           flowID,
		SessionID:        entry.sessionID,
		Seq:              entry.seq,
		SenderSeen:       entry.senderSeen,
		ReceiverSeen:     entry.receiverSeen,
		SenderReportTs:   entry.senderReportTs,
		ReceiverReportTs: entry.receiverReportTs,
		FinalState:       finalState,
		FinalizedReason:  finalizedReason,
	})
}

func effectiveReportWindowMs(windowMs uint32) uint32 {
	if windowMs == 0 {
		return 1000
	}
	return windowMs
}

func effectiveReportIntervalMs(intervalMs uint32) uint32 {
	if intervalMs == 0 {
		return 10
	}
	return intervalMs
}

func (s *RuntimeStore) ApplyControllerAgentLoss(now time.Time, staleAfter time.Duration) []*pb.ResultSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.agents {
		if a.Status == "online" && !agentCommunicating(a, now, staleAfter) {
			a.Status = "offline"
		}
	}
	var inferred []*pb.ResultSummary
	for _, r := range s.flows {
		st := r.status
		srcOK := agentCommunicating(s.agents[st.Src], now, staleAfter)
		s.updateUnmeasurableLocked(r, srcOK, now, staleAfter)
		receiverOK := agentCommunicating(s.agents[st.ReceiverAgentID], now, staleAfter)
		reason := inferredLossReason(st, srcOK, receiverOK, now, staleAfter, r.warmupUntil)
		if reason == "" {
			continue
		}
		st.LastError = reason
		if !srcOK {
			s.applyMissingFlowSample(r, now, now)
			continue
		}
		tx := latestObservedTxOrExpected(r)
		if applied, outageMs := s.applySyntheticLossBackfill(r, tx, now); applied {
			inferred = append(inferred, syntheticResultSummary(st, tx, outageMs, now))
		}
	}
	return inferred
}

func (s *RuntimeStore) updateUnmeasurableLocked(r *flowRuntime, srcOK bool, now time.Time, staleAfter time.Duration) {
	if r == nil || r.status.DesiredState != "running" {
		return
	}
	st := r.status
	if !srcOK {
		if r.unmeasurableStarted.IsZero() {
			start := now
			if a := s.agents[st.Src]; a != nil && !a.LastHeartbeat.IsZero() && a.Status == "online" {
				deadline := a.LastHeartbeat.Add(staleAfter)
				if deadline.Before(now) {
					start = deadline
				}
			}
			r.unmeasurableStarted = start
			r.unmeasurableAccounted = start
			st.UnmeasurableCount++
		}
		if now.After(r.unmeasurableAccounted) {
			st.UnmeasurableTotalMs += uint64(now.Sub(r.unmeasurableAccounted) / time.Millisecond)
			r.unmeasurableAccounted = now
		}
		st.UnmeasurableActive = true
		st.CurrentUnmeasurableMs = uint64(now.Sub(r.unmeasurableStarted) / time.Millisecond)
		if st.CurrentUnmeasurableMs > st.MaxUnmeasurableMs {
			st.MaxUnmeasurableMs = st.CurrentUnmeasurableMs
		}
		return
	}
	if r.unmeasurableStarted.IsZero() {
		return
	}
	duration := uint64(now.Sub(r.unmeasurableStarted) / time.Millisecond)
	if now.After(r.unmeasurableAccounted) {
		st.UnmeasurableTotalMs += uint64(now.Sub(r.unmeasurableAccounted) / time.Millisecond)
	}
	if duration > st.MaxUnmeasurableMs {
		st.MaxUnmeasurableMs = duration
	}
	r.unmeasurableEvents = append(r.unmeasurableEvents, unmeasurableEvent{FlowID: st.FlowID, Src: st.Src, Dst: st.Dst, StartedAt: r.unmeasurableStarted, EndedAt: now, DurationMs: duration})
	r.unmeasurableStarted = time.Time{}
	r.unmeasurableAccounted = time.Time{}
	st.UnmeasurableActive = false
	st.CurrentUnmeasurableMs = 0
}

func (s *RuntimeStore) Flows() []model.FlowRuntimeStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.FlowRuntimeStatus, 0, len(s.flows))
	for _, r := range s.flows {
		cp := s.flowStatusCopyLocked(r, time.Now(), defaultAgentStaleAfter)
		cp.LossHistory10s = append([]float64(nil), r.status.LossHistory10s...)
		cp.LossHistory20s = append([]float64(nil), r.status.LossHistory20s...)
		cp.LossHistory60s = append([]float64(nil), r.status.LossHistory60s...)
		cp.LossHistory240s = append([]float64(nil), r.status.LossHistory240s...)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FlowID < out[j].FlowID })
	return out
}

func (s *RuntimeStore) Flow(flowID string) (model.FlowRuntimeStatus, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.flows[flowID]
	if r == nil {
		return model.FlowRuntimeStatus{}, false
	}
	cp := s.flowStatusCopyLocked(r, time.Now(), defaultAgentStaleAfter)
	cp.LossHistory10s = append([]float64(nil), r.status.LossHistory10s...)
	cp.LossHistory20s = append([]float64(nil), r.status.LossHistory20s...)
	cp.LossHistory60s = append([]float64(nil), r.status.LossHistory60s...)
	cp.LossHistory240s = append([]float64(nil), r.status.LossHistory240s...)
	return cp, true
}

func (s *RuntimeStore) DrainOutageEventRecords() []OutageEventLogRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]OutageEventLogRecord(nil), s.outageRecords...)
	s.outageRecords = nil
	return out
}

func (s *RuntimeStore) CloseActiveOutages(now time.Time, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.flows {
		s.finishLossRunLocked(r, now, reason, now)
	}
}

func (s *RuntimeStore) UnmeasurableEvents(now time.Time) []unmeasurableEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []unmeasurableEvent
	for _, r := range s.flows {
		out = append(out, r.unmeasurableEvents...)
		if !r.unmeasurableStarted.IsZero() {
			d := uint64(now.Sub(r.unmeasurableStarted) / time.Millisecond)
			out = append(out, unmeasurableEvent{FlowID: r.status.FlowID, Src: r.status.Src, Dst: r.status.Dst, StartedAt: r.unmeasurableStarted, EndedAt: now, DurationMs: d})
		}
	}
	return out
}

func (s *RuntimeStore) Agents() []model.AgentRuntimeStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.AgentRuntimeStatus, 0, len(s.agents))
	now := time.Now()
	for _, a := range s.agents {
		cp := *a
		if cp.Status == "online" && !agentCommunicating(a, now, defaultAgentStaleAfter) {
			cp.Status = "offline"
		}
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AgentID < out[j].AgentID })
	return out
}

func (s *RuntimeStore) ensureAgent(agentID string) *model.AgentRuntimeStatus {
	a := s.agents[agentID]
	if a == nil {
		a = &model.AgentRuntimeStatus{AgentID: agentID, Status: "offline", Enabled: true}
		s.agents[agentID] = a
	}
	return a
}

func (s *RuntimeStore) ingestResultLocked(res *pb.ResultSummary, now time.Time) *pb.ResultSummary {
	r := s.resolveFlowRuntimeLocked(res)
	if r == nil {
		flowID := res.FlowId
		if flowID == "" {
			flowID = fmt.Sprintf("flow_key_%08x", res.FlowKey)
		}
		r = &flowRuntime{flowKey: res.FlowKey, reports: map[time.Time]*reportBucket{}, status: &model.FlowRuntimeStatus{FlowID: flowID, Src: res.Src, Dst: res.Dst, ReceiverAgentID: res.AgentId, OutageThresholdMs: 100}}
		s.flows[flowID] = r
		if res.FlowKey != 0 {
			s.flowKeys[res.FlowKey] = r
		}
	}
	st := r.status
	if res.FlowKey == 0 {
		res.FlowKey = r.flowKey
	}
	if res.FlowId == "" {
		res.FlowId = st.FlowID
	}
	if res.Src == "" {
		res.Src = st.Src
	}
	if res.Dst == "" {
		res.Dst = st.Dst
	}
	st.Src = res.Src
	st.Dst = res.Dst
	st.ReceiverAgentID = res.AgentId
	st.IntervalMs = res.IntervalMs
	st.LastReportedAt = now
	applied := cloneResultSummary(res)
	sourceOffline := agentReportedOfflineAtIngest(s.agents[st.Src], now, defaultAgentStaleAfter)
	sampleAt := now
	if applied != nil {
		applied.Ts = sampleAt.UTC().Format(time.RFC3339Nano)
	}
	if !sourceOffline {
		st.LastError = ""
	}
	if st.DesiredState != "" && st.DesiredState != "running" {
		applied.Tx, applied.Rx, applied.Lost, applied.LossRatio = 0, 0, 0, 0
		applied.Duplicate, applied.Reorder = 0, 0
	} else if applied.Tx == 0 && applied.Rx == 0 && sourceOffline {
		applied.Duplicate, applied.Reorder = 0, 0
	} else if applied.Tx == 0 && applied.Rx == 0 && hasObservedSampleInZeroReceiveSecond(r, sampleAt) {
		applied.Lost, applied.LossRatio = 0, 0
		applied.Duplicate, applied.Reorder = 0, 0
		return applied
	} else if applied.Tx == 0 && applied.Rx == 0 && !now.Before(r.warmupUntil) {
		applied.Tx = latestObservedTxOrExpected(r)
		applied.Rx = 0
		applied.Lost = applied.Tx
		applied.LossRatio = 1
		applied.Duplicate, applied.Reorder = 0, 0
		st.LastError = "receiver agent reported no packets for running flow; loss inferred by controller"
	}
	if applied.Tx == 0 && applied.Rx == 0 && sourceOffline {
		s.applyMissingFlowSample(r, now, sampleAt)
		return applied
	}
	applied.OutageMs = s.applyAggregateOutcomesLocked(r, applied.Lost, applied.Rx, sampleAt, now)
	s.applyFlowSample(r, applied.Tx, applied.Rx, applied.Lost, applied.LossRatio, applied.Duplicate, applied.Reorder, applied.IntervalMs, false, now, sampleAt)
	return applied
}

func agentReportedOfflineAtIngest(a *model.AgentRuntimeStatus, now time.Time, staleAfter time.Duration) bool {
	return a != nil && !a.LastHeartbeat.IsZero() && !agentCommunicating(a, now, staleAfter)
}

func cloneResultSummary(res *pb.ResultSummary) *pb.ResultSummary {
	if res == nil {
		return nil
	}
	cp := *res
	return &cp
}

func syntheticResultSummary(st *model.FlowRuntimeStatus, tx, outageMs uint64, now time.Time) *pb.ResultSummary {
	return &pb.ResultSummary{
		Ts:         now.UTC().Format(time.RFC3339Nano),
		AgentId:    st.ReceiverAgentID,
		FlowKey:    protocol.ComputeFlowKey(st.Src, st.Dst, st.FlowID),
		Src:        st.Src,
		Dst:        st.Dst,
		FlowId:     st.FlowID,
		IntervalMs: st.IntervalMs,
		Tx:         tx,
		Rx:         0,
		Lost:       tx,
		LossRatio:  1,
		OutageMs:   outageMs,
	}
}

func (s *RuntimeStore) applySyntheticLossBackfill(r *flowRuntime, tx uint64, now time.Time) (bool, uint64) {
	st := r.status
	interval := flowInterval(st.IntervalMs)
	buckets := int(expectedPacketsPerSecond(st.IntervalMs))
	if buckets < 1 {
		buckets = 1
	}
	base := tx / uint64(buckets)
	remainder := tx % uint64(buckets)
	endWindow := resultWindowStart(now, st.IntervalMs)
	startWindow := endWindow.Add(-time.Duration(buckets-1) * interval)
	if len(r.samples) > 0 {
		startWindow = r.samples[len(r.samples)-1].windowStart.Add(interval)
	}
	if startWindow.After(endWindow) {
		return false, 0
	}
	var outageTotal uint64
	i := 0
	for windowStart := startWindow; !windowStart.After(endWindow); windowStart = windowStart.Add(interval) {
		lost := base
		if uint64(i%buckets) < remainder {
			lost++
		}
		i++
		if lost == 0 {
			continue
		}
		outageTotal += s.applyAggregateOutcomesLocked(r, lost, 0, windowStart, now)
		s.applyFlowSample(r, lost, 0, lost, 1, 0, 0, st.IntervalMs, false, now, windowStart.Add(interval))
	}
	return true, outageTotal
}

func (s *RuntimeStore) applyAggregateOutcomesLocked(r *flowRuntime, lost, received uint64, sampleAt, now time.Time) uint64 {
	if r == nil || r.status == nil {
		return 0
	}
	intervalMs := uint64(effectiveReportIntervalMs(r.status.IntervalMs))
	start := resultWindowStart(sampleAt, r.status.IntervalMs)
	var attributed uint64
	for i := uint64(0); i < lost; i++ {
		attributed += s.extendLossRunLocked(r, start.Add(time.Duration(i*intervalMs)*time.Millisecond), intervalMs, now)
	}
	if received > 0 {
		s.finishLossRunLocked(r, start.Add(time.Duration(lost*intervalMs)*time.Millisecond), "recovered", now)
	}
	return attributed
}

func (s *RuntimeStore) resolveFlowRuntimeLocked(res *pb.ResultSummary) *flowRuntime {
	if res == nil {
		return nil
	}
	if res.FlowId != "" {
		if r := s.flows[res.FlowId]; r != nil {
			return r
		}
	}
	if res.FlowKey != 0 {
		return s.flowKeys[res.FlowKey]
	}
	return nil
}

func (s *RuntimeStore) applyMissingFlowSample(r *flowRuntime, seenAt, sampleAt time.Time) {
	s.finishLossRunLocked(r, sampleAt, "source_unmeasurable", seenAt)
	s.applyFlowSample(r, 0, 0, 0, 0, 0, 0, r.status.IntervalMs, true, seenAt, sampleAt)
}

func hasObservedSampleInZeroReceiveSecond(r *flowRuntime, zeroEnd time.Time) bool {
	if r == nil {
		return false
	}
	targetSecond := zeroEnd.UTC().Add(-time.Second).Truncate(time.Second)
	for _, s := range r.samples {
		if s.missing || (s.tx == 0 && s.rx == 0 && s.lost == 0) {
			continue
		}
		if s.windowStart.UTC().Truncate(time.Second).Equal(targetSecond) {
			return true
		}
	}
	return false
}

func (s *RuntimeStore) applyFlowSample(r *flowRuntime, tx, rx, lost uint64, lossRatio float64, duplicate, reorder uint64, sampleWindowMs uint32, missing bool, seenAt, sampleAt time.Time) {
	st := r.status
	if sampleWindowMs == 0 {
		sampleWindowMs = st.IntervalMs
	}
	windowStart := resultWindowStart(sampleAt, sampleWindowMs)
	if tx > 0 || !seenAt.Before(r.warmupUntil) {
		r.warmupUntil = time.Time{}
	}
	newSample := sample{
		windowStart: windowStart,
		windowMs:    st.ReportWindowMs,
		intervalMs:  st.IntervalMs,
		tx:          tx,
		rx:          rx,
		lost:        lost,
		duplicate:   duplicate,
		reorder:     reorder,
		missing:     missing,
	}
	if idx := sampleIndex(r.samples, windowStart); idx >= 0 {
		old := r.samples[idx]
		st.TxTotal -= old.tx
		st.RxTotal -= old.rx
		st.LostTotal -= old.lost
		st.DuplicateTotal -= old.duplicate
		st.ReorderTotal -= old.reorder
		r.samples[idx] = newSample
	} else {
		r.samples = insertSample(r.samples, newSample)
	}
	st.LastSeen = seenAt
	st.TxTotal += tx
	st.RxTotal += rx
	st.LostTotal += lost
	st.LossTimeTotalMs = st.LostTotal * uint64(st.IntervalMs)
	st.DuplicateTotal += duplicate
	st.ReorderTotal += reorder
	recomputeDerivedFlowStatus(r, sampleAt)
	_ = lossRatio
}

func flowInterval(intervalMs uint32) time.Duration {
	interval := time.Duration(intervalMs) * time.Millisecond
	if interval <= 0 {
		return time.Second
	}
	return interval
}

func resultWindowStart(sampleAt time.Time, intervalMs uint32) time.Time {
	interval := flowInterval(intervalMs)
	return sampleAt.UTC().Add(-interval).Truncate(interval)
}

func sampleIndex(samples []sample, windowStart time.Time) int {
	for i := len(samples) - 1; i >= 0; i-- {
		if samples[i].windowStart.Equal(windowStart) {
			return i
		}
		if samples[i].windowStart.Before(windowStart) {
			return -1
		}
	}
	return -1
}

func insertSample(samples []sample, next sample) []sample {
	i := sort.Search(len(samples), func(i int) bool {
		return !samples[i].windowStart.Before(next.windowStart)
	})
	if i == len(samples) {
		return append(samples, next)
	}
	samples = append(samples, sample{})
	copy(samples[i+1:], samples[i:])
	samples[i] = next
	return samples
}

func recomputeDerivedFlowStatus(r *flowRuntime, sampleAt time.Time) {
	st := r.status
	oneSecond := sumDurationWindow(r.samples, sampleAt, time.Second)
	st.Tx1s = oneSecond.tx
	st.Rx1s = oneSecond.rx
	st.Lost1s = oneSecond.lost
	st.LossTime1sMs = oneSecond.lost * uint64(st.IntervalMs)
	st.Duplicate1s = oneSecond.duplicate
	st.Reorder1s = oneSecond.reorder
	st.LossRatio1s = ratio(oneSecond.lost, oneSecond.tx)

	tenSeconds := sumDurationWindow(r.samples, sampleAt, 10*time.Second)
	sixtySeconds := sumDurationWindow(r.samples, sampleAt, 60*time.Second)
	st.Lost60s = sixtySeconds.lost
	st.Duplicate60s = sixtySeconds.duplicate
	st.Reorder60s = sixtySeconds.reorder
	st.LossRatio10s = ratio(tenSeconds.lost, tenSeconds.tx)
	expected60s := expectedPacketsPerSecond(st.IntervalMs) * 60
	if sixtySeconds.tx > expected60s {
		expected60s = sixtySeconds.tx
	}
	st.LossRatio60s = ratio(sixtySeconds.lost, expected60s)
	st.LossHistory240s = tailRatios(secondRatios(r.samples), 240)
	st.LossHistory60s = tailRatios(st.LossHistory240s, 60)
	if len(st.LossHistory240s) > 10 {
		st.LossHistory10s = append([]float64(nil), st.LossHistory240s[len(st.LossHistory240s)-10:]...)
	} else {
		st.LossHistory10s = append([]float64(nil), st.LossHistory240s...)
	}
	if len(st.LossHistory240s) > 20 {
		st.LossHistory20s = append([]float64(nil), st.LossHistory240s[len(st.LossHistory240s)-20:]...)
	} else {
		st.LossHistory20s = append([]float64(nil), st.LossHistory240s...)
	}
}

func (r *flowRuntime) resetForStartLocked(now time.Time) {
	r.status.ClearHistory(now)
	r.samples = nil
	r.reports = map[time.Time]*reportBucket{}
	r.seqs = map[string]*seqLedgerEntry{}
	r.unmeasurableStarted = time.Time{}
	r.unmeasurableAccounted = time.Time{}
	r.unmeasurableEvents = nil
	r.lossRunStart = time.Time{}
	r.lossRunMs = 0
	r.lossRunQualified = false
	r.activeOutageID = ""
}

func (s *RuntimeStore) rearmWarmupForAgentLocked(agentID string, now time.Time, includeReceiver bool) {
	for _, r := range s.flows {
		s.rearmWarmupIfMatchedLocked(r, agentID, now, includeReceiver)
	}
}

func (s *RuntimeStore) rearmWarmupForFlowLocked(r *flowRuntime, now time.Time, requireLastSeen bool) {
	if r == nil || r.status.DesiredState != "running" {
		return
	}
	if requireLastSeen && r.status.LastSeen.IsZero() {
		return
	}
	r.warmupUntil = now.Add(defaultAgentStaleAfter)
	clearControllerInferredLossError(&r.status.LastError)
}

func (s *RuntimeStore) rearmWarmupIfMatchedLocked(r *flowRuntime, agentID string, now time.Time, includeReceiver bool) {
	if r == nil || r.status.DesiredState != "running" {
		return
	}
	if r.status.Src != agentID && (!includeReceiver || r.status.ReceiverAgentID != agentID) {
		return
	}
	r.warmupUntil = now.Add(defaultAgentStaleAfter)
	clearControllerInferredLossError(&r.status.LastError)
}

func clearControllerInferredLossError(lastError *string) {
	if lastError == nil {
		return
	}
	if isControllerInferredLossError(*lastError) {
		*lastError = ""
	}
}

func isControllerInferredLossError(err string) bool {
	return strings.Contains(err, "loss inferred by controller") || strings.Contains(err, "reported no packets for running flow")
}

func (s *RuntimeStore) flowStatusCopyLocked(r *flowRuntime, now time.Time, staleAfter time.Duration) model.FlowRuntimeStatus {
	cp := *r.status
	if cp.DesiredState != "running" {
		return cp
	}
	srcOK := agentCommunicating(s.agents[cp.Src], now, staleAfter)
	receiverOK := agentCommunicating(s.agents[cp.ReceiverAgentID], now, staleAfter)
	if srcOK && receiverOK {
		return cp
	}
	cp.ActualState = "offline"
	if cp.LastError == "" {
		cp.LastError = inferredLossReason(&cp, srcOK, receiverOK, now, staleAfter, r.warmupUntil)
	}
	return cp
}

func agentCommunicating(a *model.AgentRuntimeStatus, now time.Time, staleAfter time.Duration) bool {
	return a != nil && a.Status == "online" && !a.LastHeartbeat.IsZero() && now.Sub(a.LastHeartbeat) <= staleAfter
}

func inferredLossReason(st *model.FlowRuntimeStatus, srcOK, receiverOK bool, now time.Time, staleAfter time.Duration, warmupUntil time.Time) string {
	if st.DesiredState != "running" {
		return ""
	}
	switch {
	case !srcOK && !receiverOK:
		return "source and receiver agent communication lost; loss inferred by controller"
	case !srcOK:
		return "source agent communication lost; loss inferred by controller"
	case !receiverOK:
		return "receiver agent communication lost; loss inferred by controller"
	case now.Before(warmupUntil):
		return ""
	case st.ActualState == "running" && flowResultStale(st, now, staleAfter):
		return "receiver agent has not reported packets for running flow; loss inferred by controller"
	default:
		return ""
	}
}

func flowResultStale(st *model.FlowRuntimeStatus, now time.Time, staleAfter time.Duration) bool {
	return st.LastSeen.IsZero() || now.Sub(st.LastSeen) > staleAfter
}

func expectedPacketsPerSecond(intervalMs uint32) uint64 {
	if intervalMs == 0 {
		return 1
	}
	n := uint64(1000 / intervalMs)
	if n == 0 {
		return 1
	}
	return n
}

func latestObservedTxOrExpected(r *flowRuntime) uint64 {
	expected := uint64(1)
	if r != nil && r.status != nil {
		expected = expectedPacketsPerSecond(r.status.IntervalMs)
	}
	if r != nil {
		for i := len(r.samples) - 1; i >= 0; i-- {
			s := r.samples[i]
			if !s.missing && s.tx > 0 {
				if s.tx > expected {
					return s.tx
				}
				return expected
			}
		}
	}
	return expected
}

type sampleTotals struct {
	tx        uint64
	rx        uint64
	lost      uint64
	duplicate uint64
	reorder   uint64
}

func sumDurationWindow(samples []sample, end time.Time, d time.Duration) sampleTotals {
	start := end.Add(-d)
	var out sampleTotals
	for _, s := range samples {
		if s.windowStart.Before(start) || !s.windowStart.Before(end) || s.missing {
			continue
		}
		out.tx += s.tx
		out.rx += s.rx
		out.lost += s.lost
		out.duplicate += s.duplicate
		out.reorder += s.reorder
	}
	return out
}

func ratio(lost, tx uint64) float64 {
	if tx == 0 {
		return 0
	}
	return float64(lost) / float64(tx)
}

func secondRatios(samples []sample) []float64 {
	type totals struct {
		tx      uint64
		lost    uint64
		missing bool
	}
	bySecond := map[time.Time]totals{}
	for _, s := range samples {
		sec := s.windowStart.UTC().Truncate(time.Second)
		t := bySecond[sec]
		if s.missing {
			t.missing = true
		} else {
			t.tx += s.tx
			t.lost += s.lost
		}
		bySecond[sec] = t
	}
	seconds := make([]time.Time, 0, len(bySecond))
	for sec := range bySecond {
		seconds = append(seconds, sec)
	}
	sort.Slice(seconds, func(i, j int) bool { return seconds[i].Before(seconds[j]) })
	out := make([]float64, 0, len(seconds))
	for _, sec := range seconds {
		t := bySecond[sec]
		switch {
		case t.tx > 0:
			out = append(out, float64(t.lost)/float64(t.tx))
		case t.missing:
			out = append(out, -1)
		default:
			out = append(out, 0)
		}
	}
	return out
}

func calcWindow(samples []sample, n int) float64 {
	if len(samples) > n {
		samples = samples[len(samples)-n:]
	}
	var tx, lost uint64
	for _, s := range samples {
		if s.missing {
			continue
		}
		tx += s.tx
		lost += s.lost
	}
	if tx == 0 {
		return 0
	}
	return float64(lost) / float64(tx)
}

func calcProjectedWindow(samples []sample, n int, expectedTxPerSample uint64) float64 {
	if len(samples) > n {
		samples = samples[len(samples)-n:]
	}
	var tx, lost uint64
	for _, s := range samples {
		if s.missing {
			continue
		}
		tx += s.tx
		lost += s.lost
	}
	if missing := n - len(samples); missing > 0 {
		tx += uint64(missing) * expectedTxPerSample
	}
	if tx == 0 {
		return 0
	}
	return float64(lost) / float64(tx)
}

func sumLostWindow(samples []sample, n int) uint64 {
	if len(samples) > n {
		samples = samples[len(samples)-n:]
	}
	var lost uint64
	for _, s := range samples {
		if s.missing {
			continue
		}
		lost += s.lost
	}
	return lost
}

func sumDuplicateWindow(samples []sample, n int) uint64 {
	if len(samples) > n {
		samples = samples[len(samples)-n:]
	}
	var duplicate uint64
	for _, s := range samples {
		if s.missing {
			continue
		}
		duplicate += s.duplicate
	}
	return duplicate
}

func sumReorderWindow(samples []sample, n int) uint64 {
	if len(samples) > n {
		samples = samples[len(samples)-n:]
	}
	var reorder uint64
	for _, s := range samples {
		if s.missing {
			continue
		}
		reorder += s.reorder
	}
	return reorder
}

func ratios(samples []sample) []float64 {
	out := make([]float64, 0, len(samples))
	for _, s := range samples {
		if s.missing {
			out = append(out, -1)
		} else if s.tx == 0 {
			out = append(out, 0)
		} else {
			out = append(out, float64(s.lost)/float64(s.tx))
		}
	}
	return out
}

func tailRatios(history []float64, n int) []float64 {
	if len(history) > n {
		history = history[len(history)-n:]
	}
	return append([]float64(nil), history...)
}
