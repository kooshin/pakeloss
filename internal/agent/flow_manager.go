package agent

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"pakeloss/internal/pb"
	"pakeloss/internal/protocol"
)

type FlowManager struct {
	mu          sync.Mutex
	opMu        sync.Mutex
	agentID     string
	listenVRF   string
	sessionID   uint32
	flows       map[string]*flowRunner
	seqs        map[string][]*sequenceCounter
	reports     chan<- *pb.ResultReport
	startSender func(context.Context, string, uint32, *pb.FlowConfig, []*sequenceCounter, string, chan<- *pb.ResultReport)
	version     uint64
	hash        string
}

type flowRunner struct {
	cfg    *pb.FlowConfig
	cancel context.CancelFunc
	done   chan struct{}
}

const (
	repeatedSendErrorLogInterval = 10 * time.Second
	senderCatchUpLogInterval     = 10 * time.Second
	senderReportDelay            = time.Second
	maxRecoveryDebt              = 12
	// Give the just-used socket a brief chance to surface a queued response
	// without stalling every source port on every tick.
	senderResponseDrainTimeout = 250 * time.Microsecond
)

type senderPending struct {
	bucket         time.Time
	deadline       time.Time
	senderTxTimeNS int64
}

type senderReceived struct {
	bucket         time.Time
	deadline       time.Time
	senderTxTimeNS int64
}

type senderBucketStats struct {
	tx        uint64
	duplicate uint64
	reorder   uint64
	seqs      []uint64
}

type senderConnState struct {
	pending    map[uint64]senderPending
	received   map[uint64]senderReceived
	highestSeq uint64
	seenAny    bool
}

type repeatedErrorLimiter struct {
	lastKey    string
	nextLog    time.Time
	suppressed uint64
}

func (l *repeatedErrorLimiter) shouldLog(err error, now time.Time, interval time.Duration) (uint64, bool) {
	if err == nil {
		return 0, false
	}
	key := normalizedSendErrorKey(err)
	if key != l.lastKey || now.IsZero() || !now.Before(l.nextLog) {
		suppressed := l.suppressed
		l.lastKey = key
		l.nextLog = now.Add(interval)
		l.suppressed = 0
		return suppressed, true
	}
	l.suppressed++
	return 0, false
}

func normalizedSendErrorKey(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return "unknown"
	}
	if idx := strings.LastIndex(msg, ": "); idx >= 0 {
		msg = msg[idx+2:]
	}
	return msg
}

func NewFlowManager(agentID string, listenVRF ...string) *FlowManager {
	vrf := ""
	if len(listenVRF) > 0 {
		vrf = listenVRF[0]
	}
	return &FlowManager{
		agentID:     agentID,
		listenVRF:   vrf,
		sessionID:   newSessionID(),
		flows:       map[string]*flowRunner{},
		seqs:        map[string][]*sequenceCounter{},
		startSender: runSender,
	}
}

func (m *FlowManager) SetResultReports(reports chan<- *pb.ResultReport) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reports = reports
}

func (m *FlowManager) Apply(ctx context.Context, snap *pb.ConfigSnapshot) error {
	m.opMu.Lock()
	defer m.opMu.Unlock()

	if err := validateSnapshotPacketSizes(m.agentID, snap); err != nil {
		return err
	}

	seen := map[string]bool{}
	var toStop []*flowRunner
	toStopIDs := make([]string, 0, len(snap.Flows))
	type startRequest struct {
		id   string
		cfg  *pb.FlowConfig
		seqs []*sequenceCounter
	}
	toStart := make([]startRequest, 0, len(snap.Flows))

	m.mu.Lock()
	for _, cfg := range snap.Flows {
		if seen[cfg.Id] {
			log.Printf("duplicate flow skipped: %s", cfg.Id)
			continue
		}
		seen[cfg.Id] = true
		r, exists := m.flows[cfg.Id]
		shouldRun := cfg.State == "running" && (cfg.SrcId == "" || cfg.SrcId == m.agentID)
		if !shouldRun {
			if exists {
				delete(m.flows, cfg.Id)
				toStop = append(toStop, r)
				toStopIDs = append(toStopIDs, cfg.Id)
			}
			continue
		}
		if exists && equalFlowConfig(r.cfg, cfg) {
			continue
		}
		if exists {
			delete(m.flows, cfg.Id)
			toStop = append(toStop, r)
			toStopIDs = append(toStopIDs, cfg.Id)
		}
		copied := *cfg
		toStart = append(toStart, startRequest{
			id:   cfg.Id,
			cfg:  &copied,
			seqs: m.sequenceCounters(cfg.Id, sourcePortCount(cfg)),
		})
	}
	for id, r := range m.flows {
		if !seen[id] {
			delete(m.flows, id)
			toStop = append(toStop, r)
			toStopIDs = append(toStopIDs, id)
		}
	}
	m.mu.Unlock()

	for _, r := range toStop {
		m.stopRunnerLocked(r)
	}
	for _, id := range toStopIDs {
		log.Printf("flow stopped: %s", id)
	}
	for _, req := range toStart {
		runner := m.startRunnerLocked(ctx, req.cfg, req.seqs)
		m.mu.Lock()
		m.flows[req.id] = runner
		m.mu.Unlock()
		log.Printf("flow started: %s", req.id)
	}
	m.mu.Lock()
	m.version = snap.ConfigVersion
	m.hash = snap.ConfigHash
	m.mu.Unlock()
	return nil
}

func (m *FlowManager) StopAll() {
	m.opMu.Lock()
	defer m.opMu.Unlock()

	var toStop []*flowRunner
	m.mu.Lock()
	for id, r := range m.flows {
		delete(m.flows, id)
		toStop = append(toStop, r)
	}
	m.mu.Unlock()

	for _, r := range toStop {
		m.stopRunnerLocked(r)
	}
}

func (m *FlowManager) ActiveFlows() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return uint32(len(m.flows))
}

func (m *FlowManager) Version() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.version
}

func (m *FlowManager) sequenceCounters(flowID string, count uint32) []*sequenceCounter {
	if count == 0 {
		count = 1
	}
	seqs := m.seqs[flowID]
	if len(seqs) == 0 {
		seqs = append(seqs, &sequenceCounter{})
	}
	for uint32(len(seqs)) < count {
		seqs = append(seqs, seqs[0])
	}
	m.seqs[flowID] = seqs
	return seqs[:count]
}

func (m *FlowManager) sequenceCounter(flowID string) *sequenceCounter {
	return m.sequenceCounters(flowID, 1)[0]
}

func (m *FlowManager) startRunnerLocked(ctx context.Context, cfg *pb.FlowConfig, seqs []*sequenceCounter) *flowRunner {
	child, cancel := context.WithCancel(ctx)
	runner := &flowRunner{
		cfg:    cfg,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	startSender := m.startSender
	reports := m.reports
	go func() {
		defer close(runner.done)
		startSender(child, m.agentID, m.sessionID, cfg, seqs, m.listenVRF, reports)
	}()
	return runner
}

func (m *FlowManager) stopRunnerLocked(r *flowRunner) {
	if r == nil {
		return
	}
	r.cancel()
	if r.done == nil {
		return
	}
	<-r.done
}

type sequenceCounter struct {
	mu   sync.Mutex
	next uint64
}

func (c *sequenceCounter) Next() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	seq := c.next
	c.next++
	return seq
}

func newSessionID() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err == nil {
		id := binary.BigEndian.Uint32(b[:])
		if id != 0 {
			return id
		}
	}
	id := uint32(time.Now().UnixNano())
	if id == 0 {
		return 1
	}
	return id
}

func equalFlowConfig(a, b *pb.FlowConfig) bool {
	return a.Id == b.Id && a.SrcId == b.SrcId && a.FlowKey == b.FlowKey && a.DstId == b.DstId && a.DstAddr == b.DstAddr &&
		a.IntervalMs == b.IntervalMs && a.PacketSize == b.PacketSize &&
		a.SourcePortCount == b.SourcePortCount && a.LossConfirmWindowMs == b.LossConfirmWindowMs &&
		a.ReportWindowMs == b.ReportWindowMs && a.State == b.State
}

func runSender(ctx context.Context, agentID string, sessionID uint32, cfg *pb.FlowConfig, seqs []*sequenceCounter, vrf string, reports chan<- *pb.ResultReport) {
	addr, err := net.ResolveUDPAddr("udp", cfg.DstAddr)
	if err != nil {
		log.Printf("resolve udp addr failed flow=%s err=%v", cfg.Id, err)
		return
	}
	conns, err := openUDPConns(ctx, addr, sourcePortCount(cfg), vrf)
	if err != nil {
		log.Printf("dial udp failed flow=%s err=%v", cfg.Id, err)
		return
	}
	defer closeUDPConns(conns)

	interval := time.Duration(cfg.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	if len(seqs) < len(conns) {
		for len(seqs) < len(conns) {
			seqs = append(seqs, &sequenceCounter{})
		}
	}
	nextConn := 0
	errLimiter := repeatedErrorLimiter{}
	buckets := map[time.Time]*senderBucketStats{}
	states := make([]senderConnState, len(conns))
	for i := range states {
		states[i].pending = map[uint64]senderPending{}
		states[i].received = map[uint64]senderReceived{}
	}
	responseBuf := make([]byte, 65535)
	confirmWindow := lossConfirmWindow(cfg.LossConfirmWindowMs)
	nextSendAt := time.Now().Add(interval)
	nextCatchUpLogAt := time.Time{}
	timer := time.NewTimer(time.Until(nextSendAt))
	defer stopAndDrainTimer(timer)

	for {
		select {
		case <-ctx.Done():
			now := time.Now()
			flushSenderReports(ctx, agentID, sessionID, cfg, reports, buckets, now.Add(time.Duration(flowReportWindowMs(cfg))*time.Millisecond))
			return
		case <-timer.C:
			wakeAt := time.Now()
			drainSenderResponses(conns, states, buckets, responseBuf, cfg, sessionID, wakeAt, -1)
			expireSenderPending(states, wakeAt)
			lag := senderLag(wakeAt, nextSendAt)
			windowMs := flowReportWindowMs(cfg)
			sendCount, nextPlannedSendAt, skippedDue := senderCatchUpPlanForWindow(nextSendAt, wakeAt, interval, senderBucketRemainingCapacityForWindow(buckets, interval, wakeAt, windowMs), windowMs)
			if sendCount == 0 && skippedDue == 0 {
				resetTimer(timer, time.Until(nextSendAt))
				continue
			}
			probeConn := -1
			for i := 0; i < sendCount; i++ {
				sendAt := time.Now()
				probeConn = nextConn
				err = sendSenderRequest(conns, addr, &nextConn, seqs, states, buckets, cfg, sessionID, confirmWindow, sendAt)
				if isFatalSenderError(err) {
					log.Printf("send udp failed flow=%s err=%v", cfg.Id, err)
					return
				}
				if err != nil {
					suppressed, ok := errLimiter.shouldLog(err, sendAt, repeatedSendErrorLogInterval)
					if ok {
						if suppressed > 0 {
							log.Printf("send udp failed flow=%s err=%v suppressed=%d", cfg.Id, err, suppressed)
						} else {
							log.Printf("send udp failed flow=%s err=%v", cfg.Id, err)
						}
					}
				}
			}
			now := time.Now()
			drainSenderResponses(conns, states, buckets, responseBuf, cfg, sessionID, now, probeConn)
			expireSenderPending(states, now)
			nextSendAt = nextPlannedSendAt
			if shouldLogSenderCatchUp(wakeAt, lag, interval, sendCount, skippedDue, nextCatchUpLogAt) {
				log.Printf("sender catch-up flow=%s lag=%s sent=%d skipped_due=%d next_send_at=%s", cfg.Id, lag, sendCount, skippedDue, nextSendAt.UTC().Format(time.RFC3339Nano))
				nextCatchUpLogAt = wakeAt.Add(senderCatchUpLogInterval)
			}
			flushSenderReports(ctx, agentID, sessionID, cfg, reports, buckets, now)
			resetTimer(timer, time.Until(nextSendAt))
		}
	}
}

func senderCatchUpPlan(nextSendAt, wakeAt time.Time, interval time.Duration, remainingCapacity int) (int, time.Time, int) {
	return senderCatchUpPlanForWindow(nextSendAt, wakeAt, interval, remainingCapacity, defaultReportWindowMs)
}

func senderCatchUpPlanForWindow(nextSendAt, wakeAt time.Time, interval time.Duration, remainingCapacity int, windowMs uint32) (int, time.Time, int) {
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	if nextSendAt.After(wakeAt) {
		return 0, nextSendAt, 0
	}
	totalDue := 0
	planned := nextSendAt
	for !planned.After(wakeAt) {
		totalDue++
		planned = planned.Add(interval)
	}
	if remainingCapacity <= 0 {
		return 0, senderBucketEndForWindow(wakeAt, windowMs), totalDue
	}
	if totalDue > remainingCapacity {
		return remainingCapacity, senderBucketEndForWindow(wakeAt, windowMs), totalDue - remainingCapacity
	}
	return totalDue, nextSendAt.Add(time.Duration(totalDue) * interval), 0
}

func shouldLogSenderCatchUp(now time.Time, lag, interval time.Duration, sent, skipped int, nextLogAt time.Time) bool {
	if skipped == 0 && sent <= 1 && lag < 2*interval {
		return false
	}
	if !nextLogAt.IsZero() && now.Before(nextLogAt) {
		return false
	}
	return true
}

func senderLag(now, nextSendAt time.Time) time.Duration {
	if now.Before(nextSendAt) {
		return 0
	}
	return now.Sub(nextSendAt)
}

func addRecoveryLag(current, add, limit time.Duration) (time.Duration, bool) {
	if add <= 0 {
		return current, false
	}
	if limit <= 0 {
		limit = add
	}
	next := current + add
	if next > limit {
		return limit, true
	}
	return next, false
}

func senderBucketTxLimit(interval time.Duration) int {
	window := time.Duration(defaultReportWindowMs) * time.Millisecond
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	return int((window-1)/interval) + 1
}

func senderBucketRemainingCapacity(buckets map[time.Time]*senderBucketStats, interval time.Duration, now time.Time) int {
	return senderBucketRemainingCapacityForWindow(buckets, interval, now, defaultReportWindowMs)
}

func senderBucketRemainingCapacityForWindow(buckets map[time.Time]*senderBucketStats, interval time.Duration, now time.Time, windowMs uint32) int {
	window := time.Duration(windowMs) * time.Millisecond
	limit := int((window-1)/interval) + 1
	stats := buckets[reportBucketStartForWindow(now, windowMs)]
	if stats == nil {
		return limit
	}
	remaining := limit - int(stats.tx)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func senderRecoveryInterval(interval, recoveryLag time.Duration) time.Duration {
	if interval <= time.Millisecond {
		return interval
	}
	if recoveryLag < interval {
		return interval
	}
	if recoveryLag >= 2*interval && interval > 2*time.Millisecond {
		return interval - 2*time.Millisecond
	}
	return interval - time.Millisecond
}

func senderRecoveryActive(interval, recoveryLag time.Duration) bool {
	if interval <= 0 {
		return recoveryLag > 0
	}
	return recoveryLag >= interval
}

func senderRecoveryRepay(interval, nextInterval time.Duration) time.Duration {
	if nextInterval >= interval {
		return 0
	}
	return interval - nextInterval
}

func senderNextInterval(interval, recoveryLag time.Duration, remainingCapacity int) (time.Duration, time.Duration) {
	if recoveryLag > 0 && remainingCapacity > 0 {
		nextInterval := senderRecoveryInterval(interval, recoveryLag)
		return nextInterval, senderRecoveryRepay(interval, nextInterval)
	}
	return interval, 0
}

func senderBucketEnd(now time.Time) time.Time {
	return senderBucketEndForWindow(now, defaultReportWindowMs)
}

func senderBucketEndForWindow(now time.Time, windowMs uint32) time.Time {
	return reportBucketStartForWindow(now, windowMs).Add(time.Duration(windowMs) * time.Millisecond)
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if timer == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	timer.Reset(d)
}

func stopAndDrainTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func sendSenderRequest(conns []net.PacketConn, addr net.Addr, nextConn *int, seqs []*sequenceCounter, states []senderConnState, buckets map[time.Time]*senderBucketStats, cfg *pb.FlowConfig, sessionID uint32, confirmWindow time.Duration, now time.Time) error {
	if len(conns) == 0 || nextConn == nil {
		return nil
	}
	if *nextConn < 0 || *nextConn >= len(conns) {
		*nextConn = 0
	}
	p := protocol.Packet{
		Type:           protocol.TypeRequest,
		FlowKey:        cfg.FlowKey,
		Seq:            seqs[*nextConn].Next(),
		SenderTxTimeNS: now.UnixNano(),
		SessionID:      sessionID,
		IntervalMs:     cfg.IntervalMs,
	}
	payloadSize, err := protocol.PayloadSizeFromTotalIPPacketSize(p, cfg.PacketSize)
	if err != nil {
		return err
	}
	b, err := protocol.Encode(p, payloadSize)
	if err != nil {
		return err
	}
	_, err = conns[*nextConn].WriteTo(b, addr)
	if err == nil {
		bucket := reportBucketStartForWindow(now, flowReportWindowMs(cfg))
		stats := senderBucket(buckets, bucket)
		stats.tx++
		stats.seqs = append(stats.seqs, p.Seq)
		if len(states) > 0 {
			states[*nextConn].pending[p.Seq] = senderPending{bucket: bucket, deadline: now.Add(confirmWindow), senderTxTimeNS: p.SenderTxTimeNS}
		}
	}
	*nextConn = (*nextConn + 1) % len(conns)
	return err
}

func isFatalSenderError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "packet too small")
}

func drainSenderResponses(conns []net.PacketConn, states []senderConnState, buckets map[time.Time]*senderBucketStats, buf []byte, cfg *pb.FlowConfig, sessionID uint32, now time.Time, probeConn int) {
	for i, conn := range conns {
		if conn == nil {
			continue
		}
		waited := false
		for {
			deadline := time.Now()
			if i == probeConn && !waited {
				deadline = deadline.Add(senderResponseDrainTimeout)
				waited = true
			}
			_ = conn.SetReadDeadline(deadline)
			n, _, err := conn.ReadFrom(buf)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					break
				}
				break
			}
			p, err := protocol.Decode(buf[:n])
			if err != nil || p.Type != protocol.TypeResponse || p.FlowKey != cfg.FlowKey || p.SessionID != sessionID {
				continue
			}
			state, pending, ok := findSenderPending(states, i, p.Seq, p.SenderTxTimeNS)
			if !ok {
				if received, duplicate := findSenderReceived(states, i, p.Seq, p.SenderTxTimeNS); duplicate {
					senderBucket(buckets, received.bucket).duplicate++
				}
				continue
			}
			delete(state.pending, p.Seq)
			if state.received == nil {
				state.received = map[uint64]senderReceived{}
			}
			state.received[p.Seq] = senderReceived{
				bucket:         pending.bucket,
				deadline:       now.Add(lossConfirmWindow(cfg.LossConfirmWindowMs)),
				senderTxTimeNS: pending.senderTxTimeNS,
			}
			b := senderBucket(buckets, pending.bucket)
			if state.seenAny && p.Seq < state.highestSeq {
				b.reorder++
			}
			if !state.seenAny || p.Seq > state.highestSeq {
				state.highestSeq = p.Seq
				state.seenAny = true
			}
		}
	}
}

func findSenderPending(states []senderConnState, preferred int, seq uint64, senderTxTimeNS int64) (*senderConnState, senderPending, bool) {
	if preferred >= 0 && preferred < len(states) {
		if pending, ok := matchingSenderPending(states[preferred], seq, senderTxTimeNS); ok {
			return &states[preferred], pending, true
		}
	}
	for i := range states {
		if i == preferred {
			continue
		}
		if pending, ok := matchingSenderPending(states[i], seq, senderTxTimeNS); ok {
			return &states[i], pending, true
		}
	}
	return nil, senderPending{}, false
}

func matchingSenderPending(state senderConnState, seq uint64, senderTxTimeNS int64) (senderPending, bool) {
	pending, ok := state.pending[seq]
	if !ok || pending.senderTxTimeNS != senderTxTimeNS {
		return senderPending{}, false
	}
	return pending, true
}

func findSenderReceived(states []senderConnState, preferred int, seq uint64, senderTxTimeNS int64) (senderReceived, bool) {
	if preferred >= 0 && preferred < len(states) {
		if received, ok := matchingSenderReceived(states[preferred], seq, senderTxTimeNS); ok {
			return received, true
		}
	}
	for i := range states {
		if i == preferred {
			continue
		}
		if received, ok := matchingSenderReceived(states[i], seq, senderTxTimeNS); ok {
			return received, true
		}
	}
	return senderReceived{}, false
}

func matchingSenderReceived(state senderConnState, seq uint64, senderTxTimeNS int64) (senderReceived, bool) {
	received, ok := state.received[seq]
	if !ok || received.senderTxTimeNS != senderTxTimeNS {
		return senderReceived{}, false
	}
	return received, true
}

func expireSenderPending(states []senderConnState, now time.Time) {
	for i := range states {
		for seq, pending := range states[i].pending {
			if pending.deadline.After(now) {
				continue
			}
			delete(states[i].pending, seq)
		}
		for seq, received := range states[i].received {
			if received.deadline.After(now) {
				continue
			}
			delete(states[i].received, seq)
		}
	}
}

func senderBucket(buckets map[time.Time]*senderBucketStats, bucket time.Time) *senderBucketStats {
	b := buckets[bucket]
	if b == nil {
		b = &senderBucketStats{}
		buckets[bucket] = b
	}
	return b
}

func senderBucketHasPending(states []senderConnState, bucket time.Time) bool {
	for i := range states {
		for _, pending := range states[i].pending {
			if pending.bucket.Equal(bucket) {
				return true
			}
		}
	}
	return false
}

func flushSenderReports(ctx context.Context, agentID string, sessionID uint32, cfg *pb.FlowConfig, reports chan<- *pb.ResultReport, buckets map[time.Time]*senderBucketStats, now time.Time) {
	if reports == nil {
		for bucket := range buckets {
			delete(buckets, bucket)
		}
		return
	}
	windowMs := flowReportWindowMs(cfg)
	cutoff := reportBucketStartForWindow(now, windowMs)
	for bucket, stats := range buckets {
		if !bucket.Before(cutoff) {
			continue
		}
		report := &pb.ResultReport{
			Ts:         bucket.Add(time.Duration(windowMs) * time.Millisecond).UTC().Format(time.RFC3339Nano),
			AgentId:    agentID,
			FlowKey:    cfg.FlowKey,
			Src:        cfg.SrcId,
			Dst:        cfg.DstId,
			FlowId:     cfg.Id,
			SessionId:  sessionID,
			IntervalMs: effectiveIntervalMs(cfg.IntervalMs),
			WindowMs:   windowMs,
			Role:       "sender",
			Duplicate:  stats.duplicate,
			Reorder:    stats.reorder,
			SeqRanges:  compressSeqRanges(stats.seqs),
		}
		select {
		case reports <- report:
			delete(buckets, bucket)
		case <-ctx.Done():
			return
		}
	}
}

func flowReportWindowMs(cfg *pb.FlowConfig) uint32 {
	if cfg == nil {
		return defaultReportWindowMs
	}
	return effectiveReportWindowMs(cfg.ReportWindowMs, cfg.IntervalMs)
}

func compressSeqRanges(seqs []uint64) []*pb.SeqRange {
	if len(seqs) == 0 {
		return nil
	}
	sorted := append([]uint64(nil), seqs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	out := make([]*pb.SeqRange, 0, len(sorted))
	start := sorted[0]
	end := sorted[0]
	for i := 1; i < len(sorted); i++ {
		switch {
		case sorted[i] == end:
			continue
		case sorted[i] == end+1:
			end = sorted[i]
		default:
			out = append(out, &pb.SeqRange{Start: start, End: end})
			start = sorted[i]
			end = sorted[i]
		}
	}
	out = append(out, &pb.SeqRange{Start: start, End: end})
	return out
}

func sourcePortCount(cfg *pb.FlowConfig) uint32 {
	if cfg.SourcePortCount == 0 {
		return 1
	}
	return cfg.SourcePortCount
}

func openUDPConns(ctx context.Context, dst *net.UDPAddr, count uint32, vrf ...string) ([]net.PacketConn, error) {
	if count == 0 {
		count = 1
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	selectedVRF := ""
	if len(vrf) > 0 {
		selectedVRF = vrf[0]
	}
	network, listenAddr := senderListenNetworkAndAddr(dst)
	conns := make([]net.PacketConn, 0, count)
	for i := uint32(0); i < count; i++ {
		conn, err := listenPacketWithVRF(ctx, network, listenAddr, selectedVRF)
		if err != nil {
			closeUDPConns(conns)
			return nil, err
		}
		conns = append(conns, conn)
	}
	return conns, nil
}

func senderListenNetworkAndAddr(dst *net.UDPAddr) (string, string) {
	if dst != nil && dst.IP != nil && dst.IP.To4() == nil {
		return "udp6", "[::]:0"
	}
	return "udp4", "0.0.0.0:0"
}

func closeUDPConns(conns []net.PacketConn) {
	for _, conn := range conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
}

func validateSnapshotPacketSizes(agentID string, snap *pb.ConfigSnapshot) error {
	seen := map[string]bool{}
	for _, cfg := range snap.Flows {
		if seen[cfg.Id] || cfg.State != "running" {
			continue
		}
		seen[cfg.Id] = true
		if _, err := packetPayloadSize(agentID, cfg); err != nil {
			return fmt.Errorf("invalid packet size for flow %s: %w", cfg.Id, err)
		}
	}
	return nil
}

func packetPayloadSize(agentID string, cfg *pb.FlowConfig) (uint32, error) {
	_ = agentID
	p := protocol.Packet{
		FlowKey:    cfg.FlowKey,
		IntervalMs: cfg.IntervalMs,
	}
	return protocol.PayloadSizeFromTotalIPPacketSize(p, cfg.PacketSize)
}
