package agent

import (
	"context"
	"errors"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"pakeloss/internal/pb"
	"pakeloss/internal/protocol"
)

type UDPReceiver struct {
	agentID  string
	addr     string
	vrf      string
	results  chan<- *pb.ResultReport
	reflects chan receiverReflectRequest
	mu       sync.Mutex
	configs  map[uint32]receiverFlowConfig
	buckets  map[receiverBucketKey]*receiverBucketStats
	now      func() time.Time
}

type receiverReflectRequest struct {
	dst net.Addr
	p   protocol.Packet
	rx  time.Time
}

type receiverFlowConfig struct {
	flowID         string
	srcID          string
	dstID          string
	intervalMs     uint32
	reportWindowMs uint32
}

type receiverBucketKey struct {
	flowKey     uint32
	sessionID   uint32
	bucketStart int64
	intervalMs  uint32
	windowMs    uint32
}

type receiverBucketStats struct {
	flowKey     uint32
	sessionID   uint32
	bucketStart time.Time
	intervalMs  uint32
	windowMs    uint32
	seqs        []uint64
}

const defaultReportWindowMs uint32 = 100

const (
	defaultReflectQueueSize = 256
	reflectWorkerCount      = 2
	reflectDropLogInterval  = 10 * time.Second
)

var errReflectQueueFull = errors.New("reflect queue full")

func NewUDPReceiver(agentID, addr string, results chan<- *pb.ResultReport, listenVRF ...string) *UDPReceiver {
	vrf := ""
	if len(listenVRF) > 0 {
		vrf = listenVRF[0]
	}
	return &UDPReceiver{
		agentID:  agentID,
		addr:     addr,
		vrf:      vrf,
		results:  results,
		reflects: make(chan receiverReflectRequest, defaultReflectQueueSize),
		configs:  map[uint32]receiverFlowConfig{},
		buckets:  map[receiverBucketKey]*receiverBucketStats{},
		now:      time.Now,
	}
}

func (r *UDPReceiver) ApplyConfig(snap *pb.ConfigSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()

	configs := make(map[uint32]receiverFlowConfig, len(snap.Flows))
	for _, cfg := range snap.Flows {
		configs[cfg.FlowKey] = receiverFlowConfig{
			flowID:         cfg.Id,
			srcID:          cfg.SrcId,
			dstID:          cfg.DstId,
			intervalMs:     effectiveIntervalMs(cfg.IntervalMs),
			reportWindowMs: effectiveReportWindowMs(cfg.ReportWindowMs, cfg.IntervalMs),
		}
	}
	r.configs = configs
}

func (r *UDPReceiver) Run(ctx context.Context) error {
	udpAddr, err := net.ResolveUDPAddr("udp", r.addr)
	if err != nil {
		return err
	}
	conn, err := listenPacketWithVRF(ctx, "udp", udpAddr.String(), r.vrf)
	if err != nil {
		return err
	}
	defer conn.Close()
	go r.aggregate(ctx)
	for i := 0; i < reflectWorkerCount; i++ {
		go r.reflectWorker(ctx, conn)
	}

	buf := make([]byte, 65535)
	errLimiter := repeatedErrorLimiter{}
	for {
		_ = conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
		n, srcAddr, err := conn.ReadFrom(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return nil
				default:
					continue
				}
			}
			log.Printf("udp receive failed: %v", err)
			continue
		}
		p, err := protocol.Decode(buf[:n])
		if err != nil {
			log.Printf("udp decode failed: %v", err)
			continue
		}
		source := ""
		if srcAddr != nil {
			source = srcAddr.String()
		}
		if p.Type != protocol.TypeRequest {
			continue
		}
		now := time.Now()
		r.recordFromAt(p, source, now)
		if err := r.enqueueReflect(srcAddr, p, now); err != nil {
			suppressed, ok := errLimiter.shouldLog(err, now, reflectDropLogInterval)
			if ok {
				if suppressed > 0 {
					log.Printf("udp reflect dropped flow_key=%08x src=%s err=%v suppressed=%d", p.FlowKey, source, err, suppressed)
				} else {
					log.Printf("udp reflect dropped flow_key=%08x src=%s err=%v", p.FlowKey, source, err)
				}
			}
		}
	}
}

func (r *UDPReceiver) handleRequestPacket(conn net.PacketConn, dst net.Addr, p protocol.Packet, source string, now time.Time) error {
	r.recordFromAt(p, source, now)
	return r.reflectPacket(conn, dst, p, now)
}

func (r *UDPReceiver) enqueueReflect(dst net.Addr, p protocol.Packet, now time.Time) error {
	if dst == nil {
		return nil
	}
	req := receiverReflectRequest{dst: dst, p: p, rx: now}
	select {
	case r.reflects <- req:
		return nil
	default:
		return errReflectQueueFull
	}
}

func (r *UDPReceiver) reflectWorker(ctx context.Context, conn net.PacketConn) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-r.reflects:
			if err := r.reflectPacket(conn, req.dst, req.p, req.rx); err != nil {
				log.Printf("udp reflect failed flow_key=%08x err=%v", req.p.FlowKey, err)
			}
		}
	}
}

func (r *UDPReceiver) reflectPacket(conn net.PacketConn, dst net.Addr, p protocol.Packet, rx time.Time) error {
	if conn == nil || dst == nil {
		return nil
	}
	cfg := r.flowConfig(p.FlowKey)
	if cfg.flowID == "" {
		return nil
	}
	resp := protocol.Packet{
		Type:           protocol.TypeResponse,
		FlowKey:        p.FlowKey,
		Seq:            p.Seq,
		SenderTxTimeNS: p.SenderTxTimeNS,
		ReflectorRxNS:  rx.UnixNano(),
		ReflectorTxNS:  time.Now().UnixNano(),
		SessionID:      p.SessionID,
		IntervalMs:     p.IntervalMs,
	}
	payloadSize, err := protocol.PayloadSizeFromTotalIPPacketSize(resp, packetTotalSizeFromPayload(len(p.Payload)))
	if err != nil {
		return err
	}
	b, err := protocol.Encode(resp, payloadSize)
	if err != nil {
		return err
	}
	_, err = conn.WriteTo(b, dst)
	return err
}

func (r *UDPReceiver) flowConfig(flowKey uint32) receiverFlowConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.flowConfigLocked(flowKey)
}

func packetTotalSizeFromPayload(payloadLen int) uint32 {
	encodedLen, err := protocol.EncodedLen(protocol.Packet{})
	if err != nil {
		return uint32(protocol.IPUDPHeaderSize + payloadLen)
	}
	return uint32(protocol.IPUDPHeaderSize + encodedLen + payloadLen)
}

func (r *UDPReceiver) record(p protocol.Packet) {
	r.recordFromAt(p, "", r.now())
}

func (r *UDPReceiver) recordFrom(p protocol.Packet, source string) {
	r.recordFromAt(p, source, r.now())
}

func (r *UDPReceiver) recordFromAt(p protocol.Packet, source string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.bucketLocked(p.FlowKey, p.SessionID, p.IntervalMs, now)
	b.seqs = append(b.seqs, p.Seq)
	_ = source
}

func (r *UDPReceiver) aggregate(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, res := range r.snapshot(now) {
				select {
				case r.results <- res:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (r *UDPReceiver) snapshot(now time.Time) []*pb.ResultReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]receiverBucketKey, 0, len(r.buckets))
	for key := range r.buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].bucketStart == keys[j].bucketStart {
			return keys[i].flowKey < keys[j].flowKey
		}
		return keys[i].bucketStart < keys[j].bucketStart
	})
	out := make([]*pb.ResultReport, 0, len(keys))
	for _, key := range keys {
		b := r.buckets[key]
		if !b.bucketStart.Add(time.Duration(b.windowMs)*time.Millisecond).Before(now) && !b.bucketStart.Add(time.Duration(b.windowMs)*time.Millisecond).Equal(now) {
			continue
		}
		cfg := r.flowConfigLocked(b.flowKey)
		res := &pb.ResultReport{
			Ts:         b.bucketStart.Add(time.Duration(b.windowMs) * time.Millisecond).UTC().Format(time.RFC3339Nano),
			AgentId:    r.agentID,
			FlowKey:    b.flowKey,
			Src:        cfg.srcID,
			Dst:        cfg.dstID,
			FlowId:     cfg.flowID,
			SessionId:  b.sessionID,
			IntervalMs: b.intervalMs,
			WindowMs:   b.windowMs,
			Role:       "receiver",
			SeqRanges:  compressSeqRanges(b.seqs),
		}
		out = append(out, res)
		delete(r.buckets, key)
	}
	return out
}

func (r *UDPReceiver) flowConfigLocked(flowKey uint32) receiverFlowConfig {
	cfg := r.configs[flowKey]
	return cfg
}

func (r *UDPReceiver) bucketLocked(flowKey, sessionID, intervalMs uint32, t time.Time) *receiverBucketStats {
	cfg := r.flowConfigLocked(flowKey)
	windowMs := cfg.reportWindowMs
	start := reportBucketStartForWindow(t, windowMs)
	key := receiverBucketKey{flowKey: flowKey, sessionID: sessionID, bucketStart: start.UnixNano(), intervalMs: intervalMs, windowMs: windowMs}
	b := r.buckets[key]
	if b == nil {
		b = &receiverBucketStats{flowKey: flowKey, sessionID: sessionID, bucketStart: start, intervalMs: intervalMs, windowMs: windowMs}
		r.buckets[key] = b
	}
	return b
}

func lossConfirmWindow(ms uint32) time.Duration {
	if ms == 0 {
		ms = 2000
	}
	return time.Duration(ms) * time.Millisecond
}

func effectiveIntervalMs(intervalMs uint32) uint32 {
	if intervalMs == 0 {
		return 10
	}
	return intervalMs
}

func reportBucketStart(t time.Time) time.Time {
	return reportBucketStartForWindow(t, defaultReportWindowMs)
}

func reportBucketStartForWindow(t time.Time, windowMs uint32) time.Time {
	if windowMs == 0 {
		windowMs = defaultReportWindowMs
	}
	return t.UTC().Truncate(time.Duration(windowMs) * time.Millisecond)
}

func effectiveReportWindowMs(windowMs, intervalMs uint32) uint32 {
	if windowMs > 0 {
		return windowMs
	}
	intervalMs = effectiveIntervalMs(intervalMs)
	return intervalMs * 10
}
