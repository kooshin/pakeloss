package agent

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"pakeloss/internal/pb"
	"pakeloss/internal/protocol"
)

func TestSequenceCounterContinuesAcrossFlowRestart(t *testing.T) {
	m := NewFlowManager("node-a")

	first := m.sequenceCounter("node-a<=>node-b")
	if first.Next() != 0 || first.Next() != 1 {
		t.Fatal("initial sequence did not start at zero and advance")
	}

	restarted := m.sequenceCounter("node-a<=>node-b")
	if restarted.Next() != 2 {
		t.Fatal("sequence reset across flow restart")
	}
}

func TestSequenceCounterIsPerFlow(t *testing.T) {
	m := NewFlowManager("node-a")
	if m.sequenceCounter("flow-a").Next() != 0 {
		t.Fatal("flow-a should start at zero")
	}
	if m.sequenceCounter("flow-b").Next() != 0 {
		t.Fatal("flow-b should have an independent sequence")
	}
}

func TestSequenceCountersShareSequenceSpaceAcrossSourcePorts(t *testing.T) {
	m := NewFlowManager("node-a")
	seqs := m.sequenceCounters("node-a<=>node-b", 2)
	if len(seqs) != 2 {
		t.Fatalf("sequence counter count = %d, want 2", len(seqs))
	}
	if seqs[0] != seqs[1] {
		t.Fatal("source ports should share a flow-wide sequence counter")
	}
	if seqs[0].Next() != 0 || seqs[1].Next() != 1 {
		t.Fatal("source ports should draw from a single monotonic sequence space")
	}
}

func TestSequenceCountersGrowWithoutReset(t *testing.T) {
	m := NewFlowManager("node-a")
	seqs := m.sequenceCounters("node-a<=>node-b", 1)
	if seqs[0].Next() != 0 || seqs[0].Next() != 1 {
		t.Fatal("initial source port sequence did not advance")
	}

	seqs = m.sequenceCounters("node-a<=>node-b", 2)
	if seqs[0].Next() != 2 {
		t.Fatal("existing flow sequence should not reset when count grows")
	}
	if seqs[1].Next() != 3 {
		t.Fatal("new source port should continue the shared flow sequence")
	}
}

func TestSequenceCounterWraparound(t *testing.T) {
	c := &sequenceCounter{next: math.MaxUint64 - 1}
	if c.Next() != math.MaxUint64-1 {
		t.Fatal("sequence should return max-1")
	}
	if c.Next() != math.MaxUint64 {
		t.Fatal("sequence should return max")
	}
	if c.Next() != 0 {
		t.Fatal("sequence should wrap to zero")
	}
}

func TestEqualFlowConfigIncludesSourcePortCount(t *testing.T) {
	a := &pb.FlowConfig{Id: "flow-a", SrcId: "node-a", FlowKey: 1, DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 10, PacketSize: 96, SourcePortCount: 4, State: "running"}
	b := *a
	if !equalFlowConfig(a, &b) {
		t.Fatal("identical configs should be equal")
	}
	b.SourcePortCount = 8
	if equalFlowConfig(a, &b) {
		t.Fatal("source port count change should restart the flow")
	}
}

func TestEqualFlowConfigIncludesLossConfirmWindow(t *testing.T) {
	a := &pb.FlowConfig{Id: "flow-a", SrcId: "node-a", FlowKey: 1, DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 10, PacketSize: 96, SourcePortCount: 4, LossConfirmWindowMs: 2000, State: "running"}
	b := *a
	b.LossConfirmWindowMs = 3000
	if equalFlowConfig(a, &b) {
		t.Fatal("loss confirm window change should restart the flow")
	}
}

func TestRepeatedErrorLimiterSuppressesSameError(t *testing.T) {
	limiter := repeatedErrorLimiter{}
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	errA := errors.New("connection refused")

	suppressed, ok := limiter.shouldLog(errA, now, 10*time.Second)
	if !ok || suppressed != 0 {
		t.Fatalf("first error should be logged, ok=%v suppressed=%d", ok, suppressed)
	}

	suppressed, ok = limiter.shouldLog(errA, now.Add(time.Second), 10*time.Second)
	if ok || suppressed != 0 {
		t.Fatalf("same error inside interval should be suppressed, ok=%v suppressed=%d", ok, suppressed)
	}

	suppressed, ok = limiter.shouldLog(errA, now.Add(11*time.Second), 10*time.Second)
	if !ok || suppressed != 1 {
		t.Fatalf("same error after interval should log suppressed count, ok=%v suppressed=%d", ok, suppressed)
	}
}

func TestRepeatedErrorLimiterLogsDifferentErrorImmediately(t *testing.T) {
	limiter := repeatedErrorLimiter{}
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)

	if _, ok := limiter.shouldLog(errors.New("connection refused"), now, 10*time.Second); !ok {
		t.Fatal("first error should be logged")
	}
	suppressed, ok := limiter.shouldLog(errors.New("network unreachable"), now.Add(time.Second), 10*time.Second)
	if !ok || suppressed != 0 {
		t.Fatalf("different error should be logged immediately, ok=%v suppressed=%d", ok, suppressed)
	}
}

func TestRepeatedErrorLimiterTreatsDifferentSourcePortsAsSameError(t *testing.T) {
	limiter := repeatedErrorLimiter{}
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)

	first := errors.New("write udp 127.0.0.1:45997->127.0.0.1:40003: write: connection refused")
	second := errors.New("write udp 127.0.0.1:48423->127.0.0.1:40003: write: connection refused")

	suppressed, ok := limiter.shouldLog(first, now, 10*time.Second)
	if !ok || suppressed != 0 {
		t.Fatalf("first port-specific error should be logged, ok=%v suppressed=%d", ok, suppressed)
	}

	suppressed, ok = limiter.shouldLog(second, now.Add(time.Second), 10*time.Second)
	if ok || suppressed != 0 {
		t.Fatalf("same underlying error should be suppressed despite source port changes, ok=%v suppressed=%d", ok, suppressed)
	}
}

func TestApplyWaitsForOldSenderBeforeRestartingFlow(t *testing.T) {
	m := NewFlowManager("node-a")

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	secondDone := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var releaseOnce sync.Once
	releaseFirst := func() {
		releaseOnce.Do(func() {
			close(firstRelease)
		})
	}
	defer releaseFirst()

	var mu sync.Mutex
	calls := 0
	m.startSender = func(ctx context.Context, agentID string, sessionID uint32, cfg *pb.FlowConfig, seqs []*sequenceCounter, vrf string, reports chan<- *pb.ResultReport) {
		mu.Lock()
		calls++
		call := calls
		mu.Unlock()

		switch call {
		case 1:
			close(firstStarted)
			<-ctx.Done()
			<-firstRelease
		case 2:
			close(secondStarted)
			<-ctx.Done()
			close(secondDone)
		default:
			t.Fatalf("unexpected sender call %d", call)
		}
	}

	first := &pb.ConfigSnapshot{Flows: []*pb.FlowConfig{
		{Id: "flow-a", FlowKey: 1, DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 10, PacketSize: 96, State: "running"},
	}}
	if err := m.Apply(ctx, first); err != nil {
		t.Fatal(err)
	}
	<-firstStarted

	restarted := &pb.ConfigSnapshot{Flows: []*pb.FlowConfig{
		{Id: "flow-a", FlowKey: 1, DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 20, PacketSize: 96, State: "running"},
	}}

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Apply(ctx, restarted)
	}()

	select {
	case <-secondStarted:
		t.Fatal("new sender started before old sender finished")
	case <-time.After(100 * time.Millisecond):
	}

	releaseFirst()

	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("new sender did not start after old sender finished")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Apply did not return")
	}

	cancel()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second sender did not exit after cancel")
	}
}

func TestApplySkipsDuplicateFlowIDs(t *testing.T) {
	m := NewFlowManager("node-a")

	firstStarted := make(chan struct{})
	duplicateStarted := make(chan struct{})
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	calls := 0
	m.startSender = func(ctx context.Context, agentID string, sessionID uint32, cfg *pb.FlowConfig, seqs []*sequenceCounter, vrf string, reports chan<- *pb.ResultReport) {
		mu.Lock()
		calls++
		call := calls
		mu.Unlock()

		switch call {
		case 1:
			close(firstStarted)
			<-ctx.Done()
			close(done)
		case 2:
			close(duplicateStarted)
			<-ctx.Done()
		default:
			t.Fatalf("unexpected sender call %d", call)
		}
	}

	snap := &pb.ConfigSnapshot{Flows: []*pb.FlowConfig{
		{Id: "flow-a", FlowKey: 1, DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 10, PacketSize: 96, State: "running"},
		{Id: "flow-a", FlowKey: 1, DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 20, PacketSize: 96, State: "running"},
	}}
	if err := m.Apply(ctx, snap); err != nil {
		t.Fatal(err)
	}

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first sender did not start")
	}

	select {
	case <-duplicateStarted:
		t.Fatal("duplicate flow started twice")
	case <-time.After(100 * time.Millisecond):
	}

	if got := m.ActiveFlows(); got != 1 {
		t.Fatalf("ActiveFlows() = %d, want 1", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sender did not exit after cancel")
	}
}

func TestApplyRejectsTooSmallPacketSize(t *testing.T) {
	m := NewFlowManager("node-a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := m.Apply(ctx, &pb.ConfigSnapshot{Flows: []*pb.FlowConfig{
		{Id: "flow-a", DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 10, PacketSize: 32, State: "running"},
	}})
	if err == nil {
		t.Fatal("expected invalid packet size error")
	}
}

func TestApplyDoesNotStartInboundFlow(t *testing.T) {
	m := NewFlowManager("node-b")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{}, 1)
	m.startSender = func(ctx context.Context, agentID string, sessionID uint32, cfg *pb.FlowConfig, seqs []*sequenceCounter, vrf string, reports chan<- *pb.ResultReport) {
		started <- struct{}{}
		<-ctx.Done()
	}

	err := m.Apply(ctx, &pb.ConfigSnapshot{Flows: []*pb.FlowConfig{
		{Id: "flow-a", SrcId: "node-a", DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 10, PacketSize: 96, State: "running"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
		t.Fatal("inbound flow should not start sender")
	case <-time.After(100 * time.Millisecond):
	}
	if got := m.ActiveFlows(); got != 0 {
		t.Fatalf("ActiveFlows() = %d, want 0", got)
	}
}

func TestPacketPayloadSizeUsesTotalIPPacketSize(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 0x12345678, DstId: "node-b", IntervalMs: 10, PacketSize: 96}
	got, err := packetPayloadSize("node-a", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if want := uint32(96 - protocol.IPUDPHeaderSize); got != want {
		t.Fatalf("payload size = %d, want %d", got, want)
	}
}

func TestDrainSenderResponsesCountsReflectedReply(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	resp := protocol.Packet{Type: protocol.TypeResponse, FlowKey: 1, Seq: 7, SenderTxTimeNS: 100, SessionID: 99, IntervalMs: 10}
	payloadSize, err := protocol.PayloadSizeFromTotalIPPacketSize(resp, 96)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := protocol.Encode(resp, payloadSize)
	if err != nil {
		t.Fatal(err)
	}
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: &senderBucketStats{tx: 1}}
	states := []senderConnState{{pending: map[uint64]senderPending{7: {bucket: bucket, deadline: bucket.Add(time.Second), senderTxTimeNS: 100}}}}
	conn := &fakeSenderConn{reads: [][]byte{raw}, readAddrs: []net.Addr{mustResolveUDPAddr(t, "192.0.2.10:9999")}}

	drainSenderResponses([]net.PacketConn{conn}, states, buckets, make([]byte, 1500), cfg, 99, bucket, -1)

	if len(states[0].pending) != 0 {
		t.Fatalf("pending after response = %+v", states[0].pending)
	}
	if received, ok := states[0].received[7]; !ok || received.senderTxTimeNS != 100 {
		t.Fatalf("received response was not tracked: %+v", states[0].received)
	}
}

func TestDrainSenderResponsesCountsOnlyRepeatedResponseAsDuplicate(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	resp := protocol.Packet{Type: protocol.TypeResponse, FlowKey: 1, Seq: 7, SenderTxTimeNS: 100, SessionID: 99, IntervalMs: 10}
	raw := mustEncodeSenderPacket(t, resp)
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: {tx: 1}}
	states := []senderConnState{{pending: map[uint64]senderPending{7: {bucket: bucket, deadline: bucket.Add(time.Second), senderTxTimeNS: 100}}}}
	conn := &fakeSenderConn{reads: [][]byte{raw, raw}}

	drainSenderResponses([]net.PacketConn{conn}, states, buckets, make([]byte, 1500), cfg, 99, bucket, -1)

	if buckets[bucket].duplicate != 1 {
		t.Fatalf("duplicate = %d, want 1", buckets[bucket].duplicate)
	}
}

func TestDrainSenderResponsesIgnoresFirstResponseAfterPendingExpires(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	resp := protocol.Packet{Type: protocol.TypeResponse, FlowKey: 1, Seq: 7, SenderTxTimeNS: 100, SessionID: 99, IntervalMs: 10}
	raw := mustEncodeSenderPacket(t, resp)
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: {tx: 1}}
	states := []senderConnState{{pending: map[uint64]senderPending{7: {bucket: bucket, deadline: bucket.Add(time.Second), senderTxTimeNS: 100}}}}
	expireSenderPending(states, bucket.Add(time.Second))

	drainSenderResponses([]net.PacketConn{&fakeSenderConn{reads: [][]byte{raw}}}, states, buckets, make([]byte, 1500), cfg, 99, bucket.Add(time.Second), -1)

	if buckets[bucket].duplicate != 0 {
		t.Fatalf("expired first response counted as duplicate: %+v", buckets[bucket])
	}
}

func TestDrainSenderResponsesDoesNotMatchReceivedWithDifferentSenderTime(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	resp := protocol.Packet{Type: protocol.TypeResponse, FlowKey: 1, Seq: 7, SenderTxTimeNS: 200, SessionID: 99, IntervalMs: 10}
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: {tx: 1}}
	states := []senderConnState{{received: map[uint64]senderReceived{7: {bucket: bucket, deadline: bucket.Add(time.Second), senderTxTimeNS: 100}}}}

	drainSenderResponses([]net.PacketConn{&fakeSenderConn{reads: [][]byte{mustEncodeSenderPacket(t, resp)}}}, states, buckets, make([]byte, 1500), cfg, 99, bucket, -1)

	if buckets[bucket].duplicate != 0 {
		t.Fatalf("different sender time counted as duplicate: %+v", buckets[bucket])
	}
}

func TestDrainSenderResponsesFindsReceivedAcrossSourcePorts(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	resp := protocol.Packet{Type: protocol.TypeResponse, FlowKey: 1, Seq: 7, SenderTxTimeNS: 100, SessionID: 99, IntervalMs: 10}
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: {tx: 1}}
	states := []senderConnState{
		{received: map[uint64]senderReceived{7: {bucket: bucket, deadline: bucket.Add(time.Second), senderTxTimeNS: 100}}},
		{},
	}
	conns := []net.PacketConn{&fakeSenderConn{}, &fakeSenderConn{reads: [][]byte{mustEncodeSenderPacket(t, resp)}}}

	drainSenderResponses(conns, states, buckets, make([]byte, 1500), cfg, 99, bucket, 1)

	if buckets[bucket].duplicate != 1 {
		t.Fatalf("cross-port duplicate = %d, want 1", buckets[bucket].duplicate)
	}
}

func TestDrainSenderResponsesMatchesSenderTxTimeWithSameSeq(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	resp := protocol.Packet{Type: protocol.TypeResponse, FlowKey: 1, Seq: 7, SenderTxTimeNS: 200, SessionID: 99, IntervalMs: 10}
	payloadSize, err := protocol.PayloadSizeFromTotalIPPacketSize(resp, 96)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := protocol.Encode(resp, payloadSize)
	if err != nil {
		t.Fatal(err)
	}
	bucketA := time.Unix(10, 0).UTC()
	bucketB := time.Unix(11, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucketA: &senderBucketStats{tx: 1}, bucketB: &senderBucketStats{tx: 1}}
	states := []senderConnState{
		{pending: map[uint64]senderPending{7: {bucket: bucketA, deadline: bucketA.Add(time.Second), senderTxTimeNS: 100}}},
		{pending: map[uint64]senderPending{7: {bucket: bucketB, deadline: bucketB.Add(time.Second), senderTxTimeNS: 200}}},
	}
	conns := []net.PacketConn{
		&fakeSenderConn{reads: [][]byte{raw}},
		&fakeSenderConn{},
	}

	drainSenderResponses(conns, states, buckets, make([]byte, 1500), cfg, 99, bucketB, -1)

	if len(states[0].pending) != 1 {
		t.Fatalf("first pending was changed: bucket=%+v pending=%+v", buckets[bucketA], states[0].pending)
	}
	if len(states[1].pending) != 0 {
		t.Fatalf("second pending was not matched: bucket=%+v pending=%+v", buckets[bucketB], states[1].pending)
	}
}

func TestDrainSenderResponsesIgnoresMismatchedFlowOrSession(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	badFlow := protocol.Packet{Type: protocol.TypeResponse, FlowKey: 2, Seq: 7, SenderTxTimeNS: 100, SessionID: 99, IntervalMs: 10}
	badSession := protocol.Packet{Type: protocol.TypeResponse, FlowKey: 1, Seq: 7, SenderTxTimeNS: 100, SessionID: 100, IntervalMs: 10}
	rawBadFlow := mustEncodeSenderPacket(t, badFlow)
	rawBadSession := mustEncodeSenderPacket(t, badSession)
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: &senderBucketStats{tx: 1}}
	states := []senderConnState{{pending: map[uint64]senderPending{7: {bucket: bucket, deadline: bucket.Add(time.Second), senderTxTimeNS: 100}}}}
	conn := &fakeSenderConn{reads: [][]byte{rawBadFlow, rawBadSession}}

	drainSenderResponses([]net.PacketConn{conn}, states, buckets, make([]byte, 1500), cfg, 99, bucket, -1)

	if buckets[bucket].duplicate != 0 {
		t.Fatalf("bucket after ignored responses = %+v", buckets[bucket])
	}
	if len(states[0].pending) != 1 {
		t.Fatalf("pending after ignored responses = %+v", states[0].pending)
	}
}

func TestDrainSenderResponsesReadsQueuedPacketConnResponse(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	senderConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("UDP listen is not permitted in this environment: %v", err)
		}
		t.Fatal(err)
	}
	defer senderConn.Close()
	writer, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("UDP listen is not permitted in this environment: %v", err)
		}
		t.Fatal(err)
	}
	defer writer.Close()
	resp := protocol.Packet{Type: protocol.TypeResponse, FlowKey: 1, Seq: 7, SenderTxTimeNS: 100, SessionID: 99, IntervalMs: 10}
	if _, err := writer.WriteTo(mustEncodeSenderPacket(t, resp), senderConn.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: &senderBucketStats{tx: 1}}
	states := []senderConnState{{pending: map[uint64]senderPending{7: {bucket: bucket, deadline: bucket.Add(time.Second), senderTxTimeNS: 100}}}}

	drainSenderResponses([]net.PacketConn{senderConn}, states, buckets, make([]byte, 1500), cfg, 99, bucket, 0)

	if len(states[0].pending) != 0 {
		t.Fatalf("queued response was not drained: bucket=%+v pending=%+v", buckets[bucket], states[0].pending)
	}
}

func TestSendSenderRequestAdvancesAcrossSourcePortsBeforeDrain(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10, PacketSize: 96}
	now := time.Unix(10, 123456789).UTC()
	bucket := reportBucketStart(now)
	conns := []net.PacketConn{&fakeSenderConn{}, &fakeSenderConn{}}
	seqs := []*sequenceCounter{{}, {}}
	states := []senderConnState{
		{pending: map[uint64]senderPending{}},
		{pending: map[uint64]senderPending{}},
	}
	buckets := map[time.Time]*senderBucketStats{}
	nextConn := 0
	dst := mustResolveUDPAddr(t, "127.0.0.1:40002")

	if err := sendSenderRequest(conns, dst, &nextConn, seqs, states, buckets, cfg, 99, time.Second, now); err != nil {
		t.Fatal(err)
	}
	if err := sendSenderRequest(conns, dst, &nextConn, seqs, states, buckets, cfg, 99, time.Second, now.Add(10*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	first := conns[0].(*fakeSenderConn)
	second := conns[1].(*fakeSenderConn)
	if len(first.writes) != 1 || len(second.writes) != 1 {
		t.Fatalf("writes per source port = %d/%d, want 1/1", len(first.writes), len(second.writes))
	}
	if nextConn != 0 {
		t.Fatalf("nextConn = %d, want round-robin back to 0", nextConn)
	}
	if buckets[bucket].tx != 2 {
		t.Fatalf("bucket tx = %d, want 2", buckets[bucket].tx)
	}
	if len(states[0].pending) != 1 || len(states[1].pending) != 1 {
		t.Fatalf("pending per source port = %d/%d, want 1/1", len(states[0].pending), len(states[1].pending))
	}
}

func TestSenderLagOnTimeIsZero(t *testing.T) {
	nextSendAt := time.Date(2026, 7, 3, 12, 0, 0, 10*int(time.Millisecond), time.UTC)

	lag := senderLag(nextSendAt, nextSendAt)

	if lag != 0 {
		t.Fatalf("lag = %s, want 0", lag)
	}
}

func TestSenderLagTracksSubIntervalDelay(t *testing.T) {
	nextSendAt := time.Date(2026, 7, 3, 12, 0, 0, 10*int(time.Millisecond), time.UTC)
	now := nextSendAt.Add(1500 * time.Microsecond)

	lag := senderLag(now, nextSendAt)

	if lag != 1500*time.Microsecond {
		t.Fatalf("lag = %s, want 1.5ms", lag)
	}
}

func TestSenderLagTracksLongDelay(t *testing.T) {
	nextSendAt := time.Date(2026, 7, 3, 12, 0, 0, 10*int(time.Millisecond), time.UTC)
	now := nextSendAt.Add(80 * time.Millisecond)

	lag := senderLag(now, nextSendAt)

	if lag != 80*time.Millisecond {
		t.Fatalf("lag = %s, want 80ms", lag)
	}
}

func TestSenderBucketTxLimitForTenMillisecondsIsTenPerBucket(t *testing.T) {
	if got := senderBucketTxLimit(10 * time.Millisecond); got != 10 {
		t.Fatalf("senderBucketTxLimit(10ms) = %d, want 10", got)
	}
}

func TestSenderBucketRemainingCapacityUsesCurrentWindow(t *testing.T) {
	interval := 10 * time.Millisecond
	bucket := time.Date(2026, 7, 3, 12, 0, 0, 900*1e6, time.UTC)
	now := bucket.Add(50 * time.Millisecond)
	buckets := map[time.Time]*senderBucketStats{
		bucket: {tx: 9},
	}

	if got := senderBucketRemainingCapacity(buckets, interval, now); got != 1 {
		t.Fatalf("remaining = %d, want 1", got)
	}
	if got := senderBucketRemainingCapacity(buckets, interval, bucket.Add(100*time.Millisecond)); got != 10 {
		t.Fatalf("next bucket remaining = %d, want 10", got)
	}
}

func TestSenderRecoveryIntervalShortensByLag(t *testing.T) {
	if got := senderRecoveryInterval(10*time.Millisecond, time.Millisecond); got != 10*time.Millisecond {
		t.Fatalf("senderRecoveryInterval(10ms, lag=1ms) = %s, want 10ms", got)
	}
	if got := senderRecoveryInterval(10*time.Millisecond, 10*time.Millisecond); got != 9*time.Millisecond {
		t.Fatalf("senderRecoveryInterval(10ms, lag=10ms) = %s, want 9ms", got)
	}
	if got := senderRecoveryInterval(10*time.Millisecond, 20*time.Millisecond); got != 8*time.Millisecond {
		t.Fatalf("senderRecoveryInterval(10ms, lag=20ms) = %s, want 8ms", got)
	}
	if got := senderRecoveryInterval(10*time.Millisecond, 0); got != 10*time.Millisecond {
		t.Fatalf("senderRecoveryInterval(10ms, lag=0) = %s, want 10ms", got)
	}
}

func TestSenderRecoveryActiveStartsAtFullInterval(t *testing.T) {
	if senderRecoveryActive(10*time.Millisecond, 500*time.Microsecond) {
		t.Fatal("senderRecoveryActive(10ms, 500us) = true, want false")
	}
	if !senderRecoveryActive(10*time.Millisecond, 10*time.Millisecond) {
		t.Fatal("senderRecoveryActive(10ms, 10ms) = false, want true")
	}
}

func TestSenderNextIntervalUsesRecoveryOnlyWhileLagRemains(t *testing.T) {
	interval := 10 * time.Millisecond
	if got, repaid := senderNextInterval(interval, 3*time.Millisecond, 5); got != interval || repaid != 0 {
		t.Fatalf("sub-interval recovery = %s repay=%s, want %s/0", got, repaid, interval)
	}
	if got, repaid := senderNextInterval(interval, 10*time.Millisecond, 5); got != 9*time.Millisecond || repaid != time.Millisecond {
		t.Fatalf("full-interval recovery = %s repay=%s, want 9ms/1ms", got, repaid)
	}
	if got, repaid := senderNextInterval(interval, 0, 5); got != interval || repaid != 0 {
		t.Fatalf("normal interval without lag = %s repay=%s, want %s/0", got, repaid, interval)
	}
	if got, repaid := senderNextInterval(interval, 3*time.Millisecond, 0); got != interval || repaid != 0 {
		t.Fatalf("interval with full bucket = %s repay=%s, want %s/0", got, repaid, interval)
	}
}

func TestSenderNextSendAtStaysAnchoredToPlannedSchedule(t *testing.T) {
	interval := 10 * time.Millisecond
	planned := time.Date(2026, 7, 6, 8, 49, 0, 10*int(time.Millisecond), time.UTC)
	wakeAt := planned.Add(2 * time.Millisecond)

	lag := senderLag(wakeAt, planned)
	if lag != 2*time.Millisecond {
		t.Fatalf("lag = %s, want 2ms", lag)
	}
	nextInterval, repaid := senderNextInterval(interval, lag, 100)
	if nextInterval != interval || repaid != 0 {
		t.Fatalf("next interval = %s repay=%s, want %s/0", nextInterval, repaid, interval)
	}

	got := planned.Add(nextInterval)
	legacy := wakeAt.Add(nextInterval)
	want := time.Date(2026, 7, 6, 8, 49, 0, 20*int(time.Millisecond), time.UTC)

	if !got.Equal(want) {
		t.Fatalf("planned schedule nextSendAt = %v, want %v", got, want)
	}
	if !legacy.Equal(want.Add(2 * time.Millisecond)) {
		t.Fatalf("legacy schedule nextSendAt = %v, want %v", legacy, want.Add(2*time.Millisecond))
	}
}

func TestSenderCatchUpPlanSendsAllDueTicksWithinCapacity(t *testing.T) {
	interval := 10 * time.Millisecond
	nextSendAt := time.Date(2026, 7, 6, 8, 49, 0, 10*int(time.Millisecond), time.UTC)
	wakeAt := nextSendAt.Add(25 * time.Millisecond)

	sendCount, nextPlannedSendAt, skippedDue := senderCatchUpPlan(nextSendAt, wakeAt, interval, 10)

	if sendCount != 3 {
		t.Fatalf("sendCount = %d, want 3", sendCount)
	}
	if skippedDue != 0 {
		t.Fatalf("skippedDue = %d, want 0", skippedDue)
	}
	wantNext := nextSendAt.Add(30 * time.Millisecond)
	if !nextPlannedSendAt.Equal(wantNext) {
		t.Fatalf("nextPlannedSendAt = %v, want %v", nextPlannedSendAt, wantNext)
	}
}

func TestSenderCatchUpPlanSkipsRemainderWhenBucketCapacityIsExhausted(t *testing.T) {
	interval := 10 * time.Millisecond
	nextSendAt := time.Date(2026, 7, 6, 8, 49, 0, 10*int(time.Millisecond), time.UTC)
	wakeAt := time.Date(2026, 7, 6, 8, 49, 0, 95*int(time.Millisecond), time.UTC)

	sendCount, nextPlannedSendAt, skippedDue := senderCatchUpPlan(nextSendAt, wakeAt, interval, 3)

	if sendCount != 3 {
		t.Fatalf("sendCount = %d, want 3", sendCount)
	}
	if skippedDue != 6 {
		t.Fatalf("skippedDue = %d, want 6", skippedDue)
	}
	wantNext := time.Date(2026, 7, 6, 8, 49, 0, 100*int(time.Millisecond), time.UTC)
	if !nextPlannedSendAt.Equal(wantNext) {
		t.Fatalf("nextPlannedSendAt = %v, want %v", nextPlannedSendAt, wantNext)
	}
}

func TestShouldLogSenderCatchUpOnlyOnMeaningfulDelay(t *testing.T) {
	now := time.Date(2026, 7, 6, 8, 49, 0, 0, time.UTC)
	interval := 10 * time.Millisecond

	if shouldLogSenderCatchUp(now, time.Millisecond, interval, 1, 0, time.Time{}) {
		t.Fatal("unexpected log for normal single send")
	}
	if !shouldLogSenderCatchUp(now, 25*time.Millisecond, interval, 3, 0, time.Time{}) {
		t.Fatal("expected catch-up log for burst send")
	}
	if shouldLogSenderCatchUp(now, 25*time.Millisecond, interval, 3, 0, now.Add(time.Second)) {
		t.Fatal("unexpected log before limiter expires")
	}
}

func TestSenderPlannedScheduleAvoidsAccumulatedWakeLagDrift(t *testing.T) {
	interval := 10 * time.Millisecond
	const ticks = 100
	wakeLag := 2 * time.Millisecond
	start := time.Date(2026, 7, 6, 8, 49, 0, 0, time.UTC)
	planned := start
	legacy := start

	for i := 0; i < ticks; i++ {
		plannedWake := planned.Add(wakeLag)
		legacyWake := legacy.Add(wakeLag)
		if got := senderLag(plannedWake, planned); got != wakeLag {
			t.Fatalf("planned lag at tick %d = %s, want %s", i, got, wakeLag)
		}
		if got := senderLag(legacyWake, legacy); got != wakeLag {
			t.Fatalf("legacy lag at tick %d = %s, want %s", i, got, wakeLag)
		}
		planned = planned.Add(interval)
		legacy = legacyWake.Add(interval)
	}

	if !planned.Equal(start.Add(time.Second)) {
		t.Fatalf("planned schedule = %v, want %v", planned, start.Add(time.Second))
	}
	if !legacy.Equal(start.Add(1200 * time.Millisecond)) {
		t.Fatalf("legacy schedule = %v, want %v", legacy, start.Add(1200*time.Millisecond))
	}
}

func TestAddRecoveryLagCapsAtConfiguredLimit(t *testing.T) {
	limit := 12 * time.Millisecond
	lag, capped := addRecoveryLag(5*time.Millisecond, 20*time.Millisecond, limit)
	if lag != limit {
		t.Fatalf("lag = %s, want %s", lag, limit)
	}
	if !capped {
		t.Fatal("expected lag cap to apply")
	}
}

func TestSenderBucketEndAdvancesToNextSecond(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 950*int(time.Millisecond), time.UTC)
	if got := senderBucketEnd(now); !got.Equal(time.Date(2026, 7, 3, 12, 0, 1, 0, time.UTC)) {
		t.Fatalf("bucket end = %v", got)
	}
}

func TestDrainSenderResponsesUsesImmediateDeadlineWithoutProbeConn(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	conns := []net.PacketConn{&fakeSenderConn{}, &fakeSenderConn{}, &fakeSenderConn{}}
	states := []senderConnState{
		{pending: map[uint64]senderPending{}},
		{pending: map[uint64]senderPending{}},
		{pending: map[uint64]senderPending{}},
	}

	drainSenderResponses(conns, states, map[time.Time]*senderBucketStats{}, make([]byte, 1500), cfg, 99, time.Unix(10, 0), -1)

	for i, conn := range conns {
		fake := conn.(*fakeSenderConn)
		if len(fake.readDeadlines) != 1 {
			t.Fatalf("conn %d read deadlines = %d, want 1", i, len(fake.readDeadlines))
		}
		if !fake.readDeadlines[0].IsZero() && fake.readDeadlines[0].After(time.Now().Add(10*time.Millisecond)) {
			t.Fatalf("conn %d deadline = %v, want immediate deadline", i, fake.readDeadlines[0])
		}
	}
	if senderResponseDrainTimeout <= 0 || senderResponseDrainTimeout > time.Millisecond {
		t.Fatalf("senderResponseDrainTimeout = %s, want short non-zero probe timeout", senderResponseDrainTimeout)
	}
}

func TestDrainSenderResponsesWaitsOnlyOnProbeConn(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", FlowKey: 1, IntervalMs: 10}
	conns := []net.PacketConn{&fakeSenderConn{}, &fakeSenderConn{}, &fakeSenderConn{}}
	states := []senderConnState{
		{pending: map[uint64]senderPending{}},
		{pending: map[uint64]senderPending{}},
		{pending: map[uint64]senderPending{}},
	}

	before := time.Now()
	drainSenderResponses(conns, states, map[time.Time]*senderBucketStats{}, make([]byte, 1500), cfg, 99, time.Unix(10, 0), 1)
	after := time.Now()

	for i, conn := range conns {
		fake := conn.(*fakeSenderConn)
		if len(fake.readDeadlines) != 1 {
			t.Fatalf("conn %d read deadlines = %d, want 1", i, len(fake.readDeadlines))
		}
		if i == 1 {
			if fake.readDeadlines[0].Before(before.Add(senderResponseDrainTimeout)) {
				t.Fatalf("probe conn deadline = %v, want delayed read deadline", fake.readDeadlines[0])
			}
			if fake.readDeadlines[0].After(after.Add(senderResponseDrainTimeout + 10*time.Millisecond)) {
				t.Fatalf("probe conn deadline = %v, unexpectedly late", fake.readDeadlines[0])
			}
			continue
		}
		if fake.readDeadlines[0].After(after.Add(10 * time.Millisecond)) {
			t.Fatalf("conn %d deadline = %v, want immediate deadline", i, fake.readDeadlines[0])
		}
	}
}

func TestExpireSenderPendingOnlyExpiresResponseTracking(t *testing.T) {
	bucket := time.Unix(10, 0).UTC()
	states := []senderConnState{{
		pending:  map[uint64]senderPending{7: {bucket: bucket, deadline: bucket.Add(time.Second)}},
		received: map[uint64]senderReceived{8: {bucket: bucket, deadline: bucket.Add(time.Second)}},
	}}

	expireSenderPending(states, bucket.Add(time.Second))
	if len(states[0].pending) != 0 {
		t.Fatalf("pending after expire = %+v", states[0].pending)
	}
	if len(states[0].received) != 0 {
		t.Fatalf("received after expire = %+v", states[0].received)
	}
}

func TestFlushSenderReportsWaitsForNextBucket(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", SrcId: "node-a", DstId: "node-b", FlowKey: 1, IntervalMs: 10}
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: {tx: 1}}
	reports := make(chan *pb.ResultReport, 1)

	flushSenderReports(context.Background(), "node-a", 99, cfg, reports, buckets, bucket.Add(50*time.Millisecond))

	if len(reports) != 0 {
		t.Fatalf("report flushed before next bucket")
	}
	if _, ok := buckets[bucket]; !ok {
		t.Fatalf("bucket deleted before next bucket")
	}
}

func TestFlushSenderReportsSendsAfterBucketCloses(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", SrcId: "node-a", DstId: "node-b", FlowKey: 1, IntervalMs: 10}
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: {tx: 1}}
	reports := make(chan *pb.ResultReport, 1)

	flushSenderReports(context.Background(), "node-a", 99, cfg, reports, buckets, bucket.Add(100*time.Millisecond))

	if len(reports) != 1 {
		t.Fatalf("report count = %d, want 1", len(reports))
	}
	report := <-reports
	if report.Ts != bucket.Add(100*time.Millisecond).UTC().Format(time.RFC3339Nano) || len(report.SeqRanges) != 0 {
		t.Fatalf("report = %+v", report)
	}
	if _, ok := buckets[bucket]; ok {
		t.Fatalf("bucket was not deleted after flush")
	}
}

func TestFlushSenderReportsDoesNotSynthesizeLoss(t *testing.T) {
	cfg := &pb.FlowConfig{Id: "flow-a", SrcId: "node-a", DstId: "node-b", FlowKey: 1, IntervalMs: 10}
	bucket := time.Unix(10, 0).UTC()
	buckets := map[time.Time]*senderBucketStats{bucket: {tx: 1}}
	reports := make(chan *pb.ResultReport, 1)

	flushSenderReports(context.Background(), "node-a", 99, cfg, reports, buckets, bucket.Add(100*time.Millisecond))

	if len(reports) != 1 {
		t.Fatalf("report count = %d, want 1", len(reports))
	}
	report := <-reports
	if report.Lost != 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestOpenUDPConnsUsesMultipleSourcePorts(t *testing.T) {
	listener, err := net.ListenUDP("udp", mustResolveUDPAddr(t, "127.0.0.1:0"))
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("UDP listen is not permitted in this environment: %v", err)
		}
		t.Fatal(err)
	}
	defer listener.Close()

	conns, err := openUDPConns(context.Background(), listener.LocalAddr().(*net.UDPAddr), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer closeUDPConns(conns)

	if len(conns) != 4 {
		t.Fatalf("conn count = %d, want 4", len(conns))
	}
	ports := map[int]bool{}
	for _, conn := range conns {
		port := conn.LocalAddr().(*net.UDPAddr).Port
		if ports[port] {
			t.Fatalf("duplicate source port: %d", port)
		}
		ports[port] = true
	}
}

type fakeSenderConn struct {
	reads         [][]byte
	readAddrs     []net.Addr
	writes        [][]byte
	writeAddr     []net.Addr
	readDeadlines []time.Time
}

func (c *fakeSenderConn) ReadFrom(b []byte) (int, net.Addr, error) {
	if len(c.reads) == 0 {
		return 0, nil, timeoutError{}
	}
	next := c.reads[0]
	c.reads = c.reads[1:]
	addr := net.Addr(&net.UDPAddr{})
	if len(c.readAddrs) > 0 {
		addr = c.readAddrs[0]
		c.readAddrs = c.readAddrs[1:]
	}
	copy(b, next)
	return len(next), addr, nil
}

func (c *fakeSenderConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	copied := append([]byte(nil), b...)
	c.writes = append(c.writes, copied)
	c.writeAddr = append(c.writeAddr, addr)
	return len(b), nil
}
func (c *fakeSenderConn) Close() error                { return nil }
func (c *fakeSenderConn) LocalAddr() net.Addr         { return &net.UDPAddr{} }
func (c *fakeSenderConn) SetDeadline(time.Time) error { return nil }
func (c *fakeSenderConn) SetReadDeadline(t time.Time) error {
	c.readDeadlines = append(c.readDeadlines, t)
	return nil
}
func (c *fakeSenderConn) SetWriteDeadline(time.Time) error { return nil }

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestOpenUDPConnsRespectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := openUDPConns(ctx, mustResolveUDPAddr(t, "127.0.0.1:0"), 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("openUDPConns error = %v, want context.Canceled", err)
	}
}

func mustEncodeSenderPacket(t *testing.T, p protocol.Packet) []byte {
	t.Helper()
	payloadSize, err := protocol.PayloadSizeFromTotalIPPacketSize(p, 96)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := protocol.Encode(p, payloadSize)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestActiveFlowsDoesNotBlockDuringRestartWait(t *testing.T) {
	m := NewFlowManager("node-a")

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	secondDone := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var releaseOnce sync.Once
	releaseFirst := func() {
		releaseOnce.Do(func() {
			close(firstRelease)
		})
	}
	defer releaseFirst()

	m.startSender = func(ctx context.Context, agentID string, sessionID uint32, cfg *pb.FlowConfig, seqs []*sequenceCounter, vrf string, reports chan<- *pb.ResultReport) {
		switch cfg.IntervalMs {
		case 10:
			close(firstStarted)
			<-ctx.Done()
			<-firstRelease
		case 20:
			close(secondStarted)
			<-ctx.Done()
			close(secondDone)
		default:
			t.Fatalf("unexpected interval %d", cfg.IntervalMs)
		}
	}

	first := &pb.ConfigSnapshot{Flows: []*pb.FlowConfig{
		{Id: "flow-a", FlowKey: 1, DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 10, PacketSize: 96, State: "running"},
	}}
	if err := m.Apply(ctx, first); err != nil {
		t.Fatal(err)
	}
	<-firstStarted

	restarted := &pb.ConfigSnapshot{Flows: []*pb.FlowConfig{
		{Id: "flow-a", FlowKey: 1, DstId: "node-b", DstAddr: "127.0.0.1:40002", IntervalMs: 20, PacketSize: 96, State: "running"},
	}}
	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Apply(ctx, restarted)
	}()

	select {
	case <-secondStarted:
		t.Fatal("new sender started before old sender finished")
	case <-time.After(100 * time.Millisecond):
	}

	activeCh := make(chan uint32, 1)
	go func() {
		activeCh <- m.ActiveFlows()
	}()
	select {
	case <-activeCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ActiveFlows blocked during restart wait")
	}

	releaseFirst()

	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("new sender did not start after old sender finished")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Apply did not return")
	}

	cancel()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second sender did not exit after cancel")
	}
}

func mustResolveUDPAddr(t *testing.T, addr string) *net.UDPAddr {
	t.Helper()
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return udpAddr
}
