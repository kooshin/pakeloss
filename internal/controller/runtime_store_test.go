package controller

import (
	"math"
	"testing"
	"time"

	"pakeloss/internal/model"
	"pakeloss/internal/pb"
	"pakeloss/internal/protocol"
)

func testSeqRanges(bounds ...uint64) []*pb.SeqRange {
	out := make([]*pb.SeqRange, 0, len(bounds)/2)
	for i := 0; i+1 < len(bounds); i += 2 {
		out = append(out, &pb.SeqRange{Start: bounds[i], End: bounds[i+1]})
	}
	return out
}

func TestRuntimeRollingWindow(t *testing.T) {
	mesh := model.MeshConfig{ConfigVersion: 1, Nodes: []model.NodeConfig{{ID: "a"}, {ID: "b"}}, Flows: []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}}}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 242; i++ {
		lost := uint64(0)
		lossRatio := 0.0
		if i >= 2 {
			lost = 1
			lossRatio = 0.01
		}
		duplicate := uint64(0)
		reorder := uint64(0)
		if i >= 2 {
			duplicate = 2
			reorder = 3
		}
		s.ingestResultLocked(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100 - lost, Lost: lost, LossRatio: lossRatio, Duplicate: duplicate, Reorder: reorder}, now.Add(time.Duration(i)*time.Second))
	}
	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossRatio10s != 0.01 || f.LossRatio60s != 0.01 || len(f.LossHistory10s) != 10 || len(f.LossHistory20s) != 20 || len(f.LossHistory60s) != 60 || len(f.LossHistory240s) != 240 {
		t.Fatalf("bad rolling status: %+v", f)
	}
	if f.Lost60s != 60 {
		t.Fatalf("lost60s = %d, want 60", f.Lost60s)
	}
	if f.Duplicate60s != 120 || f.Reorder60s != 180 {
		t.Fatalf("duplicate60s/reorder60s = %d/%d, want 120/180", f.Duplicate60s, f.Reorder60s)
	}
	if f.LossHistory240s[0] != 0.01 || f.LossHistory240s[len(f.LossHistory240s)-1] != 0.01 || f.LossHistory60s[0] != 0.01 {
		t.Fatalf("unexpected rolling history contents: loss240=%v ... %v loss60=%v", f.LossHistory240s[:1], f.LossHistory240s[len(f.LossHistory240s)-1:], f.LossHistory60s[:1])
	}
}

func TestRuntimeControllerAgentLossForOfflineReceiver(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.AgentOnline("a", "127.0.0.1:40001", 1)

	if n := len(s.ApplyControllerAgentLoss(time.Now(), 3*time.Second)); n != 1 {
		t.Fatalf("synthetic loss flows = %d, want 1", n)
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossRatio1s != 1 || f.LossRatio10s != 1 || math.Abs(f.LossRatio60s-(1.0/60.0)) > 1e-12 {
		t.Fatalf("offline receiver should be full loss: %+v", f)
	}
	if f.Tx1s != 100 || f.Rx1s != 0 || f.Lost1s != 100 || f.LastSeen.IsZero() {
		t.Fatalf("offline receiver should update counters and last seen: %+v", f)
	}
}

func TestRuntimeLossRatio60sProjectsMissingHistory(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.IngestResult(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 99, Lost: 1, LossRatio: 0.01})

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossRatio1s != 0.01 {
		t.Fatalf("loss ratio 1s = %v, want 0.01", f.LossRatio1s)
	}
	if math.Abs(f.LossRatio60s-(1.0/6000.0)) > 1e-12 {
		t.Fatalf("loss ratio 60s = %v, want %v", f.LossRatio60s, 1.0/6000.0)
	}
	if f.Lost60s != 1 {
		t.Fatalf("lost60s = %d, want 1", f.Lost60s)
	}
	if f.Duplicate60s != 0 || f.Reorder60s != 0 {
		t.Fatalf("duplicate60s/reorder60s = %d/%d, want 0/0", f.Duplicate60s, f.Reorder60s)
	}
	if f.TxTotal != 100 || f.RxTotal != 99 {
		t.Fatalf("tx/rx total = %d/%d, want 100/99", f.TxTotal, f.RxTotal)
	}
}

func TestRuntimeAggregatesTenMillisecondSamplesForOneSecondStatus(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		lost := uint64(0)
		rx := uint64(1)
		if i%10 == 0 {
			lost = 1
			rx = 0
		}
		ts := now.Add(time.Duration(i+1) * 10 * time.Millisecond).UTC().Format(time.RFC3339Nano)
		s.ingestResultLocked(&pb.ResultSummary{Ts: ts, FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 1, Rx: rx, Lost: lost, LossRatio: float64(lost)}, now.Add(time.Duration(i+1)*10*time.Millisecond))
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.Tx1s != 100 || f.Rx1s != 90 || f.Lost1s != 10 || f.LossRatio1s != 0.1 {
		t.Fatalf("1s status = tx:%d rx:%d lost:%d ratio:%v, want 100/90/10/0.1", f.Tx1s, f.Rx1s, f.Lost1s, f.LossRatio1s)
	}
}

func TestRuntimeOutageMetricsSeparateIsolatedAndConsecutiveLoss(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.July, 9, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 10; i++ {
		ts := now.Add(time.Duration(i+1) * 10 * time.Millisecond)
		lost := uint64(0)
		rx := uint64(1)
		if i == 0 {
			lost = 1
			rx = 0
		}
		s.ingestResultLocked(&pb.ResultSummary{Ts: ts.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 1, Rx: rx, Lost: lost, LossRatio: float64(lost)}, ts)
	}
	for i := 10; i < 30; i++ {
		ts := now.Add(time.Duration(i+1) * 10 * time.Millisecond)
		s.ingestResultLocked(&pb.ResultSummary{Ts: ts.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 1, Rx: 0, Lost: 1, LossRatio: 1}, ts)
	}
	recovery := now.Add(310 * time.Millisecond)
	s.ingestResultLocked(&pb.ResultSummary{Ts: recovery.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 1, Rx: 1, Lost: 0, LossRatio: 0}, recovery)

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.IsolatedLossEvents != 1 {
		t.Fatalf("isolated_loss_events = %d, want 1", f.IsolatedLossEvents)
	}
	if f.OutageCount != 1 {
		t.Fatalf("outage_count = %d, want 1", f.OutageCount)
	}
	if f.OutageActive {
		t.Fatalf("outage_active = true, want false")
	}
	if f.LastOutageMs != 200 || f.MaxOutageMs != 200 || f.OutageTotalMs != 200 {
		t.Fatalf("outage ms = last:%d max:%d total:%d, want 200/200/200", f.LastOutageMs, f.MaxOutageMs, f.OutageTotalMs)
	}
}

func TestRuntimeOutageMetricsTrackActiveSyntheticLoss(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.July, 9, 12, 0, 0, 0, time.UTC)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ingestResultLocked(&pb.ResultSummary{Ts: now.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 1, Rx: 1}, now)
	s.AgentOffline("b")
	s.agents["a"].LastHeartbeat = now.Add(10 * time.Second)

	for i := 1; i <= 2; i++ {
		s.ApplyControllerAgentLoss(now.Add(time.Duration(i)*time.Second), 3*time.Second)
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if !f.OutageActive {
		t.Fatalf("outage_active = false, want true")
	}
	if f.OutageCount != 1 {
		t.Fatalf("outage_count = %d, want 1", f.OutageCount)
	}
	if f.CurrentOutageMs < 1900 || f.CurrentOutageMs > 2000 {
		t.Fatalf("current_outage_ms = %d, want around 2000", f.CurrentOutageMs)
	}
}

func TestRuntimeTracksSourceUnmeasurableSeparately(t *testing.T) {
	mesh := model.MeshConfig{ReportBucketFactor: 10, Nodes: []model.NodeConfig{{ID: "a"}, {ID: "b"}}, Flows: []model.MeshFlowConfig{{ID: "a->b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}}}
	s := NewRuntimeStore(mesh)
	base := time.Now().UTC().Truncate(time.Second)
	s.updateUnmeasurableLocked(s.flows["a->b"], false, base, 3*time.Second)
	s.updateUnmeasurableLocked(s.flows["a->b"], false, base.Add(2*time.Second), 3*time.Second)
	s.updateUnmeasurableLocked(s.flows["a->b"], true, base.Add(2500*time.Millisecond), 3*time.Second)
	f := s.flows["a->b"].status
	if f.UnmeasurableCount != 1 || f.UnmeasurableTotalMs != 2500 || f.OutageCount != 0 {
		t.Fatalf("unmeasurable=%d/%dms outage=%d, want 1/2500ms/0", f.UnmeasurableCount, f.UnmeasurableTotalMs, f.OutageCount)
	}
}

func TestRuntimeResolvesFlowMetadataFromFlowKey(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	key := protocol.ComputeFlowKey("a", "b", "a<=>b")

	applied := s.IngestResult(&pb.ResultSummary{FlowKey: key, AgentId: "b", IntervalMs: 10, Tx: 100, Rx: 99, Lost: 1, LossRatio: 0.01})
	if applied.FlowId != "a<=>b" || applied.Src != "a" || applied.Dst != "b" {
		t.Fatalf("flow metadata not resolved from flow key: %+v", applied)
	}
}

func TestRuntimeLossTimeAndActiveOutageAreNotCappedByHistoryTrim(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 241; i++ {
		s.ingestResultLocked(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 0, Lost: 100, LossRatio: 1}, now.Add(time.Duration(i)*time.Second))
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossTimeTotalMs != 241000 || f.CurrentOutageMs != 241000 {
		t.Fatalf("loss/outage time = %d/%dms, want 241000/241000", f.LossTimeTotalMs, f.CurrentOutageMs)
	}
}

func TestRuntimeTracksMaxOutageMs(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.ingestResultLocked(&pb.ResultSummary{Ts: now.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 0, Lost: 100, LossRatio: 1}, now)
	s.ingestResultLocked(&pb.ResultSummary{Ts: now.Add(time.Second).UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 0, Lost: 100, LossRatio: 1}, now.Add(time.Second))
	s.ingestResultLocked(&pb.ResultSummary{Ts: now.Add(2 * time.Second).UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100, Lost: 0, LossRatio: 0}, now.Add(2*time.Second))
	s.ingestResultLocked(&pb.ResultSummary{Ts: now.Add(3 * time.Second).UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 50, Rx: 0, Lost: 50, LossRatio: 1}, now.Add(3*time.Second))

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.MaxOutageMs != 2000 {
		t.Fatalf("max outage ms = %d, want 2000", f.MaxOutageMs)
	}
}

func TestRuntimeStoreReplacesSameWindowSamples(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}

	t.Run("real then synthetic in same window", func(t *testing.T) {
		s := NewRuntimeStore(mesh)
		now := time.Date(2026, time.June, 28, 12, 0, 0, 100*1e6, time.UTC)
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:00Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100, Lost: 0, LossRatio: 0}, now)
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:00Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 0, Lost: 100, LossRatio: 1}, now.Add(250*time.Millisecond))

		f, ok := s.Flow("a<=>b")
		if !ok {
			t.Fatal("flow not found")
		}
		if f.TxTotal != 200 || f.RxTotal != 100 || f.LostTotal != 100 {
			t.Fatalf("controller-time accumulation failed: %+v", f)
		}
		if f.Tx1s != 200 || f.Rx1s != 100 || f.Lost1s != 100 {
			t.Fatalf("latest 1s counters not applied: %+v", f)
		}
	})

	t.Run("synthetic then real in same window", func(t *testing.T) {
		s := NewRuntimeStore(mesh)
		now := time.Date(2026, time.June, 28, 12, 0, 0, 100*1e6, time.UTC)
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:00Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 0, Lost: 100, LossRatio: 1}, now)
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:00Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100, Lost: 0, LossRatio: 0}, now.Add(250*time.Millisecond))

		f, ok := s.Flow("a<=>b")
		if !ok {
			t.Fatal("flow not found")
		}
		if f.TxTotal != 200 || f.RxTotal != 100 || f.LostTotal != 100 {
			t.Fatalf("controller-time accumulation failed: %+v", f)
		}
		if f.Tx1s != 200 || f.Rx1s != 100 || f.Lost1s != 100 {
			t.Fatalf("latest 1s counters not applied: %+v", f)
		}
	})

	t.Run("different seconds still accumulate", func(t *testing.T) {
		s := NewRuntimeStore(mesh)
		now := time.Date(2026, time.June, 28, 12, 0, 0, 100*1e6, time.UTC)
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:00Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100, Lost: 0, LossRatio: 0}, now)
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:01Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 99, Lost: 1, LossRatio: 0.01}, now.Add(time.Second))

		f, ok := s.Flow("a<=>b")
		if !ok {
			t.Fatal("flow not found")
		}
		if f.TxTotal != 200 || f.RxTotal != 199 || f.LostTotal != 1 {
			t.Fatalf("different-window samples should accumulate: %+v", f)
		}
	})

	t.Run("same controller time but different summary windows do not replace", func(t *testing.T) {
		s := NewRuntimeStore(mesh)
		now := time.Date(2026, time.June, 28, 12, 0, 1, 500*1e6, time.UTC)
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:00Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100, Lost: 0, LossRatio: 0}, now)
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:01Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 99, Lost: 1, LossRatio: 0.01}, now)

		f, ok := s.Flow("a<=>b")
		if !ok {
			t.Fatal("flow not found")
		}
		if f.TxTotal != 100 {
			t.Fatalf("tx total = %d, want 100", f.TxTotal)
		}
		if f.LostTotal != 1 {
			t.Fatalf("lost total = %d, want 1", f.LostTotal)
		}
	})

	t.Run("later controller time with same summary window replaces", func(t *testing.T) {
		s := NewRuntimeStore(mesh)
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:00Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 0, Lost: 100, LossRatio: 1}, time.Date(2026, time.June, 28, 12, 0, 0, 100*1e6, time.UTC))
		s.ingestResultLocked(&pb.ResultSummary{Ts: "2026-06-28T12:00:00Z", FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100, Lost: 0, LossRatio: 0}, time.Date(2026, time.June, 28, 12, 0, 1, 100*1e6, time.UTC))

		f, ok := s.Flow("a<=>b")
		if !ok {
			t.Fatal("flow not found")
		}
		if f.TxTotal != 200 || f.RxTotal != 100 || f.LostTotal != 100 {
			t.Fatalf("controller-time separated windows should accumulate: %+v", f)
		}
	})
}

func TestRuntimeControllerAgentLossSkipsOnlineReceiver(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)

	if n := len(s.ApplyControllerAgentLoss(time.Now(), 3*time.Second)); n != 0 {
		t.Fatalf("synthetic loss flows = %d, want 0", n)
	}

	f, _ := s.Flow("a<=>b")
	if f.LossRatio1s != 0 || f.Tx1s != 0 || f.Lost1s != 0 {
		t.Fatalf("online receiver should not be changed: %+v", f)
	}
}

func TestRuntimeControllerAgentLossSkipsStaleFlowAfterReceiverReconnect(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Now()
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ConfigAck(&pb.ConfigAck{AgentId: "a", ConfigVersion: 1})
	s.IngestResult(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100})

	s.mu.Lock()
	s.flows["a<=>b"].status.LastSeen = now.Add(-4 * time.Second)
	s.mu.Unlock()

	s.AgentOffline("b")
	s.AgentOnline("b", "127.0.0.1:40002", 1)

	if n := len(s.ApplyControllerAgentLoss(now.Add(100*time.Millisecond), 3*time.Second)); n != 0 {
		t.Fatalf("synthetic loss flows after receiver reconnect = %d, want 0", n)
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossRatio1s != 0 || f.Tx1s != 100 || f.Rx1s != 100 || f.Lost1s != 0 {
		t.Fatalf("receiver reconnect warmup should preserve last measured sample: %+v", f)
	}
}

func TestRuntimeControllerAgentLossSkipsMissingFlowResultAfterReceiverOnlineWithZeroLastSeen(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ConfigAck(&pb.ConfigAck{AgentId: "a", ConfigVersion: 1})

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if !f.LastSeen.IsZero() {
		t.Fatalf("expected zero last seen before any result, got %s", f.LastSeen)
	}

	if n := len(s.ApplyControllerAgentLoss(time.Now(), 3*time.Second)); n != 0 {
		t.Fatalf("synthetic loss flows after receiver online with zero last seen = %d, want 0", n)
	}

	f, ok = s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossRatio1s != 0 || f.Tx1s != 0 || f.Rx1s != 0 || f.Lost1s != 0 {
		t.Fatalf("receiver online warmup should suppress missing-result loss: %+v", f)
	}
}

func TestRuntimeAgentOnlineDoesNotClearConfigError(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.ConfigError(&pb.ConfigError{AgentId: "a", Error: "invalid packet size"})

	s.AgentOnline("b", "127.0.0.1:40002", 1)

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LastError != "invalid packet size" {
		t.Fatalf("config error should be preserved, got %q", f.LastError)
	}
}

func TestRuntimeFlowActualStateReflectsOfflineAgent(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ConfigAck(&pb.ConfigAck{AgentId: "a", ConfigVersion: 1})

	f, _ := s.Flow("a<=>b")
	if f.ActualState != "running" {
		t.Fatalf("actual state before offline = %q, want running", f.ActualState)
	}

	s.AgentOffline("b")
	f, _ = s.Flow("a<=>b")
	if f.ActualState != "offline" {
		t.Fatalf("actual state after receiver offline = %q, want offline", f.ActualState)
	}
	if f.LastError == "" {
		t.Fatal("offline flow should explain inferred loss")
	}
}

func TestRuntimeHeartbeatUsesControllerReceiveTime(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Now()
	s.Heartbeat(&pb.Heartbeat{AgentId: "a", TsUnixNano: now.Add(-4 * time.Second).UnixNano()})

	agents := s.Agents()
	if len(agents) != 1 {
		t.Fatalf("agents len = %d, want 1", len(agents))
	}
	if agents[0].Status != "online" {
		t.Fatalf("agent status = %q, want online", agents[0].Status)
	}
	if agents[0].LastHeartbeat.Before(now.Add(-time.Second)) {
		t.Fatalf("last heartbeat = %s, want controller receive time", agents[0].LastHeartbeat)
	}
}

func TestRuntimeControllerAgentLossForMissingFlowResult(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Now()
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ConfigAck(&pb.ConfigAck{AgentId: "a", ConfigVersion: 1})
	s.agents["a"].LastHeartbeat = now.Add(2 * time.Second)
	s.agents["b"].LastHeartbeat = now.Add(2 * time.Second)

	if n := len(s.ApplyControllerAgentLoss(now.Add(4*time.Second), 3*time.Second)); n != 1 {
		t.Fatalf("synthetic loss flows = %d, want 1", n)
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossRatio1s != 1 || f.Tx1s != 100 || f.Rx1s != 0 || f.Lost1s != 100 {
		t.Fatalf("missing flow result should be full loss: %+v", f)
	}
	s.mu.Lock()
	samples := len(s.flows["a<=>b"].samples)
	s.mu.Unlock()
	if samples != 100 {
		t.Fatalf("synthetic loss samples = %d, want 100 interval buckets", samples)
	}
}

func TestRuntimeControllerAgentLossSkipsStaleFlowAfterSourceConfigAck(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Now()
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ConfigAck(&pb.ConfigAck{AgentId: "a", ConfigVersion: 1})
	s.IngestResult(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100})

	s.mu.Lock()
	s.flows["a<=>b"].status.LastSeen = now.Add(-4 * time.Second)
	s.mu.Unlock()

	s.ConfigAck(&pb.ConfigAck{AgentId: "a", ConfigVersion: 1})

	if n := len(s.ApplyControllerAgentLoss(now.Add(100*time.Millisecond), 3*time.Second)); n != 0 {
		t.Fatalf("synthetic loss flows after source config ack = %d, want 0", n)
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossRatio1s != 0 || f.Tx1s != 100 || f.Rx1s != 100 || f.Lost1s != 0 {
		t.Fatalf("source config ack warmup should preserve last measured sample: %+v", f)
	}
}

func TestRuntimeControllerAgentLossSkipsMissingFlowResultDuringStartWarmup(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "stopped", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)

	mesh.ConfigVersion = 2
	mesh.Flows[0].State = "running"
	s.SetDesiredConfigVersion(mesh)
	s.ConfigAck(&pb.ConfigAck{AgentId: "a", ConfigVersion: 2})

	if n := len(s.ApplyControllerAgentLoss(time.Now(), 3*time.Second)); n != 0 {
		t.Fatalf("synthetic loss flows during warmup = %d, want 0", n)
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossRatio1s != 0 || f.Tx1s != 0 || f.Lost1s != 0 || f.LastError != "" {
		t.Fatalf("start warmup should not report loss: %+v", f)
	}
}

func TestRuntimeZeroReceiveResultDuringStartWarmupIsNotLoss(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "stopped", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)

	mesh.ConfigVersion = 2
	mesh.Flows[0].State = "running"
	s.SetDesiredConfigVersion(mesh)
	s.ConfigAck(&pb.ConfigAck{AgentId: "a", ConfigVersion: 2})
	s.IngestResult(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 0, Rx: 0, Lost: 0})

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LossRatio1s != 0 || f.Tx1s != 0 || f.Rx1s != 0 || f.Lost1s != 0 || f.LastError != "" {
		t.Fatalf("start warmup zero result should not be loss: %+v", f)
	}
	if n := len(s.ApplyControllerAgentLoss(time.Now(), 3*time.Second)); n != 0 {
		t.Fatalf("warmup should remain active after zero result, synthetic loss flows = %d", n)
	}
}

func TestRuntimeControllerAgentLossSkipsRecentFlowResult(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Now()
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ConfigAck(&pb.ConfigAck{AgentId: "a", ConfigVersion: 1})
	s.IngestResult(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 100, Rx: 100})

	if n := len(s.ApplyControllerAgentLoss(now, 3*time.Second)); n != 0 {
		t.Fatalf("synthetic loss flows = %d, want 0", n)
	}
}

func TestRuntimeSetDesiredConfigVersionRemovesDeletedFlowsAndMarksDisabledAgents(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		DiscoveryMode: "auto",
		Nodes: []model.NodeConfig{
			{ID: "node-a", UDPAddr: "127.0.0.1:40001", Enabled: true},
			{ID: "node-b", UDPAddr: "127.0.0.1:40002", Enabled: true},
			{ID: "node-c", UDPAddr: "127.0.0.1:40003", Enabled: true},
		},
		Flows: []model.MeshFlowConfig{
			{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", State: "stopped"},
			{ID: "node-a<=>node-c", Src: "node-a", Dst: "node-c", State: "stopped"},
		},
	}
	s := NewRuntimeStore(mesh)

	mesh.ConfigVersion = 2
	mesh.Nodes = []model.NodeConfig{
		{ID: "node-a", UDPAddr: "127.0.0.1:40001", Enabled: true},
		{ID: "node-b", UDPAddr: "127.0.0.1:40002", Enabled: true},
	}
	mesh.Flows = []model.MeshFlowConfig{
		{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", State: "stopped"},
	}
	s.SetDesiredConfigVersion(mesh)

	flows := s.Flows()
	if len(flows) != 1 || flows[0].FlowID != "node-a<=>node-b" {
		t.Fatalf("flows after shrink = %+v, want only node-a<=>node-b", flows)
	}
	agents := s.Agents()
	if len(agents) != 3 {
		t.Fatalf("agents len = %d, want 3", len(agents))
	}
	enabledByID := map[string]bool{}
	for _, agent := range agents {
		enabledByID[agent.AgentID] = agent.Enabled
	}
	if !enabledByID["node-a"] || !enabledByID["node-b"] {
		t.Fatalf("node-a and node-b should stay enabled: %+v", enabledByID)
	}
	if enabledByID["node-c"] {
		t.Fatalf("node-c should be marked disabled: %+v", enabledByID)
	}
}

func TestRuntimeControllerAgentLossForStaleReceiver(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Now()
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.Heartbeat(&pb.Heartbeat{AgentId: "b", TsUnixNano: now.UnixNano()})
	s.agents["b"].LastHeartbeat = now.Add(-4 * time.Second)

	if n := len(s.ApplyControllerAgentLoss(now, 3*time.Second)); n != 1 {
		t.Fatalf("synthetic loss flows = %d, want 1", n)
	}

	agents := s.Agents()
	for _, ag := range agents {
		if ag.AgentID == "b" && ag.Status != "offline" {
			t.Fatalf("stale receiver status = %q, want offline", ag.Status)
		}
	}
}

func TestRuntimeControllerAgentLossForOfflineSender(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.AgentOnline("b", "127.0.0.1:40002", 1)

	if n := len(s.ApplyControllerAgentLoss(time.Now(), 3*time.Second)); n != 0 {
		t.Fatalf("synthetic loss flows = %d, want 0", n)
	}

	f, _ := s.Flow("a<=>b")
	if f.LossRatio1s != 0 || f.Tx1s != 0 || f.Rx1s != 0 || f.Lost1s != 0 || f.LastSeen.IsZero() {
		t.Fatalf("offline sender should be treated as missing data: %+v", f)
	}
	if got := f.LossHistory240s[len(f.LossHistory240s)-1]; got != -1 {
		t.Fatalf("offline sender history tail = %v, want -1", got)
	}
}

func TestRuntimeSetDesiredConfigVersionAddsDynamicNodesAndPreservesOfflineFlow(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{ConfigVersion: 1, DiscoveryMode: "auto"})
	mesh := model.MeshConfig{
		ConfigVersion: 2,
		DiscoveryMode: "auto",
		Nodes: []model.NodeConfig{
			{ID: "node-a", UDPAddr: "192.0.2.10:40001"},
			{ID: "node-b", UDPAddr: "192.0.2.11:40002"},
		},
		Flows: []model.MeshFlowConfig{
			{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", State: "stopped", IntervalMs: 10},
		},
	}

	s.SetDesiredConfigVersion(mesh)
	s.AgentOnline("node-a", "192.0.2.10:40001", mesh.ConfigVersion)
	s.AgentOnline("node-b", "192.0.2.11:40002", mesh.ConfigVersion)
	s.AgentOffline("node-b")

	agents := s.Agents()
	if len(agents) != 2 {
		t.Fatalf("agents len = %d, want 2", len(agents))
	}
	foundOffline := false
	for _, agent := range agents {
		if agent.AgentID == "node-b" {
			foundOffline = true
			if agent.Status != "offline" || agent.UDPAddr != "192.0.2.11:40002" {
				t.Fatalf("unexpected node-b status: %+v", agent)
			}
		}
	}
	if !foundOffline {
		t.Fatal("node-b not found")
	}
	if _, ok := s.Flow("node-a<=>node-b"); !ok {
		t.Fatal("node-a<=>node-b should remain in runtime store")
	}
}

func TestRuntimeControllerAgentLossReplacesLastMeasuredSampleWhenAgentOffline(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.IngestResult(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 77, Rx: 76, Lost: 1, LossRatio: 1.0 / 77.0})

	before, _ := s.Flow("a<=>b")
	s.AgentOffline("b")
	now := time.Now()
	s.Heartbeat(&pb.Heartbeat{AgentId: "a", TsUnixNano: now.UnixNano()})
	s.agents["a"].LastHeartbeat = now.Add(2 * time.Second)
	if n := len(s.ApplyControllerAgentLoss(now.Add(5*time.Second), 3*time.Second)); n != 1 {
		t.Fatalf("offline flows = %d, want 1", n)
	}

	after, _ := s.Flow("a<=>b")
	if after.Tx1s != 100 || after.Rx1s != 0 || after.Lost1s != 100 || after.LossRatio1s != 1 {
		t.Fatalf("offline agent should replace counters with inferred full loss: before=%+v after=%+v", before, after)
	}
	if !after.LastSeen.After(before.LastSeen) {
		t.Fatalf("offline agent should update last seen: before=%s after=%s", before.LastSeen, after.LastSeen)
	}
}

func TestRuntimeZeroReceiveResultForRunningFlowIsFullLoss(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	applied := s.IngestResult(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 0, Rx: 0, Lost: 0})

	if applied.Tx != 100 || applied.Rx != 0 || applied.Lost != 100 || applied.LossRatio != 1 {
		t.Fatalf("applied result = %+v, want inferred full loss", applied)
	}

	f, _ := s.Flow("a<=>b")
	if f.LossRatio1s != 1 || f.Tx1s != 100 || f.Rx1s != 0 || f.Lost1s != 100 {
		t.Fatalf("zero receive result should be full loss: %+v", f)
	}
	if f.LastError == "" {
		t.Fatal("zero receive result should explain inferred loss")
	}
}

func TestRuntimeZeroReceiveResultDoesNotDoubleCountObservedSecond(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= 100; i++ {
		ts := now.Add(time.Duration(i) * 10 * time.Millisecond)
		s.ingestResultLocked(&pb.ResultSummary{Ts: ts.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 1, Rx: 1}, ts)
	}

	applied := s.ingestResultLocked(&pb.ResultSummary{Ts: now.Add(time.Second).UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 0, Rx: 0, Lost: 0}, now.Add(time.Second))

	if applied.Tx != 0 || applied.Rx != 0 || applied.Lost != 0 || applied.LossRatio != 0 {
		t.Fatalf("applied stale zero result = %+v, want no-op", applied)
	}
	f, _ := s.Flow("a<=>b")
	if f.Tx1s != 100 || f.Rx1s != 100 || f.Lost1s != 0 || f.LossRatio1s != 0 {
		t.Fatalf("stale zero receive should not double count observed second: %+v", f)
	}
	if f.TxTotal != 100 || f.RxTotal != 100 || f.LostTotal != 0 {
		t.Fatalf("stale zero receive should not change totals: %+v", f)
	}
}

func TestRuntimeZeroReceiveResultThenObservedSecondReplacesFalseLoss(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	applied := s.ingestResultLocked(&pb.ResultSummary{Ts: now.Add(time.Second).UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 0, Rx: 0, Lost: 0}, now.Add(time.Second))
	if applied.Tx != 100 || applied.Rx != 0 || applied.Lost != 100 || applied.LossRatio != 1 {
		t.Fatalf("applied zero result = %+v, want inferred full loss", applied)
	}

	for i := 1; i <= 100; i++ {
		ts := now.Add(time.Duration(i) * 10 * time.Millisecond)
		s.ingestResultLocked(&pb.ResultSummary{Ts: ts.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 1, Rx: 1}, ts)
	}

	f, _ := s.Flow("a<=>b")
	if f.Tx1s != 100 || f.Rx1s != 100 || f.Lost1s != 0 || f.LossRatio1s != 0 {
		t.Fatalf("observed samples should replace same-second false loss: %+v", f)
	}
	if f.TxTotal != 100 || f.RxTotal != 100 || f.LostTotal != 0 {
		t.Fatalf("observed samples should clear false loss totals: %+v", f)
	}
}

func TestRuntimeZeroReceiveResultUsesLatestObservedTx(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.ingestResultLocked(&pb.ResultSummary{Ts: now.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 77, Rx: 76, Lost: 1, LossRatio: 1.0 / 77.0}, now)

	applied := s.ingestResultLocked(&pb.ResultSummary{Ts: now.Add(time.Second).UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 0, Rx: 0, Lost: 0}, now.Add(time.Second))

	if applied.Tx != 100 || applied.Rx != 0 || applied.Lost != 100 || applied.LossRatio != 1 {
		t.Fatalf("applied result = %+v, want inferred loss from expected tx", applied)
	}

	f, _ := s.Flow("a<=>b")
	if f.LossRatio1s != 1 || f.Tx1s != 100 || f.Rx1s != 0 || f.Lost1s != 100 {
		t.Fatalf("zero receive result should use expected tx floor: %+v", f)
	}
}

func TestRuntimeControllerAgentLossUsesExpectedTxForOutageAfterPartialSample(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ingestResultLocked(&pb.ResultSummary{Ts: now.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 77, Rx: 76, Lost: 1, LossRatio: 1.0 / 77.0}, now)
	s.AgentOffline("b")
	s.agents["a"].LastHeartbeat = now.Add(10 * time.Second)

	for i := 1; i <= 3; i++ {
		if n := len(s.ApplyControllerAgentLoss(now.Add(time.Duration(i)*time.Second), 3*time.Second)); n != 1 {
			t.Fatalf("synthetic loss flows at second %d = %d, want 1", i, n)
		}
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.Tx1s != 100 || f.Rx1s != 0 || f.Lost1s != 100 {
		t.Fatalf("latest inferred counters = tx:%d rx:%d lost:%d, want 100/0/100", f.Tx1s, f.Rx1s, f.Lost1s)
	}
	if f.MaxOutageMs != 3000 {
		t.Fatalf("max outage ms = %d, want 3000", f.MaxOutageMs)
	}
}

func TestRuntimeControllerAgentLossBackfillsStaleGap(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ingestResultLocked(&pb.ResultSummary{Ts: now.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 1, Rx: 1}, now)
	s.agents["a"].LastHeartbeat = now.Add(10 * time.Second)
	s.agents["b"].LastHeartbeat = now

	if n := len(s.ApplyControllerAgentLoss(now.Add(5*time.Second), 3*time.Second)); n != 1 {
		t.Fatalf("synthetic loss flows = %d, want 1", n)
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.Tx1s != 100 || f.Rx1s != 0 || f.Lost1s != 100 || f.LossRatio1s != 1 {
		t.Fatalf("latest inferred counters = tx:%d rx:%d lost:%d loss:%v, want 100/0/100/1", f.Tx1s, f.Rx1s, f.Lost1s, f.LossRatio1s)
	}
	if f.LostTotal != 500 {
		t.Fatalf("backfilled lost total = %d, want 500", f.LostTotal)
	}
	if f.MaxOutageMs != 5000 {
		t.Fatalf("max outage ms = %d, want 5000", f.MaxOutageMs)
	}
}

func TestRuntimeControllerAgentLossBackfillDoesNotDoubleCountSameWindow(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.ingestResultLocked(&pb.ResultSummary{Ts: now.UTC().Format(time.RFC3339Nano), FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 1, Rx: 1}, now)
	s.agents["a"].LastHeartbeat = now.Add(10 * time.Second)
	s.agents["b"].LastHeartbeat = now

	checkAt := now.Add(5 * time.Second)
	if n := len(s.ApplyControllerAgentLoss(checkAt, 3*time.Second)); n != 1 {
		t.Fatalf("synthetic loss flows = %d, want 1", n)
	}
	if n := len(s.ApplyControllerAgentLoss(checkAt, 3*time.Second)); n != 0 {
		t.Fatalf("duplicate synthetic loss flows = %d, want 0", n)
	}

	f, ok := s.Flow("a<=>b")
	if !ok {
		t.Fatal("flow not found")
	}
	if f.LostTotal != 500 {
		t.Fatalf("duplicated lost total = %d, want 500", f.LostTotal)
	}
}

func TestRuntimeZeroReceiveResultWhileSourceAgentOfflineIsMissingData(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Now()
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.Heartbeat(&pb.Heartbeat{AgentId: "b", TsUnixNano: now.UnixNano()})

	applied := s.ingestResultLocked(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 0, Rx: 0, Lost: 0}, now.Add(4*time.Second))

	if applied.Tx != 0 || applied.Rx != 0 || applied.Lost != 0 || applied.LossRatio != 0 {
		t.Fatalf("applied result = %+v, want missing zero result", applied)
	}

	f, _ := s.Flow("a<=>b")
	if f.Tx1s != 0 || f.Rx1s != 0 || f.Lost1s != 0 || f.LossRatio1s != 0 {
		t.Fatalf("source-offline zero result should stay missing: %+v", f)
	}
	if got := f.LossHistory240s[len(f.LossHistory240s)-1]; got != -1 {
		t.Fatalf("source-offline history tail = %v, want -1", got)
	}
}

func TestRuntimeZeroReceiveResultForStoppedFlowIsNotLoss(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "stopped", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.IngestResult(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 0, Rx: 0, Lost: 0})

	f, _ := s.Flow("a<=>b")
	if f.LossRatio1s != 0 || f.Tx1s != 0 || f.Rx1s != 0 || f.Lost1s != 0 {
		t.Fatalf("stopped zero result should not be loss: %+v", f)
	}
}

func TestRuntimeControllerAgentLossReturnsSyntheticResultSummary(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	s.AgentOnline("a", "127.0.0.1:40001", 1)

	got := s.ApplyControllerAgentLoss(time.Now(), 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("synthetic results = %d, want 1", len(got))
	}
	if got[0].FlowId != "a<=>b" || got[0].AgentId != "b" || got[0].Tx != 100 || got[0].Rx != 0 || got[0].Lost != 100 || got[0].LossRatio != 1 {
		t.Fatalf("synthetic result = %+v", got[0])
	}
}

func TestRuntimeIngestReportFinalizesMatchingRawReports(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         now.Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		AgentId:    "a",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  testSeqRanges(0, 9),
	}, now)
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         now.Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         9,
		SeqRanges:  testSeqRanges(0, 8),
	}, now.Add(20*time.Millisecond))

	got := s.finalizeDueReportsLocked(now.Add(2100 * time.Millisecond))
	if len(got) != 1 {
		t.Fatalf("finalized reports len = %d, want 1: %+v", len(got), got)
	}
	if got[0].Tx != 10 || got[0].Rx != 9 || got[0].Lost != 1 || got[0].LossRatio != 0.1 {
		t.Fatalf("finalized report = %+v, want tx/rx/lost 10/9/1", got[0])
	}
	flow, _ := s.Flow("a<=>b")
	if flow.Tx1s != 10 || flow.Rx1s != 9 || flow.Lost1s != 1 || flow.LostTotal != 1 {
		t.Fatalf("flow status = %+v, want finalized controller loss", flow)
	}
}

func TestRuntimeSenderOnlyReportFinalizesAfterDelay(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	ts := now.Add(100 * time.Millisecond).Format(time.RFC3339Nano)
	s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "a", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "sender", Tx: 10, SeqRanges: testSeqRanges(0, 9)}, now)
	got := s.finalizeDueReportsLocked(now.Add(2 * time.Second))
	if len(got) != 0 {
		t.Fatalf("finalized sender report too early = %+v", got)
	}
	got = s.finalizeDueReportsLocked(now.Add(2100 * time.Millisecond))
	if len(got) != 1 || got[0].Lost != 10 || got[0].Rx != 0 {
		t.Fatalf("finalized sender report = %+v, want timeout loss", got)
	}
	flow, _ := s.Flow("a<=>b")
	if flow.LostTotal != 10 || flow.RxTotal != 0 {
		t.Fatalf("flow totals = lost:%d rx:%d, want timeout counters", flow.LostTotal, flow.RxTotal)
	}
}

func TestRuntimeReceiverReportUsesLargestAbsoluteBucketValue(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	ts := now.Add(100 * time.Millisecond).Format(time.RFC3339Nano)
	s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "a", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "sender", Tx: 10, SeqRanges: testSeqRanges(0, 9)}, now)
	s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "b", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "receiver", Rx: 3, SeqRanges: testSeqRanges(0, 2)}, now.Add(10*time.Millisecond))
	s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "b", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "receiver", Rx: 8, SeqRanges: testSeqRanges(0, 7)}, now.Add(20*time.Millisecond))

	got := s.finalizeDueReportsLocked(now.Add(2100 * time.Millisecond))
	if len(got) != 1 {
		t.Fatalf("finalized reports len = %d, want 1: %+v", len(got), got)
	}
	if got[0].Tx != 10 || got[0].Rx != 8 || got[0].Lost != 2 {
		t.Fatalf("finalized report = %+v, want tx/rx/lost 10/8/2", got[0])
	}
}

func TestRuntimeSeparatesAlternatingLossTimeFromOutage(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{OutageThresholdMs: 100, Flows: []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", IntervalMs: 10, State: "running"}}})
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	ts := now.Add(100 * time.Millisecond).Format(time.RFC3339Nano)
	s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "a", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "sender", SeqRanges: testSeqRanges(0, 9)}, now)
	s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "b", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "receiver", SeqRanges: []*pb.SeqRange{{Start: 0, End: 0}, {Start: 2, End: 2}, {Start: 4, End: 4}, {Start: 6, End: 6}, {Start: 8, End: 8}}}, now)
	got := s.finalizeDueReportsLocked(now.Add(3 * time.Second))
	if len(got) != 1 || got[0].Lost != 5 || got[0].OutageMs != 0 {
		t.Fatalf("summary = %+v, want lost=5 outage=0", got)
	}
	flow, _ := s.Flow("a<=>b")
	if flow.LossTimeTotalMs != 50 || flow.OutageCount != 0 || flow.OutageActive {
		t.Fatalf("separated metrics = %+v", flow)
	}
	if records := s.DrainOutageEventRecords(); len(records) != 0 {
		t.Fatalf("outage records = %+v, want none", records)
	}
}

func TestRuntimeDetectsConsecutiveLossAcrossReportBuckets(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{OutageThresholdMs: 100, Flows: []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", IntervalMs: 10, State: "running"}}})
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	for bucket := 0; bucket < 2; bucket++ {
		end := now.Add(time.Duration(bucket+1) * 100 * time.Millisecond)
		startSeq := uint64(bucket * 10)
		s.ingestReportLocked(&pb.ResultReport{Ts: end.Format(time.RFC3339Nano), AgentId: "a", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "sender", SeqRanges: testSeqRanges(startSeq, startSeq+9)}, now)
		var received []*pb.SeqRange
		if bucket == 0 {
			received = testSeqRanges(0, 4)
		} else {
			received = testSeqRanges(15, 19)
		}
		s.ingestReportLocked(&pb.ResultReport{Ts: end.Format(time.RFC3339Nano), AgentId: "b", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "receiver", SeqRanges: received}, now)
	}
	got := s.finalizeDueReportsLocked(now.Add(3 * time.Second))
	if len(got) != 2 || got[1].OutageMs != 100 {
		t.Fatalf("summaries = %+v, want second outage attribution 100ms", got)
	}
	flow, _ := s.Flow("a<=>b")
	if flow.OutageCount != 1 || flow.OutageActive || flow.LastOutageMs != 100 || flow.OutageTotalMs != 100 {
		t.Fatalf("outage metrics = %+v", flow)
	}
	records := s.DrainOutageEventRecords()
	if len(records) != 2 || records[0].State != "started" || records[1].State != "ended" || records[0].EventID != records[1].EventID || records[1].EndReason != "recovered" {
		t.Fatalf("outage records = %+v", records)
	}
}

func TestRuntimeOutageEventEndsOnlyOnRecoveryAcrossSequenceGap(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{OutageThresholdMs: 100, Flows: []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", IntervalMs: 10, State: "running"}}})
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	reports := []struct {
		end      time.Time
		sender   []*pb.SeqRange
		receiver []*pb.SeqRange
	}{
		{end: now.Add(100 * time.Millisecond), sender: testSeqRanges(0, 8)},
		{end: now.Add(200 * time.Millisecond), sender: testSeqRanges(10, 19)},
		{end: now.Add(300 * time.Millisecond), sender: testSeqRanges(20, 20), receiver: testSeqRanges(20, 20)},
	}
	for _, report := range reports {
		ts := report.end.Format(time.RFC3339Nano)
		s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "a", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "sender", SeqRanges: report.sender}, now)
		if len(report.receiver) > 0 {
			s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "b", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "receiver", SeqRanges: report.receiver}, now)
		}
	}

	got := s.finalizeDueReportsLocked(now.Add(3 * time.Second))
	if len(got) != 3 {
		t.Fatalf("finalized reports len = %d, want 3: %+v", len(got), got)
	}
	flow, _ := s.Flow("a<=>b")
	if flow.OutageCount != 1 || flow.OutageActive || flow.LastOutageMs != 190 || flow.OutageTotalMs != 190 {
		t.Fatalf("outage metrics = %+v, want one recovered 190ms outage", flow)
	}
	records := s.DrainOutageEventRecords()
	if len(records) != 2 || records[0].State != "started" || records[1].State != "ended" {
		t.Fatalf("outage records = %+v, want one started/ended pair", records)
	}
	if records[0].EventID != records[1].EventID || records[1].EndReason != "recovered" || records[1].DurationMs != 190 {
		t.Fatalf("outage records = %+v, want matching recovered 190ms event", records)
	}
}

func TestRuntimeClosesActiveOutageWithReason(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{OutageThresholdMs: 100, Flows: []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", IntervalMs: 10, State: "running"}}})
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	r := s.flows["a<=>b"]
	if got := s.applyAggregateOutcomesLocked(r, 10, 0, now, now.Add(2*time.Second)); got != 100 {
		t.Fatalf("attributed outage = %d, want 100", got)
	}
	started := s.DrainOutageEventRecords()
	if len(started) != 1 || started[0].State != "started" {
		t.Fatalf("started records = %+v", started)
	}
	s.CloseActiveOutages(now.Add(100*time.Millisecond), "measurement_stopped")
	ended := s.DrainOutageEventRecords()
	if len(ended) != 1 || ended[0].State != "ended" || ended[0].EndReason != "measurement_stopped" || ended[0].EventID != started[0].EventID {
		t.Fatalf("ended records = %+v", ended)
	}
}

func TestRuntimeIngestReportUsesControllerTimeInsteadOfReportTimestamp(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	now := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         "2030-01-02T03:04:05Z",
		AgentId:    "a",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  testSeqRanges(0, 9),
	}, now)
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         "2020-01-02T03:04:05Z",
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         9,
		SeqRanges:  testSeqRanges(0, 8),
	}, now.Add(20*time.Millisecond))

	got := s.finalizeDueReportsLocked(now.Add(2100 * time.Millisecond))
	if len(got) != 1 {
		t.Fatalf("finalized reports len = %d, want 1: %+v", len(got), got)
	}
	if got[0].Ts != "2026-06-28T12:00:00.1Z" || got[0].Tx != 10 || got[0].Rx != 9 || got[0].Lost != 1 {
		t.Fatalf("finalized report = %+v, want controller-time bucket end with 10/9/1", got[0])
	}
}

func TestRuntimeIngestReportMergesAcrossOneBucketBoundary(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	base := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         "1999-01-01T00:00:00Z",
		AgentId:    "a",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  testSeqRanges(0, 9),
	}, base.Add(95*time.Millisecond))
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         "2040-01-01T00:00:00Z",
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         9,
		SeqRanges:  testSeqRanges(0, 8),
	}, base.Add(105*time.Millisecond))

	got := s.finalizeDueReportsLocked(base.Add(2200 * time.Millisecond))
	if len(got) != 1 {
		t.Fatalf("finalized reports len = %d, want 1: %+v", len(got), got)
	}
	if got[0].Ts != "2026-06-28T12:00:00.1Z" || got[0].Tx != 10 || got[0].Rx != 9 || got[0].Lost != 1 {
		t.Fatalf("finalized report = %+v, want merged first bucket 10/9/1", got[0])
	}
}

func TestRuntimeIngestReportUsesSenderReportTimestampAcrossBunchedArrivals(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	base := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	report1 := "2030-01-02T03:04:05.1Z"
	report2 := "2030-01-02T03:04:05.2Z"

	s.ingestReportLocked(&pb.ResultReport{
		Ts:         report1,
		AgentId:    "a",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  testSeqRanges(0, 9),
	}, base.Add(5*time.Millisecond))
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         report2,
		AgentId:    "a",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  testSeqRanges(10, 19),
	}, base.Add(10*time.Millisecond))
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         "2020-01-02T03:04:05.1Z",
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         10,
		SeqRanges:  testSeqRanges(0, 9),
	}, base.Add(15*time.Millisecond))
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         "2020-01-02T03:04:05.2Z",
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         10,
		SeqRanges:  testSeqRanges(10, 19),
	}, base.Add(20*time.Millisecond))

	got := s.finalizeDueReportsLocked(base.Add(2200 * time.Millisecond))
	if len(got) != 2 {
		t.Fatalf("finalized reports len = %d, want 2: %+v", len(got), got)
	}
	if got[0].Ts != "2026-06-28T12:00:00.1Z" || got[0].Tx != 10 || got[0].Rx != 10 || got[0].Lost != 0 {
		t.Fatalf("first finalized report = %+v, want first 100ms bucket 10/10/0", got[0])
	}
	if got[1].Ts != "2026-06-28T12:00:00.2Z" || got[1].Tx != 10 || got[1].Rx != 10 || got[1].Lost != 0 {
		t.Fatalf("second finalized report = %+v, want second 100ms bucket 10/10/0", got[1])
	}
}

func TestRuntimeIngestReportPrefersOldestUnmatchedOppositeRoleBucket(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	base := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.ingestReportLocked(&pb.ResultReport{
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         9,
		SeqRanges:  testSeqRanges(0, 8),
	}, base.Add(5*time.Millisecond))
	s.ingestReportLocked(&pb.ResultReport{
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         8,
		SeqRanges:  testSeqRanges(10, 17),
	}, base.Add(105*time.Millisecond))
	s.ingestReportLocked(&pb.ResultReport{
		AgentId:    "a",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  testSeqRanges(0, 9),
	}, base.Add(100*time.Millisecond))

	got := s.finalizeDueReportsLocked(base.Add(2200 * time.Millisecond))
	if len(got) != 1 {
		t.Fatalf("finalized reports len = %d, want 1: %+v", len(got), got)
	}
	if got[0].Ts != "2026-06-28T12:00:00.2Z" || got[0].Tx != 10 || got[0].Rx != 9 || got[0].Lost != 1 {
		t.Fatalf("first finalized report = %+v, want sender merged into oldest unmatched receiver bucket", got[0])
	}
	debug := s.DrainDebugRecords()
	foundReceiverOnly := false
	for _, record := range debug {
		if record.FinalState == "receiver_only" {
			foundReceiverOnly = true
			break
		}
	}
	if !foundReceiverOnly {
		t.Fatalf("debug records = %+v, want receiver_only for unmatched newer bucket", debug)
	}
}

func TestRuntimeIngestReportDoesNotMergeOldStaleBucketIntoCurrentReport(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	base := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)

	s.ingestReportLocked(&pb.ResultReport{
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         9,
		SeqRanges:  testSeqRanges(0, 8),
	}, base.Add(5*time.Millisecond))

	s.ingestReportLocked(&pb.ResultReport{
		AgentId:    "a",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  testSeqRanges(10, 19),
	}, base.Add(250*time.Millisecond))
	s.ingestReportLocked(&pb.ResultReport{
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         10,
		SeqRanges:  testSeqRanges(10, 19),
	}, base.Add(260*time.Millisecond))

	got := s.finalizeDueReportsLocked(base.Add(2500 * time.Millisecond))
	if len(got) != 1 {
		t.Fatalf("finalized reports len = %d, want 1: %+v", len(got), got)
	}
	if got[0].Ts != "2026-06-28T12:00:00.3Z" || got[0].Tx != 10 || got[0].Rx != 10 || got[0].Lost != 0 {
		t.Fatalf("finalized report = %+v, want current sender/receiver pair to stay in current bucket", got[0])
	}
	debug := s.DrainDebugRecords()
	if len(debug) == 0 || debug[len(debug)-1].FinalState != "receiver_only" {
		t.Fatalf("debug records = %+v, want stale receiver-only bucket in debug log", debug)
	}
}

func TestRuntimeIngestReportDoesNotDropLateMergeWhenCurrentBucketAlreadyFinalized(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	base := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)

	// Older receiver-only bucket remains pending, but still within one-window merge tolerance.
	s.ingestReportLocked(&pb.ResultReport{
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         9,
		SeqRanges:  testSeqRanges(0, 8),
	}, base.Add(300*time.Millisecond))

	// Current controller bucket already has a finalized sample.
	s.ingestResultLocked(&pb.ResultSummary{
		AgentId:    "b",
		FlowId:     "a<=>b",
		FlowKey:    protocol.ComputeFlowKey("a", "b", "a<=>b"),
		Src:        "a",
		Dst:        "b",
		IntervalMs: 100,
		Tx:         10,
		Rx:         10,
		Lost:       0,
		LossRatio:  0,
	}, base.Add(350*time.Millisecond))

	// A late sender report for the older bucket arrives while the current 100ms bucket
	// already has a finalized sample. It must still merge into the old pending bucket.
	s.ingestReportLocked(&pb.ResultReport{
		AgentId:    "a",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  testSeqRanges(0, 9),
	}, base.Add(395*time.Millisecond))

	got := s.finalizeDueReportsLocked(base.Add(2500 * time.Millisecond))
	if len(got) != 1 {
		t.Fatalf("late finalized reports len = %d, want 1: %+v", len(got), got)
	}
	if got[0].Ts != "2026-06-28T12:00:00.4Z" || got[0].Tx != 10 || got[0].Rx != 9 || got[0].Lost != 1 {
		t.Fatalf("late finalized report = %+v, want old pending bucket merged after current bucket finalized", got[0])
	}
}

func TestRuntimeIngestReportDoesNotMergeWhenControllerArrivalTooFarApart(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	base := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         "1999-01-01T00:00:00Z",
		AgentId:    "a",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  testSeqRanges(0, 9),
	}, base)
	got := s.finalizeDueReportsLocked(base.Add(2100 * time.Millisecond))
	if len(got) != 1 {
		t.Fatalf("pre-finalized reports len = %d, want 1: %+v", len(got), got)
	}
	if got[0].Ts != "2026-06-28T12:00:00.1Z" || got[0].Tx != 10 || got[0].Rx != 0 || got[0].Lost != 10 {
		t.Fatalf("pre-finalized report = %+v, want sender-only timeout loss", got[0])
	}
	s.ingestReportLocked(&pb.ResultReport{
		Ts:         "2040-01-01T00:00:00Z",
		AgentId:    "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         9,
		SeqRanges:  testSeqRanges(0, 8),
	}, base.Add(2200*time.Millisecond))
	got = s.finalizeDueReportsLocked(base.Add(4300 * time.Millisecond))
	if len(got) != 0 {
		t.Fatalf("second finalized reports len = %d, want 0: %+v", len(got), got)
	}
	debug := s.DrainDebugRecords()
	if len(debug) == 0 || debug[len(debug)-1].FinalState != "receiver_only" {
		t.Fatalf("debug records = %+v, want receiver_only after delayed receiver report", debug)
	}
}

func TestRuntimeSlidingWindowUpdatesOneSecondView(t *testing.T) {
	s := NewRuntimeStore(model.MeshConfig{Flows: []model.MeshFlowConfig{{
		ID:         "a<=>b",
		Src:        "a",
		Dst:        "b",
		IntervalMs: 10,
		State:      "running",
	}}})
	base := time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		ts := base.Add(time.Duration(i+1) * 100 * time.Millisecond).Format(time.RFC3339Nano)
		now := base.Add(time.Duration(i) * 100 * time.Millisecond)
		start := uint64(i * 10)
		s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "a", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "sender", Tx: 10, SeqRanges: testSeqRanges(start, start+9)}, now)
		s.ingestReportLocked(&pb.ResultReport{Ts: ts, AgentId: "b", FlowId: "a<=>b", SessionId: 1, Src: "a", Dst: "b", IntervalMs: 10, WindowMs: 100, Role: "receiver", Rx: 9, SeqRanges: testSeqRanges(start, start+8)}, now.Add(10*time.Millisecond))
	}
	got := s.finalizeDueReportsLocked(base.Add(3100 * time.Millisecond))
	if len(got) != 10 {
		t.Fatalf("finalized reports len = %d, want 10", len(got))
	}
	flow, _ := s.Flow("a<=>b")
	if flow.Tx1s != 100 || flow.Rx1s != 90 || flow.Lost1s != 10 || flow.LossRatio1s != 0.1 {
		t.Fatalf("flow 1s status = %+v, want 100/90/10", flow)
	}
}

func TestRuntimeControllerAgentLossSyntheticResultUsesLatestObservedTx(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}
	s := NewRuntimeStore(mesh)
	now := time.Now()
	s.AgentOnline("a", "127.0.0.1:40001", 1)
	s.AgentOnline("b", "127.0.0.1:40002", 1)
	s.IngestResult(&pb.ResultSummary{FlowId: "a<=>b", AgentId: "b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 77, Rx: 76, Lost: 1, LossRatio: 1.0 / 77.0})
	s.AgentOffline("b")
	s.agents["a"].LastHeartbeat = now.Add(2 * time.Second)

	got := s.ApplyControllerAgentLoss(now.Add(5*time.Second), 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("synthetic results = %d, want 1", len(got))
	}
	if got[0].FlowId != "a<=>b" || got[0].AgentId != "b" || got[0].Tx != 100 || got[0].Rx != 0 || got[0].Lost != 100 || got[0].LossRatio != 1 {
		t.Fatalf("synthetic result = %+v", got[0])
	}
}
