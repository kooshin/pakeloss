package model

import (
	"testing"
	"time"
)

func TestNewFlowSnapshotPadsLossHistories(t *testing.T) {
	snap := NewFlowSnapshot(FlowRuntimeStatus{
		LossHistory60s:  []float64{0.01, 0.02},
		LossHistory240s: []float64{0.03, 0.04},
	})

	if len(snap.LossHistory60s) != flowSnapshotHistory60sLength {
		t.Fatalf("loss_history_60s len = %d, want %d", len(snap.LossHistory60s), flowSnapshotHistory60sLength)
	}
	if len(snap.LossHistory240s) != flowSnapshotHistory240sLength {
		t.Fatalf("loss_history_240s len = %d, want %d", len(snap.LossHistory240s), flowSnapshotHistory240sLength)
	}
	if snap.LossHistory60s[flowSnapshotHistory60sLength-2] != 0.01 || snap.LossHistory60s[flowSnapshotHistory60sLength-1] != 0.02 {
		t.Fatalf("loss_history_60s tail = %v, want [0.01 0.02]", snap.LossHistory60s[flowSnapshotHistory60sLength-2:])
	}
	if snap.LossHistory240s[flowSnapshotHistory240sLength-2] != 0.03 || snap.LossHistory240s[flowSnapshotHistory240sLength-1] != 0.04 {
		t.Fatalf("loss_history_240s tail = %v, want [0.03 0.04]", snap.LossHistory240s[flowSnapshotHistory240sLength-2:])
	}
	for i := 0; i < flowSnapshotHistory60sLength-2; i++ {
		if snap.LossHistory60s[i] != 0 {
			t.Fatalf("loss_history_60s[%d] = %v, want 0", i, snap.LossHistory60s[i])
		}
	}
}

func TestNewFlowSnapshotTrimsLossHistoriesToNewestSamples(t *testing.T) {
	h60 := make([]float64, flowSnapshotHistory60sLength+5)
	h240 := make([]float64, flowSnapshotHistory240sLength+5)
	for i := range h60 {
		h60[i] = float64(i)
	}
	for i := range h240 {
		h240[i] = float64(i)
	}

	snap := NewFlowSnapshot(FlowRuntimeStatus{
		LossHistory60s:  h60,
		LossHistory240s: h240,
	})

	if len(snap.LossHistory60s) != flowSnapshotHistory60sLength {
		t.Fatalf("loss_history_60s len = %d, want %d", len(snap.LossHistory60s), flowSnapshotHistory60sLength)
	}
	if len(snap.LossHistory240s) != flowSnapshotHistory240sLength {
		t.Fatalf("loss_history_240s len = %d, want %d", len(snap.LossHistory240s), flowSnapshotHistory240sLength)
	}
	if snap.LossHistory60s[0] != 5 || snap.LossHistory60s[len(snap.LossHistory60s)-1] != float64(len(h60)-1) {
		t.Fatalf("unexpected 60s trim: first=%v last=%v", snap.LossHistory60s[0], snap.LossHistory60s[len(snap.LossHistory60s)-1])
	}
	if snap.LossHistory240s[0] != 5 || snap.LossHistory240s[len(snap.LossHistory240s)-1] != float64(len(h240)-1) {
		t.Fatalf("unexpected 240s trim: first=%v last=%v", snap.LossHistory240s[0], snap.LossHistory240s[len(snap.LossHistory240s)-1])
	}
}

func TestNewFlowSnapshotCopiesTrafficTotals(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	snap := NewFlowSnapshot(FlowRuntimeStatus{
		LastSeen:           now.Add(5 * time.Second),
		LastReportedAt:     now,
		Tx1s:               10,
		Rx1s:               9,
		TxTotal:            120,
		RxTotal:            110,
		LossTime1sMs:       10,
		LossTimeTotalMs:    100,
		IsolatedLossEvents: 2,
		OutageCount:        3,
		OutageActive:       true,
		CurrentOutageMs:    400,
		LastOutageMs:       300,
		MaxOutageMs:        900,
		OutageTotalMs:      1600,
		OutageThresholdMs:  100,
	})

	if snap.Tx1s != 10 || snap.Rx1s != 9 {
		t.Fatalf("unexpected 1s counters: %+v", snap)
	}
	if snap.TxTotal != 120 || snap.RxTotal != 110 {
		t.Fatalf("unexpected total counters: %+v", snap)
	}
	if snap.LossTime1sMs != 10 || snap.LossTimeTotalMs != 100 || snap.OutageThresholdMs != 100 {
		t.Fatalf("unexpected loss time fields: %+v", snap)
	}
	if snap.IsolatedLossEvents != 2 || snap.OutageCount != 3 || !snap.OutageActive || snap.CurrentOutageMs != 400 || snap.LastOutageMs != 300 || snap.MaxOutageMs != 900 || snap.OutageTotalMs != 1600 {
		t.Fatalf("unexpected outage counters: %+v", snap)
	}
	if !snap.LastSeen.Equal(now) {
		t.Fatalf("last_seen = %s, want %s", snap.LastSeen, now)
	}
}
