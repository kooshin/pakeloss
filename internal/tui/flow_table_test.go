package tui

import (
	"reflect"
	"testing"

	"pakeloss/internal/model"
)

func TestFlowTableValuesMatchTUIDisplay(t *testing.T) {
	flow := model.FlowSnapshot{
		FlowID:          "rt1->rt2",
		Src:             "node-a",
		Dst:             "node-b",
		ActualState:     "running",
		OutageTotalMs:   400,
		LostTotal:       3,
		LossRatio1s:     0.1,
		LossTimeTotalMs: 30,
		ReorderTotal:    4,
		DuplicateTotal:  5,
		Tx1s:            10,
		Rx1s:            9,
		TxTotal:         10,
		RxTotal:         9,
	}
	want := []string{"rt1->rt2", "RUN", "0.4s", "0.0s", "3", "4", "5", "10", "9"}
	if got := tuiFlowTableValues(flow); !reflect.DeepEqual(got, want) {
		t.Fatalf("tuiFlowTableValues() = %v, want %v", got, want)
	}
}

func TestFlowTableValuesShowOfflineTXAsDash(t *testing.T) {
	flow := model.FlowSnapshot{
		FlowID:      "flow-1",
		ActualState: "offline",
		LastError:   "source agent communication lost; loss inferred by controller",
		Tx1s:        10,
		Rx1s:        7,
		TxTotal:     10,
		RxTotal:     7,
	}
	got := tuiFlowTableValues(flow)
	if got[7] != "-" {
		t.Fatalf("tuiFlowTableValues()[7] = %q, want -", got[7])
	}
	if got[8] != "7" {
		t.Fatalf("tuiFlowTableValues()[8] = %q, want 7", got[8])
	}
}

func TestFlowTableValuesKeepCLIFlowTotals(t *testing.T) {
	flow := model.FlowSnapshot{
		FlowID:             "rt1->rt2",
		Src:                "node-a",
		Dst:                "node-b",
		ActualState:        "running",
		OutageTotalMs:      400,
		OutageCount:        2,
		IsolatedLossEvents: 3,
		LostTotal:          3,
		LossRatio1s:        0.1,
		LossTimeTotalMs:    30,
		ReorderTotal:       4,
		DuplicateTotal:     5,
		Tx1s:               10,
		Rx1s:               9,
		TxTotal:            100,
		RxTotal:            90,
	}
	want := []string{"rt1->rt2", "RUN", "0.4s", "10.0%", "0.0s", "2", "3", "3", "4", "5", "100", "90"}
	if got := FlowTableValues(flow); !reflect.DeepEqual(got, want) {
		t.Fatalf("FlowTableValues() = %v, want %v", got, want)
	}
}
