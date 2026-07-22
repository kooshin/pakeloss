package controller

import (
	"testing"

	"pakeloss/internal/model"
)

func TestCompileSnapshot(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion:      3,
		ReportBucketFactor: 5,
		Nodes:              []model.NodeConfig{{ID: "node-a", UDPAddr: "127.0.0.1:1"}, {ID: "node-b", UDPAddr: "127.0.0.1:2"}},
		Flows:              []model.MeshFlowConfig{{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", IntervalMs: 10, PacketSize: 256, SourcePortCount: 4, LossConfirmWindowMs: 2500, State: "running"}},
	}
	snap, err := CompileSnapshot(mesh, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if snap.ConfigVersion != 3 || len(snap.Flows) != 1 || snap.Flows[0].SrcId != "node-a" || snap.Flows[0].DstAddr != "127.0.0.1:2" || snap.Flows[0].SourcePortCount != 4 || snap.Flows[0].LossConfirmWindowMs != 2500 || snap.Flows[0].FlowKey == 0 || snap.ConfigHash == "" {
		t.Fatalf("bad snapshot: %+v", snap)
	}
	if snap.Flows[0].ReportWindowMs != 50 {
		t.Fatalf("report window = %d, want 50", snap.Flows[0].ReportWindowMs)
	}
}

func TestCompileSnapshotRejectsTooSmallPacketSize(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 3,
		Nodes:         []model.NodeConfig{{ID: "node-a", UDPAddr: "127.0.0.1:1"}, {ID: "node-b", UDPAddr: "127.0.0.1:2"}},
		Flows:         []model.MeshFlowConfig{{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", IntervalMs: 10, PacketSize: 32, State: "running"}},
	}
	if _, err := CompileSnapshot(mesh, "node-a"); err == nil {
		t.Fatal("expected invalid packet size error")
	}
}

func TestApplyDefaultsStartsStopped(t *testing.T) {
	mesh := model.MeshConfig{
		Flows: []model.MeshFlowConfig{{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b"}},
	}

	applyDefaults(&mesh)

	if mesh.AutoFlow.State != "stopped" {
		t.Fatalf("default state = %q, want stopped", mesh.AutoFlow.State)
	}
	if mesh.AutoFlow.SourcePortCount != 8 {
		t.Fatalf("default source port count = %d, want 8", mesh.AutoFlow.SourcePortCount)
	}
	if mesh.AutoFlow.LossConfirmWindowMs != 2000 {
		t.Fatalf("default loss confirm window = %d, want 2000", mesh.AutoFlow.LossConfirmWindowMs)
	}
	if mesh.Flows[0].State != "stopped" {
		t.Fatalf("flow state = %q, want stopped", mesh.Flows[0].State)
	}
	if mesh.Flows[0].SourcePortCount != 8 {
		t.Fatalf("flow source port count = %d, want 8", mesh.Flows[0].SourcePortCount)
	}
	if mesh.Flows[0].LossConfirmWindowMs != 2000 {
		t.Fatalf("flow loss confirm window = %d, want 2000", mesh.Flows[0].LossConfirmWindowMs)
	}
}

func TestApplyDefaultsGeneratesFullMeshFlows(t *testing.T) {
	mesh := model.MeshConfig{
		Nodes: []model.NodeConfig{
			{ID: "node-a", UDPAddr: "127.0.0.1:40001"},
			{ID: "node-b", UDPAddr: "127.0.0.1:40002"},
			{ID: "node-c", UDPAddr: "127.0.0.1:40003"},
		},
		AutoFlow: model.AutoFlowConfig{
			IntervalMs: 10,
			PacketSize: 96,
			State:      "running",
		},
	}

	applyDefaults(&mesh)

	if len(mesh.Flows) != 6 {
		t.Fatalf("flows = %d, want 6", len(mesh.Flows))
	}

	want := map[string]struct{}{
		"node-a->node-b": {},
		"node-b->node-a": {},
		"node-a->node-c": {},
		"node-c->node-a": {},
		"node-b->node-c": {},
		"node-c->node-b": {},
	}
	for _, f := range mesh.Flows {
		if _, ok := want[f.ID]; !ok {
			t.Fatalf("unexpected flow generated: %+v", f)
		}
		delete(want, f.ID)
		if f.IntervalMs != 10 || f.PacketSize != 96 || f.SourcePortCount != 8 || f.LossConfirmWindowMs != 2000 || f.State != "running" {
			t.Fatalf("generated flow defaults were not applied: %+v", f)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing generated flows: %+v", want)
	}
}

func TestCompileSnapshotIncludesInboundFlowForReceiver(t *testing.T) {
	mesh := model.MeshConfig{
		ConfigVersion: 3,
		Nodes: []model.NodeConfig{
			{ID: "node-a", UDPAddr: "127.0.0.1:1"},
			{ID: "node-b", UDPAddr: "127.0.0.1:2"},
		},
		Flows: []model.MeshFlowConfig{{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", IntervalMs: 10, PacketSize: 256, LossConfirmWindowMs: 3000, State: "running"}},
	}
	snap, err := CompileSnapshot(mesh, "node-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Flows) != 1 {
		t.Fatalf("flows = %d, want 1", len(snap.Flows))
	}
	if snap.Flows[0].SrcId != "node-a" || snap.Flows[0].DstId != "node-b" || snap.Flows[0].LossConfirmWindowMs != 3000 {
		t.Fatalf("unexpected inbound flow snapshot: %+v", snap.Flows[0])
	}
}

func TestSetAllFlowStates(t *testing.T) {
	store := NewConfigStore(model.MeshConfig{
		ConfigVersion: 7,
		Flows: []model.MeshFlowConfig{
			{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", State: "stopped"},
			{ID: "node-a<=>node-c", Src: "node-a", Dst: "node-c", State: "running"},
		},
	})

	mesh := store.SetAllFlowStates("running")

	if mesh.ConfigVersion != 8 {
		t.Fatalf("config version = %d, want 8", mesh.ConfigVersion)
	}
	for _, f := range mesh.Flows {
		if f.State != "running" {
			t.Fatalf("flow %s state = %q, want running", f.ID, f.State)
		}
	}
}

func TestFinalizeMeshConfigRejectsStaticNodesInAutoMode(t *testing.T) {
	mesh := model.MeshConfig{
		DiscoveryMode: "auto",
		Nodes:         []model.NodeConfig{{ID: "node-a", UDPAddr: "127.0.0.1:40001"}},
	}

	if err := FinalizeMeshConfig(&mesh); err == nil {
		t.Fatal("expected auto mode validation error")
	}
}

func TestAutoDiscoveryBuildsBidirectionalFullMesh(t *testing.T) {
	store := NewConfigStore(model.MeshConfig{
		ConfigVersion: 1,
		DiscoveryMode: "auto",
		AutoFlow: model.AutoFlowConfig{
			IntervalMs:      10,
			PacketSize:      96,
			SourcePortCount: 8,
			State:           "running",
		},
	})

	mesh, changed, err := store.UpsertDiscoveredNode("node-a", "192.0.2.10:40001")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("first node should change mesh")
	}
	if mesh.ConfigVersion != 2 || len(mesh.Nodes) != 1 || len(mesh.Flows) != 0 {
		t.Fatalf("unexpected mesh after first node: %+v", mesh)
	}

	mesh, changed, err = store.UpsertDiscoveredNode("node-b", "192.0.2.11:40002")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("second node should change mesh")
	}
	if mesh.ConfigVersion != 3 || len(mesh.Nodes) != 2 || len(mesh.Flows) != 2 {
		t.Fatalf("unexpected mesh after second node: %+v", mesh)
	}
	for _, flow := range mesh.Flows {
		if flow.State != "stopped" {
			t.Fatalf("auto-discovered flow state = %q, want stopped: %+v", flow.State, flow)
		}
	}
}

func TestAutoDiscoveryReRegisterOnlyChangesVersionOnAddrChange(t *testing.T) {
	store := NewConfigStore(model.MeshConfig{
		ConfigVersion: 1,
		DiscoveryMode: "auto",
	})

	mesh, changed, err := store.UpsertDiscoveredNode("node-a", "192.0.2.10:40001")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || mesh.ConfigVersion != 2 {
		t.Fatalf("first registration changed=%v version=%d, want true/2", changed, mesh.ConfigVersion)
	}

	mesh, changed, err = store.UpsertDiscoveredNode("node-a", "192.0.2.10:40001")
	if err != nil {
		t.Fatal(err)
	}
	if changed || mesh.ConfigVersion != 2 {
		t.Fatalf("same registration changed=%v version=%d, want false/2", changed, mesh.ConfigVersion)
	}

	mesh, changed, err = store.UpsertDiscoveredNode("node-a", "192.0.2.20:40001")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || mesh.ConfigVersion != 3 || mesh.Nodes[0].UDPAddr != "192.0.2.20:40001" {
		t.Fatalf("updated registration changed=%v mesh=%+v", changed, mesh)
	}
}

func TestAutoDiscoveryKeepsExistingFlowStateAndNewFlowsStartStopped(t *testing.T) {
	store := NewConfigStore(model.MeshConfig{
		ConfigVersion: 1,
		DiscoveryMode: "auto",
		AutoFlow: model.AutoFlowConfig{
			IntervalMs:      10,
			PacketSize:      96,
			SourcePortCount: 8,
			State:           "running",
		},
	})

	if _, _, err := store.UpsertDiscoveredNode("node-a", "192.0.2.10:40001"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.UpsertDiscoveredNode("node-b", "192.0.2.11:40002"); err != nil {
		t.Fatal(err)
	}
	mesh := store.SetAllFlowStates("running")
	if mesh.ConfigVersion != 4 {
		t.Fatalf("version after start all = %d, want 4", mesh.ConfigVersion)
	}

	mesh, changed, err := store.UpsertDiscoveredNode("node-c", "192.0.2.12:40003")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || mesh.ConfigVersion != 5 || len(mesh.Flows) != 6 {
		t.Fatalf("unexpected mesh after third node: changed=%v mesh=%+v", changed, mesh)
	}

	states := map[string]string{}
	for _, flow := range mesh.Flows {
		states[flow.ID] = flow.State
	}
	if states["node-a->node-b"] != "running" || states["node-b->node-a"] != "running" {
		t.Fatalf("existing flow states not preserved: %+v", states)
	}
	if states["node-a->node-c"] != "stopped" || states["node-c->node-a"] != "stopped" || states["node-b->node-c"] != "stopped" || states["node-c->node-b"] != "stopped" {
		t.Fatalf("new flow states should start stopped: %+v", states)
	}
}

func TestAutoDiscoveryDisabledAgentIsRemovedFromMeshAndReenabledFlowsStartStopped(t *testing.T) {
	store := NewConfigStore(model.MeshConfig{
		ConfigVersion: 1,
		DiscoveryMode: "auto",
		AutoFlow: model.AutoFlowConfig{
			IntervalMs:      10,
			PacketSize:      96,
			SourcePortCount: 8,
			State:           "running",
		},
	})

	if _, _, err := store.UpsertDiscoveredNode("node-a", "192.0.2.10:40001"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.UpsertDiscoveredNode("node-b", "192.0.2.11:40002"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.UpsertDiscoveredNode("node-c", "192.0.2.12:40003"); err != nil {
		t.Fatal(err)
	}
	mesh := store.SetAllFlowStates("running")
	if mesh.ConfigVersion != 5 {
		t.Fatalf("config version after start all = %d, want 5", mesh.ConfigVersion)
	}
	mesh = store.SetAllFlowStates("stopped")

	mesh, err := store.SetAgentEnabled("node-c", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(mesh.Nodes) != 3 {
		t.Fatalf("nodes len = %d, want 3", len(mesh.Nodes))
	}
	if len(mesh.Flows) != 2 {
		t.Fatalf("flows len after disable = %d, want 2", len(mesh.Flows))
	}
	for _, flow := range mesh.Flows {
		if flow.Src == "node-c" || flow.Dst == "node-c" {
			t.Fatalf("disabled node-c should be removed from auto mesh: %+v", flow)
		}
	}

	mesh, err = store.SetAgentEnabled("node-c", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(mesh.Flows) != 6 {
		t.Fatalf("flows len after re-enable = %d, want 6", len(mesh.Flows))
	}
	states := map[string]string{}
	for _, flow := range mesh.Flows {
		states[flow.ID] = flow.State
	}
	if states["node-a->node-b"] != "stopped" || states["node-b->node-a"] != "stopped" {
		t.Fatalf("existing flows should stay stopped after re-enable: %+v", states)
	}
	if states["node-a->node-c"] != "stopped" || states["node-c->node-a"] != "stopped" || states["node-b->node-c"] != "stopped" || states["node-c->node-b"] != "stopped" {
		t.Fatalf("re-enabled flows should start stopped: %+v", states)
	}
}

func TestAutoDiscoverySetAgentEnabledRequiresAllFlowsStopped(t *testing.T) {
	store := NewConfigStore(model.MeshConfig{
		ConfigVersion: 1,
		DiscoveryMode: "auto",
	})

	if _, _, err := store.UpsertDiscoveredNode("node-a", "192.0.2.10:40001"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.UpsertDiscoveredNode("node-b", "192.0.2.11:40002"); err != nil {
		t.Fatal(err)
	}
	store.SetAllFlowStates("running")

	if _, err := store.SetAgentEnabled("node-a", false); err != ErrAllFlowsMustBeStopped {
		t.Fatalf("err = %v, want %v", err, ErrAllFlowsMustBeStopped)
	}
}
