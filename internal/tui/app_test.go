package tui

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"pakeloss/internal/model"
)

func TestSortFlowsByFlowAscending(t *testing.T) {
	flows := []model.FlowSnapshot{
		{FlowID: "node-a<=>node-c", LossRatio1s: 0.20},
		{FlowID: "node-a<=>node-b", LossRatio1s: 0.01},
		{FlowID: "node-b<=>node-c", LossRatio1s: 0.10},
	}

	got := sortFlows(flows, "flow_asc")

	want := []string{"node-a<=>node-b", "node-a<=>node-c", "node-b<=>node-c"}
	for i, id := range want {
		if got[i].FlowID != id {
			t.Fatalf("flow asc sort[%d] = %s, want %s", i, got[i].FlowID, id)
		}
	}
}

func TestSortFlowsByFlowDescending(t *testing.T) {
	flows := []model.FlowSnapshot{
		{FlowID: "node-a<=>node-c"},
		{FlowID: "node-a<=>node-b"},
		{FlowID: "node-b<=>node-c"},
	}

	got := sortFlows(flows, "flow_desc")

	want := []string{"node-b<=>node-c", "node-a<=>node-c", "node-a<=>node-b"}
	for i, id := range want {
		if got[i].FlowID != id {
			t.Fatalf("flow desc sort[%d] = %s, want %s", i, got[i].FlowID, id)
		}
	}
}

func TestSortFlowsByActualState(t *testing.T) {
	flows := []model.FlowSnapshot{
		{FlowID: "b", ActualState: "stopped"},
		{FlowID: "a", ActualState: "running"},
		{FlowID: "c", ActualState: "offline"},
	}

	got := sortFlows(flows, "act_asc")
	if got[0].FlowID != "c" || got[1].FlowID != "a" || got[2].FlowID != "b" {
		t.Fatalf("unexpected act asc sort: %+v", got)
	}

	got = sortFlows(flows, "act_desc")
	if got[0].FlowID != "b" || got[1].FlowID != "a" || got[2].FlowID != "c" {
		t.Fatalf("unexpected act desc sort: %+v", got)
	}
}

func TestSortFlowsByOutage(t *testing.T) {
	flows := []model.FlowSnapshot{
		{FlowID: "low", OutageTotalMs: 100},
		{FlowID: "high", OutageActive: true, CurrentOutageMs: 50, LastOutageMs: 50, OutageTotalMs: 500},
		{FlowID: "zero"},
	}

	got := sortFlows(flows, "outage_asc")
	if got[0].FlowID != "zero" || got[1].FlowID != "low" || got[2].FlowID != "high" {
		t.Fatalf("unexpected outage asc sort: %+v", got)
	}

	got = sortFlows(flows, "outage_desc")
	if got[0].FlowID != "high" || got[1].FlowID != "low" || got[2].FlowID != "zero" {
		t.Fatalf("unexpected outage desc sort: %+v", got)
	}
}

func TestSortFlowsByLost(t *testing.T) {
	flows := []model.FlowSnapshot{
		{FlowID: "low", LostTotal: 10},
		{FlowID: "high", LostTotal: 50},
		{FlowID: "zero"},
	}

	got := sortFlows(flows, "lost_asc")
	if got[0].FlowID != "zero" || got[1].FlowID != "low" || got[2].FlowID != "high" {
		t.Fatalf("unexpected lost asc sort: %+v", got)
	}

	got = sortFlows(flows, "lost_desc")
	if got[0].FlowID != "high" || got[1].FlowID != "low" || got[2].FlowID != "zero" {
		t.Fatalf("unexpected lost desc sort: %+v", got)
	}
}

func TestNextSortMode(t *testing.T) {
	sequence := []string{"flow_asc", "flow_desc", "act_asc", "act_desc", "outage_asc", "outage_desc", "lost_asc", "lost_desc", "flow_asc"}
	mode := sequence[0]
	for i := 1; i < len(sequence); i++ {
		mode = nextSortMode(mode)
		if mode != sequence[i] {
			t.Fatalf("mode step %d = %s, want %s", i, mode, sequence[i])
		}
	}
}

func TestSortModeLabel(t *testing.T) {
	tests := map[string]string{
		"flow_asc":    "Flow asc",
		"flow_desc":   "Flow desc",
		"act_asc":     "Act asc",
		"act_desc":    "Act desc",
		"outage_asc":  "Outage asc",
		"outage_desc": "Outage desc",
		"lost_asc":    "Lost asc",
		"lost_desc":   "Lost desc",
		"unknown":     "Flow asc",
	}
	for mode, want := range tests {
		if got := sortModeLabel(mode); got != want {
			t.Fatalf("sortModeLabel(%q) = %q, want %q", mode, got, want)
		}
	}
}

func TestRestartAllKeyRunsImmediately(t *testing.T) {
	var restartCalls atomic.Int32
	client := &Client{
		base: "http://controller.test",
		http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body := "{}"
			switch r.URL.Path {
			case "/api/v1/flows":
				body = "[]"
			case "/api/v1/agents":
				body = "[]"
			case "/api/v1/status":
				body = `{"measurement_session_id":""}`
			case "/api/v1/flows/restart":
				if r.Method != http.MethodPost {
					t.Fatalf("restart all method = %s, want POST", r.Method)
				}
				restartCalls.Add(1)
			default:
				return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}

	app := NewApp(client, "unicode")
	app.handleKey(tcell.NewEventKey(tcell.KeyRune, 'c', 0))

	if restartCalls.Load() != 1 {
		t.Fatalf("restart all calls = %d, want 1", restartCalls.Load())
	}

	app.handleKey(tcell.NewEventKey(tcell.KeyRune, 'y', 0))
	if restartCalls.Load() != 1 {
		t.Fatalf("unexpected extra restart call, got %d", restartCalls.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestAllFlowsRunning(t *testing.T) {
	if allFlowsRunning(nil) {
		t.Fatal("empty flow list should not be treated as running")
	}
	if allFlowsRunning([]model.FlowSnapshot{{FlowID: "a", DesiredState: "running"}, {FlowID: "b", DesiredState: "stopped"}}) {
		t.Fatal("mixed flow states should not be treated as all running")
	}
	if !allFlowsRunning([]model.FlowSnapshot{{FlowID: "a", DesiredState: "running"}, {FlowID: "b", DesiredState: "running"}}) {
		t.Fatal("all running flow states should be treated as running")
	}
}

func TestNewAppUsesConfiguredRefreshInterval(t *testing.T) {
	app := NewApp(nil, "unicode", 1500*time.Millisecond)
	if app.refreshInterval != 1500*time.Millisecond {
		t.Fatalf("refresh interval = %s, want 1.5s", app.refreshInterval)
	}
}

func TestFlowHasNoTraffic(t *testing.T) {
	if !flowHasNoTraffic(model.FlowSnapshot{DesiredState: "stopped", Tx1s: 100, LossHistory240s: []float64{0}}) {
		t.Fatal("stopped flow should be no traffic")
	}
	if !flowHasNoTraffic(model.FlowSnapshot{DesiredState: "running", LossHistory240s: []float64{0}}) {
		t.Fatal("zero-counter flow should be no traffic")
	}
	if flowHasNoTraffic(model.FlowSnapshot{DesiredState: "running", Tx1s: 100, Rx1s: 100, LossHistory240s: []float64{0}}) {
		t.Fatal("active zero-loss flow should not be no traffic")
	}
	if flowHasNoTraffic(model.FlowSnapshot{DesiredState: "running", LastError: "receiver agent communication lost; loss inferred from sender agent"}) {
		t.Fatal("receiver communication loss should not be no traffic")
	}
}

func TestStateShort(t *testing.T) {
	tests := map[string]string{
		"running": "RUN",
		"stopped": "STP",
		"offline": "OFF",
		"unknown": "UNK",
		"":        "UNK",
		"paused":  "PAUSED",
	}
	for in, want := range tests {
		if got := stateShort(in); got != want {
			t.Fatalf("stateShort(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFlowDisplayUsesMeasuredResult(t *testing.T) {
	flow := model.FlowSnapshot{
		FlowID:          "node-a<=>node-b",
		Src:             "node-a",
		Dst:             "node-b",
		DesiredState:    "running",
		Tx1s:            100,
		Rx1s:            100,
		LossHistory240s: []float64{0},
	}

	if flow.LossRatio1s != 0 || flow.Tx1s != 100 || flow.Rx1s != 100 || flow.Lost1s != 0 {
		t.Fatalf("measured result should be unchanged: %+v", flow)
	}
}

func TestRefreshHeaderShowsRealtimeTraffic(t *testing.T) {
	client := &Client{
		base: "http://controller.test",
		http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body := "[]"
			switch r.URL.Path {
			case "/api/v1/flows":
				body = `[{"flow_id":"node-a<=>node-b","desired_state":"running","actual_state":"running","packet_size":64,"tx_1s":100,"rx_1s":90}]`
			case "/api/v1/agents":
				body = `[{"agent_id":"node-a","status":"online"}]`
			case "/api/v1/status":
				body = `{"measurement_session_id":"26062301"}`
			default:
				return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}

	app := NewApp(client, "unicode")
	app.refresh()

	header := app.header.GetText(false)
	if !strings.Contains(header, "Agents On:1 Off:0   Flows Run:1 Stop:0 Off:0") {
		t.Fatalf("header = %q, want clearer compact agent/flow counters", header)
	}
	if !strings.Contains(header, "Session: 26062301") {
		t.Fatalf("header = %q, want session id", header)
	}
	if !strings.Contains(header, "TX: 51.2 kbps / 100 pps   RX: 46.1 kbps / 90 pps") {
		t.Fatalf("header = %q, want separate tx/rx realtime summary", header)
	}
	if strings.Contains(header, "Realtime:") || strings.Contains(header, "Expected:") || strings.Contains(header, "Agents:") || strings.Contains(header, "Flows:") {
		t.Fatalf("header should not contain verbose traffic or status labels: %q", header)
	}
}

func TestFlowSparklineUsesDashForOfflineFlow(t *testing.T) {
	app := &App{graphMode: "unicode"}
	got := app.flowSparkline(model.FlowSnapshot{
		DesiredState:    "running",
		ActualState:     "running",
		LossHistory240s: []float64{0, -1, -1, 0.01, 0.05},
	}, 5)
	want := "█▄--▁"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFlowSparklineUsesControllerHistory(t *testing.T) {
	app := &App{graphMode: "unicode"}
	got := app.flowSparkline(model.FlowSnapshot{
		DesiredState:    "running",
		Tx1s:            100,
		Rx1s:            99,
		LossHistory60s:  []float64{0.05},
		LossHistory240s: []float64{0, 0.01},
	}, 5)
	want := "▄▁---"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFlowHasNoTrafficWithFixedLengthZeroHistory(t *testing.T) {
	zero60 := make([]float64, 60)
	zero240 := make([]float64, 240)
	if !flowHasNoTraffic(model.FlowSnapshot{DesiredState: "running", LossHistory60s: zero60, LossHistory240s: zero240}) {
		t.Fatal("fixed-length zero history should still be treated as no traffic")
	}
}

func TestFlowLossColorUsesLoss60s(t *testing.T) {
	got := flowLossColor(model.FlowSnapshot{
		DesiredState: "running",
		Tx1s:         100,
		Rx1s:         100,
		LossRatio60s: 0.03,
	})
	if got != tcell.ColorOrange {
		t.Fatalf("color = %v, want orange", got)
	}
}

func TestFlowLossColorIgnoresHistoryWhenLoss60sZero(t *testing.T) {
	got := flowLossColor(model.FlowSnapshot{
		DesiredState:    "running",
		Tx1s:            100,
		Rx1s:            100,
		LossRatio60s:    0,
		LossHistory240s: []float64{0, 0.03},
	})
	if got != tcell.ColorGreen {
		t.Fatalf("color = %v, want green", got)
	}
}

func TestMatrixNodesIncludeAgentsAndFlowEndpoints(t *testing.T) {
	got := matrixNodes(
		[]model.FlowSnapshot{{Src: "node-c", Dst: "node-a"}},
		[]model.AgentSnapshot{{AgentID: "node-b"}},
	)
	want := []string{"node-a", "node-b", "node-c"}
	if len(got) != len(want) {
		t.Fatalf("nodes len = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("nodes[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestRenderMatrixReferencesBidirectionalFlowCell(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.view = "matrix"
	app.flows = []model.FlowSnapshot{
		{FlowID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", DesiredState: "running", ActualState: "running", Tx1s: 100, Rx1s: 95, Lost1s: 5, OutageTotalMs: 400, LossRatio1s: 0.05, LossRatio60s: 0.04},
	}

	app.renderMatrix()

	cell := app.table.GetCell(1, 2)
	if cell == nil {
		t.Fatal("matrix cell node-a<=>node-b was not rendered")
	}
	if cell.Text != "0.4s" {
		t.Fatalf("cell text = %q, want 0.4s", cell.Text)
	}
	if cell.GetReference() != "node-a<=>node-b" {
		t.Fatalf("cell reference = %v, want node-a<=>node-b", cell.GetReference())
	}
}

func TestMatrixCellTextUsesOutage(t *testing.T) {
	flow := model.FlowSnapshot{
		FlowID:        "node-a<=>node-b",
		Src:           "node-a",
		Dst:           "node-b",
		DesiredState:  "running",
		ActualState:   "running",
		Tx1s:          100,
		Rx1s:          99,
		OutageTotalMs: 1200,
		LossRatio1s:   0.20,
		LossRatio60s:  0.012,
	}

	got := matrixCellText(flow)
	if got != "1.2s" {
		t.Fatalf("matrix cell text = %q, want 1.2s", got)
	}
}

func TestSelectedFlowWorksInMatrixView(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.view = "matrix"
	app.flows = []model.FlowSnapshot{
		{FlowID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", DesiredState: "running", ActualState: "running", Tx1s: 100, Rx1s: 99, LossHistory60s: []float64{0}},
	}
	app.renderMatrix()
	app.table.Select(1, 2)

	if got := app.selectedFlow(); got != "node-a<=>node-b" {
		t.Fatalf("selected flow = %q, want node-a<=>node-b", got)
	}
}

func TestRenderMatrixSeparatesDirectionalFlows(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.view = "matrix"
	app.flows = []model.FlowSnapshot{
		{FlowID: "node-a->node-b", Src: "node-a", Dst: "node-b", DesiredState: "running", ActualState: "running", Tx1s: 100, Rx1s: 97, OutageTotalMs: 300, LossRatio60s: 0.03},
		{FlowID: "node-b->node-a", Src: "node-b", Dst: "node-a", DesiredState: "running", ActualState: "running", Tx1s: 100, Rx1s: 100, LossRatio60s: 0},
	}

	app.renderMatrix()

	ab := app.table.GetCell(1, 2)
	ba := app.table.GetCell(2, 1)
	if ab == nil || ba == nil {
		t.Fatal("directional matrix cells were not rendered")
	}
	if ab.Text != "0.3s" {
		t.Fatalf("a->b text = %q, want 0.3s", ab.Text)
	}
	if ba.Text != "0.0s" {
		t.Fatalf("b->a text = %q, want 0.0s", ba.Text)
	}
	if ab.GetReference() != "node-a->node-b" {
		t.Fatalf("a->b reference = %v, want node-a->node-b", ab.GetReference())
	}
	if ba.GetReference() != "node-b->node-a" {
		t.Fatalf("b->a reference = %v, want node-b->node-a", ba.GetReference())
	}
}

func TestSelectedFlowWorksInFlowView(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.view = "flows"
	app.flows = []model.FlowSnapshot{
		{FlowID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", DesiredState: "running", ActualState: "running", Tx1s: 100, Rx1s: 99, LossHistory240s: []float64{0}},
	}
	app.renderFlows()
	app.table.Select(1, 0)

	if got := app.selectedFlow(); got != "node-a<=>node-b" {
		t.Fatalf("selected flow = %q, want node-a<=>node-b", got)
	}
}

func TestRenderFlowsPlacesGraphLastAndRightAlignsNumbers(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.table.SetRect(0, 0, 120, 10)
	app.flows = []model.FlowSnapshot{
		{FlowID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", DesiredState: "running", ActualState: "running", IntervalMs: 10, Tx1s: 100, Rx1s: 98, TxTotal: 1200, RxTotal: 1180, Lost1s: 2, LostTotal: 30, Reorder1s: 1, ReorderTotal: 8, Duplicate1s: 0, DuplicateTotal: 6, OutageTotalMs: 200, LossRatio1s: 0.02, LossRatio10s: 0.01, LossRatio60s: 0.005, LossHistory240s: []float64{0, 0.01, 0.02}},
	}

	app.renderFlows()

	graphHeader := app.table.GetCell(0, flowColGraph).Text
	if !strings.HasPrefix(graphHeader, "0s") {
		t.Fatalf("graph header should start with 0s, got %q", graphHeader)
	}
	if len(graphHeader) <= 30 || graphHeader[30:33] != "30s" {
		t.Fatalf("graph header should include 30s marker at offset 30, got %q", graphHeader)
	}
	if got := app.table.GetCell(0, flowColOutage).Text; got != "Outage" {
		t.Fatalf("outage header = %q, want Outage", got)
	}
	if got := app.table.GetCell(0, flowColLossTime).Text; got != "LossTime" {
		t.Fatalf("loss time header = %q, want LossTime", got)
	}
	if got := app.table.GetCell(0, flowColLost).Text; got != "Lost" {
		t.Fatalf("lost header = %q, want Lost", got)
	}
	if got := app.table.GetCell(0, flowColTX).Text; got != "TX" {
		t.Fatalf("tx header = %q, want TX", got)
	}
	if got := app.table.GetCell(0, flowColRX).Text; got != "RX" {
		t.Fatalf("rx header = %q, want RX", got)
	}
	if got := app.table.GetCell(0, flowColGraph).Expansion; got != 1 {
		t.Fatalf("graph expansion = %d, want 1", got)
	}
	if got := app.table.GetCell(1, flowColOutage).Align; got != tview.AlignRight {
		t.Fatalf("outage align = %d, want right", got)
	}
	if got, _, _ := app.table.GetCell(1, flowColOutage).Style.Decompose(); got != tcell.ColorAqua {
		t.Fatalf("outage color = %v, want aqua", got)
	}
	if got := app.table.GetCell(1, flowColTX).Align; got != tview.AlignRight {
		t.Fatalf("tx align = %d, want right", got)
	}
	if got := app.table.GetCell(1, flowColRX).Text; got != "98" {
		t.Fatalf("rx should be immediately left of graph, got %q", got)
	}
	if got := app.table.GetCell(1, flowColTX).Text; got != "100" {
		t.Fatalf("tx text = %q, want 100", got)
	}
	if got := app.table.GetCell(1, flowColAct).Align; got != tview.AlignCenter {
		t.Fatalf("act align = %d, want center", got)
	}
	if got := app.table.GetCell(1, flowColOutage).Text; got != "0.2s" {
		t.Fatalf("outage text = %q, want 0.2s", got)
	}
	if got := app.table.GetCell(1, flowColLost).Text; got != "30" {
		t.Fatalf("lost text = %q, want 30", got)
	}
	if got := app.table.GetCell(1, flowColReorder).Text; got != "8" {
		t.Fatalf("reorder text = %q, want 8", got)
	}
	if got := app.table.GetCell(1, flowColDup).Text; got != "6" {
		t.Fatalf("dup text = %q, want 6", got)
	}
}

func TestOutageUsesSameColorAsGraph(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.table.SetRect(0, 0, 120, 10)
	app.flows = []model.FlowSnapshot{
		{FlowID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", DesiredState: "running", ActualState: "running", Tx1s: 100, Rx1s: 97, Lost1s: 3, OutageTotalMs: 30, LossRatio60s: 0.03, LossHistory240s: []float64{0.03}},
	}

	app.renderFlows()

	lossColor, _, _ := app.table.GetCell(1, flowColOutage).Style.Decompose()
	graphColor, _, _ := app.table.GetCell(1, flowColGraph).Style.Decompose()
	if lossColor != graphColor {
		t.Fatalf("outage color = %v, want graph color %v", lossColor, graphColor)
	}
}

func TestGraphHeaderTextUsesThirtySecondMarkers(t *testing.T) {
	got := graphHeaderText(65)
	if len(got) != 65 {
		t.Fatalf("header len = %d, want 65", len(got))
	}
	if !strings.HasPrefix(got, "0s") {
		t.Fatalf("header should start with 0s, got %q", got)
	}
	if got[30:33] != "30s" {
		t.Fatalf("header[30:33] = %q, want 30s", got[30:33])
	}
	if got[60:63] != "60s" {
		t.Fatalf("header[60:63] = %q, want 60s", got[60:63])
	}
}

func TestGraphHeaderTextKeepsOnlyZeroSecondsWhenNarrow(t *testing.T) {
	got := graphHeaderText(20)
	if len(got) != 20 {
		t.Fatalf("header len = %d, want 20", len(got))
	}
	if !strings.HasPrefix(got, "0s") {
		t.Fatalf("header should start with 0s, got %q", got)
	}
	if strings.Contains(got, "30s") {
		t.Fatalf("narrow header should not contain 30s, got %q", got)
	}
}

func TestRenderFlowsGraphUsesAvailableWidth(t *testing.T) {
	newApp := func(width int) *App {
		app := NewApp(nil, "unicode")
		app.table.SetRect(0, 0, width, 10)
		app.flows = []model.FlowSnapshot{
			{FlowID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", DesiredState: "running", ActualState: "running", IntervalMs: 10, Tx1s: 100, Rx1s: 98, Lost1s: 2, LostTotal: 30, ReorderTotal: 8, DuplicateTotal: 6, LossRatio1s: 0.02, LossRatio10s: 0.01, LossRatio60s: 0.005, LossHistory240s: []float64{0, 0.01, 0.02}},
		}
		app.renderFlows()
		return app
	}

	narrow := newApp(100)
	wide := newApp(160)

	narrowWidth := len(narrow.table.GetCell(1, flowColGraph).Text)
	wideWidth := len(wide.table.GetCell(1, flowColGraph).Text)
	if wideWidth <= narrowWidth {
		t.Fatalf("graph width should grow with table width: narrow=%d wide=%d", narrowWidth, wideWidth)
	}
	if wideWidth > maxGraphHistorySize {
		t.Fatalf("graph width = %d, want <= %d", wideWidth, maxGraphHistorySize)
	}
}

func TestRealtimeTrafficTotals(t *testing.T) {
	flows := []model.FlowSnapshot{
		{FlowID: "a<=>b", DesiredState: "running", PacketSize: 64, Tx1s: 100, Rx1s: 95},
		{FlowID: "a<=>c", DesiredState: "running", PacketSize: 128, Tx1s: 50, Rx1s: 45},
		{FlowID: "b<=>c", DesiredState: "stopped", PacketSize: 512, Tx1s: 0, Rx1s: 0},
	}

	txBPS, txPPS, rxBPS, rxPPS := realtimeTrafficTotals(flows)

	if txPPS != 150 {
		t.Fatalf("tx pps = %d, want 150", txPPS)
	}
	if txBPS != 102400 {
		t.Fatalf("tx bps = %d, want 102400", txBPS)
	}
	if rxPPS != 140 {
		t.Fatalf("rx pps = %d, want 140", rxPPS)
	}
	if rxBPS != 94720 {
		t.Fatalf("rx bps = %d, want 94720", rxBPS)
	}
}

func TestUpdateDetailShowsTxRxTotals(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.table.SetRect(0, 0, 120, 10)
	app.flows = []model.FlowSnapshot{
		{
			FlowID:             "node-a<=>node-b",
			DesiredState:       "running",
			ActualState:        "running",
			Tx1s:               100,
			Rx1s:               98,
			TxTotal:            1200,
			RxTotal:            1180,
			LostTotal:          20,
			ReorderTotal:       3,
			DuplicateTotal:     1,
			IsolatedLossEvents: 2,
			OutageCount:        3,
			LastOutageMs:       400,
			MaxOutageMs:        900,
			OutageTotalMs:      1600,
			LossHistory240s:    []float64{0, 0.01, 0.02},
		},
	}
	app.renderFlows()
	app.table.Select(1, 0)

	app.updateDetail()

	got := app.detail.GetText(false)
	if !strings.Contains(got, "tx=100/1200 rx=98/1180") {
		t.Fatalf("detail = %q, want tx/rx totals", got)
	}
	if !strings.Contains(got, "outages=3 isolated=2 last_outage=0.4s max_outage=0.9s") {
		t.Fatalf("detail = %q, want outage details", got)
	}
	if !strings.Contains(got, "outage_total=1.6s") {
		t.Fatalf("detail = %q, want total outage", got)
	}
	if strings.Contains(got, "loss_1s=") {
		t.Fatalf("detail = %q, want no loss_1s", got)
	}
	if strings.Contains(got, "graph=") {
		t.Fatalf("detail = %q, want no graph", got)
	}
}

func TestUpdateDetailHidesTXWhenSourceAgentOffline(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.table.SetRect(0, 0, 120, 10)
	app.flows = []model.FlowSnapshot{
		{
			FlowID:       "node-a<=>node-b",
			DesiredState: "running",
			ActualState:  "offline",
			LastError:    "source agent communication lost; loss inferred by controller",
			Tx1s:         100,
			TxTotal:      1200,
			Rx1s:         0,
			RxTotal:      0,
		},
	}
	app.renderFlows()
	app.table.Select(1, 0)

	app.updateDetail()

	got := app.detail.GetText(false)
	if !strings.Contains(got, "tx=-/- rx=0/0") {
		t.Fatalf("detail = %q, want hidden tx for offline source", got)
	}
}

func TestRenderFlowsHidesTXWhenSourceAgentOffline(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.table.SetRect(0, 0, 120, 10)
	app.flows = []model.FlowSnapshot{
		{
			FlowID:       "node-a<=>node-b",
			Src:          "node-a",
			Dst:          "node-b",
			DesiredState: "running",
			ActualState:  "offline",
			LastError:    "source agent communication lost; loss inferred by controller",
			Tx1s:         100,
			Rx1s:         0,
		},
	}

	app.renderFlows()

	if got := app.table.GetCell(1, flowColTX).Text; got != "-" {
		t.Fatalf("tx cell = %q, want -", got)
	}
	if got := app.table.GetCell(1, flowColGraph).Text; got == "" || strings.Trim(got, "-") != "" {
		t.Fatalf("graph cell = %q, want only '-' timeline", got)
	}
}

func TestOutageText(t *testing.T) {
	if got := outageText(model.FlowSnapshot{OutageTotalMs: 750}); got != "0.8s" {
		t.Fatalf("outageText total = %q, want 0.8s", got)
	}
	if got := outageText(model.FlowSnapshot{OutageActive: true, CurrentOutageMs: 1250, LastOutageMs: 750, OutageTotalMs: 2400}); got != "2.4s" {
		t.Fatalf("outageText active total = %q, want 2.4s", got)
	}
}

func TestFormatTrafficRates(t *testing.T) {
	if got := formatBitRate(102400); got != "102 kbps" {
		t.Fatalf("bit rate = %q, want %q", got, "102 kbps")
	}
	if got := formatPacketRate(1500); got != "1.50 kpps" {
		t.Fatalf("packet rate = %q, want %q", got, "1.50 kpps")
	}
}

func TestAgentsViewEnableDisableKeyCallsAPI(t *testing.T) {
	var disableCalls atomic.Int32
	client := &Client{
		base: "http://controller.test",
		http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body := "[]"
			switch r.URL.Path {
			case "/api/v1/flows":
				body = "[]"
			case "/api/v1/agents":
				body = `[{"agent_id":"node-a","status":"online","enabled":true}]`
			case "/api/v1/status":
				body = `{"measurement_session_id":"26062301"}`
			case "/api/v1/agents/node-a/disable":
				if r.Method != http.MethodPost {
					t.Fatalf("disable method = %s, want POST", r.Method)
				}
				disableCalls.Add(1)
			default:
				return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}

	app := NewApp(client, "unicode")
	app.view = "agents"
	app.refresh()
	app.renderAgents()
	app.table.Select(1, 0)
	app.handleKey(tcell.NewEventKey(tcell.KeyRune, 'e', 0))

	if disableCalls.Load() != 1 {
		t.Fatalf("disable calls = %d, want 1", disableCalls.Load())
	}
	if app.lastAction != "disabled node-a" {
		t.Fatalf("lastAction = %q, want disabled node-a", app.lastAction)
	}
}

func TestAgentsViewEnableDisableErrorShowsMessage(t *testing.T) {
	client := &Client{
		base: "http://controller.test",
		http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body := "[]"
			switch r.URL.Path {
			case "/api/v1/flows":
				body = "[]"
			case "/api/v1/agents":
				body = `[{"agent_id":"node-a","status":"online","enabled":true}]`
			case "/api/v1/status":
				body = `{"measurement_session_id":"26062301"}`
			case "/api/v1/agents/node-a/disable":
				return &http.Response{StatusCode: http.StatusConflict, Status: "409 Conflict", Body: io.NopCloser(strings.NewReader(`{"status":"error","message":"all flows must be stopped"}`))}, nil
			default:
				return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
		})},
	}

	app := NewApp(client, "unicode")
	app.view = "agents"
	app.refresh()
	app.renderAgents()
	app.table.Select(1, 0)
	app.handleKey(tcell.NewEventKey(tcell.KeyRune, 'e', 0))

	if !strings.Contains(app.lastAction, "all flows must be stopped") {
		t.Fatalf("lastAction = %q, want error message", app.lastAction)
	}
}

func TestRenderAgentsIncludesEnabledColumn(t *testing.T) {
	app := NewApp(nil, "unicode")
	app.view = "agents"
	app.agents = []model.AgentSnapshot{{AgentID: "node-a", Status: "online", Enabled: true}}

	app.renderAgents()

	if got := app.table.GetCell(0, 2).Text; got != "Enabled" {
		t.Fatalf("enabled header = %q, want Enabled", got)
	}
	if got := app.table.GetCell(1, 2).Text; got != "ENABLED" {
		t.Fatalf("enabled value = %q, want ENABLED", got)
	}
}
