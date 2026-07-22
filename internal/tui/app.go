package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"pakeloss/internal/model"
)

type App struct {
	client          *Client
	graphMode       string
	refreshInterval time.Duration
	app             *tview.Application
	table           *tview.Table
	detail          *tview.TextView
	header          *tview.TextView
	view            string
	sortMode        string
	lossOnly        bool
	flows           []model.FlowSnapshot
	agents          []model.AgentSnapshot
	sessionID       string
	lastAction      string
}

const defaultSortMode = "flow_asc"

const (
	flowColFlow = iota
	flowColAct
	flowColOutage
	flowColLossTime
	flowColLost
	flowColReorder
	flowColDup
	flowColTX
	flowColRX
	flowColGraph
)

const (
	defaultGraphWidth   = 60
	maxGraphHistorySize = 240
	minGraphWidth       = 10
)

var flowHeaders = []string{"Flow", "Act", "Outage", "LossTime", "Lost", "Reorder", "Dup", "TX", "RX", "Graph"}

func FlowTableHeaders() []string {
	return []string{"Flow", "Act", "Outage", "Loss1s", "LossTime", "Outages", "Isolated", "Lost", "Reorder", "Dup", "TX", "RX"}
}

func FlowTableValues(f model.FlowSnapshot) []string {
	return []string{
		flowDisplayName(f),
		stateShort(f.ActualState),
		outageText(f),
		lossRatioText(f.LossRatio1s),
		durationMsText(f.LossTimeTotalMs),
		fmt.Sprint(f.OutageCount),
		fmt.Sprint(f.IsolatedLossEvents),
		fmt.Sprint(f.LostTotal),
		fmt.Sprint(f.ReorderTotal),
		fmt.Sprint(f.DuplicateTotal),
		flowTotalTXText(f),
		flowTotalRXText(f),
	}
}

func tuiFlowTableValues(f model.FlowSnapshot) []string {
	return []string{
		flowDisplayName(f),
		stateShort(f.ActualState),
		outageText(f),
		durationMsText(f.LossTimeTotalMs),
		fmt.Sprint(f.LostTotal),
		fmt.Sprint(f.ReorderTotal),
		fmt.Sprint(f.DuplicateTotal),
		flowTXText(f),
		flowRXText(f),
	}
}

func NewApp(client *Client, graphMode string, refreshInterval ...time.Duration) *App {
	if graphMode == "" {
		graphMode = "unicode"
	}
	interval := time.Second
	if len(refreshInterval) > 0 && refreshInterval[0] > 0 {
		interval = refreshInterval[0]
	}
	return &App{client: client, graphMode: graphMode, refreshInterval: interval, app: tview.NewApplication(), table: tview.NewTable().SetSelectable(true, false), detail: tview.NewTextView(), header: tview.NewTextView(), view: "flows", sortMode: defaultSortMode}
}

func (a *App) Run() error {
	a.table.SetBorder(true)
	a.detail.SetBorder(true).SetTitle("Detail")
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(a.header, 2, 0, false).
		AddItem(a.table, 0, 1, true).
		AddItem(a.detail, 4, 0, false)
	a.app.SetRoot(root, true)
	a.app.SetInputCapture(a.handleKey)
	a.refresh()
	go func() {
		ticker := time.NewTicker(a.refreshInterval)
		defer ticker.Stop()
		for range ticker.C {
			a.app.QueueUpdateDraw(a.refresh)
		}
	}()
	return a.app.Run()
}

func (a *App) handleKey(ev *tcell.EventKey) *tcell.EventKey {
	switch ev.Rune() {
	case 'q':
		a.app.Stop()
		return nil
	case 'r':
		a.refresh()
		return nil
	case 'a':
		a.view = "agents"
		a.refresh()
		return nil
	case 'f':
		a.view = "flows"
		a.refresh()
		return nil
	case 'm':
		a.view = "matrix"
		a.refresh()
		return nil
	case 'l':
		a.lossOnly = !a.lossOnly
		a.refresh()
		return nil
	case 'o':
		a.sortMode = nextSortMode(a.sortMode)
		a.refresh()
		return nil
	case 's':
		a.toggleAll()
		return nil
	case 'p':
		a.toggleSelected()
		return nil
	case 'c':
		a.restartAll()
		return nil
	case 'e':
		a.toggleSelectedAgent()
		return nil
	case 'C':
		return nil
	case '/':
		return nil
	}
	if ev.Key() == tcell.KeyEnter {
		a.updateDetail()
		return nil
	}
	return ev
}

func (a *App) refresh() {
	flows, errF := a.client.Flows()
	agents, errA := a.client.Agents()
	status, errS := a.client.Status()
	if errF == nil {
		a.flows = sortFlows(flows, a.sortMode)
	}
	if errA == nil {
		a.agents = agents
	}
	if errS == nil {
		a.sessionID = status.MeasurementSessionID
	}
	online := 0
	for _, ag := range a.agents {
		if ag.Status == "online" {
			online++
		}
	}
	running := 0
	offlineFlows := 0
	for _, f := range a.flows {
		if f.DesiredState == "running" {
			running++
		}
		if strings.EqualFold(f.ActualState, "offline") {
			offlineFlows++
		}
	}
	txBPS, txPPS, rxBPS, rxPPS := realtimeTrafficTotals(a.flows)
	errText := ""
	if errF != nil || errA != nil || errS != nil {
		errText = fmt.Sprintf(" err=%v %v %v", errF, errA, errS)
	}
	actionText := ""
	if a.lastAction != "" {
		actionText = "   Action: " + a.lastAction
	}
	a.header.SetText(fmt.Sprintf("Pakeloss - Controller TUI\nAgents On:%d Off:%d   Flows Run:%d Stop:%d Off:%d   Session: %s   TX: %s / %s   RX: %s / %s   Sort: %s   Updated: %s%s",
		online, len(a.agents)-online, running, len(a.flows)-running, offlineFlows, sessionIDText(a.sessionID), formatBitRate(txBPS), formatPacketRate(txPPS), formatBitRate(rxBPS), formatPacketRate(rxPPS), sortModeLabel(a.sortMode), time.Now().Format("15:04:05"), errText+actionText))
	switch a.view {
	case "agents":
		a.renderAgents()
	case "matrix":
		a.renderMatrix()
	default:
		a.renderFlows()
	}
	a.updateDetail()
}

func (a *App) renderFlows() {
	a.table.Clear()
	a.table.SetSelectable(true, false)
	flows := a.visibleFlows()
	widths := a.flowColumnWidths(flows)
	graphWidth := a.flowGraphWidth(widths)

	for i, h := range flowHeaders {
		if i == flowColGraph {
			h = graphHeaderText(graphWidth)
		}
		cell := tview.NewTableCell(h).SetTextColor(tcell.ColorYellow).SetSelectable(false)
		switch i {
		case flowColAct:
			cell.SetAlign(tview.AlignCenter)
		case flowColOutage, flowColLossTime, flowColTX, flowColRX, flowColLost, flowColReorder, flowColDup:
			cell.SetAlign(tview.AlignRight).SetMaxWidth(widths[i])
		case flowColGraph:
			cell.SetExpansion(1)
		default:
			cell.SetMaxWidth(widths[i])
		}
		a.table.SetCell(0, i, cell)
	}
	row := 1
	for _, f := range flows {
		values := append(tuiFlowTableValues(f), a.flowSparkline(f, graphWidth))
		lossColor := flowLossColor(f)
		for col, v := range values {
			cell := tview.NewTableCell(v)
			if col == 0 {
				cell.SetReference(f.FlowID)
			}
			switch col {
			case flowColAct:
				cell.SetAlign(tview.AlignCenter).SetMaxWidth(widths[col])
			case flowColOutage, flowColLossTime, flowColTX, flowColRX, flowColLost, flowColReorder, flowColDup:
				cell.SetAlign(tview.AlignRight).SetMaxWidth(widths[col])
			case flowColGraph:
				cell.SetExpansion(1)
			default:
				cell.SetMaxWidth(widths[col])
			}
			switch col {
			case flowColOutage, flowColLossTime, flowColLost, flowColReorder, flowColDup, flowColGraph:
				cell.SetTextColor(lossColor)
			}
			a.table.SetCell(row, col, cell)
		}
		row++
	}
}

func (a *App) renderAgents() {
	a.table.Clear()
	a.table.SetSelectable(true, false)
	headers := []string{"Agent ID", "Status", "Enabled", "UDP Addr", "Active Config", "Desired Config", "Active Flows", "Last Heartbeat", "Last Result"}
	widths := a.agentColumnWidths()
	for i, h := range headers {
		cell := tview.NewTableCell(h).SetTextColor(tcell.ColorYellow).SetSelectable(false)
		if isAgentNumericColumn(i) {
			cell.SetAlign(tview.AlignRight).SetMaxWidth(widths[i])
		}
		a.table.SetCell(0, i, cell)
	}
	for row, ag := range a.agents {
		values := []string{ag.AgentID, strings.ToUpper(ag.Status), enabledText(ag.Enabled), ag.UDPAddr, fmt.Sprint(ag.ActiveConfigVersion), fmt.Sprint(ag.DesiredConfigVersion), fmt.Sprint(ag.ActiveFlows), ago(ag.LastHeartbeat), ago(ag.LastResult)}
		for col, v := range values {
			cell := tview.NewTableCell(v)
			if col == 0 {
				cell.SetReference(ag.AgentID)
			}
			if isAgentNumericColumn(col) {
				cell.SetAlign(tview.AlignRight).SetMaxWidth(widths[col])
			}
			a.table.SetCell(row+1, col, cell)
		}
	}
}

func (a *App) selectedAgent() string {
	if a.view != "agents" {
		return ""
	}
	row, _ := a.table.GetSelection()
	if row <= 0 {
		return ""
	}
	cell := a.table.GetCell(row, 0)
	if cell == nil || cell.GetReference() == nil {
		return ""
	}
	id, ok := cell.GetReference().(string)
	if !ok {
		return ""
	}
	return id
}

func (a *App) selectedAgentStatus() *model.AgentSnapshot {
	id := a.selectedAgent()
	for i := range a.agents {
		if a.agents[i].AgentID == id {
			return &a.agents[i]
		}
	}
	return nil
}

func (a *App) selectedFlow() string {
	if a.view != "flows" && a.view != "matrix" {
		return ""
	}
	row, _ := a.table.GetSelection()
	if row <= 0 {
		return ""
	}
	col := 0
	if a.view == "matrix" {
		_, col = a.table.GetSelection()
		if col <= 0 {
			return ""
		}
	}
	cell := a.table.GetCell(row, col)
	if cell == nil || cell.GetReference() == nil {
		return ""
	}
	id, ok := cell.GetReference().(string)
	if !ok {
		return ""
	}
	return id
}

func (a *App) selectedFlowStatus() *model.FlowSnapshot {
	id := a.selectedFlow()
	for i := range a.flows {
		if a.flows[i].FlowID == id {
			return &a.flows[i]
		}
	}
	return nil
}

func (a *App) toggleAll() {
	if len(a.flows) == 0 {
		return
	}
	run := !allFlowsRunning(a.flows)
	if run {
		if err := a.client.StartAll(); err != nil {
			a.lastAction = err.Error()
			a.refresh()
			return
		}
		a.lastAction = "all flows started"
	} else {
		if err := a.client.StopAll(); err != nil {
			a.lastAction = err.Error()
			a.refresh()
			return
		}
		a.lastAction = "all flows stopped"
	}
	a.refresh()
}

func (a *App) toggleSelected() {
	f := a.selectedFlowStatus()
	if f == nil {
		return
	}
	if f.DesiredState == "running" {
		if err := a.client.PauseFlow(f.FlowID); err != nil {
			a.lastAction = err.Error()
			a.refresh()
			return
		}
		a.lastAction = "paused " + f.FlowID
	} else {
		if err := a.client.ResumeFlow(f.FlowID); err != nil {
			a.lastAction = err.Error()
			a.refresh()
			return
		}
		a.lastAction = "resumed " + f.FlowID
	}
	a.refresh()
}

func (a *App) toggleSelectedAgent() {
	ag := a.selectedAgentStatus()
	if ag == nil {
		return
	}
	var err error
	if ag.Enabled {
		err = a.client.DisableAgent(ag.AgentID)
		if err == nil {
			a.lastAction = "disabled " + ag.AgentID
		}
	} else {
		err = a.client.EnableAgent(ag.AgentID)
		if err == nil {
			a.lastAction = "enabled " + ag.AgentID
		}
	}
	if err != nil {
		a.lastAction = err.Error()
	}
	a.refresh()
}

func (a *App) updateDetail() {
	if a.view == "agents" {
		if ag := a.selectedAgentStatus(); ag != nil {
			a.detail.SetText(fmt.Sprintf("%s: status=%s enabled=%s udp=%s flows=%d\nKeys: q quit | r refresh | f flows | m matrix(outage) | e enable/disable agent",
				ag.AgentID, strings.ToUpper(ag.Status), enabledText(ag.Enabled), ag.UDPAddr, ag.ActiveFlows))
			return
		}
		a.detail.SetText("Keys: q quit | r refresh | f flows | m matrix(outage) | e enable/disable agent")
		return
	}
	f := a.selectedFlowStatus()
	if f == nil {
		a.detail.SetText("Keys: q quit | r refresh | f flows | m matrix(outage) | a agents | l loss-only | o sort | s start/stop all | p pause/resume flow | c restart all")
		return
	}
	a.detail.SetText(fmt.Sprintf("%s: outage_total=%s threshold=%dms loss_time=%s outages=%d isolated=%d last_outage=%s max_outage=%s unmeasurable=%s/%s count=%d lost=%d reorder=%d dup=%d tx=%s rx=%d/%d\nKeys: q quit | r refresh | f flows | m matrix(outage) | a agents | l loss-only | o sort | s start/stop all | p pause/resume flow | c restart all",
		f.FlowID, outageText(*f), f.OutageThresholdMs, durationMsText(f.LossTimeTotalMs), f.OutageCount, f.IsolatedLossEvents, durationMsText(f.LastOutageMs), durationMsText(f.MaxOutageMs), durationMsText(f.CurrentUnmeasurableMs), durationMsText(f.UnmeasurableTotalMs), f.UnmeasurableCount, f.LostTotal, f.ReorderTotal, f.DuplicateTotal, flowDetailTX(*f), f.Rx1s, f.RxTotal))
}

func (a *App) restartAll() {
	if err := a.client.RestartAll(); err != nil {
		a.lastAction = err.Error()
		a.refresh()
		return
	}
	a.lastAction = "all flows restarted"
	a.refresh()
}

func sortFlows(flows []model.FlowSnapshot, mode string) []model.FlowSnapshot {
	out := append([]model.FlowSnapshot(nil), flows...)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch mode {
		case "flow_desc":
			if flowDisplayName(a) != flowDisplayName(b) {
				return flowDisplayName(a) > flowDisplayName(b)
			}
		case "act_asc":
			if a.ActualState != b.ActualState {
				return a.ActualState < b.ActualState
			}
		case "act_desc":
			if a.ActualState != b.ActualState {
				return a.ActualState > b.ActualState
			}
		case "outage_asc":
			if outageDisplayMs(a) != outageDisplayMs(b) {
				return outageDisplayMs(a) < outageDisplayMs(b)
			}
		case "outage_desc":
			if outageDisplayMs(a) != outageDisplayMs(b) {
				return outageDisplayMs(a) > outageDisplayMs(b)
			}
		case "lost_asc":
			if a.LostTotal != b.LostTotal {
				return a.LostTotal < b.LostTotal
			}
		case "lost_desc":
			if a.LostTotal != b.LostTotal {
				return a.LostTotal > b.LostTotal
			}
		}
		if flowDisplayName(a) != flowDisplayName(b) {
			return flowDisplayName(a) < flowDisplayName(b)
		}
		return a.FlowID < b.FlowID
	})
	return out
}

func flowDisplayName(f model.FlowSnapshot) string {
	if f.FlowID != "" {
		return f.FlowID
	}
	if f.Src != "" || f.Dst != "" {
		return f.Src + "<=>" + f.Dst
	}
	return f.FlowID
}

func nextSortMode(mode string) string {
	switch mode {
	case "flow_asc":
		return "flow_desc"
	case "flow_desc":
		return "act_asc"
	case "act_asc":
		return "act_desc"
	case "act_desc":
		return "outage_asc"
	case "outage_asc":
		return "outage_desc"
	case "outage_desc":
		return "lost_asc"
	case "lost_asc":
		return "lost_desc"
	default:
		return defaultSortMode
	}
}

func sortModeLabel(mode string) string {
	switch mode {
	case "flow_desc":
		return "Flow desc"
	case "act_asc":
		return "Act asc"
	case "act_desc":
		return "Act desc"
	case "outage_asc":
		return "Outage asc"
	case "outage_desc":
		return "Outage desc"
	case "lost_asc":
		return "Lost asc"
	case "lost_desc":
		return "Lost desc"
	default:
		return "Flow asc"
	}
}

func allFlowsRunning(flows []model.FlowSnapshot) bool {
	if len(flows) == 0 {
		return false
	}
	for _, f := range flows {
		if f.DesiredState != "running" {
			return false
		}
	}
	return true
}

func enabledText(enabled bool) string {
	if enabled {
		return "ENABLED"
	}
	return "DISABLED"
}

func (a *App) flowSparkline(f model.FlowSnapshot, width int) string {
	if flowHasNoTraffic(f) {
		return NoTrafficSparkline(width)
	}
	history := flowGraphHistory(f)
	if flowSourceOffline(f) && len(history) == 0 {
		return strings.Repeat("-", normalizedSparklineWidth(width))
	}
	if f.LastError != "" && len(history) == 0 {
		return LossSparkline(filledSparklineLossHistory(maxGraphHistorySize), a.graphMode, width)
	}
	return LossSparkline(history, a.graphMode, width)
}

func filledSparklineLossHistory(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = 1
	}
	return out
}

func flowHasNoTraffic(f model.FlowSnapshot) bool {
	history := flowGraphHistory(f)
	if f.DesiredState != "running" {
		return true
	}
	if f.Lost1s > 0 || f.LossRatio1s > 0 || f.LossRatio10s > 0 || f.LossRatio60s > 0 || hasLoss(history) || f.LastError != "" {
		return false
	}
	return len(history) == 0 || (f.Tx1s == 0 && f.Rx1s == 0)
}

func flowTXText(f model.FlowSnapshot) string {
	if flowSourceOffline(f) {
		return "-"
	}
	return fmt.Sprint(f.Tx1s)
}

func flowRXText(f model.FlowSnapshot) string {
	return fmt.Sprint(f.Rx1s)
}

func flowTotalTXText(f model.FlowSnapshot) string {
	if flowSourceOffline(f) {
		return "-"
	}
	return fmt.Sprint(f.TxTotal)
}

func flowTotalRXText(f model.FlowSnapshot) string {
	return fmt.Sprint(f.RxTotal)
}

func flowDetailTX(f model.FlowSnapshot) string {
	if flowSourceOffline(f) {
		return "-/-"
	}
	return fmt.Sprintf("%d/%d", f.Tx1s, f.TxTotal)
}

func flowSourceOffline(f model.FlowSnapshot) bool {
	if !strings.EqualFold(f.ActualState, "offline") {
		return false
	}
	errText := strings.ToLower(f.LastError)
	return strings.Contains(errText, "source agent communication lost")
}

func hasLoss(v []float64) bool {
	for _, x := range v {
		if x > 0 {
			return true
		}
	}
	return false
}

func flowLossColor(f model.FlowSnapshot) tcell.Color {
	if flowHasNoTraffic(f) {
		return tcell.ColorDarkGray
	}
	if f.LastError != "" || strings.EqualFold(f.ActualState, "offline") {
		return tcell.ColorRed
	}
	loss := f.LossRatio60s
	switch {
	case loss >= 0.05:
		return tcell.ColorRed
	case loss >= 0.02:
		return tcell.ColorOrange
	case loss >= 0.01:
		return tcell.ColorYellow
	case loss > 0:
		return tcell.ColorAqua
	default:
		return tcell.ColorGreen
	}
}

func flowGraphHistory(f model.FlowSnapshot) []float64 {
	if len(f.LossHistory240s) > 0 {
		return f.LossHistory240s
	}
	return f.LossHistory60s
}

func (a *App) visibleFlows() []model.FlowSnapshot {
	if !a.lossOnly {
		return a.flows
	}
	out := make([]model.FlowSnapshot, 0, len(a.flows))
	for _, f := range a.flows {
		if f.LossRatio60s == 0 && !hasLoss(flowGraphHistory(f)) && f.LastError == "" {
			continue
		}
		out = append(out, f)
	}
	return out
}

func (a *App) flowColumnWidths(flows []model.FlowSnapshot) []int {
	widths := make([]int, len(flowHeaders))
	for i, header := range flowHeaders {
		if i == flowColGraph {
			continue
		}
		widths[i] = len(header)
	}
	for _, f := range flows {
		values := tuiFlowTableValues(f)
		for i, value := range values {
			if len(value) > widths[i] {
				widths[i] = len(value)
			}
		}
	}
	return widths
}

func (a *App) flowGraphWidth(widths []int) int {
	_, _, innerWidth, _ := a.table.GetInnerRect()
	if innerWidth <= 0 {
		return defaultGraphWidth
	}
	fixedWidth := 0
	for i := range widths {
		if i == flowColGraph {
			continue
		}
		fixedWidth += widths[i]
	}
	graphWidth := innerWidth - fixedWidth - (len(flowHeaders) - 1)
	if graphWidth < minGraphWidth {
		graphWidth = minGraphWidth
	}
	if graphWidth > maxGraphHistorySize {
		graphWidth = maxGraphHistorySize
	}
	return graphWidth
}

func (a *App) agentColumnWidths() []int {
	headers := []string{"Agent ID", "Status", "Enabled", "UDP Addr", "Active Config", "Desired Config", "Active Flows", "Last Heartbeat", "Last Result"}
	widths := make([]int, len(headers))
	for i, header := range headers {
		if !isAgentNumericColumn(i) {
			continue
		}
		widths[i] = len(header)
	}
	for _, ag := range a.agents {
		values := []string{ag.AgentID, strings.ToUpper(ag.Status), enabledText(ag.Enabled), ag.UDPAddr, fmt.Sprint(ag.ActiveConfigVersion), fmt.Sprint(ag.DesiredConfigVersion), fmt.Sprint(ag.ActiveFlows), ago(ag.LastHeartbeat), ago(ag.LastResult)}
		for i, value := range values {
			if !isAgentNumericColumn(i) {
				continue
			}
			if len(value) > widths[i] {
				widths[i] = len(value)
			}
		}
	}
	return widths
}

func isAgentNumericColumn(col int) bool {
	switch col {
	case 4, 5, 6:
		return true
	default:
		return false
	}
}

func outageText(f model.FlowSnapshot) string {
	return durationMsText(outageDisplayMs(f))
}

func outageDisplayMs(f model.FlowSnapshot) uint64 {
	return f.OutageTotalMs
}

func durationMsText(v uint64) string {
	return fmt.Sprintf("%.1fs", float64(v)/1000)
}

func lossRatioText(v float64) string {
	return fmt.Sprintf("%.1f%%", v*100)
}

func graphHeaderText(width int) string {
	if width <= 0 {
		width = defaultGraphWidth
	}
	if width > maxGraphHistorySize {
		width = maxGraphHistorySize
	}
	header := make([]rune, width)
	for i := range header {
		header[i] = ' '
	}
	for offset := 0; offset < width; offset += 30 {
		label := fmt.Sprintf("%ds", offset)
		if offset+len(label) > width {
			break
		}
		for i, r := range label {
			header[offset+i] = r
		}
	}
	return string(header)
}

func pct(v float64) string { return fmt.Sprintf("%.1f%%", v*100) }

func stateShort(state string) string {
	switch strings.ToLower(state) {
	case "running":
		return "RUN"
	case "stopped":
		return "STP"
	case "offline":
		return "OFF"
	case "unknown", "":
		return "UNK"
	default:
		return strings.ToUpper(state)
	}
}

func ago(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t)
	if d < time.Second {
		return "now"
	}
	return fmt.Sprintf("%ds ago", int(d.Seconds()))
}

func realtimeTrafficTotals(flows []model.FlowSnapshot) (uint64, uint64, uint64, uint64) {
	var txBPS uint64
	var txPPS uint64
	var rxBPS uint64
	var rxPPS uint64
	for _, flow := range flows {
		txPPS += flow.Tx1s
		txBPS += uint64(flow.PacketSize) * 8 * flow.Tx1s
		rxPPS += flow.Rx1s
		rxBPS += uint64(flow.PacketSize) * 8 * flow.Rx1s
	}
	return txBPS, txPPS, rxBPS, rxPPS
}

func formatBitRate(bps uint64) string {
	return formatRate(float64(bps), []string{"bps", "kbps", "Mbps", "Gbps", "Tbps"})
}

func formatPacketRate(pps uint64) string {
	return formatRate(float64(pps), []string{"pps", "kpps", "Mpps", "Gpps", "Tpps"})
}

func formatRate(v float64, units []string) string {
	unit := 0
	for v >= 1000 && unit < len(units)-1 {
		v /= 1000
		unit++
	}
	switch {
	case unit == 0:
		return fmt.Sprintf("%.0f %s", v, units[unit])
	case v >= 100:
		return fmt.Sprintf("%.0f %s", v, units[unit])
	case v >= 10:
		return fmt.Sprintf("%.1f %s", v, units[unit])
	default:
		return fmt.Sprintf("%.2f %s", v, units[unit])
	}
}

func sessionIDText(sessionID string) string {
	if sessionID == "" {
		return "-"
	}
	return sessionID
}
