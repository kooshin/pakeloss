package tui

import (
	"sort"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"pakeloss/internal/model"
)

func (a *App) renderMatrix() {
	a.table.Clear()
	a.table.SetSelectable(true, true)

	nodes := matrixNodes(a.flows, a.agents)
	flows := matrixFlowsByDirection(a.flows)

	a.table.SetCell(0, 0, tview.NewTableCell("Src \\ Dst").SetTextColor(tcell.ColorYellow).SetSelectable(false))
	for col, node := range nodes {
		a.table.SetCell(0, col+1, tview.NewTableCell(node).SetTextColor(tcell.ColorYellow).SetSelectable(false).SetAlign(tview.AlignCenter))
	}

	for row, src := range nodes {
		a.table.SetCell(row+1, 0, tview.NewTableCell(src).SetTextColor(tcell.ColorYellow).SetSelectable(false))
		for col, dst := range nodes {
			cell := tview.NewTableCell("----").SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGray)
			if src == dst {
				cell.SetText("-").SetSelectable(false)
				a.table.SetCell(row+1, col+1, cell)
				continue
			}

			flow, ok := flows[matrixPairKey(src, dst)]
			if !ok {
				a.table.SetCell(row+1, col+1, cell)
				continue
			}
			if a.lossOnly && !matrixFlowHasLoss(flow) {
				cell.SetText(".").SetReference(flow.FlowID)
				a.table.SetCell(row+1, col+1, cell)
				continue
			}
			cell.SetText(matrixCellText(flow)).
				SetTextColor(matrixCellColor(flow)).
				SetReference(flow.FlowID)
			a.table.SetCell(row+1, col+1, cell)
		}
	}
}

func matrixNodes(flows []model.FlowSnapshot, agents []model.AgentSnapshot) []string {
	seen := make(map[string]struct{})
	for _, ag := range agents {
		if ag.AgentID != "" {
			seen[ag.AgentID] = struct{}{}
		}
	}
	for _, f := range flows {
		if f.Src != "" {
			seen[f.Src] = struct{}{}
		}
		if f.Dst != "" {
			seen[f.Dst] = struct{}{}
		}
	}
	nodes := make([]string, 0, len(seen))
	for node := range seen {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

func matrixFlowsByDirection(flows []model.FlowSnapshot) map[string]model.FlowSnapshot {
	out := make(map[string]model.FlowSnapshot, len(flows))
	for _, f := range flows {
		if _, ok := out[matrixPairKey(f.Src, f.Dst)]; !ok {
			out[matrixPairKey(f.Src, f.Dst)] = f
		}
	}
	return out
}

func matrixPairKey(src, dst string) string {
	return src + "\x00" + dst
}

func matrixCellText(f model.FlowSnapshot) string {
	if matrixFlowHasNoTraffic(f) {
		return "----"
	}
	return outageText(f)
}

func matrixCellColor(f model.FlowSnapshot) tcell.Color {
	if matrixFlowHasNoTraffic(f) {
		return tcell.ColorDarkGray
	}
	if f.LastError != "" || strings.EqualFold(f.ActualState, "offline") {
		return tcell.ColorRed
	}
	loss := matrixLossRatio(f)
	switch {
	case loss >= 0.05:
		return tcell.ColorRed
	case loss >= 0.01:
		return tcell.ColorYellow
	case loss > 0:
		return tcell.ColorAqua
	default:
		return tcell.ColorGreen
	}
}

func matrixLossRatio(f model.FlowSnapshot) float64 {
	if f.LastError != "" && f.LossRatio60s == 0 && len(flowGraphHistory(f)) == 0 {
		return 1
	}
	return f.LossRatio60s
}

func matrixFlowHasLoss(f model.FlowSnapshot) bool {
	return f.Lost1s > 0 || f.LossRatio1s > 0 || f.LossRatio10s > 0 || f.LossRatio60s > 0 || hasLoss(flowGraphHistory(f)) || f.LastError != ""
}

func matrixFlowHasNoTraffic(f model.FlowSnapshot) bool {
	history := flowGraphHistory(f)
	if f.DesiredState != "running" {
		return true
	}
	if f.Lost1s > 0 || f.LossRatio1s > 0 || f.LossRatio10s > 0 || f.LossRatio60s > 0 || hasLoss(history) || f.LastError != "" {
		return false
	}
	return len(history) == 0 && f.Tx1s == 0 && f.Rx1s == 0
}
