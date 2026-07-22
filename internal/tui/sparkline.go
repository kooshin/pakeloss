package tui

import "strings"

const defaultSparklineWidth = 60

func LossSparkline(history []float64, mode string, width int) string {
	width = normalizedSparklineWidth(width)
	if len(history) == 0 {
		return strings.Repeat("-", width)
	}
	history = fixedWidthHistory(history, width)
	suffix := strings.Repeat("-", width-len(history))
	if mode == "ascii" {
		return asciiSparkline(history) + suffix
	}
	blocks := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}
	var b strings.Builder
	for _, v := range history {
		switch {
		case v < 0:
			b.WriteRune('-')
		case v <= 0:
			b.WriteRune(blocks[0])
		case v < 0.005:
			b.WriteRune(blocks[1])
		case v < 0.01:
			b.WriteRune(blocks[2])
		case v < 0.02:
			b.WriteRune(blocks[3])
		case v < 0.03:
			b.WriteRune(blocks[4])
		case v < 0.04:
			b.WriteRune(blocks[5])
		case v < 0.05:
			b.WriteRune(blocks[6])
		default:
			b.WriteRune(blocks[7])
		}
	}
	b.WriteString(suffix)
	return b.String()
}

func fixedWidthHistory(history []float64, width int) []float64 {
	width = normalizedSparklineWidth(width)
	if len(history) > width {
		history = history[len(history)-width:]
	}
	return reverseHistory(history)
}

func NoTrafficSparkline(width int) string {
	width = normalizedSparklineWidth(width)
	if width <= len("no traffic") {
		return strings.Repeat("-", width)
	}
	return "no traffic" + strings.Repeat("-", width-len("no traffic"))
}

func asciiSparkline(history []float64) string {
	chars := []rune(".:-=+*#%@")
	var b strings.Builder
	for _, v := range history {
		switch {
		case v < 0:
			b.WriteRune('-')
		case v <= 0:
			b.WriteRune(chars[0])
		case v < 0.005:
			b.WriteRune(chars[1])
		case v < 0.01:
			b.WriteRune(chars[2])
		case v < 0.02:
			b.WriteRune(chars[3])
		case v < 0.03:
			b.WriteRune(chars[4])
		case v < 0.04:
			b.WriteRune(chars[5])
		case v < 0.05:
			b.WriteRune(chars[6])
		case v < 0.10:
			b.WriteRune(chars[7])
		default:
			b.WriteRune(chars[8])
		}
	}
	return b.String()
}

func reverseHistory(history []float64) []float64 {
	out := make([]float64, len(history))
	for i := range history {
		out[i] = history[len(history)-1-i]
	}
	return out
}

func normalizedSparklineWidth(width int) int {
	if width <= 0 {
		return defaultSparklineWidth
	}
	return width
}
