package tui

import (
	"strings"
	"testing"
)

func TestLossSparkline(t *testing.T) {
	got := LossSparkline([]float64{0, 0.001, 0.007, 0.015, 0.025, 0.035, 0.045, 0.05}, "unicode", 10)
	want := "█▇▆▅▄▃▂▁--"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if LossSparkline(nil, "unicode", 10) != strings.Repeat("-", 10) {
		t.Fatal("empty history should be placeholder")
	}
	if NoTrafficSparkline(12) != "no traffic--" {
		t.Fatal("no traffic placeholder changed")
	}
}

func TestLossSparklineKeepsLastRequestedSamples(t *testing.T) {
	history := make([]float64, 241)
	history[0] = 0.05

	got := LossSparkline(history, "unicode", 240)
	want := strings.Repeat("▁", 240)
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestLossSparklinePadsWhenWidthExceedsHistory(t *testing.T) {
	got := LossSparkline([]float64{0, 0.01}, "unicode", 5)
	want := "▄▁---"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
