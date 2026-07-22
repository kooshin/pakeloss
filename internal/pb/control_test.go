package pb

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestResultReportWireFormatUsesSequenceRangesInsteadOfLegacyCounters(t *testing.T) {
	b, err := json.Marshal(ResultReport{
		Role:      "sender",
		Tx:        10,
		Rx:        9,
		Lost:      1,
		SeqRanges: []*SeqRange{{Start: 0, End: 9}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if strings.Contains(got, `"tx"`) || strings.Contains(got, `"rx"`) || strings.Contains(got, `"lost"`) {
		t.Fatalf("legacy counters leaked into wire format: %s", got)
	}
	if !strings.Contains(got, `"seq_ranges":[{"start":0,"end":9}]`) {
		t.Fatalf("sequence ranges missing from wire format: %s", got)
	}
}
