package controller

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pakeloss/internal/model"
	"pakeloss/internal/pb"
)

func TestResultStoreAggregatesWindowAndWritesTimestampedFiles(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "results.csv")
	jsonlPath := filepath.Join(dir, "results.jsonl")
	store, err := NewResultStore(csvPath, jsonlPath, "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	if err := store.StartSession("26062101"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := store.Write(&pb.ResultSummary{
			Ts:         base.Add(time.Duration(i+1) * time.Second).Format(time.RFC3339Nano),
			AgentId:    "node-b",
			Src:        "node-a",
			Dst:        "node-b",
			FlowId:     "node-a<=>node-b",
			IntervalMs: 10,
			Tx:         100,
			Rx:         97,
			Lost:       3,
			LossRatio:  0.03,
			Reorder:    1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	current = base.Add(13 * time.Second)
	if err := store.flushTick(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(csvPath); !os.IsNotExist(err) {
		t.Fatalf("base csv path should not be used directly: err=%v", err)
	}
	if _, err := os.Stat(jsonlPath); !os.IsNotExist(err) {
		t.Fatalf("base jsonl path should not be used directly: err=%v", err)
	}

	csvFiles, err := filepath.Glob(filepath.Join(dir, "results_*.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(csvFiles) != 1 {
		t.Fatalf("csv files = %v, want 1 timestamped file", csvFiles)
	}
	jsonlFiles, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(jsonlFiles) != 1 {
		t.Fatalf("jsonl files = %v, want 1 timestamped file", jsonlFiles)
	}

	b, err := os.ReadFile(csvFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, "session_id,ts,window_start,window_end,src,dst,flow_id,interval_ms,tx,rx,lost,loss_ratio,loss_time_ms,loss_time_sec,duplicate,reorder,outage_ms,outage_sec\n") {
		t.Fatalf("missing csv header: %q", got)
	}
	if strings.Count(strings.TrimSpace(got), "\n") != 10 {
		t.Fatalf("csv should contain header and ten 1s rows: %q", got)
	}
	if !strings.Contains(got, "26062101,2026-06-21 21:00:01,2026-06-21 21:00:00,2026-06-21 21:00:01,node-a,node-b,node-a<=>node-b,10,100,97,3,0.03,30,0.03,0,1,0,0\n") {
		t.Fatalf("missing first 1s csv row: %q", got)
	}
	if !strings.Contains(got, "26062101,2026-06-21 21:00:10,2026-06-21 21:00:09,2026-06-21 21:00:10,node-a,node-b,node-a<=>node-b,10,100,97,3,0.03,30,0.03,0,1,0,0\n") {
		t.Fatalf("missing last 1s csv row: %q", got)
	}

	b, err = os.ReadFile(jsonlFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got = string(b)
	if strings.Count(strings.TrimSpace(got), "\n") != 9 {
		t.Fatalf("jsonl should contain ten 1s rows: %q", got)
	}
	if !strings.Contains(got, `"ts":"2026-06-21T12:00:01Z"`) ||
		!strings.Contains(got, `"session_id":"26062101"`) ||
		!strings.Contains(got, `"window_start":"2026-06-21T12:00:00Z"`) ||
		!strings.Contains(got, `"window_end":"2026-06-21T12:00:01Z"`) ||
		!strings.Contains(got, `"tx":100`) ||
		!strings.Contains(got, `"lost":3`) ||
		!strings.Contains(got, `"loss_time_ms":30`) ||
		!strings.Contains(got, `"reorder":1`) ||
		!strings.Contains(got, `"outage_ms":0`) ||
		!strings.Contains(got, `"outage_sec":0`) {
		t.Fatalf("missing 1s jsonl result: %q", got)
	}
}

func TestResultStoreLogsOnlyFullLossBucketDurationAsOutage(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), "", "", "", "UTC", "10s")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	if err := store.StartSession("26071301"); err != nil {
		t.Fatal(err)
	}
	for _, summary := range []*pb.ResultSummary{
		{Ts: base.Add(100 * time.Millisecond).Format(time.RFC3339Nano), FlowId: "a->b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 10, Rx: 0, Lost: 10, OutageMs: 100},
		{Ts: base.Add(200 * time.Millisecond).Format(time.RFC3339Nano), FlowId: "a->b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 10, Rx: 9, Lost: 1},
		{Ts: base.Add(300 * time.Millisecond).Format(time.RFC3339Nano), FlowId: "a->b", Src: "a", Dst: "b", IntervalMs: 10, Tx: 10, Rx: 10},
	} {
		if err := store.Write(summary); err != nil {
			t.Fatal(err)
		}
	}
	current = base.Add(2 * time.Second)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "results_*.csv"))
	if err != nil || len(files) != 1 {
		t.Fatalf("csv files = %v err=%v", files, err)
	}
	b, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), ",30,19,11,0.36666666666666664,110,0.11,0,0,100,0.1\n") {
		t.Fatalf("outage should include only full-loss bucket: %q", string(b))
	}
}

func TestResultStoreWritesSessionOutageEventsToCSVAndJSONL(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore("", "", "", "", "Asia/Tokyo", "1s")
	if err != nil {
		t.Fatal(err)
	}
	store.SetOutageEventPaths(filepath.Join(dir, "outage_events.csv"), filepath.Join(dir, "outage_events.jsonl"))
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base }
	if err := store.StartSession("26071401"); err != nil {
		t.Fatal(err)
	}
	started := OutageEventLogRecord{EventID: "a->b:1", State: "started", Ts: base.Add(2 * time.Second), FlowID: "a->b", Src: "a", Dst: "b", StartedAt: base, OutageThresholdMs: 100}
	ended := started
	ended.State = "ended"
	ended.Ts = base.Add(3 * time.Second)
	ended.EndedAt = base.Add(150 * time.Millisecond)
	ended.DurationMs = 150
	ended.EndReason = "recovered"
	if err := store.WriteOutageEvent(started); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteOutageEvent(started); err != nil {
		t.Fatal(err)
	}
	csvFiles, _ := filepath.Glob(filepath.Join(dir, "outage_events_*.csv"))
	jsonlFiles, _ := filepath.Glob(filepath.Join(dir, "outage_events_*.jsonl"))
	if len(csvFiles) != 1 || len(jsonlFiles) != 1 {
		t.Fatalf("event files csv=%v jsonl=%v", csvFiles, jsonlFiles)
	}
	activeJSONL, _ := os.ReadFile(jsonlFiles[0])
	if !strings.Contains(string(activeJSONL), `"state":"started"`) || strings.Count(string(activeJSONL), `"event_id":`) != 1 {
		t.Fatalf("active event jsonl = %q", activeJSONL)
	}
	if err := store.WriteOutageEvent(ended); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteOutageEvent(ended); err != nil {
		t.Fatal(err)
	}
	nextStarted := started
	nextStarted.EventID = "a->b:2"
	nextStarted.Ts = base.Add(4 * time.Second)
	nextStarted.StartedAt = base.Add(170 * time.Millisecond)
	nextEnded := nextStarted
	nextEnded.State = "ended"
	nextEnded.Ts = base.Add(5 * time.Second)
	nextEnded.EndedAt = base.Add(270 * time.Millisecond)
	nextEnded.DurationMs = 100
	nextEnded.EndReason = "recovered"
	if err := store.WriteOutageEvent(nextStarted); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteOutageEvent(nextEnded); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	csvBody, _ := os.ReadFile(csvFiles[0])
	if !strings.Contains(string(csvBody), "session_id,event_id,state,ts,flow_id,src,dst,started_at,ended_at,duration_ms,duration_sec,outage_threshold_ms,end_reason") || !strings.Contains(string(csvBody), "a->b:1,ended,2026-07-14 21:00:05,a->b,a,b,2026-07-14 21:00:00,2026-07-14 21:00:00.27,250,0.25,100,recovered") {
		t.Fatalf("event csv = %q", csvBody)
	}
	if strings.Count(strings.TrimSpace(string(csvBody)), "\n") != 1 {
		t.Fatalf("event csv rows were not aggregated: %q", csvBody)
	}
	jsonlBody, _ := os.ReadFile(jsonlFiles[0])
	if strings.Contains(string(jsonlBody), `"state":"started"`) || !strings.Contains(string(jsonlBody), `"state":"ended"`) || !strings.Contains(string(jsonlBody), `"event_id":"a-\u003eb:1"`) || !strings.Contains(string(jsonlBody), `"ts":"2026-07-14T12:00:05Z"`) || !strings.Contains(string(jsonlBody), `"duration_ms":250`) || strings.Count(string(jsonlBody), `"event_id":`) != 1 {
		t.Fatalf("event jsonl = %q", jsonlBody)
	}
}

func TestOutageEventsShouldMergeOnlyBriefRecoveryOnSameFlow(t *testing.T) {
	previous := outageEventLogRow{
		State:             "ended",
		FlowID:            "a->b",
		StartedAt:         "2026-07-14T12:00:00Z",
		EndedAt:           "2026-07-14T12:00:01Z",
		OutageThresholdMs: 100,
		EndReason:         "recovered",
	}
	current := outageEventLogRow{State: "started", FlowID: "a->b", StartedAt: "2026-07-14T12:00:01.1Z", OutageThresholdMs: 100}
	if !outageEventsShouldMerge(previous, current) {
		t.Fatal("events separated by the outage threshold should merge")
	}
	current.StartedAt = "2026-07-14T12:00:01.101Z"
	if outageEventsShouldMerge(previous, current) {
		t.Fatal("events separated by more than the outage threshold should not merge")
	}
	current.StartedAt = "2026-07-14T12:00:01Z"
	previous.EndReason = "measurement_stopped"
	if outageEventsShouldMerge(previous, current) {
		t.Fatal("events across an actual measurement stop should not merge")
	}
}

func TestResultStoreAggregatesTenMillisecondResultsByFlushInterval(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "results.csv")
	store, err := NewResultStore(csvPath, "", "", "", "UTC", "1s")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	if err := store.StartSession("26062102"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := store.Write(&pb.ResultSummary{
			Ts:         base.Add(time.Duration(i+1) * 10 * time.Millisecond).Format(time.RFC3339Nano),
			AgentId:    "node-b",
			Src:        "node-a",
			Dst:        "node-b",
			FlowId:     "node-a<=>node-b",
			IntervalMs: 10,
			Tx:         1,
			Rx:         1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	current = base.Add(3 * time.Second)
	if err := store.flushTick(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	csvFiles, err := filepath.Glob(filepath.Join(dir, "results_*.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(csvFiles) != 1 {
		t.Fatalf("csv files = %v, want 1 timestamped file", csvFiles)
	}
	b, err := os.ReadFile(csvFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if strings.Count(got, "\n") != 2 {
		t.Fatalf("csv should contain header and one aggregated row: %q", got)
	}
	if !strings.Contains(got, "26062102,2026-06-21 12:00:01,2026-06-21 12:00:00,2026-06-21 12:00:01,node-a,node-b,node-a<=>node-b,10,100,100,0,0,0,0,0,0,0,0\n") {
		t.Fatalf("missing 1s aggregated row for 10ms results: %q", got)
	}
}

func TestResultStoreKeepsPreviousWindowSeparate(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "results.csv")
	store, err := NewResultStore(csvPath, "", "", "", "UTC", "1s")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 7, 3, 2, 43, 37, 72969725, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	if err := store.StartSession("26070305"); err != nil {
		t.Fatal(err)
	}
	if err := store.flushTick(); err != nil {
		t.Fatal(err)
	}

	current = time.Date(2026, 7, 3, 2, 43, 45, 500*1e6, time.UTC)
	if err := store.Write(&pb.ResultSummary{
		Ts:         time.Date(2026, 7, 3, 2, 43, 45, 0, time.UTC).Format(time.RFC3339Nano),
		AgentId:    "rt1",
		Src:        "rt1",
		Dst:        "rt3",
		FlowId:     "rt1<=>rt3",
		IntervalMs: 10,
		Tx:         94,
		Rx:         93,
		Lost:       1,
	}); err != nil {
		t.Fatal(err)
	}
	current = time.Date(2026, 7, 3, 2, 43, 46, 500*1e6, time.UTC)
	if err := store.Write(&pb.ResultSummary{
		Ts:         time.Date(2026, 7, 3, 2, 43, 46, 0, time.UTC).Format(time.RFC3339Nano),
		AgentId:    "rt1",
		Src:        "rt1",
		Dst:        "rt3",
		FlowId:     "rt1<=>rt3",
		IntervalMs: 10,
		Tx:         99,
		Rx:         98,
		Lost:       1,
	}); err != nil {
		t.Fatal(err)
	}
	current = time.Date(2026, 7, 3, 2, 43, 46, 500*1e6, time.UTC)
	if err := store.flushTick(); err != nil {
		t.Fatal(err)
	}
	current = time.Date(2026, 7, 3, 2, 43, 47, 500*1e6, time.UTC)
	if err := store.flushTick(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	csvFiles, err := filepath.Glob(filepath.Join(dir, "results_*.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(csvFiles) != 1 {
		t.Fatalf("csv files = %v, want 1 timestamped file", csvFiles)
	}
	b, err := os.ReadFile(csvFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if strings.Count(got, "\n") != 3 {
		t.Fatalf("csv should contain header and two rows: %q", got)
	}
	if !strings.Contains(got, "26070305,2026-07-03 02:43:45,2026-07-03 02:43:44,2026-07-03 02:43:45,rt1,rt3,rt1<=>rt3,10,94,93,1,0.010638297872340425,10,0.01,0,0,0,0\n") {
		t.Fatalf("missing previous-window row: %q", got)
	}
	if !strings.Contains(got, "26070305,2026-07-03 02:43:46,2026-07-03 02:43:45,2026-07-03 02:43:46,rt1,rt3,rt1<=>rt3,10,99,98,1,0.010101010101010102,10,0.01,0,0,0,0\n") {
		t.Fatalf("missing current-window row: %q", got)
	}
	if strings.Contains(got, ",rt1,rt3,rt1<=>rt3,10,193,191,2,") {
		t.Fatalf("previous-window result should not merge into 193 tx row: %q", got)
	}
}

func TestResultStoreInactiveWriteDoesNotCreateFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Write(&pb.ResultSummary{
		Ts:         "2026-06-21T12:00:01Z",
		AgentId:    "node-b",
		Src:        "node-a",
		Dst:        "node-b",
		FlowId:     "node-a<=>node-b",
		IntervalMs: 10,
		Tx:         100,
		Rx:         99,
		Lost:       1,
	}); err != nil {
		t.Fatal(err)
	}
	if len(store.pending) != 0 {
		t.Fatalf("pending results = %d, want 0", len(store.pending))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if matches, err := filepath.Glob(filepath.Join(dir, "results_*")); err != nil || len(matches) != 0 {
		t.Fatalf("timestamped result files = %v err=%v, want none", matches, err)
	}
}

func TestResultStoreStopSessionFlushesPartialWindowAndStopsWrites(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	if err := store.StartSession("26062101"); err != nil {
		t.Fatal(err)
	}

	current = base.Add(4 * time.Second)
	if err := store.Write(&pb.ResultSummary{
		Ts:         base.Add(time.Second).Format(time.RFC3339Nano),
		AgentId:    "node-b",
		Src:        "node-a",
		Dst:        "node-b",
		FlowId:     "node-a<=>node-b",
		IntervalMs: 10,
		Tx:         100,
		Rx:         99,
		Lost:       1,
		LossRatio:  0.01,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StopSession(); err != nil {
		t.Fatal(err)
	}

	current = base.Add(5 * time.Second)
	if err := store.Write(&pb.ResultSummary{
		Ts:         base.Add(5 * time.Second).Format(time.RFC3339Nano),
		AgentId:    "node-b",
		Src:        "node-a",
		Dst:        "node-b",
		FlowId:     "node-a<=>node-b",
		IntervalMs: 10,
		Tx:         100,
		Rx:         100,
		Lost:       0,
	}); err != nil {
		t.Fatal(err)
	}

	jsonlFiles, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl"))
	if err != nil || len(jsonlFiles) != 1 {
		t.Fatalf("jsonl files = %v err=%v", jsonlFiles, err)
	}
	b, err := os.ReadFile(jsonlFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 1 {
		t.Fatalf("jsonl lines = %d, want 1: %q", len(lines), string(b))
	}
	got := lines[0]
	if !strings.Contains(got, `"window_start":"2026-06-21T12:00:00Z"`) || !strings.Contains(got, `"window_end":"2026-06-21T12:00:01Z"`) {
		t.Fatalf("partial window timestamps not flushed on stop: %q", got)
	}
}

func TestResultStoreOnlyCreatesConfiguredSinks(t *testing.T) {
	tests := []struct {
		name      string
		csvPath   string
		jsonlPath string
		wantCSV   int
		wantJSONL int
	}{
		{name: "csv only", csvPath: "results.csv", wantCSV: 1, wantJSONL: 0},
		{name: "jsonl only", jsonlPath: "results.jsonl", wantCSV: 0, wantJSONL: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			csvPath := ""
			jsonlPath := ""
			if tc.csvPath != "" {
				csvPath = filepath.Join(dir, tc.csvPath)
			}
			if tc.jsonlPath != "" {
				jsonlPath = filepath.Join(dir, tc.jsonlPath)
			}
			store, err := NewResultStore(csvPath, jsonlPath, "", "", "Asia/Tokyo", "10s")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.StartSession("26062101"); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			csvFiles, err := filepath.Glob(filepath.Join(dir, "results_*.csv"))
			if err != nil {
				t.Fatal(err)
			}
			if len(csvFiles) != tc.wantCSV {
				t.Fatalf("csv files = %v, want %d", csvFiles, tc.wantCSV)
			}
			jsonlFiles, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			if len(jsonlFiles) != tc.wantJSONL {
				t.Fatalf("jsonl files = %v, want %d", jsonlFiles, tc.wantJSONL)
			}
		})
	}
}

func TestResultStoreWithoutConfiguredSinksCreatesNoFiles(t *testing.T) {
	store, err := NewResultStore("", "", "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartSession("26062101"); err != nil {
		t.Fatal(err)
	}
	if store.active {
		t.Fatal("store should remain inactive without configured sinks")
	}
	if err := store.Write(&pb.ResultSummary{FlowId: "node-a<=>node-b", Tx: 100}); err != nil {
		t.Fatal(err)
	}
	if len(store.pending) != 0 {
		t.Fatalf("pending results = %d, want 0", len(store.pending))
	}
	if err := store.StopSession(); err != nil {
		t.Fatal(err)
	}
}

func TestResultStoreFlushTickSyncsDirtyDebugFile(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), "", "", "", "UTC", "1s")
	if err != nil {
		t.Fatal(err)
	}
	store.SetDebugJSONLPath(filepath.Join(dir, "debug.jsonl"))

	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	if err := store.StartSession("26062101"); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteDebug(SeqDebugRecord{
		ControllerTs:    base.Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		FlowID:          "a<=>b",
		SessionID:       1,
		Seq:             10,
		SenderSeen:      true,
		FinalState:      "lost",
		FinalizedReason: "sender_only_expired",
	}); err != nil {
		t.Fatal(err)
	}
	if !store.debugDirty {
		t.Fatal("debugDirty = false, want true after WriteDebug")
	}

	current = base.Add(2 * time.Second)
	if err := store.flushTick(); err != nil {
		t.Fatal(err)
	}
	if store.debugDirty {
		t.Fatal("debugDirty = true, want false after flushTick sync")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	debugFiles, err := filepath.Glob(filepath.Join(dir, "debug_*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(debugFiles) != 1 {
		t.Fatalf("debug files = %v, want 1", debugFiles)
	}
	b, err := os.ReadFile(debugFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, `"flow_id":"a\u003c=\u003eb"`) || !strings.Contains(got, `"finalized_reason":"sender_only_expired"`) {
		t.Fatalf("debug jsonl missing record: %q", got)
	}
}

func TestResultStoreStopSessionWithSummaryWritesSummaryFiles(t *testing.T) {
	dir := t.TempDir()
	summaryCSV := filepath.Join(dir, "session_summaries.csv")
	summaryJSONL := filepath.Join(dir, "session_summaries.jsonl")
	store, err := NewResultStore("", "", summaryCSV, summaryJSONL, "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	if err := store.StartSession("26062101"); err != nil {
		t.Fatal(err)
	}
	current = base.Add(5 * time.Second)
	flows := []model.FlowRuntimeStatus{
		{FlowID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", TxTotal: 100, RxTotal: 90, LostTotal: 10, IsolatedLossEvents: 1, OutageCount: 1, LastOutageMs: 1250, MaxOutageMs: 1250, OutageTotalMs: 1250},
		{FlowID: "node-a<=>node-c", Src: "node-a", Dst: "node-c", TxTotal: 200, RxTotal: 150, LostTotal: 50, IsolatedLossEvents: 2, OutageCount: 2, LastOutageMs: 1500, MaxOutageMs: 2500, OutageTotalMs: 4000},
	}
	if err := store.StopSessionWithSummary(flows); err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(summaryCSV)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, "row_type,session_id,started_at,ended_at,duration_sec,flow_count,flow_id,src,dst,tx,rx,lost,loss_ratio,loss_time_total_sec,isolated_loss_events,outage_count,last_outage_sec,max_outage_sec,outage_total_sec\n") {
		t.Fatalf("missing summary csv header: %q", got)
	}
	if !strings.Contains(got, "flow,26062101,2026-06-21 21:00:00,2026-06-21 21:00:05,5.00,2,node-a<=>node-b,node-a,node-b,100,90,10,0.1,0.00,1,1,1.25,1.25,1.25\n") {
		t.Fatalf("missing flow summary row: %q", got)
	}
	if !strings.Contains(got, "session_peak_flow,26062101,2026-06-21 21:00:00,2026-06-21 21:00:05,5.00,2,node-a<=>node-c,node-a,node-c,200,150,50,0.25,0.00,2,2,1.50,2.50,4.00\n") {
		t.Fatalf("missing session peak row: %q", got)
	}
	if strings.Contains(got, "outage_event") {
		t.Fatalf("summary csv should not include outage event rows: %q", got)
	}

	b, err = os.ReadFile(summaryJSONL)
	if err != nil {
		t.Fatal(err)
	}
	got = string(b)
	if !strings.Contains(got, `"row_type":"session_summary"`) ||
		!strings.Contains(got, `"session_id":"26062101"`) ||
		!strings.Contains(got, `"duration_sec":"5.00"`) ||
		!strings.Contains(got, `"peak_flow_id":"node-a\u003c=\u003enode-c"`) ||
		!strings.Contains(got, `"max_outage_sec":"2.50"`) {
		t.Fatalf("missing summary jsonl fields: %q", got)
	}
	if strings.Contains(got, `"row_type":"outage_event"`) {
		t.Fatalf("summary jsonl should not include outage event rows: %q", got)
	}
	if strings.Contains(got, `"total_tx":`) || strings.Contains(got, `"total_rx":`) || strings.Contains(got, `"total_lost":`) {
		t.Fatalf("summary jsonl should not include total fields: %q", got)
	}
	if strings.Contains(got, `"avg_loss_ratio":`) || strings.Contains(got, `"max_loss_ratio":`) {
		t.Fatalf("summary jsonl should not include top-level loss ratio fields: %q", got)
	}
}

func TestResultStoreRotateCreatesNewTimestampedFilesAndFlushesPending(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2030, 1, 2, 3, 4, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	if err := store.StartSession("30010201"); err != nil {
		t.Fatal(err)
	}

	current = base.Add(5 * time.Second)
	if err := store.Write(&pb.ResultSummary{
		Ts:         base.Add(time.Second).Format(time.RFC3339Nano),
		AgentId:    "node-b",
		Src:        "node-a",
		Dst:        "node-b",
		FlowId:     "node-a<=>node-b",
		IntervalMs: 10,
		Tx:         100,
		Rx:         97,
		Lost:       3,
		LossRatio:  0.03,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Rotate(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, "results_20300102-030400_30010201.csv")); err != nil {
		t.Fatalf("initial csv file not found: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "results_20300102-030405_30010201.csv")); err != nil {
		t.Fatalf("rotated csv file not found: %v", err)
	}

	csvFiles, err := filepath.Glob(filepath.Join(dir, "results_*.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(csvFiles) != 2 {
		t.Fatalf("csv files = %v, want 2", csvFiles)
	}
	first, err := os.ReadFile(csvFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(csvFiles[1])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(first)+string(second), "2030-01-02 12:04:01") {
		t.Fatalf("rotated output should include partial window end time: %q %q", string(first), string(second))
	}
}

func TestNewResultStoreRejectsInvalidFlushInterval(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "bad"); err == nil {
		t.Fatal("expected invalid flush interval error")
	}
}

func TestNewServerStartsLoggingSessionWhenAnyFlowRunning(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base }

	_ = NewServer(model.ControllerConfig{}, model.MeshConfig{
		Flows: []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running"}},
	}, store)

	if matches, err := filepath.Glob(filepath.Join(dir, "results_*.csv")); err != nil || len(matches) != 1 {
		t.Fatalf("csv files = %v err=%v, want 1", matches, err)
	}
	if matches, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl")); err != nil || len(matches) != 1 {
		t.Fatalf("jsonl files = %v err=%v, want 1", matches, err)
	}
}

func TestNewServerDoesNotCreateLogsWhenAllFlowsStopped(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base }

	_ = NewServer(model.ControllerConfig{}, model.MeshConfig{
		Flows: []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "stopped"}},
	}, store)

	if matches, err := filepath.Glob(filepath.Join(dir, "results_*.csv")); err != nil || len(matches) != 0 {
		t.Fatalf("csv files = %v err=%v, want none", matches, err)
	}
	if matches, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl")); err != nil || len(matches) != 0 {
		t.Fatalf("jsonl files = %v err=%v, want none", matches, err)
	}
}

func TestServerFlowStateTransitionsControlLoggingSession(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a", UDPAddr: "127.0.0.1:40001"}, {ID: "b", UDPAddr: "127.0.0.1:40002"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "stopped", IntervalMs: 10}},
	}, store)

	if matches, err := filepath.Glob(filepath.Join(dir, "results_*.csv")); err != nil || len(matches) != 0 {
		t.Fatalf("csv files = %v err=%v, want none", matches, err)
	}
	if err := srv.SetFlowState("a<=>b", "running"); err != nil {
		t.Fatal(err)
	}
	if matches, err := filepath.Glob(filepath.Join(dir, "results_*.csv")); err != nil || len(matches) != 1 {
		t.Fatalf("csv files = %v err=%v, want 1", matches, err)
	}

	current = base.Add(time.Second)
	if err := store.Write(&pb.ResultSummary{
		Ts:         current.Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		AgentId:    "b",
		Src:        "a",
		Dst:        "b",
		FlowId:     "a<=>b",
		IntervalMs: 10,
		Tx:         100,
		Rx:         99,
		Lost:       1,
	}); err != nil {
		t.Fatal(err)
	}
	current = base.Add(2 * time.Second)
	if err := srv.SetFlowState("a<=>b", "stopped"); err != nil {
		t.Fatal(err)
	}

	jsonlFiles, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl"))
	if err != nil || len(jsonlFiles) != 1 {
		t.Fatalf("jsonl files = %v err=%v", jsonlFiles, err)
	}
	b, err := os.ReadFile(jsonlFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(string(b))
	if !strings.Contains(got, `"window_end":"2026-06-21T12:00:02Z"`) || !strings.Contains(got, `"lost":1`) {
		t.Fatalf("stopped session did not flush expected record: %q", got)
	}
}

func TestServerKeepsLoggingWhileAnotherFlowStillRunning(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		ConfigVersion: 1,
		Nodes: []model.NodeConfig{
			{ID: "a", UDPAddr: "127.0.0.1:40001"},
			{ID: "b", UDPAddr: "127.0.0.1:40002"},
			{ID: "c", UDPAddr: "127.0.0.1:40003"},
		},
		Flows: []model.MeshFlowConfig{
			{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10},
			{ID: "a<=>c", Src: "a", Dst: "c", State: "running", IntervalMs: 10},
		},
	}, store)

	current = base.Add(time.Second)
	if err := store.Write(&pb.ResultSummary{
		Ts:         current.Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		AgentId:    "b",
		Src:        "a",
		Dst:        "b",
		FlowId:     "a<=>b",
		IntervalMs: 10,
		Tx:         100,
		Rx:         100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.SetFlowState("a<=>b", "stopped"); err != nil {
		t.Fatal(err)
	}
	current = base.Add(2 * time.Second)
	if err := store.Write(&pb.ResultSummary{
		Ts:         current.Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		AgentId:    "c",
		Src:        "a",
		Dst:        "c",
		FlowId:     "a<=>c",
		IntervalMs: 10,
		Tx:         100,
		Rx:         98,
		Lost:       2,
	}); err != nil {
		t.Fatal(err)
	}
	current = base.Add(3 * time.Second)
	if err := srv.SetFlowState("a<=>c", "stopped"); err != nil {
		t.Fatal(err)
	}

	csvFiles, err := filepath.Glob(filepath.Join(dir, "results_*.csv"))
	if err != nil || len(csvFiles) != 1 {
		t.Fatalf("csv files = %v err=%v, want 1", csvFiles, err)
	}
	b, err := os.ReadFile(csvFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, "a<=>b") || !strings.Contains(got, "a<=>c") {
		t.Fatalf("single logging session should contain both flows: %q", got)
	}
}

func TestServerWritesFinalizedReportToLogs(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }

	NewServer(model.ControllerConfig{}, model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}, store)

	current = base.Add(time.Second)
	if err := store.Write(&pb.ResultSummary{
		Ts:         current.Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		AgentId:    "b",
		Src:        "a",
		Dst:        "b",
		FlowId:     "a<=>b",
		IntervalMs: 10,
		Tx:         0,
		Rx:         0,
	}); err != nil {
		t.Fatal(err)
	}
	current = base.Add(2 * time.Second)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	csvFiles, err := filepath.Glob(filepath.Join(dir, "results_*.csv"))
	if err != nil || len(csvFiles) != 1 {
		t.Fatalf("csv files = %v err=%v", csvFiles, err)
	}
	b, err := os.ReadFile(csvFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, ",a<=>b,10,0,0,0,0,0,0,0,0,0,0\n") {
		t.Fatalf("finalized csv row not found: %q", got)
	}
}

func TestSyntheticControllerLossIsWrittenToLogs(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }

	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	}, store)
	srv.Runtime().AgentOnline("a", "127.0.0.1:40001", 1)

	current = base.Add(5 * time.Second)
	srv.Runtime().agents["a"].LastHeartbeat = current
	for _, res := range srv.Runtime().ApplyControllerAgentLoss(current, 3*time.Second) {
		if err := store.Write(res); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	jsonlFiles, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl"))
	if err != nil || len(jsonlFiles) != 1 {
		t.Fatalf("jsonl files = %v err=%v", jsonlFiles, err)
	}
	b, err := os.ReadFile(jsonlFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, `"flow_id":"a\u003c=\u003eb"`) ||
		!strings.Contains(got, `"tx":100`) ||
		!strings.Contains(got, `"lost":100`) ||
		!strings.Contains(got, `"loss_ratio":1`) ||
		!strings.Contains(got, `"outage_ms":1000`) ||
		!strings.Contains(got, `"outage_sec":1`) {
		t.Fatalf("synthetic jsonl result not found: %q", got)
	}
}

func TestRestartAllRotatesSessionAndKeepsExistingLogs(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "Asia/Tokyo", "10s")
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2030, 1, 2, 3, 4, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		Flows: []model.MeshFlowConfig{{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", State: "running", IntervalMs: 10}},
	}, store)

	current = base.Add(time.Second)
	srv.handleAgentMessage(&pb.AgentMessage{ResultReport: &pb.ResultReport{
		Ts:         "2030-01-02T03:04:01Z",
		AgentId:    "node-a",
		Src:        "node-a",
		Dst:        "node-b",
		FlowId:     "node-a<=>node-b",
		IntervalMs: 10,
		WindowMs:   1000,
		Role:       "sender",
		Tx:         100,
		Rx:         99,
		Lost:       1,
	}})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/flows/restart", nil)
	rec := httptest.NewRecorder()
	srv.handleFlowPath(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	if matches, err := filepath.Glob(filepath.Join(dir, "results_*.csv")); err != nil || len(matches) != 2 {
		t.Fatalf("csv files = %v err=%v, want 2", matches, err)
	}
	if matches, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl")); err != nil || len(matches) != 2 {
		t.Fatalf("jsonl files = %v err=%v, want 2", matches, err)
	}

	current = base.Add(4 * time.Second)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	jsonlFiles, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl"))
	if err != nil || len(jsonlFiles) != 2 {
		t.Fatalf("jsonl files = %v err=%v", jsonlFiles, err)
	}
	for _, path := range jsonlFiles {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("restarted session file missing: %v", err)
		}
	}
}

func TestServerWritesLogsUsingControllerTimeWindows(t *testing.T) {
	dir := t.TempDir()
	store, err := NewResultStore(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"), "", "", "UTC", "10s")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	current := base
	store.now = func() time.Time { return current }
	if err := store.StartSession("26062103"); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntimeStore(model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "a"}, {ID: "b"}},
		Flows:         []model.MeshFlowConfig{{ID: "a<=>b", Src: "a", Dst: "b", State: "running", IntervalMs: 10}},
	})
	current = base.Add(95 * time.Millisecond)
	runtime.ingestReportLocked(&pb.ResultReport{
		Ts:         "2035-01-02T03:04:05Z",
		AgentId:    "a",
		Src:        "a",
		Dst:        "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "sender",
		Tx:         10,
		SeqRanges:  []*pb.SeqRange{{Start: 0, End: 9}},
	}, current)
	current = base.Add(105 * time.Millisecond)
	runtime.ingestReportLocked(&pb.ResultReport{
		Ts:         "2015-01-02T03:04:05Z",
		AgentId:    "b",
		Src:        "a",
		Dst:        "b",
		FlowId:     "a<=>b",
		SessionId:  1,
		IntervalMs: 10,
		WindowMs:   100,
		Role:       "receiver",
		Rx:         10,
		SeqRanges:  []*pb.SeqRange{{Start: 0, End: 9}},
	}, current)
	if summaries := runtime.finalizeDueReportsLocked(current); len(summaries) != 1 {
		t.Fatalf("finalized summaries = %d, want 1", len(summaries))
	} else if err := store.Write(summaries[0]); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	jsonlFiles, err := filepath.Glob(filepath.Join(dir, "results_*.jsonl"))
	if err != nil || len(jsonlFiles) != 1 {
		t.Fatalf("jsonl files = %v err=%v", jsonlFiles, err)
	}
	b, err := os.ReadFile(jsonlFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(string(b))
	if !strings.Contains(got, `"ts":"2026-06-21T12:00:00.105Z"`) ||
		!strings.Contains(got, `"window_start":"2026-06-21T12:00:00Z"`) ||
		!strings.Contains(got, `"window_end":"2026-06-21T12:00:00.105Z"`) {
		t.Fatalf("controller-time log window not found: %q", got)
	}
}
