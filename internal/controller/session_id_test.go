package controller

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEncodeDecodeSessionID(t *testing.T) {
	id, err := encodeSessionID("260623", 3)
	if err != nil {
		t.Fatal(err)
	}
	if id != "26062303" {
		t.Fatalf("session id = %q, want %q", id, "26062303")
	}

	date, seq, err := decodeSessionID(id)
	if err != nil {
		t.Fatal(err)
	}
	if date != "260623" || seq != 3 {
		t.Fatalf("decode(%q) = (%q, %d), want (%q, %d)", id, date, seq, "260623", 3)
	}

	id, err = encodeSessionID("260623", 10)
	if err != nil {
		t.Fatal(err)
	}
	if id != "2606230A" {
		t.Fatalf("session id = %q, want %q", id, "2606230A")
	}
	date, seq, err = decodeSessionID(id)
	if err != nil {
		t.Fatal(err)
	}
	if date != "260623" || seq != 10 {
		t.Fatalf("decode(%q) = (%q, %d), want (%q, %d)", id, date, seq, "260623", 10)
	}
}

func TestEncodeSessionIDKeepsLexicalTimeOrder(t *testing.T) {
	if maxSessionSequence != 933 {
		t.Fatalf("max session sequence = %d, want 933", maxSessionSequence)
	}
	cases := []struct {
		seq int
		id  string
	}{
		{1, "26070201"},
		{9, "26070209"},
		{10, "2607020A"},
		{31, "2607020Z"},
		{32, "2607021A"},
		{100, "2607024C"},
		{maxSessionSequence, "260702ZZ"},
	}

	var prev string
	for _, tc := range cases {
		id, err := encodeSessionID("260702", tc.seq)
		if err != nil {
			t.Fatal(err)
		}
		if id != tc.id {
			t.Fatalf("encode seq %d = %q, want %q", tc.seq, id, tc.id)
		}
		if prev != "" && prev >= id {
			t.Fatalf("session ids should sort by time: %q >= %q", prev, id)
		}
		prev = id
	}
}

func TestEncodeSessionIDRejectsOverflow(t *testing.T) {
	if _, err := encodeSessionID("260702", maxSessionSequence+1); err == nil {
		t.Fatal("expected session id exhaustion error")
	}
}

func TestDecodeSessionIDAcceptsLegacyDecimalSequence(t *testing.T) {
	date, seq, err := decodeSessionID("260623100")
	if err != nil {
		t.Fatal(err)
	}
	if date != "260623" || seq != 100 {
		t.Fatalf("decode legacy id = (%q, %d), want (%q, %d)", date, seq, "260623", 100)
	}
}

func TestRecoverLatestSessionIDPrefersLargestReadableID(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"results_20260623-120001_26062301.csv",
		"results_20260623-120003_26062399.jsonl",
		"results_20260623-120004_2606234C.jsonl",
		"results_00000C_20260623-120002.csv",
		"results_s00000D_20260623-120005.csv",
		"results_invalid_20260623-120006.csv",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	id, ok, err := recoverLatestSessionID(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected recovered session id")
	}
	if id != "2606234C" {
		t.Fatalf("recovered session id = %s, want 2606234C", id)
	}
}

func TestRecoverLatestSessionIDAcceptsLegacyDecimalSequence(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"results_20260623-120003_26062399.jsonl",
		"results_20260623-120004_260623100.jsonl",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	id, ok, err := recoverLatestSessionID(filepath.Join(dir, "results.csv"), filepath.Join(dir, "results.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected recovered session id")
	}
	if id != "260623100" {
		t.Fatalf("recovered session id = %s, want 260623100", id)
	}
}

func TestSessionDateFromTime(t *testing.T) {
	now := time.Date(2026, 6, 23, 15, 0, 0, 0, time.UTC)
	if got := sessionDateFromTime(now); got != "260624" {
		t.Fatalf("session date = %s, want 260624", got)
	}
	now = time.Date(2026, 6, 23, 12, 34, 56, 0, time.FixedZone("JST", 9*60*60))
	if got := sessionDateFromTime(now); got != "260623" {
		t.Fatalf("session date = %s, want 260623", got)
	}
}

func TestAllocateNextSessionIDIncrementsWithinSameDay(t *testing.T) {
	srv := &Server{
		lastSessionDate: "260623",
		lastSessionSeq:  3,
		now: func() time.Time {
			return time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
		},
	}

	got, err := srv.allocateNextSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if got != "26062304" {
		t.Fatalf("next session id = %s, want 26062304", got)
	}
}

func TestAllocateNextSessionIDIncrementsPast99WithinSameDay(t *testing.T) {
	srv := &Server{
		lastSessionDate: "260623",
		lastSessionSeq:  99,
		now: func() time.Time {
			return time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
		},
	}

	got, err := srv.allocateNextSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if got != "2606234C" {
		t.Fatalf("next session id = %s, want 2606234C", got)
	}
}

func TestAllocateNextSessionIDFailsAfterMaxSequence(t *testing.T) {
	srv := &Server{
		lastSessionDate: "260623",
		lastSessionSeq:  maxSessionSequence,
		now: func() time.Time {
			return time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
		},
	}

	if _, err := srv.allocateNextSessionID(); err == nil {
		t.Fatal("expected session id exhaustion error")
	}
}

func TestAllocateNextSessionIDResetsOnNextDay(t *testing.T) {
	srv := &Server{
		lastSessionDate: "260623",
		lastSessionSeq:  9,
		now: func() time.Time {
			return time.Date(2026, 6, 23, 15, 0, 0, 0, time.UTC)
		},
	}

	got, err := srv.allocateNextSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if got != "26062401" {
		t.Fatalf("next session id = %s, want 26062401", got)
	}
}
