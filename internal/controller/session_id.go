package controller

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	sessionIDWidth     = 8
	sessionDateWidth   = 6
	sessionSuffixWidth = 2
)

const sessionSuffixAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var (
	sessionIDLocation     = time.FixedZone("JST", 9*60*60)
	maxSessionSequence    = countSessionSuffixes()
	errSessionIDExhausted = fmt.Errorf("session id exhausted")
)

func sessionDateFromTime(now time.Time) string {
	return now.In(sessionIDLocation).Format("060102")
}

func encodeSessionID(date string, seq int) (string, error) {
	suffix, err := encodeSessionSuffix(seq)
	if err != nil {
		return "", err
	}
	return date + suffix, nil
}

func decodeSessionID(id string) (string, int, error) {
	if len(id) <= sessionDateWidth {
		return "", 0, fmt.Errorf("invalid session id length: %q", id)
	}
	if _, err := time.Parse("060102", id[:sessionDateWidth]); err != nil {
		return "", 0, fmt.Errorf("invalid session date: %q", id)
	}
	seq, err := decodeSessionSequence(id[sessionDateWidth:])
	if err != nil {
		return "", 0, err
	}
	return id[:sessionDateWidth], seq, nil
}

func recoverLatestSessionID(paths ...string) (string, bool, error) {
	var maxID string
	var found bool
	for _, path := range paths {
		id, ok, err := recoverLatestSessionIDForPath(path)
		if err != nil {
			return "", false, err
		}
		if ok && (!found || compareSessionID(id, maxID) > 0) {
			maxID = id
			found = true
		}
	}
	return maxID, found, nil
}

func recoverLatestSessionIDForPath(basePath string) (string, bool, error) {
	if basePath == "" {
		return "", false, nil
	}
	ext := filepath.Ext(basePath)
	dir := filepath.Dir(basePath)
	base := strings.TrimSuffix(filepath.Base(basePath), ext)
	matches, err := filepath.Glob(filepath.Join(dir, base+"_*"+ext))
	if err != nil {
		return "", false, err
	}
	var maxID string
	var found bool
	for _, match := range matches {
		id, ok := parseSessionIDFromLogPath(match, base, ext)
		if !ok {
			continue
		}
		if _, _, err := decodeSessionID(id); err != nil {
			continue
		}
		if !found || compareSessionID(id, maxID) > 0 {
			maxID = id
			found = true
		}
	}
	return maxID, found, nil
}

func compareSessionID(a, b string) int {
	aDate, aSeq, aErr := decodeSessionID(a)
	bDate, bSeq, bErr := decodeSessionID(b)
	if aErr != nil || bErr != nil {
		return strings.Compare(a, b)
	}
	if aDate != bDate {
		return strings.Compare(aDate, bDate)
	}
	switch {
	case aSeq > bSeq:
		return 1
	case aSeq < bSeq:
		return -1
	default:
		return 0
	}
}

func encodeSessionSuffix(seq int) (string, error) {
	if seq < 1 {
		return "", fmt.Errorf("session sequence out of range: %d", seq)
	}
	n := 0
	for i := 0; i < len(sessionSuffixAlphabet); i++ {
		for j := 0; j < len(sessionSuffixAlphabet); j++ {
			suffix := string([]byte{sessionSuffixAlphabet[i], sessionSuffixAlphabet[j]})
			if skipGeneratedSessionSuffix(suffix) {
				continue
			}
			n++
			if n == seq {
				return suffix, nil
			}
		}
	}
	return "", fmt.Errorf("%w: max %d per day", errSessionIDExhausted, maxSessionSequence)
}

func decodeSessionSuffix(suffix string) (int, error) {
	if len(suffix) != sessionSuffixWidth {
		return 0, fmt.Errorf("invalid session suffix length: %q", suffix)
	}
	if isLegacyDecimalSessionSuffix(suffix) {
		seq, err := strconv.Atoi(suffix)
		if err != nil || seq < 1 {
			return 0, fmt.Errorf("invalid legacy session suffix: %q", suffix)
		}
		return seq, nil
	}
	n := 0
	for i := 0; i < len(sessionSuffixAlphabet); i++ {
		for j := 0; j < len(sessionSuffixAlphabet); j++ {
			candidate := string([]byte{sessionSuffixAlphabet[i], sessionSuffixAlphabet[j]})
			if skipGeneratedSessionSuffix(candidate) {
				continue
			}
			n++
			if candidate == suffix {
				return n, nil
			}
		}
	}
	return 0, fmt.Errorf("invalid session suffix: %q", suffix)
}

func decodeSessionSequence(value string) (int, error) {
	if len(value) == sessionSuffixWidth {
		return decodeSessionSuffix(value)
	}
	seq, err := strconv.Atoi(value)
	if err != nil || seq < 1 {
		return 0, fmt.Errorf("invalid legacy session sequence: %q", value)
	}
	return seq, nil
}

func skipGeneratedSessionSuffix(suffix string) bool {
	return suffix == "00" || isLegacyDecimalSessionSuffix(suffix)
}

func isLegacyDecimalSessionSuffix(suffix string) bool {
	return len(suffix) == sessionSuffixWidth &&
		suffix[0] >= '1' && suffix[0] <= '9' &&
		suffix[1] >= '0' && suffix[1] <= '9'
}

func countSessionSuffixes() int {
	total := 0
	for i := 0; i < len(sessionSuffixAlphabet); i++ {
		for j := 0; j < len(sessionSuffixAlphabet); j++ {
			if !skipGeneratedSessionSuffix(string([]byte{sessionSuffixAlphabet[i], sessionSuffixAlphabet[j]})) {
				total++
			}
		}
	}
	return total
}

func parseSessionIDFromLogPath(path, base, ext string) (string, bool) {
	name := filepath.Base(path)
	if !strings.HasSuffix(name, ext) {
		return "", false
	}
	stem := strings.TrimSuffix(name, ext)
	prefix := base + "_"
	if !strings.HasPrefix(stem, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(stem, prefix)
	parts := strings.Split(rest, "_")
	if len(parts) < 2 {
		return "", false
	}
	for _, candidate := range []string{parts[0], parts[len(parts)-1]} {
		if len(candidate) <= sessionDateWidth {
			continue
		}
		if _, _, err := decodeSessionID(candidate); err == nil {
			return candidate, true
		}
	}
	return "", false
}
