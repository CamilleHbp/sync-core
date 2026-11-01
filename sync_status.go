package synccore

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

type SyncStatus struct {
	Checked       bool  `json:"checked"`
	Confirmed     bool  `json:"confirmed"`
	Supported     bool  `json:"supported"`
	LastAttemptAt int64 `json:"lastAttemptAt"`
	LastContactAt int64 `json:"lastContactAt"`
	LastSuccessAt int64 `json:"lastSuccessAt"`
}

// SyncTracker is transport-neutral. Persisted timestamps are history, not proof
// after restart. UI consumers should also bound freshness and account for their
// application outbox before reporting "up to date".
type SyncTracker struct {
	mu         sync.Mutex
	status     SyncStatus
	generation uint64
	proof      string
	path       string
}

func NewSyncTracker(path string) *SyncTracker {
	tracker := &SyncTracker{path: path}
	if raw, err := os.ReadFile(path); err == nil {
		var saved SyncStatus
		if json.Unmarshal(raw, &saved) == nil {
			tracker.status.LastContactAt = saved.LastContactAt
			tracker.status.LastSuccessAt = saved.LastSuccessAt
		}
	}
	return tracker
}
func (t *SyncTracker) Invalidate() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.generation++
	t.status.Confirmed = false
}
func (t *SyncTracker) Begin() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.LastAttemptAt = time.Now().UnixMilli()
	return t.generation
}
func (t *SyncTracker) Complete(view ClientView, generation uint64, confirmed, supported bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Checked = true
	t.status.Supported = supported
	t.status.Confirmed = err == nil && view.Authenticated && confirmed && generation == t.generation
	if err == nil && view.Authenticated {
		t.status.LastContactAt = time.Now().UnixMilli()
	}
	if t.status.Confirmed {
		t.status.LastSuccessAt = t.status.LastContactAt
		t.proof = proofFor(view)
	}
	if t.path != "" {
		_ = AtomicJSON(t.path, t.status)
	}
}
func (t *SyncTracker) Snapshot(view ClientView) SyncStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := t.status
	result.Checked = true
	result.Confirmed = result.Confirmed && view.Authenticated && proofFor(view) == t.proof
	return result
}
func proofFor(view ClientView) string {
	return view.Origin + "\n" + view.User + "\n" + view.Workspace + "\n" + RecordsDigest(view.Records)
}

const DefaultConfirmationAge = 2 * time.Minute
const DefaultCheckInterval = time.Minute

// Fresh rejects future timestamps as well as stale proof after a clock change.
func (s SyncStatus) Fresh(now time.Time, maxAge time.Duration) bool {
	age := now.UnixMilli() - s.LastSuccessAt
	return s.Checked && s.Confirmed && s.LastSuccessAt > 0 && age >= 0 && maxAge >= 0 && age <= maxAge.Milliseconds()
}
