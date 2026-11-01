package synccore

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestCheckpointCanonicalAndCausal(t *testing.T) {
	records := []Record{{ID: "b", Kind: "task", Fields: map[string][]Version{"title": {{ID: "2", Value: "B"}, {ID: "1", Value: "A"}}}}, {ID: "a", Kind: "tag", Fields: map[string][]Version{}}}
	before, _ := json.Marshal(records)
	ordered := []Record{records[1], {ID: "b", Kind: "task", Fields: map[string][]Version{"title": {{ID: "1", Value: "A"}, {ID: "2", Value: "B"}}}}}
	if RecordsDigest(records) != RecordsDigest(ordered) {
		t.Fatal("ordering changed checkpoint")
	}
	after, _ := json.Marshal(records)
	if string(before) != string(after) {
		t.Fatal("digest mutated records")
	}
	ordered[1].Fields["title"][0].ID = "new-causal-version"
	if RecordsDigest(records) == RecordsDigest(ordered) {
		t.Fatal("causal edit hidden by equal visible value")
	}
	if RecordsDigest(nil) != RecordsDigest([]Record{}) {
		t.Fatal("empty digests differ")
	}
}
func TestCheckpointAuthenticationAndReplayProtection(t *testing.T) {
	cfg := taskConfig("checkpoint.test")
	bundles, err := NewWorkspace(cfg, []string{"Owner"})
	must(t, err)
	engine, err := Open(cfg, bundles[0], nil)
	must(t, err)
	mode := "ok"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer synthetic" || r.Header.Get("X-Sync-Namespace") != cfg.Namespace {
			t.Error("missing authentication or namespace")
		}
		if mode == "old" {
			w.WriteHeader(404)
			return
		}
		if mode == "unauthorized" {
			w.WriteHeader(401)
			return
		}
		var request CheckpointRequest
		json.NewDecoder(r.Body).Decode(&request)
		response, err := NewCheckpoint(request, cfg.Namespace, bundles[0].Root.Id, nil)
		if err != nil {
			t.Error(err)
		}
		switch mode {
		case "replay":
			response.Nonce = RandomID()
		case "workspace":
			response.Workspace = "other"
		case "namespace":
			response.Namespace = "other"
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	client, err := NewClient(t.TempDir(), cfg)
	must(t, err)
	defer client.Close()
	client.engine = engine
	client.state = identity{Origin: server.URL, Token: "synthetic", Bundle: bundles[0]}
	response, supported, err := client.Checkpoint(context.Background())
	must(t, err)
	if !supported || response.Digest != RecordsDigest(nil) {
		t.Fatal("checkpoint was not confirmed")
	}
	for _, bad := range []string{"replay", "workspace", "namespace"} {
		mode = bad
		_, _, err = client.Checkpoint(context.Background())
		if ErrorCode(err) != "invalid_response" {
			t.Fatal(bad, err)
		}
	}
	mode = "old"
	_, supported, err = client.Checkpoint(context.Background())
	if err != nil || supported {
		t.Fatal("old server was treated as supported", err)
	}
	mode = "unauthorized"
	_, _, err = client.Checkpoint(context.Background())
	if ErrorCode(err) != "sign_in_required" {
		t.Fatal(err)
	}
}
func TestSyncTrackerInvalidationPersistenceAndFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "health.json")
	tracker := NewSyncTracker(path)
	view := ClientView{Authenticated: true, Origin: "https://example.test", User: "owner", Workspace: "workspace"}
	if tracker.Snapshot(view).Confirmed {
		t.Fatal("startup confirmation")
	}
	generation := tracker.Begin()
	tracker.Complete(view, generation, true, true, nil)
	confirmed := tracker.Snapshot(view)
	if !confirmed.Confirmed || confirmed.LastSuccessAt == 0 {
		t.Fatal(confirmed)
	}
	restored := NewSyncTracker(path).Snapshot(view)
	if restored.Confirmed || restored.LastSuccessAt != confirmed.LastSuccessAt {
		t.Fatal(restored)
	}
	generation = tracker.Begin()
	tracker.Invalidate()
	tracker.Complete(view, generation, true, true, nil)
	if tracker.Snapshot(view).Confirmed {
		t.Fatal("concurrent edit confirmed")
	}
	generation = tracker.Begin()
	tracker.Complete(view, generation, true, true, nil)
	other := view
	other.Workspace = "other"
	if tracker.Snapshot(other).Confirmed {
		t.Fatal("cross-workspace proof")
	}
	other = view
	other.Records = []Record{{ID: "changed"}}
	if tracker.Snapshot(other).Confirmed {
		t.Fatal("changed snapshot confirmed")
	}
	prior := tracker.Snapshot(view)
	tracker.Complete(view, tracker.Begin(), false, true, errors.New("offline"))
	failed := tracker.Snapshot(view)
	if failed.Confirmed || failed.LastSuccessAt != prior.LastSuccessAt {
		t.Fatal(failed)
	}
	tracker.Complete(view, tracker.Begin(), false, false, nil)
	if got := tracker.Snapshot(view); got.Confirmed || got.Supported {
		t.Fatal(got)
	}
	if !reflect.DeepEqual(view.Records, []Record(nil)) {
		t.Fatal("mutated view")
	}
}

func TestConfirmationFreshness(t *testing.T) {
	now := time.UnixMilli(500000)
	status := SyncStatus{Checked: true, Confirmed: true, LastSuccessAt: now.UnixMilli()}
	if !status.Fresh(now, DefaultConfirmationAge) || status.Fresh(now.Add(DefaultConfirmationAge+time.Millisecond), DefaultConfirmationAge) || status.Fresh(now.Add(-time.Millisecond), DefaultConfirmationAge) {
		t.Fatal("invalid freshness decision")
	}
}
