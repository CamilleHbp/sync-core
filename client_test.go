package synccore

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func taskConfig(namespace string) Config {
	return Config{Namespace: namespace, Platforms: []string{"android", "macos", "windows", "linux"}, ValidateOperation: func(o Operation) error {
		if o.Kind != "task" || (o.Field != "title" && o.Field != "done") {
			return errors.New("invalid task field")
		}
		return nil
	}}
}
func TestIndependentNamespacesMembershipAndDesktopSources(t *testing.T) {
	for _, count := range []int{1, 3} {
		config := taskConfig("example.tasks.v1")
		names := []string{"Owner", "Editor", "Another editor"}[:count]
		bundles, err := NewWorkspace(config, names)
		must(t, err)
		for _, bundle := range bundles {
			e, err := Open(config, bundle, nil)
			must(t, err)
			must(t, e.Apply(Operation{ID: RandomID(), Record: RandomID(), Kind: "task", Field: "title", Value: "Desktop", Source: &Source{ID: RandomID(), Name: "Mac", Platform: "macos"}}))
			if _, err = Open(taskConfig("example.other.v1"), bundle, nil); ErrorCode(err) != "namespace_mismatch" {
				t.Fatal("cross-app bundle accepted", err)
			}
			if e.Apply(Operation{ID: RandomID(), Record: RandomID(), Kind: "product", Field: "name", Value: "Wrong schema"}) == nil {
				t.Fatal("application validation ignored")
			}
		}
	}
}
func TestGroceriesSignedFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/groceries-v1.json")
	must(t, err)
	var fixture struct {
		Bundles []Bundle `json:"bundles"`
		Packet  Packet   `json:"packet"`
		View    View     `json:"view"`
	}
	must(t, json.Unmarshal(raw, &fixture))
	restoreFixtureIdentities(t, fixture.Bundles)
	e, err := Open(testConfig, fixture.Bundles[0], nil)
	must(t, err)
	must(t, e.Merge(fixture.Packet, ""))
	actual, err := e.View()
	must(t, err)
	if !reflect.DeepEqual(actual, fixture.View) {
		t.Fatal("legacy projection changed")
	}
}

type testGateway struct {
	mu         sync.Mutex
	db         *Database
	e          *Engine
	bundle     Bundle
	config     Config
	beforeSync func()
}

func gateway(t *testing.T, config Config) (*testGateway, *httptest.Server) {
	t.Helper()
	bundles, err := NewWorkspace(config, []string{"Owner"})
	must(t, err)
	e, err := Open(config, bundles[0], nil)
	must(t, err)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "server.sqlite"))
	must(t, err)
	must(t, db.Commit(e, nil, false, nil))
	t.Cleanup(func() { db.Close() })
	g := &testGateway{db: db, e: e, bundle: bundles[0], config: config}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Sync-Namespace") != config.Namespace {
			w.WriteHeader(403)
			return
		}
		if r.URL.Path == "/api/v1/login" {
			var in struct {
				Code string `json:"code"`
			}
			json.NewDecoder(r.Body).Decode(&in)
			if in.Code != "synthetic-test-code" {
				w.WriteHeader(401)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"token": "test-token", "bundle": g.bundle})
			return
		}
		if r.URL.Path != "/api/v1/sync" || r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(401)
			return
		}
		if g.beforeSync != nil {
			g.beforeSync()
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		var in SyncRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			w.WriteHeader(400)
			return
		}
		next, out, err := Exchange(g.e, g.db, in)
		if err != nil {
			w.WriteHeader(409)
			json.NewEncoder(w).Encode(map[string]string{"code": ErrorCode(err)})
			return
		}
		g.e = next
		json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(server.Close)
	return g, server
}
func TestClientOfflineRestartAndIdempotentAcknowledgement(t *testing.T) {
	config := taskConfig("example.tasks.v1")
	_, server := gateway(t, config)
	ctx := context.Background()
	directory := t.TempDir()
	a, err := NewClient(directory, config)
	must(t, err)
	must(t, a.Connect(ctx, server.URL, "synthetic-test-code"))
	b, err := NewClient(t.TempDir(), config)
	must(t, err)
	defer b.Close()
	must(t, b.Connect(ctx, server.URL, "synthetic-test-code"))
	first := Operation{ID: RandomID(), Record: RandomID(), Kind: "task", Field: "title", Value: "From Android"}
	must(t, a.Apply([]Operation{first}))
	must(t, a.Close())
	a, err = NewClient(directory, config)
	must(t, err)
	defer a.Close()
	view, err := a.View()
	must(t, err)
	if view.Pending != 1 {
		t.Fatal("lost pending edit")
	}
	must(t, a.Apply([]Operation{first}))
	must(t, a.Sync(ctx))
	must(t, b.Sync(ctx))
	view, err = b.View()
	must(t, err)
	if view.Records[0].Value("title") != "From Android" {
		t.Fatal("lost task")
	}
	left := Operation{ID: RandomID(), Record: first.Record, Kind: "task", Field: "title", Value: "Phone", Replaces: []string{first.ID}}
	right := left
	right.ID = RandomID()
	right.Value = "Mac"
	must(t, a.Apply([]Operation{left}))
	must(t, b.Apply([]Operation{right}))
	must(t, b.Sync(ctx))
	must(t, a.Sync(ctx))
	must(t, b.Sync(ctx))
	view, err = b.View()
	must(t, err)
	if !view.Records[0].Conflict("title") || view.Pending != 0 {
		t.Fatal("conflict or acknowledgement lost")
	}
	if err = a.Connect(ctx, server.URL, "bad-code"); ErrorCode(err) != "sign_in_required" {
		t.Fatal(err)
	}
	view, err = a.View()
	must(t, err)
	if !view.Authenticated || !view.Records[0].Conflict("title") {
		t.Fatal("reauthentication lost replica")
	}
}
func TestEditDuringNetworkAndCommitFailure(t *testing.T) {
	config := taskConfig("example.tasks.v1")
	g, server := gateway(t, config)
	ctx := context.Background()
	c, err := NewClient(t.TempDir(), config)
	must(t, err)
	defer c.Close()
	must(t, c.Connect(ctx, server.URL, "synthetic-test-code"))
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	g.beforeSync = func() { once.Do(func() { close(entered); <-resume }) }
	result := make(chan error, 1)
	go func() { result <- c.Sync(ctx) }()
	<-entered
	op := Operation{ID: RandomID(), Record: RandomID(), Kind: "task", Field: "title", Value: "During HTTP"}
	must(t, c.Apply([]Operation{op}))
	close(resume)
	must(t, <-result)
	view, err := c.View()
	must(t, err)
	if view.Pending != 0 || view.Records[0].Value("title") != op.Value {
		t.Fatal("concurrent local edit lost")
	}
	_, err = c.db.SQL.Exec("CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT, 'disk failure'); END")
	must(t, err)
	op.ID = RandomID()
	op.Value = "Must not commit"
	if c.Apply([]Operation{op}) == nil {
		t.Fatal("failed write acknowledged")
	}
	view, err = c.View()
	must(t, err)
	if view.Records[0].Value("title") == op.Value {
		t.Fatal("failed write changed memory")
	}
}
func TestTransportRejectsInsecureOrigins(t *testing.T) {
	for _, origin := range []string{"http://example.com", "https://user:password@example.com", "https://example.com?token=secret", "file:///tmp/data"} {
		if _, err := validOrigin(origin); err == nil {
			t.Fatal("accepted", origin)
		}
	}
}
