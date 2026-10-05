package synccore

import (
	"crypto/ed25519"
	"crypto/sha256"
	fixturecrypto "github.com/anyproto/any-sync/util/crypto"

	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func seed(t *testing.T) (*Engine, []Bundle, string) {
	t.Helper()
	bs, err := NewWorkspace(testConfig, []string{"User 1", "User 2"})
	must(t, err)
	e, err := Open(testConfig, bs[0], nil)
	must(t, err)
	rid := RandomID()
	must(t, e.Apply(Operation{ID: RandomID(), Record: rid, Kind: "list", Field: "name", Value: "Weekly", Replaces: []string{}}))
	return e, bs, rid
}
func field(t *testing.T, e *Engine, id, f string) Record {
	t.Helper()
	v, err := e.View()
	must(t, err)
	for _, r := range v.Records {
		if r.ID == id {
			return r
		}
	}
	t.Fatal("missing record")
	return Record{}
}
func TestOfflineConflictsAndResolution(t *testing.T) {
	a, bs, id := seed(t)
	b, err := Open(testConfig, bs[1], nil)
	must(t, err)
	must(t, b.Merge(a.Packet(), ""))
	original := field(t, a, id, "name").Fields["name"][0].ID
	must(t, a.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: "Alice", Replaces: []string{original}}))
	must(t, b.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: "Bob", Replaces: []string{original}}))
	must(t, a.Merge(b.Packet(), ""))
	must(t, b.Merge(a.Packet(), ""))
	r := field(t, a, id, "name")
	if !r.Conflict("name") {
		t.Fatal("lost conflict")
	}
	ids := []string{}
	for _, v := range r.Fields["name"] {
		ids = append(ids, v.ID)
	}
	must(t, b.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: "Resolved", Replaces: ids}))
	must(t, a.Merge(b.Packet(), ""))
	if field(t, a, id, "name").Conflict("name") {
		t.Fatal("not resolved")
	}
	must(t, a.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "notes", Value: "Independent", Replaces: []string{}}))
	must(t, b.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "archived", Value: "true", Replaces: []string{}}))
	must(t, a.Merge(b.Packet(), ""))
	if field(t, a, id, "notes").Value("notes") != "Independent" {
		t.Fatal("lost independent edit")
	}
}
func TestRetryAndInvalidWrites(t *testing.T) {
	e, _, id := seed(t)
	o := Operation{ID: RandomID(), Record: id, Kind: "list", Field: "notes", Value: "OK", Replaces: []string{}}
	must(t, e.Apply(o))
	must(t, e.Apply(o))
	o.Value = "Changed"
	if e.Apply(o) == nil {
		t.Fatal("accepted reused id")
	}
	o.ID = RandomID()
	o.Kind = "shop"
	if e.Apply(o) == nil {
		t.Fatal("accepted kind change")
	}
	o.Kind = "list"
	o.Field = "notes"
	o.Replaces = []string{"nonexistent-version"}
	if e.Apply(o) == nil {
		t.Fatal("accepted invalid replacement")
	}
}
func TestSQLiteOutboxPagingAndRestore(t *testing.T) {
	e, bs, id := seed(t)
	dir := t.TempDir()
	db, err := OpenDatabase(filepath.Join(dir, "state.sqlite"))
	must(t, err)
	must(t, db.Commit(e, map[string]any{"cursor": 0}, true, nil))
	if db.PendingCount() != 1 {
		t.Fatal("missing outbox")
	}
	ids, err := db.Pending()
	must(t, err)
	must(t, db.Commit(e, map[string]any{"cursor": 1}, false, ids))
	if db.PendingCount() != 0 {
		t.Fatal("ack not saved")
	}
	backup := filepath.Join(dir, "backup.sqlite")
	_, err = db.SQL.Exec("VACUUM INTO ?", backup)
	must(t, err)
	must(t, db.Close())
	db, err = OpenDatabase(backup)
	must(t, err)
	defer db.Close()
	st, err := db.Load()
	must(t, err)
	copy, err := Open(testConfig, bs[0], st)
	must(t, err)
	if field(t, copy, id, "name").Value("name") != "Weekly" {
		t.Fatal("bad restore")
	}
	page, err := db.Records(0)
	must(t, err)
	if len(page.Records) != 1 {
		t.Fatal("projection missing")
	}
	page, err = db.Records(page.Cursor)
	must(t, err)
	if len(page.Records) != 0 {
		t.Fatal("not incremental")
	}
}
func TestSignedPaginationAndLongHistory(t *testing.T) {
	e, bs, id := seed(t)
	n := 13000
	if os.Getenv("GROCERIES_STRESS") == "1" {
		n = 50000
	}
	last := field(t, e, id, "name").Fields["name"][0].ID
	for start := 0; start < n; start += 200 {
		batch := []Operation{}
		for i := start; i < start+200 && i < n; i++ {
			o := Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: fmt.Sprintf("Trip %d", i), Replaces: []string{last}}
			last = o.ID
			batch = append(batch, o)
		}
		must(t, e.ApplyBatch(batch))
	}
	b, err := Open(testConfig, bs[1], nil)
	must(t, err)
	var cursor uint64
	pages := 0
	for {
		p := e.Page(cursor)
		if len(p.Packet.Changes) > MaxBatch {
			t.Fatal("unbounded page")
		}
		must(t, b.Merge(p.Packet, ""))
		cursor = p.Cursor
		pages++
		if !p.More {
			break
		}
	}
	v, err := b.View()
	must(t, err)
	if v.Changes != n+1 {
		t.Fatalf("lost changes: %d", v.Changes)
	}
	if e.Page(cursor).More || len(e.Page(cursor).Packet.Changes) != 0 {
		t.Fatal("not incremental")
	}
	if n == 50000 && pages < 2 {
		t.Fatal("paging untested")
	}
}
func TestSignerAndTamperRejection(t *testing.T) {
	e, bs, id := seed(t)
	b, err := Open(testConfig, bs[1], nil)
	must(t, err)
	if b.Merge(e.Packet(), b.Keys.SignKey.GetPublic().Account()) == nil {
		t.Fatal("wrong signer accepted")
	}
	p := e.Packet()
	data, _ := json.Marshal(p)
	var corrupt Packet
	must(t, json.Unmarshal(data, &corrupt))
	corrupt.Changes[0].RawChange[20] ^= 1
	b, err = Open(testConfig, bs[1], nil)
	must(t, err)
	if b.Merge(corrupt, "") == nil {
		t.Fatal("tampering accepted")
	}
	_ = id
}

// This compatibility fixture uses publicly reproducible, test-only identities.
func fixtureIdentity(name string) fixturecrypto.PrivKey {
	seed := sha256.Sum256([]byte("sync-core public compatibility fixture v1/" + name))
	return fixturecrypto.NewEd25519PrivKey(ed25519.NewKeyFromSeed(seed[:]))
}

func restoreFixtureIdentities(t *testing.T, bundles []Bundle) {
	t.Helper()
	for index := range bundles {
		key, err := fixtureIdentity(bundles[index].User).Marshall()
		must(t, err)
		bundles[index].Key = key
	}
}
