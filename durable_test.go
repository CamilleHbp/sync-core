package synccore

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestDurableAppendRollbackRestartAndMemberIdentity(t *testing.T) {
	e, bundles, id := seed(t)
	db, err := OpenDatabase(filepath.Join(t.TempDir(), "replica.sqlite"))
	must(t, err)
	defer db.Close()
	must(t, db.Commit(e, nil, false, nil))
	before, err := e.View()
	must(t, err)
	heads := append([]string{}, e.Tree.Heads()...)
	op := Operation{ID: RandomID(), Record: id, Kind: "list", Field: "notes", Value: "Durable", Replaces: []string{}}
	_, err = db.SQL.Exec("CREATE TRIGGER reject_projection BEFORE UPDATE ON records BEGIN SELECT RAISE(FAIL,'injected storage failure'); END")
	must(t, err)
	if err = db.ApplyBatchAs(e, bundles[1], []Operation{op}, true); err == nil {
		t.Fatal("expected failure")
	}
	actual, err := e.View()
	must(t, err)
	if !reflect.DeepEqual(before, actual) || !reflect.DeepEqual(heads, e.Tree.Heads()) || db.PendingCount() != 0 {
		t.Fatal("failed commit escaped")
	}
	_, err = db.SQL.Exec("DROP TRIGGER reject_projection")
	must(t, err)
	must(t, db.ApplyBatchAs(e, bundles[1], []Operation{op}, true))
	must(t, db.ApplyBatchAs(e, bundles[1], []Operation{op}, true))
	if db.PendingCount() != 1 {
		t.Fatal("duplicate append")
	}
	storage, err := db.Load()
	must(t, err)
	restarted, err := Open(e.config, bundles[0], storage)
	must(t, err)
	actual, err = e.View()
	must(t, err)
	restored, err := restarted.View()
	must(t, err)
	if !reflect.DeepEqual(actual, restored) {
		t.Fatal("restart differs")
	}
	ops, authors, err := restarted.Operations()
	must(t, err)
	if !reflect.DeepEqual(actual, Materialize(ops, authors, bundles[0].User)) || authors[op.ID] != bundles[1].User {
		t.Fatal("replay/identity differs")
	}
	foreign, err := NewWorkspace(e.config, []string{"Other 1", "Other 2"})
	must(t, err)
	if err = db.ApplyBatchAs(e, foreign[0], []Operation{op}, false); err == nil {
		t.Fatal("foreign identity accepted")
	}
}
