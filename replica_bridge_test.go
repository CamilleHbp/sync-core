package synccore

import (
	"context"
	"reflect"
	"testing"
)

func TestBridgeSkipsKnownHistoryAndPreservesConcurrentAuthors(t *testing.T) {
	source, bundles, id := seed(t)
	native, err := source.Clone()
	must(t, err)
	replica, _, wire := attachTestReplica(t, native)
	validations := 0
	validate := replica.config.ValidateRecords
	replica.config.ValidateRecords = func(v View) error { validations++; return validate(v) }
	before := len(source.Store.Changes)
	same, err := replica.ReconcileLegacy(context.Background(), source, bundles)
	must(t, err)
	if same != source || validations != 0 || wire.broadcasts != 0 {
		t.Fatal("unchanged reconciliation rebuilt history")
	}
	must(t, replica.Apply(context.Background(), []Operation{{ID: RandomID(), Record: id, Kind: "list", Field: "notes", Value: "Native", Replaces: []string{}}}))
	old, err := Open(testConfig, bundles[1], source.Store)
	must(t, err)
	must(t, old.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "notes", Value: "Legacy", Replaces: []string{}}))
	merged, err := replica.ReconcileLegacy(context.Background(), old, bundles)
	must(t, err)
	if len(source.Store.Changes) != before+1 {
		t.Fatal("bridge mutated source history")
	}
	legacyView, err := merged.View()
	must(t, err)
	nativeView, err := replica.View()
	must(t, err)
	if !reflect.DeepEqual(legacyView.Records, nativeView.Records) {
		t.Fatal("bridge lost values or authors")
	}
	if !field(t, merged, id, "notes").Conflict("notes") {
		t.Fatal("lost concurrent edit")
	}
	validations = 0
	same, err = replica.ReconcileLegacy(context.Background(), merged, bundles)
	must(t, err)
	if same != merged || validations != 0 {
		t.Fatal("replay revalidated all records")
	}
}

func TestKnownTransactionRejectsReusedIDsAndAuthors(t *testing.T) {
	op := Operation{ID: RandomID(), Record: RandomID(), Kind: "list", Field: "name", Value: "Original"}
	ops := map[string]Operation{op.ID: op}
	authors := map[string]string{op.ID: "Owner"}
	for _, changed := range []bool{false, true} {
		candidate := op
		author := "Other"
		if changed {
			candidate.Value = "Changed"
			author = "Owner"
		}
		if _, err := knownTransaction(Transaction{Operations: []Operation{{ID: RandomID()}, candidate}}, ops, authors, author); err == nil {
			t.Fatal("accepted reused ID in mixed batch")
		}
	}
}
