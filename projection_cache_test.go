package synccore

import (
	"reflect"
	"testing"
)

func TestProjectionCacheMatchesReplayAndIsIsolated(t *testing.T) {
	e, _, id := seed(t)
	for i := 0; i < 50; i++ {
		view, err := e.View()
		must(t, err)
		var replaces []string
		for _, r := range view.Records {
			if r.ID == id {
				for _, v := range r.Fields["notes"] {
					replaces = append(replaces, v.ID)
				}
			}
		}
		must(t, e.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "notes", Value: RandomID(), Replaces: replaces}))
		actual, err := e.View()
		must(t, err)
		ops, authors, err := e.Operations()
		must(t, err)
		expected := Materialize(ops, authors, e.Bundle.User)
		if !reflect.DeepEqual(actual, expected) {
			t.Fatal("incremental projection differs from replay")
		}
		if len(actual.Records) > 0 {
			actual.Records[0].Fields["name"] = nil
		}
		again, err := e.View()
		must(t, err)
		if !reflect.DeepEqual(again, expected) {
			t.Fatal("caller mutated cached projection")
		}
	}
}
