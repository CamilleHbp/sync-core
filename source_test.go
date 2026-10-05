package synccore

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestSameAccountDeviceSourcesSurviveSyncAndRestart(t *testing.T) {
	a, bundles, id := seed(t)
	b, err := Open(testConfig, bundles[0], nil)
	must(t, err)
	must(t, b.Merge(a.Packet(), ""))
	original := field(t, a, id, "name").Fields["name"][0].ID
	phone := &Source{ID: RandomID(), Name: "Fairphone 5", Platform: "android"}
	web := &Source{ID: RandomID(), Name: "camille-mbp", Platform: "web"}
	left := Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: "Phone choice", Replaces: []string{original}, Source: phone}
	right := Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: "Web choice", Replaces: []string{original}, Source: web}
	must(t, a.Apply(left))
	must(t, b.Apply(right))
	phone.Name = "Renamed later"
	if err := a.Apply(left); err == nil {
		t.Fatal("accepted an operation ID with changed source metadata")
	}
	must(t, a.Merge(b.Packet(), ""))
	must(t, b.Merge(a.Packet(), ""))
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	db, err := OpenDatabase(path)
	must(t, err)
	must(t, db.Commit(a, nil, true, nil))
	must(t, db.Close())
	db, err = OpenDatabase(path)
	must(t, err)
	defer db.Close()
	store, err := db.Load()
	must(t, err)
	restored, err := Open(testConfig, bundles[0], store)
	must(t, err)
	r := field(t, restored, id, "name")
	if !r.Conflict("name") || len(r.Fields["name"]) != 2 {
		t.Fatal("lost concurrent versions")
	}
	for _, version := range r.Fields["name"] {
		if version.Author != bundles[0].User || version.Source == nil {
			t.Fatal("lost account or device origin")
		}
		if version.ID == left.ID && (version.Source.Name != "Fairphone 5" || version.Source.Platform != "android") {
			t.Fatal("rewrote the original device name")
		}
		if version.ID == right.ID && (version.Source.Name != "camille-mbp" || version.Source.Platform != "web") {
			t.Fatal("lost browser origin")
		}
	}
	page, err := db.Records(0)
	must(t, err)
	if page.Records[0].Fields["name"][0].Source == nil {
		t.Fatal("lost source in persisted browser projection")
	}
}

func TestDeviceSourceValidationAndLegacyCompatibility(t *testing.T) {
	op := Operation{ID: RandomID(), Record: RandomID(), Kind: "list", Field: "name", Value: "Legacy", Replaces: []string{}}
	must(t, ValidateOperation(op))
	raw, err := json.Marshal(op)
	must(t, err)
	if strings.Contains(string(raw), "source") {
		t.Fatal("changed legacy operation serialization")
	}
	for _, source := range []Source{
		{ID: "short", Name: "Phone", Platform: "android"},
		{ID: RandomID(), Name: "", Platform: "web"},
		{ID: RandomID(), Name: "Phone\n", Platform: "android"},
		{ID: RandomID(), Name: strings.Repeat("x", 65), Platform: "web"},
		{ID: RandomID(), Name: "Phone", Platform: "invalid"},
	} {
		op.Source = &source
		if ValidateOperation(op) == nil {
			t.Fatalf("accepted invalid source: %q", source)
		}
	}
}
