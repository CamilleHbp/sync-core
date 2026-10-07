package synccore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMigrationPreservesHistoryAndConflicts(t *testing.T) {
	a, bundles, id := seed(t)
	b, err := Open(testConfig, bundles[1], nil)
	must(t, err)
	must(t, b.Merge(a.Packet(), ""))
	original := field(t, a, id, "name").Fields["name"][0].ID
	for i, e := range []*Engine{a, b} {
		must(t, e.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: []string{"first", "second"}[i], Replaces: []string{original}}))
	}
	must(t, a.Merge(b.Packet(), ""))
	space, err := MigrationSpace(testConfig, bundles[0], 1)
	must(t, err)
	again, err := MigrationSpace(testConfig, bundles[0], 1)
	must(t, err)
	if !reflect.DeepEqual(space, again) {
		t.Fatal("envelope is not reproducible")
	}
	pending := append([]string{}, a.Tree.Heads()...)
	destination := filepath.Join(t.TempDir(), "native")
	manifest, err := StageMigration(context.Background(), destination, a, space, pending)
	must(t, err)
	if manifest.Status != "local-only" {
		t.Fatal("staging must not imply network admission")
	}
	if manifest.TreeID != a.Tree.Id() || !reflect.DeepEqual(manifest.Pending, pending) {
		t.Fatal("lost tree identity or pending writes")
	}
	if !field(t, a, id, "name").Conflict("name") {
		t.Fatal("changed source")
	}
	_, err = StageMigration(context.Background(), destination, a, space, pending)
	if !os.IsExist(err) {
		t.Fatalf("must not replace an existing replica: %v", err)
	}
	// The same public envelope must also import a non-owner's offline branch.
	_, err = StageMigration(context.Background(), filepath.Join(t.TempDir(), "native"), b, space, b.Tree.Heads())
	must(t, err)
}

func TestMigrationRejectsWrongOwnerAndCancelledCopy(t *testing.T) {
	a, bundles, _ := seed(t)
	if _, err := MigrationSpace(testConfig, bundles[1], 1); err == nil {
		t.Fatal("writer created owner envelope")
	}
	space, err := MigrationSpace(testConfig, bundles[0], 1)
	must(t, err)
	dir := filepath.Join(t.TempDir(), "native")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = StageMigration(ctx, dir, a, space, nil); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err = os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("published cancelled migration: %v", err)
	}
	if _, err = StageMigration(context.Background(), dir, a, space, []string{"missing"}); err == nil {
		t.Fatal("accepted missing pending write")
	}
}

func TestStageGroceriesSignedFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/groceries-v1.json")
	must(t, err)
	var fixture struct {
		Bundles []Bundle
		Packet  Packet
		View    View
	}
	must(t, json.Unmarshal(raw, &fixture))
	restoreFixtureIdentities(t, fixture.Bundles)
	space, err := MigrationSpace(testConfig, fixture.Bundles[0], 1)
	must(t, err)
	for _, bundle := range fixture.Bundles {
		e, err := Open(testConfig, bundle, nil)
		must(t, err)
		must(t, e.Merge(fixture.Packet, ""))
		destination := filepath.Join(t.TempDir(), "native")
		manifest, err := StageMigration(context.Background(), destination, e, space, nil)
		must(t, err)
		var saved MigrationManifest
		data, err := os.ReadFile(filepath.Join(destination, "replica", "manifest.json"))
		must(t, err)
		must(t, json.Unmarshal(data, &saved))
		if !reflect.DeepEqual(saved, manifest) {
			t.Fatal("manifest did not round trip")
		}
	}
}

func TestClientStagePreservesAllPendingChanges(t *testing.T) {
	config := taskConfig("example.tasks.v1")
	gateway, server := gateway(t, config)
	defer server.Close()
	client, err := NewClient(t.TempDir(), config)
	must(t, err)
	defer client.Close()
	ctx := context.Background()
	must(t, client.Connect(ctx, server.URL, "synthetic-test-code"))
	// More than a bounded transport page. Every pending signed change must
	// survive, even though staging must not acknowledge any of them.
	for i := 0; i < 205; i++ {
		must(t, client.Apply([]Operation{{ID: RandomID(), Record: RandomID(), Kind: "task", Field: "title", Value: fmt.Sprintf("offline %d", i)}}))
	}
	before, err := client.View()
	must(t, err)
	space, err := MigrationSpace(config, gateway.bundle, 1)
	must(t, err)
	manifest, err := client.StageMigration(ctx, filepath.Join(t.TempDir(), "native"), space)
	must(t, err)
	if len(manifest.Pending) != 205 {
		t.Fatalf("lost pending changes: %d", len(manifest.Pending))
	}
	after, err := client.View()
	must(t, err)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("staging mutated active replica")
	}
}

func TestStageRejectsOtherWorkspaceAndExistingDirectory(t *testing.T) {
	source, _, _ := seed(t)
	_, other, _ := seed(t)
	space, err := MigrationSpace(testConfig, other[0], 1)
	must(t, err)
	dir := filepath.Join(t.TempDir(), "native")
	if _, err = StageMigration(context.Background(), dir, source, space, nil); err == nil {
		t.Fatal("accepted foreign workspace")
	}
	if _, err = os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("published invalid workspace")
	}
	space, err = MigrationSpace(testConfig, source.Bundle, 1)
	must(t, err)
	must(t, os.Mkdir(dir, 0700))
	if _, err = StageMigration(context.Background(), dir, source, space, nil); !errors.Is(err, os.ErrExist) {
		t.Fatalf("replaced existing empty directory: %v", err)
	}
}
