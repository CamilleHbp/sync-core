package privatenetwork

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	core "github.com/CamilleHbp/sync-core"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/util/crypto"
)

func TestPrivateLegacyMigration(t *testing.T) {
	data, networkID := privateTestConfig(t)
	config := core.Config{Namespace: "sync-core.private-migration-test", Platforms: []string{"desktop"}, ValidateOperation: func(core.Operation) error { return nil }}
	bundles, err := core.NewWorkspace(config, []string{"Owner", "Writer"})
	if err != nil {
		t.Fatal(err)
	}
	source, err := core.Open(config, bundles[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := core.Open(config, bundles[1], nil)
	if err != nil {
		t.Fatal(err)
	}
	record, original := core.RandomID(), core.RandomID()
	if err = source.Apply(core.Operation{ID: original, Record: record, Kind: "task", Field: "title", Value: "Original"}); err != nil {
		t.Fatal(err)
	}
	if err = writer.Merge(source.Packet(), ""); err != nil {
		t.Fatal(err)
	}
	for i, e := range []*core.Engine{source, writer} {
		if err = e.Apply(core.Operation{ID: core.RandomID(), Record: record, Kind: "task", Field: "title", Value: []string{"Owner's offline edit", "Writer's offline edit"}[i], Replaces: []string{original}, Source: &core.Source{ID: core.RandomID(), Name: "Original desktop", Platform: "desktop"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = source.Merge(writer.Packet(), ""); err != nil {
		t.Fatal(err)
	}
	// Exceed the old exchange page size with individually signed pending writes.
	pending := []string{}
	for i := 0; i < 205; i++ {
		if err = source.Apply(core.Operation{ID: core.RandomID(), Record: core.RandomID(), Kind: "task", Field: "title", Value: "Pending offline task"}); err != nil {
			t.Fatal(err)
		}
		pending = append(pending, source.Tree.Heads()[0])
	}
	expected, err := source.View()
	if err != nil {
		t.Fatal(err)
	}
	originalPacket, err := json.Marshal(source.Packet())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	envelope, err := core.MigrationSpace(config, bundles[0], 1)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := core.StageMigration(ctx, filepath.Join(t.TempDir(), "archive"), source, envelope, pending)
	if err != nil {
		t.Fatal(err)
	}
	if len(archive.Pending) != 205 {
		t.Fatal("archive lost pending writes")
	}
	ownerPeer, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	writerPeer, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	ownerKeys := accountdata.New(ownerPeer.PeerKey, source.Keys.SignKey)
	writerKeys := accountdata.New(writerPeer.PeerKey, writer.Keys.SignKey)
	r1, h1 := startHost(t, data, networkID, t.TempDir(), ownerKeys)
	defer closeHost(t, r1, h1)
	r2, h2 := startHost(t, data, networkID, t.TempDir(), writerKeys)
	defer closeHost(t, r2, h2)
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	metadata, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	sp, err := h1.CreateSpace(ctx, spacepayloads.SpaceCreatePayload{SigningKey: ownerKeys.SignKey, MasterKey: master, MetadataKey: metadata, ReadKey: crypto.NewAES(), SpacePayload: []byte(config.Namespace)})
	if err != nil {
		t.Fatal(err)
	}
	if err = sp.SyncHeads(ctx); err != nil {
		t.Fatal(err)
	}
	if err = r1.Coordinator.SpaceMakeShareable(ctx, sp.Id()); err != nil {
		t.Fatal(err)
	}
	if err = sp.AclClient().AddAccounts(ctx, list.AccountsAddPayload{Additions: []list.AccountAdd{{Identity: writerKeys.SignKey.GetPublic(), Permissions: list.AclPermissionsWriter}}}); err != nil {
		t.Fatal(err)
	}
	empty, err := sp.TreeBuilder().CreateTree(ctx, objecttree.ObjectTreeCreatePayload{PrivKey: ownerKeys.SignKey, ChangeType: config.Namespace, SpaceId: sp.Id(), IsEncrypted: true, Seed: []byte(core.RandomID()), Timestamp: time.Now().Unix()})
	if err != nil {
		t.Fatal(err)
	}
	sp.Acl().RLock()
	records, err := sp.Acl().RecordsAfter(ctx, "")
	sp.Acl().RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	replay, err := core.ReplayMigration(ctx, source, empty.RootRawChange, records, networkID, map[string]crypto.PrivKey{ownerKeys.SignKey.GetPublic().Account(): ownerKeys.SignKey, writerKeys.SignKey.GetPublic().Account(): writerKeys.SignKey})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range pending {
		if replay.Changes[id] == "" {
			t.Fatal("unmapped pending write")
		}
	}
	tree1, err := h1.PutTree(ctx, sp.Id(), replay.Payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err := core.AttachReplica(config, tree1.(synctree.SyncTree), ownerKeys, "Owner", bundles[0].Members)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := a.View()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatal("owner's migrated state differs")
	}
	if err = sp.SyncHeads(ctx); err != nil {
		t.Fatal(err)
	}
	sp2, err := h2.OpenSpace(ctx, sp.Id())
	if err != nil {
		t.Fatal(err)
	}
	tree2, err := h2.GetTree(ctx, sp.Id(), tree1.Id())
	if err != nil {
		t.Fatal(err)
	}
	b, err := core.AttachReplica(config, tree2.(synctree.SyncTree), writerKeys, "Writer", bundles[1].Members)
	if err != nil {
		t.Fatal(err)
	}
	if err = sp2.SyncHeads(ctx); err != nil {
		t.Fatal(err)
	}
	expected.User = "Writer"
	actual, err = b.View()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatal("remote migration lost conflicts, authors, sources, or operations")
	}
	// Continue normal live editing after migration, resolving both old versions.
	var replacements []string
	for _, r := range actual.Records {
		if r.ID == record {
			if !r.Conflict("title") {
				t.Fatal("lost conflict")
			}
			for _, v := range r.Fields["title"] {
				replacements = append(replacements, v.ID)
			}
		}
	}
	if err = b.Apply(ctx, []core.Operation{{ID: core.RandomID(), Record: record, Kind: "task", Field: "title", Value: "Resolved after migration", Replaces: replacements}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		view, err := a.View()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range view.Records {
			if r.ID == record && r.Value("title") == "Resolved after migration" && !r.Conflict("title") {
				found = true
			}
		}
		if found {
			break
		}
		select {
		case <-a.Changes():
		case <-deadline.C:
			t.Fatal("no live resolution after migration")
		}
	}
	unchanged, err := json.Marshal(source.Packet())
	if err != nil {
		t.Fatal(err)
	}
	if string(originalPacket) != string(unchanged) || len(archive.Pending) != 205 {
		t.Fatal("mutated legacy rollback state")
	}
	// Old installations can return with signed offline edits while native peers
	// continue writing. The gateway must preserve both authors and transactions.
	legacy, err := source.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if err = legacy.Apply(core.Operation{ID: core.RandomID(), Record: core.RandomID(), Kind: "task", Field: "title", Value: "Late legacy edit"}); err != nil {
		t.Fatal(err)
	}
	merged, err := a.ReconcileLegacy(ctx, legacy, bundles)
	if err != nil {
		t.Fatal(err)
	}
	nativeView, err := a.View()
	if err != nil {
		t.Fatal(err)
	}
	legacyView, err := merged.View()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nativeView, legacyView) {
		t.Fatal("gateway projections differ")
	}
	again, err := a.ReconcileLegacy(ctx, merged, bundles)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Store.Changes) != len(merged.Store.Changes) {
		t.Fatal("gateway replay duplicated history")
	}
}
