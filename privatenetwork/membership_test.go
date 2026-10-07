package privatenetwork

import (
	"context"
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

// Unlike the two-device test, these peers have different account signing keys.
// Admission uses the private nodes/consensus service, never an unsigned local ACL.
func TestPrivateMembership(t *testing.T) {
	data, networkID := privateTestConfig(t)
	owner, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	writer, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	outsider, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	r1, h1 := startHost(t, data, networkID, t.TempDir(), owner)
	defer closeHost(t, r1, h1)
	r2, h2 := startHost(t, data, networkID, t.TempDir(), writer)
	defer closeHost(t, r2, h2)
	r3, h3 := startHost(t, data, networkID, t.TempDir(), outsider)
	defer closeHost(t, r3, h3)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	metadata, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	config := core.Config{Namespace: "sync-core.membership-test", Platforms: []string{"desktop"}, ValidateOperation: func(core.Operation) error { return nil }}
	sp, err := h1.CreateSpace(ctx, spacepayloads.SpaceCreatePayload{SigningKey: owner.SignKey, MasterKey: master, MetadataKey: metadata, ReadKey: crypto.NewAES(), SpacePayload: []byte(config.Namespace)})
	if err != nil {
		t.Fatal(err)
	}
	if err = sp.SyncHeads(ctx); err != nil {
		t.Fatal(err)
	}
	if err = r1.Coordinator.SpaceMakeShareable(ctx, sp.Id()); err != nil {
		t.Fatal(err)
	}
	err = sp.AclClient().AddAccounts(ctx, list.AccountsAddPayload{Additions: []list.AccountAdd{{Identity: writer.SignKey.GetPublic(), Permissions: list.AclPermissionsWriter, Metadata: []byte("Writer")}}})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := sp.TreeBuilder().CreateTree(ctx, objecttree.ObjectTreeCreatePayload{PrivKey: owner.SignKey, SpaceId: sp.Id(), ChangeType: config.Namespace, IsEncrypted: true, Seed: []byte(core.RandomID()), Timestamp: time.Now().Unix()})
	if err != nil {
		t.Fatal(err)
	}
	tree1, err := h1.PutTree(ctx, sp.Id(), payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	members := map[string]string{owner.SignKey.GetPublic().Account(): "Owner", writer.SignKey.GetPublic().Account(): "Writer"}
	a, err := core.AttachReplica(config, tree1.(synctree.SyncTree), owner, "Owner", members)
	if err != nil {
		t.Fatal(err)
	}
	record := core.RandomID()
	initial := core.Operation{ID: core.RandomID(), Record: record, Kind: "task", Field: "title", Value: "shared"}
	if err = a.Apply(ctx, []core.Operation{initial}); err != nil {
		t.Fatal(err)
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
	b, err := core.AttachReplica(config, tree2.(synctree.SyncTree), writer, "Writer", members)
	if err != nil {
		t.Fatal(err)
	}
	if err = sp2.SyncHeads(ctx); err != nil {
		t.Fatal(err)
	}
	reply := core.Operation{ID: core.RandomID(), Record: record, Kind: "task", Field: "notes", Value: "writer reply"}
	if err = b.Apply(ctx, []core.Operation{reply}); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		view, err := a.View()
		if err != nil {
			t.Fatal(err)
		}
		if len(view.Records) == 1 && view.Records[0].Value("notes") == reply.Value {
			if view.Records[0].Fields["notes"][0].Author != "Writer" {
				t.Fatal("wrong writer attribution")
			}
			break
		}
		select {
		case <-a.Changes():
		case <-deadline.C:
			t.Fatal("no independent writer push")
		}
	}
	if err = sp2.AclClient().AddAccounts(ctx, list.AccountsAddPayload{Additions: []list.AccountAdd{{Identity: outsider.SignKey.GetPublic(), Permissions: list.AclPermissionsWriter}}}); err == nil {
		t.Fatal("writer escalated another account's permissions")
	}
	// An unaffiliated account must not obtain a usable application replica.
	if _, err = h3.OpenSpace(ctx, sp.Id()); err == nil {
		tr, err := h3.GetTree(ctx, sp.Id(), tree1.Id())
		if err == nil {
			foreign := map[string]string{owner.SignKey.GetPublic().Account(): "Owner", writer.SignKey.GetPublic().Account(): "Writer", outsider.SignKey.GetPublic().Account(): "Outsider"}
			if _, err = core.AttachReplica(config, tr.(synctree.SyncTree), outsider, "Outsider", foreign); err == nil {
				t.Fatal("outsider decrypted shared tree")
			}
		}
	}
}
