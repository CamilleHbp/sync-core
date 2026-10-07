package synccore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/cidutil"
	"github.com/anyproto/any-sync/util/crypto"
)

// Synthetic acceptance signatures for offline unit tests only. The independent
// integration test obtains these from the actual private consensus service.
func replayTarget(t *testing.T, source *Engine, bundles []Bundle) (*treechangeproto.RawTreeChangeWithId, []*consensusproto.RawRecordWithId, string, map[string]crypto.PrivKey) {
	t.Helper()
	network, public, err := crypto.GenerateRandomEd25519KeyPair()
	must(t, err)
	space, err := cidutil.NewCidFromBytes([]byte(RandomID()))
	must(t, err)
	acl, err := list.NewInMemoryDerivedAcl(space, source.Keys)
	must(t, err)
	records := []*consensusproto.RawRecordWithId{acl.Root()}
	signers := map[string]crypto.PrivKey{}
	additions := []list.AccountAdd{}
	for i, bundle := range bundles {
		key, err := crypto.UnmarshalEd25519PrivateKeyProto(bundle.Key)
		must(t, err)
		signers[key.GetPublic().Account()] = key
		if i > 0 {
			additions = append(additions, list.AccountAdd{Identity: key.GetPublic(), Permissions: list.AclPermissionsWriter})
		}
	}
	if len(additions) > 0 {
		rec, err := acl.RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: additions})
		must(t, err)
		rec.AcceptorIdentity, err = public.Marshall()
		must(t, err)
		rec.AcceptorSignature, err = network.Sign(rec.Payload)
		must(t, err)
		raw, err := rec.MarshalVT()
		must(t, err)
		id, err := cidutil.NewCidFromBytes(raw)
		must(t, err)
		accepted := &consensusproto.RawRecordWithId{Id: id, Payload: raw}
		must(t, acl.AddRawRecord(accepted))
		records = append(records, accepted)
	}
	_, root, err := objecttree.NewChangeBuilder(crypto.NewKeyStorage(), nil).BuildRoot(objecttree.InitialContent{PrivKey: source.Keys.SignKey, AclHeadId: acl.Head().Id, SpaceId: space, ChangeType: source.config.Namespace, Seed: []byte(RandomID())})
	must(t, err)
	return root, records, public.Network(), signers
}

func TestReplayMigrationPreservesAuthorsConflictsAndSources(t *testing.T) {
	source, bundles, id := seed(t)
	writer, err := Open(testConfig, bundles[1], nil)
	must(t, err)
	must(t, writer.Merge(source.Packet(), ""))
	original := field(t, source, id, "name").Fields["name"][0].ID
	for i, e := range []*Engine{source, writer} {
		must(t, e.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: []string{"Left", "Right"}[i], Replaces: []string{original}, Source: &Source{ID: RandomID(), Name: "Original installation", Platform: "web"}}))
	}
	must(t, source.Merge(writer.Packet(), ""))
	before, err := source.View()
	must(t, err)
	packetBefore, err := json.Marshal(source.Packet())
	must(t, err)
	root, records, network, signers := replayTarget(t, source, bundles)
	result, err := ReplayMigration(context.Background(), source, root, records, network, signers)
	must(t, err)
	if len(result.Changes) != len(source.Packet().Changes) || !reflect.DeepEqual(result.SourceHeads, source.Tree.Heads()) {
		t.Fatal("missing history mapping")
	}
	for old, next := range result.Changes {
		if old == next || next == root.Id {
			t.Fatal("history was not newly signed")
		}
	}
	ast, err := list.NewInMemoryStorage(records[0].Id, records)
	must(t, err)
	networkKey, err := crypto.DecodeNetworkId(network)
	must(t, err)
	acl, err := list.BuildAclListWithIdentity(source.Keys, ast, recordverifier.New(networkKey))
	must(t, err)
	st := NewStorage(root)
	tree, err := objecttree.BuildObjectTree(st, acl)
	must(t, err)
	tree.Lock()
	_, err = tree.AddRawChanges(context.Background(), objecttree.RawChangesPayload{NewHeads: result.Payload.Heads, RawChanges: result.Payload.Changes})
	tree.Unlock()
	must(t, err)
	defer tree.Close()
	candidate := &Engine{config: testConfig, Bundle: source.Bundle, Store: st, Tree: tree, Keys: source.Keys}
	after, err := candidate.View()
	must(t, err)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("changed migration projection")
	}
	packetAfter, err := json.Marshal(source.Packet())
	must(t, err)
	if string(packetBefore) != string(packetAfter) {
		t.Fatal("mutated original signed history")
	}
}

func TestReplayMigrationFailsClosed(t *testing.T) {
	source, bundles, id := seed(t)
	writer, err := Open(testConfig, bundles[1], nil)
	must(t, err)
	must(t, writer.Merge(source.Packet(), ""))
	must(t, writer.Apply(Operation{ID: RandomID(), Record: id, Kind: "list", Field: "notes", Value: "Writer"}))
	must(t, source.Merge(writer.Packet(), ""))
	root, records, network, signers := replayTarget(t, source, bundles)
	assertRejected := func(ctx context.Context, r *treechangeproto.RawTreeChangeWithId, acl []*consensusproto.RawRecordWithId, n string, keys map[string]crypto.PrivKey) {
		t.Helper()
		candidate, err := ReplayMigration(ctx, source, r, acl, n, keys)
		if err == nil || candidate.Payload.RootRawChange != nil || len(candidate.Changes) > 0 {
			t.Fatal("returned partial or unauthorized migration")
		}
	}
	assertRejected(context.Background(), root, records, network, map[string]crypto.PrivKey{source.Keys.SignKey.GetPublic().Account(): source.Keys.SignKey})
	assertRejected(context.Background(), root, source.Bundle.ACL, network, signers)
	assertRejected(context.Background(), root, records, "", signers)
	assertRejected(context.Background(), source.Bundle.Root, records, network, signers)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	candidate, err := ReplayMigration(ctx, source, root, records, network, signers)
	if !errors.Is(err, context.Canceled) || candidate.Payload.RootRawChange != nil {
		t.Fatal("ignored cancellation")
	}
}

func TestReplayEmptyWorkspaceCreatesReplicableSeed(t *testing.T) {
	bundles, err := NewWorkspace(testConfig, []string{"Owner"})
	must(t, err)
	source, err := Open(testConfig, bundles[0], nil)
	must(t, err)
	root, records, network, signers := replayTarget(t, source, bundles)
	candidate, err := ReplayMigration(context.Background(), source, root, records, network, signers)
	must(t, err)
	if len(candidate.Payload.Changes) != 1 || len(candidate.Payload.Heads) != 1 || candidate.Payload.Heads[0] == root.Id {
		t.Fatal("empty workspace will not replicate upstream")
	}
	if len(candidate.Changes) != 0 {
		t.Fatal("fabricated source history mapping")
	}
	view, err := source.View()
	must(t, err)
	if len(view.Records) != 0 || view.Changes != 0 {
		t.Fatal("seed changed source records")
	}
}

func TestReplicaRejectsAnotherAccountReusingOperationID(t *testing.T) {
	source, bundles, _ := seed(t)
	writer, err := Open(testConfig, bundles[1], nil)
	must(t, err)
	must(t, writer.Merge(source.Packet(), ""))
	ops, _, err := source.Operations()
	must(t, err)
	var duplicate Operation
	for _, op := range ops {
		duplicate = op
		break
	}
	// Bypass Apply's local idempotence to simulate a malicious signed peer.
	raw, err := json.Marshal([]Operation{duplicate})
	must(t, err)
	writer.Tree.Lock()
	_, err = writer.Tree.AddContent(context.Background(), objecttree.SignableChangeContent{Data: raw, DataType: testConfig.Namespace, Key: writer.Keys.SignKey, ShouldBeEncrypted: true})
	writer.Tree.Unlock()
	must(t, err)
	replica, tree, _ := attachTestReplica(t, source)
	tree.Lock()
	_, err = tree.AddRawChanges(context.Background(), objecttree.RawChangesPayload{NewHeads: writer.Packet().Heads, RawChanges: writer.Packet().Changes})
	tree.Unlock()
	if err == nil {
		t.Fatal("accepted forged operation attribution")
	}
	view, err := replica.View()
	must(t, err)
	if view.Records[0].Fields["name"][0].Author != bundles[0].User {
		t.Fatal("overwrote original author")
	}
}
