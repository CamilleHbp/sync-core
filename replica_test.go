package synccore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/commonspace/sync/syncdeps"
	"github.com/anyproto/any-sync/commonspace/syncstatus"
)

// Only transport is stubbed here. Projection, pre-commit rollback, signatures,
// ACL verification, and broadcast construction use the real upstream SyncTree.
// Actual service/any-store replication is exercised by privatenetwork tests.
type localTreeSpace struct {
	spacestorage.SpaceStorage
	storage objecttree.Storage
}

func (s localTreeSpace) TreeStorage(context.Context, string) (objecttree.Storage, error) {
	return s.storage, nil
}

type recordingSync struct {
	synctree.RequestFactory
	broadcasts int
}

func (s *recordingSync) Broadcast(context.Context, *objectmessages.HeadUpdate) error {
	s.broadcasts++
	return nil
}
func (*recordingSync) SendTreeRequest(context.Context, syncdeps.Request, syncdeps.ResponseCollector) error {
	return errors.New("unexpected network fetch")
}
func (*recordingSync) QueueRequest(context.Context, syncdeps.Request) error {
	return errors.New("unexpected network request")
}
func attachTestReplica(t *testing.T, e *Engine) (*Replica, synctree.SyncTree, *recordingSync) {
	t.Helper()
	wire := &recordingSync{RequestFactory: synctree.NewRequestFactory("unit-test")}
	tree, err := synctree.BuildSyncTreeOrGetRemote(context.Background(), e.Tree.Id(), synctree.BuildDeps{
		SpaceId: "unit-test", SpaceStorage: localTreeSpace{storage: e.Store}, AclList: e.Tree.AclList(),
		BuildObjectTree: objecttree.BuildObjectTree, SyncClient: wire, SyncStatus: syncstatus.NewNoOpSyncStatus(), OnClose: func(string) {},
	})
	must(t, err)
	t.Cleanup(func() { must(t, tree.Close()) })
	r, err := AttachReplica(testConfig, tree, e.Keys, e.Bundle.User, e.Bundle.Members)
	must(t, err)
	return r, tree, wire
}
func TestReplicaRejectsWrongNamespaceAndIdentity(t *testing.T) {
	e, _, _ := seed(t)
	_, tree, _ := attachTestReplica(t, e)
	wrong := testConfig
	wrong.Namespace = "another.application"
	if _, err := AttachReplica(wrong, tree, e.Keys, e.Bundle.User, e.Bundle.Members); err == nil {
		t.Fatal("accepted foreign namespace")
	}
	if _, err := AttachReplica(testConfig, tree, e.Keys, "wrong user", e.Bundle.Members); err == nil {
		t.Fatal("accepted foreign identity")
	}
}

func TestReplicaApplyReplayAndInvalidation(t *testing.T) {
	e, _, id := seed(t)
	r, tree, wire := attachTestReplica(t, e)
	<-r.Changes()
	op := Operation{ID: RandomID(), Record: id, Kind: "list", Field: "notes", Value: "native", Replaces: []string{}}
	must(t, r.Apply(context.Background(), []Operation{op}))
	select {
	case <-r.Changes():
	default:
		t.Fatal("local write did not notify")
	}
	if wire.broadcasts != 1 {
		t.Fatalf("broadcasts=%d", wire.broadcasts)
	}
	tree.Lock()
	heads := append([]string{}, tree.Heads()...)
	tree.Unlock()
	must(t, r.Apply(context.Background(), []Operation{op}))
	if wire.broadcasts != 1 {
		t.Fatal("replay created another signed change")
	}
	op.Value = "invalid ID reuse"
	if r.Apply(context.Background(), []Operation{op}) == nil {
		t.Fatal("accepted ID reuse")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(r.Apply(ctx, nil), context.Canceled) {
		t.Fatal("ignored cancellation")
	}
	tree.Lock()
	unchanged := reflect.DeepEqual(heads, tree.Heads())
	tree.Unlock()
	if !unchanged {
		t.Fatal("failed write changed heads")
	}
	view, err := r.View()
	must(t, err)
	if view.Records[0].Value("notes") != "native" {
		t.Fatal("incorrect native projection")
	}
}
func TestReplicaRejectsSignedInvalidRemoteBeforeCommit(t *testing.T) {
	e, _, id := seed(t)
	remote, err := e.Clone()
	must(t, err)
	r, tree, _ := attachTestReplica(t, e)
	before, err := r.View()
	must(t, err)
	raw, _ := json.Marshal([]Operation{{ID: RandomID(), Record: id, Kind: "list", Field: "not-a-schema-field", Value: "invalid"}})
	remote.Tree.Lock()
	added, err := remote.Tree.AddContent(context.Background(), objecttree.SignableChangeContent{Data: raw, DataType: testConfig.Namespace, Key: remote.Keys.SignKey, ShouldBeEncrypted: true})
	remote.Tree.Unlock()
	must(t, err)
	tree.Lock()
	_, err = tree.AddRawChangesFromPeer(context.Background(), "synthetic-peer", objecttree.RawChangesPayload{NewHeads: added.Heads, RawChanges: added.RawChanges()})
	exists := tree.HasChanges(added.Heads[0])
	tree.Unlock()
	if err == nil || exists {
		t.Fatal("invalid remote update was accepted")
	}
	if _, stored := e.Store.Changes[added.Heads[0]]; stored {
		t.Fatal("invalid remote update reached durable storage")
	}
	after, err := r.View()
	must(t, err)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("remote rejection corrupted projection")
	}
}
func TestReplicaRemoteConflictsAndResolution(t *testing.T) {
	e, bundles, id := seed(t)
	remote, err := Open(testConfig, bundles[1], nil)
	must(t, err)
	must(t, remote.Merge(e.Packet(), ""))
	original := field(t, e, id, "name").Fields["name"][0].ID
	r, tree, _ := attachTestReplica(t, e)
	left := Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: "left", Replaces: []string{original}}
	right := Operation{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: "right", Replaces: []string{original}}
	must(t, r.Apply(context.Background(), []Operation{left}))
	must(t, remote.Apply(right))
	packet := remote.Packet()
	tree.Lock()
	_, err = tree.AddRawChangesFromPeer(context.Background(), "synthetic-peer", objecttree.RawChangesPayload{NewHeads: packet.Heads, RawChanges: packet.Changes})
	tree.Unlock()
	must(t, err)
	view, err := r.View()
	must(t, err)
	if !view.Records[0].Conflict("name") {
		t.Fatal("lost remote conflict")
	}
	must(t, r.Apply(context.Background(), []Operation{{ID: RandomID(), Record: id, Kind: "list", Field: "name", Value: "resolved", Replaces: []string{left.ID, right.ID}}}))
	view, err = r.View()
	must(t, err)
	if view.Records[0].Conflict("name") || view.Records[0].Value("name") != "resolved" {
		t.Fatal("resolution failed")
	}
}
