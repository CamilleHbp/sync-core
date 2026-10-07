package privatenetwork

import (
	"context"
	"errors"
	"os"

	core "github.com/CamilleHbp/sync-core"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/util/crypto"
)

func privateTestConfig(t *testing.T) ([]byte, string) {
	t.Helper()
	path := os.Getenv("SYNC_CORE_PRIVATE_CONFIG")
	if path == "" {
		t.Skip("set SYNC_CORE_PRIVATE_CONFIG and SYNC_CORE_PRIVATE_NETWORK_ID for private integration tests")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data, os.Getenv("SYNC_CORE_PRIVATE_NETWORK_ID")
}
func startHost(t *testing.T, data []byte, id, dir string, keys *accountdata.AccountKeys) (*Runtime, *Host) {
	t.Helper()
	r, err := New(data, id, dir, keys)
	if err != nil {
		t.Fatal(err)
	}
	h, err := AttachSpaces(r, dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = r.App.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return r, h
}
func closeHost(t *testing.T, r *Runtime, h *Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		t.Error(err)
	}
	if err := r.Close(ctx); err != nil {
		t.Error(err)
	}
}
func waitChange(t *testing.T, tree objecttree.ObjectTree, want string) {
	t.Helper()
	timeout := time.NewTimer(15 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		tree.Lock()
		found := tree.HasChanges(want)
		tree.Unlock()
		if found {
			return
		}
		select {
		case <-timeout.C:
			t.Fatalf("upstream stream did not deliver change %s", want)
		case <-tick.C:
		}
	}
}
func writeChange(t *testing.T, tree objecttree.ObjectTree, keys *accountdata.AccountKeys, value string) string {
	t.Helper()
	tree.Lock()
	defer tree.Unlock()
	res, err := tree.AddContent(context.Background(), objecttree.SignableChangeContent{Data: []byte(value), DataType: "sync-core.test", Key: keys.SignKey, ShouldBeEncrypted: true})
	if err != nil {
		t.Fatal(err)
	}
	return res.Heads[0]
}

// Two independent peers share a synthetic owner identity, as two installations
// of one account do. No app sync loop is run between writes and assertions.
func TestPrivateSpaceLiveReplication(t *testing.T) {
	data, id := privateTestConfig(t)
	keys, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	peerKeys, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	other := accountdata.New(peerKeys.PeerKey, keys.SignKey)
	dir1, dir2 := t.TempDir(), t.TempDir()
	r1, h1 := startHost(t, data, id, dir1, keys)
	defer func() { closeHost(t, r1, h1) }()
	r2, h2 := startHost(t, data, id, dir2, other)
	defer func() { closeHost(t, r2, h2) }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	metadata, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	sp, err := h1.CreateSpace(ctx, spacepayloads.SpaceCreatePayload{SigningKey: keys.SignKey, MasterKey: master, MetadataKey: metadata, ReadKey: crypto.NewAES(), SpacePayload: []byte("sync-core.test")})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := sp.TreeBuilder().CreateTree(ctx, objecttree.ObjectTreeCreatePayload{PrivKey: keys.SignKey, SpaceId: sp.Id(), ChangeType: "sync-core.test", IsEncrypted: true, Seed: []byte("synthetic integration tree"), Timestamp: time.Now().Unix()})
	if err != nil {
		t.Fatal(err)
	}
	tr1, err := h1.PutTree(ctx, sp.Id(), payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := writeChange(t, tr1, keys, "first")
	if err = sp.SyncHeads(ctx); err != nil {
		t.Fatal(err)
	}
	sp2, err := h2.OpenSpace(ctx, sp.Id())
	if err != nil {
		t.Fatal(err)
	}
	tr2, err := h2.GetTree(ctx, sp.Id(), tr1.Id())
	if err != nil {
		t.Fatal(err)
	}
	if err = sp2.SyncHeads(ctx); err != nil {
		t.Fatal(err)
	}
	waitChange(t, tr2, first)
	live := writeChange(t, tr1, keys, "live")
	waitChange(t, tr2, live)
	back := writeChange(t, tr2, other, "back")
	waitChange(t, tr1, back)
	// Shutdown/reopen retains any-store history and resubscribes immediately.
	closeHost(t, r2, h2)
	offline := writeChange(t, tr1, keys, "while second peer stopped")
	r2, h2 = startHost(t, data, id, dir2, other)
	if _, err = h2.OpenSpace(ctx, sp.Id()); err != nil {
		t.Fatal(err)
	}
	tr2, err = h2.GetTree(ctx, sp.Id(), tr1.Id())
	if err != nil {
		t.Fatal(err)
	}
	if err = h2.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	waitChange(t, tr2, offline)
	waitChange(t, tr2, back)

	// Both transports are stopped. Each persisted replica accepts a different
	// encrypted change based on the same history, with no server available to
	// serialize the edits. Restart must retain both branches and converge.
	spaceID, treeID := sp.Id(), tr1.Id()
	closeHost(t, r1, h1)
	closeHost(t, r2, h2)
	left := offlineWrite(t, h1.storage.path(spaceID), spaceID, treeID, id, keys, "offline left")
	right := offlineWrite(t, h2.storage.path(spaceID), spaceID, treeID, id, other, "offline right")
	r1, h1 = startHost(t, data, id, dir1, keys)
	r2, h2 = startHost(t, data, id, dir2, other)
	if _, err = h1.OpenSpace(ctx, spaceID); err != nil {
		t.Fatal(err)
	}
	if _, err = h2.OpenSpace(ctx, spaceID); err != nil {
		t.Fatal(err)
	}
	tr1, err = h1.GetTree(ctx, spaceID, treeID)
	if err != nil {
		t.Fatal(err)
	}
	tr2, err = h2.GetTree(ctx, spaceID, treeID)
	if err != nil {
		t.Fatal(err)
	}
	if err = h1.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if err = h2.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	waitChange(t, tr1, left)
	waitChange(t, tr1, right)
	waitChange(t, tr2, left)
	waitChange(t, tr2, right)
}

// Open just the upstream database/tree: there is no runtime, peer, or socket.
func offlineWrite(t *testing.T, path, spaceID, treeID, networkID string, keys *accountdata.AccountKeys, value string) string {
	t.Helper()
	ctx := context.Background()
	db, err := openSpaceDB(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st, err := spacestorage.New(ctx, spaceID, db)
	if err != nil {
		t.Fatal(err)
	}
	networkKey, err := crypto.DecodeNetworkId(networkID)
	if err != nil {
		t.Fatal(err)
	}
	aclStorage, err := st.AclStorage()
	if err != nil {
		t.Fatal(err)
	}
	acl, err := list.BuildAclListWithIdentity(keys, aclStorage, recordverifier.New(networkKey))
	if err != nil {
		t.Fatal(err)
	}
	storage, err := st.TreeStorage(ctx, treeID)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := objecttree.BuildObjectTree(storage, acl)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	return writeChange(t, tree, keys, value)
}

func TestPrivateApplicationReplica(t *testing.T) {
	data, networkID := privateTestConfig(t)
	keys, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	device, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	secondKeys := accountdata.New(device.PeerKey, keys.SignKey)
	r1, h1 := startHost(t, data, networkID, t.TempDir(), keys)
	defer closeHost(t, r1, h1)
	r2, h2 := startHost(t, data, networkID, t.TempDir(), secondKeys)
	defer closeHost(t, r2, h2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	metadata, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	config := core.Config{Namespace: "sync-core.application-test", Platforms: []string{"desktop"}, ValidateOperation: func(op core.Operation) error {
		if op.Kind != "task" || (op.Field != "title" && op.Field != "notes") {
			return errors.New("invalid task field")
		}
		return nil
	}}
	sp, err := h1.CreateSpace(ctx, spacepayloads.SpaceCreatePayload{SigningKey: keys.SignKey, MasterKey: master, MetadataKey: metadata, ReadKey: crypto.NewAES(), SpacePayload: []byte(config.Namespace)})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := sp.TreeBuilder().CreateTree(ctx, objecttree.ObjectTreeCreatePayload{PrivKey: keys.SignKey, SpaceId: sp.Id(), ChangeType: config.Namespace, IsEncrypted: true, Seed: []byte(core.RandomID()), Timestamp: time.Now().Unix()})
	if err != nil {
		t.Fatal(err)
	}
	tree1, err := h1.PutTree(ctx, sp.Id(), payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	members := map[string]string{keys.SignKey.GetPublic().Account(): "Test owner"}
	a, err := core.AttachReplica(config, tree1.(synctree.SyncTree), keys, "Test owner", members)
	if err != nil {
		t.Fatal(err)
	}
	record := core.RandomID()
	op := core.Operation{ID: core.RandomID(), Record: record, Kind: "task", Field: "title", Value: "Initial"}
	if err = a.Apply(ctx, []core.Operation{op}); err != nil {
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
	b, err := core.AttachReplica(config, tree2.(synctree.SyncTree), secondKeys, "Test owner", members)
	if err != nil {
		t.Fatal(err)
	}
	if err = sp2.SyncHeads(ctx); err != nil {
		t.Fatal(err)
	}
	waitRecord := func(replica *core.Replica, field, value string) {
		t.Helper()
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		for {
			view, err := replica.View()
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range view.Records {
				if r.ID == record && r.Value(field) == value {
					return
				}
			}
			select {
			case <-replica.Changes():
			case <-deadline.C:
				t.Fatalf("no pushed %s=%s", field, value)
			}
		}
	}
	waitRecord(b, "title", "Initial")
	op.ID, op.Value, op.Replaces = core.RandomID(), "Pushed title", []string{op.ID}
	if err = a.Apply(ctx, []core.Operation{op}); err != nil {
		t.Fatal(err)
	}
	waitRecord(b, "title", "Pushed title")
	if err = b.Apply(ctx, []core.Operation{{ID: core.RandomID(), Record: record, Kind: "task", Field: "notes", Value: "Reply"}}); err != nil {
		t.Fatal(err)
	}
	waitRecord(a, "notes", "Reply")
	// Simulate lost sockets without stopping the app or its database. A single
	// connectivity event restores subscriptions; no app polling follows it.
	for _, nodeID := range r2.Nodes.NodeIds(sp.Id()) {
		p, err := r2.Pool.Get(ctx, nodeID)
		if err != nil {
			t.Fatal(err)
		}
		if err = p.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err = h2.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	op.ID, op.Value, op.Replaces = core.RandomID(), "After reconnect", []string{op.ID}
	if err = a.Apply(ctx, []core.Operation{op}); err != nil {
		t.Fatal(err)
	}
	waitRecord(b, "title", "After reconnect")
}
