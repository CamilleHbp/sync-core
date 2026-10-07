package privateclient

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	core "github.com/CamilleHbp/sync-core"
	"github.com/CamilleHbp/sync-core/privatenetwork"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/util/crypto"
	"gopkg.in/yaml.v3"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func testConnection(t *testing.T) (SignedConnection, *accountdata.AccountKeys) {
	t.Helper()
	keys, err := accountdata.NewRandom()
	must(t, err)
	_, network, err := crypto.GenerateRandomEd25519KeyPair()
	must(t, err)
	connection := Connection{Version: 1, Namespace: "test.tasks", NetworkID: network.Network(), SpaceID: "synthetic-space", TreeID: "synthetic-tree", Owner: keys.SignKey.GetPublic().Account(), Members: map[string]string{keys.SignKey.GetPublic().Account(): "Owner"}}
	signed, err := SignConnection(connection, keys.SignKey)
	must(t, err)
	connection.Members[connection.Owner] = "Mutated caller map"
	if signed.Connection.Members[connection.Owner] != "Owner" {
		t.Fatal("retained mutable membership map")
	}
	return signed, keys
}
func TestConnectionTrustAndTamper(t *testing.T) {
	signed, _ := testConnection(t)
	must(t, signed.Verify(signed.Connection.Namespace, signed.Connection.NetworkID, signed.Connection.Owner))
	for _, args := range [][3]string{{"other.app", signed.Connection.NetworkID, signed.Connection.Owner}, {signed.Connection.Namespace, "", signed.Connection.Owner}, {signed.Connection.Namespace, signed.Connection.NetworkID, ""}} {
		if signed.Verify(args[0], args[1], args[2]) == nil {
			t.Fatal("accepted substituted trust")
		}
	}
	signed.Connection.Members[signed.Connection.Owner] = "Forged attribution"
	if signed.Verify(signed.Connection.Namespace, signed.Connection.NetworkID, signed.Connection.Owner) == nil {
		t.Fatal("accepted changed membership labels")
	}
}
func TestOpenRejectsBeforeWriting(t *testing.T) {
	signed, keys := testConnection(t)
	dir := filepath.Join(t.TempDir(), "replica")
	signed.Signature[0] ^= 1
	_, err := Open(context.Background(), dir, core.Config{Namespace: signed.Connection.Namespace, ValidateOperation: func(core.Operation) error { return nil }}, nil, signed.Connection.NetworkID, signed.Connection.Owner, keys, signed)
	if err == nil {
		t.Fatal("accepted invalid signature")
	}
	if _, err = os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("wrote state before trust checks")
	}
}
func TestBindingIsPinnedAndRaceSafe(t *testing.T) {
	dir := t.TempDir()
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- pinBinding(dir, map[string]string{"account": "same"}) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		must(t, err)
	}
	if err := pinBinding(dir, map[string]string{"account": "foreign"}); core.ErrorCode(err) != "identity_mismatch" {
		t.Fatalf("binding overwrite: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "connection.json"))
	must(t, err)
	if info.Mode().Perm() != 0600 {
		t.Fatal("binding not private")
	}
}

func TestPrivateClientLifecycle(t *testing.T) {
	path := os.Getenv("SYNC_CORE_PRIVATE_CONFIG")
	if path == "" {
		t.Skip("private network integration configuration required")
	}
	data, err := os.ReadFile(path)
	must(t, err)
	networkID := os.Getenv("SYNC_CORE_PRIVATE_NETWORK_ID")
	owner, err := accountdata.NewRandom()
	must(t, err)
	writer, err := accountdata.NewRandom()
	must(t, err)
	dir := t.TempDir()
	runtime, err := privatenetwork.New(data, networkID, dir, owner)
	must(t, err)
	host, err := privatenetwork.AttachSpaces(runtime, dir)
	must(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	must(t, runtime.App.Start(ctx))
	defer func() { must(t, host.Close(context.Background())); must(t, runtime.Close(context.Background())) }()
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	must(t, err)
	metadata, _, err := crypto.GenerateRandomEd25519KeyPair()
	must(t, err)
	config := core.Config{Namespace: "sync-core.client-lifecycle", ValidateOperation: func(core.Operation) error { return nil }}
	sp, err := host.CreateSpace(ctx, spacepayloads.SpaceCreatePayload{SigningKey: owner.SignKey, MasterKey: master, MetadataKey: metadata, ReadKey: crypto.NewAES(), SpacePayload: []byte(config.Namespace)})
	must(t, err)
	must(t, sp.SyncHeads(ctx))
	must(t, runtime.Coordinator.SpaceMakeShareable(ctx, sp.Id()))
	must(t, sp.AclClient().AddAccounts(ctx, list.AccountsAddPayload{Additions: []list.AccountAdd{{Identity: writer.SignKey.GetPublic(), Permissions: list.AclPermissionsWriter}}}))
	payload, err := sp.TreeBuilder().CreateTree(ctx, objecttree.ObjectTreeCreatePayload{PrivKey: owner.SignKey, ChangeType: config.Namespace, SpaceId: sp.Id(), IsEncrypted: true, Seed: []byte(core.RandomID()), Timestamp: time.Now().Unix()})
	must(t, err)
	payload, err = core.InitializeReplicaTree(ctx, config, payload.RootRawChange, sp.Acl(), owner)
	must(t, err)
	tree, err := host.PutTree(ctx, sp.Id(), payload, nil)
	must(t, err)
	must(t, sp.SyncHeads(ctx))
	connection := Connection{Version: 1, Namespace: config.Namespace, NetworkID: networkID, SpaceID: sp.Id(), TreeID: tree.Id(), Owner: owner.SignKey.GetPublic().Account(), Members: map[string]string{owner.SignKey.GetPublic().Account(): "Owner", writer.SignKey.GetPublic().Account(): "Writer"}}
	signed, err := SignConnection(connection, owner.SignKey)
	must(t, err)
	// Provisioning and native owner are distinct peers of the same account.
	device, err := accountdata.NewRandom()
	must(t, err)
	ownerDevice := accountdata.New(device.PeerKey, owner.SignKey)
	ownerDir, writerDir := t.TempDir(), t.TempDir()
	a, err := Open(ctx, ownerDir, config, data, networkID, connection.Owner, ownerDevice, signed)
	must(t, err)
	defer func() { must(t, a.Close(context.Background())) }()
	b, err := Open(ctx, writerDir, config, data, networkID, connection.Owner, writer, signed)
	must(t, err)
	defer func() { must(t, b.Close(context.Background())) }()
	record := core.RandomID()
	op := core.Operation{ID: core.RandomID(), Record: record, Kind: "task", Field: "title", Value: "Native client"}
	must(t, a.Apply(ctx, []core.Operation{op}))
	wait := func(c *Client, want string) {
		t.Helper()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		for {
			view, err := c.View()
			must(t, err)
			for _, r := range view.Records {
				if r.ID == record && r.Value("title") == want {
					return
				}
			}
			select {
			case <-c.Changes():
			case <-timer.C:
				t.Fatal("no pushed client update")
			}
		}
	}
	wait(b, op.Value)
	must(t, b.Close(ctx))
	select {
	case <-b.Done():
	default:
		t.Fatal("close did not signal listener shutdown")
	}
	if err = b.Apply(ctx, []core.Operation{op}); err == nil {
		t.Fatal("closed client accepted write")
	}
	op.ID, op.Replaces, op.Value = core.RandomID(), []string{op.ID}, "While client stopped"
	must(t, a.Apply(ctx, []core.Operation{op}))
	b, err = Open(ctx, writerDir, config, data, networkID, connection.Owner, writer, signed)
	must(t, err)
	wait(b, op.Value)
	// Simulate no reachable service using only this test replica's topology.
	// No real service is stopped and no production configuration is changed.
	must(t, b.Close(ctx))
	offline, err := privatenetwork.ParseConfig(data, networkID)
	must(t, err)
	for i := range offline.Nodes {
		offline.Nodes[i].Addresses = []string{"127.0.0.1:1"}
	}
	offlineYAML, err := yaml.Marshal(offline)
	must(t, err)
	cachePath := filepath.Join(writerDir, "network", networkID+".yml")
	must(t, os.WriteFile(cachePath, offlineYAML, 0600))
	b, err = Open(ctx, writerDir, config, offlineYAML, networkID, connection.Owner, writer, signed)
	must(t, err)
	wait(b, op.Value)
	op.ID, op.Replaces, op.Value = core.RandomID(), []string{op.ID}, "Written entirely offline"
	must(t, b.Apply(ctx, []core.Operation{op}))
	wait(b, op.Value)
	must(t, b.Close(ctx))
	// Restore only this disposable replica's topology; startup reconciles and
	// pushes the durable offline operation without an application sync timer.
	must(t, os.WriteFile(cachePath, data, 0600))
	b, err = Open(ctx, writerDir, config, data, networkID, connection.Owner, writer, signed)
	must(t, err)
	wait(a, op.Value)
	// Pins survive restart; even a correctly signed replacement descriptor must
	// not silently redirect a directory to another tree or account.
	alternate := connection
	alternate.TreeID = "another-tree"
	foreign, err := SignConnection(alternate, owner.SignKey)
	must(t, err)
	if _, err = Open(ctx, writerDir, config, data, networkID, connection.Owner, writer, foreign); core.ErrorCode(err) != "identity_mismatch" {
		t.Fatalf("replica rebound: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(writerDir, "connection.json"))
	must(t, err)
	var stored map[string]any
	must(t, json.Unmarshal(raw, &stored))
	if stored["account"] != writer.SignKey.GetPublic().Account() {
		t.Fatal("wrong persisted account")
	}
}
