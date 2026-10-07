package synccore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/util/cidutil"
	"github.com/anyproto/any-sync/util/crypto"
)

// MigrationSpace creates a reproducible, owner-signed v1 space envelope around
// an existing ACL. It never rewrites historical signed changes or account keys.
// All replicas must use the same envelope and replication key. Network admission
// by a coordinator is a separate step; local validation is not admission.
func MigrationSpace(config Config, owner Bundle, replicationKey uint64) (spacestorage.SpaceStorageCreatePayload, error) {
	var payload spacestorage.SpaceStorageCreatePayload
	engine, err := Open(config, owner, nil)
	if err != nil {
		return payload, err
	}
	if !engine.Tree.AclList().AclState().Permissions(engine.Keys.SignKey.GetPublic()).IsOwner() {
		return payload, list.ErrInsufficientPermissions
	}
	root, err := objecttree.UnmarshallRoot(owner.Root)
	if err != nil {
		return payload, err
	}
	seed := sha256.Sum256([]byte("sync-core/migration/v1/" + owner.Root.Id))
	_, settings, err := objecttree.NewChangeBuilder(crypto.NewKeyStorage(), nil).BuildRoot(objecttree.InitialContent{
		AclHeadId: owner.ACL[0].Id, PrivKey: engine.Keys.SignKey,
		Seed: seed[:], Timestamp: root.Timestamp, ChangeType: spacepayloads.SpaceReserved,
	})
	if err != nil {
		return payload, err
	}
	identity, err := engine.Keys.SignKey.GetPublic().Marshall()
	if err != nil {
		return payload, err
	}
	header := &spacesyncproto.SpaceHeader{
		Identity: identity, Timestamp: root.Timestamp, SpaceType: config.Namespace,
		SpaceHeaderPayload: []byte(owner.Root.Id), ReplicationKey: replicationKey,
		Seed: seed[:], Version: spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion1,
		AclPayload: owner.ACL[0].Payload, SettingPayload: settings.RawChange,
	}
	headerBytes, err := header.MarshalVT()
	if err != nil {
		return payload, err
	}
	signature, err := engine.Keys.SignKey.Sign(headerBytes)
	if err != nil {
		return payload, err
	}
	raw, err := (&spacesyncproto.RawSpaceHeader{SpaceHeader: headerBytes, Signature: signature}).MarshalVT()
	if err != nil {
		return payload, err
	}
	id, err := cidutil.NewCidFromBytes(raw)
	if err != nil {
		return payload, err
	}
	payload = spacestorage.SpaceStorageCreatePayload{
		AclWithId: owner.ACL[0], SpaceSettingsWithId: settings,
		SpaceHeaderWithId: &spacesyncproto.RawSpaceHeaderWithId{Id: spacepayloads.NewSpaceId(id, replicationKey), RawHeader: raw},
	}
	return payload, spacepayloads.ValidateSpaceStorageCreatePayload(payload)
}

// MigrationManifest describes a staged replica, not an activated migration.
// Pending IDs remain pending until a remote peer confirms durable receipt.
// No account private key or authentication token is written into this manifest.
type MigrationManifest struct {
	Version   int      `json:"version"`
	Status    string   `json:"status"`
	Namespace string   `json:"namespace"`
	SpaceID   string   `json:"spaceId"`
	TreeID    string   `json:"treeId"`
	Heads     []string `json:"heads"`
	Pending   []string `json:"pending"`
}

// StageMigration writes a validated any-store replica to a NEW directory. The
// caller must keep source quiescent for this call. Source is never mutated and
// the result must not be activated until network and application checks pass.
// Publication of destination/replica uses a directory rename only after close,
// reopen and verification. A successful result is LOCAL-ONLY, not a network
// replica: ValidateMigrationACL and coordinator admission are separate gates.
func StageMigration(ctx context.Context, destination string, source *Engine, space spacestorage.SpaceStorageCreatePayload, pending []string) (manifest MigrationManifest, err error) {
	if source == nil {
		return manifest, errors.New("missing migration source")
	}
	if err = ctx.Err(); err != nil {
		return
	}
	if space.SpaceHeaderWithId == nil || space.SpaceSettingsWithId == nil || space.AclWithId == nil {
		return manifest, errors.New("invalid migration space")
	}
	if err = spacepayloads.ValidateSpaceStorageCreatePayload(space); err != nil {
		return
	}
	if space.AclWithId.Id != source.Bundle.ACL[0].Id || !bytes.Equal(space.AclWithId.Payload, source.Bundle.ACL[0].Payload) {
		return manifest, errors.New("migration ACL mismatch")
	}
	var raw spacesyncproto.RawSpaceHeader
	var header spacesyncproto.SpaceHeader
	if err = raw.UnmarshalVT(space.SpaceHeaderWithId.RawHeader); err != nil {
		return
	}
	if err = header.UnmarshalVT(raw.SpaceHeader); err != nil {
		return
	}
	if header.SpaceType != source.config.Namespace || string(header.SpaceHeaderPayload) != source.Tree.Id() {
		return manifest, errors.New("migration workspace mismatch")
	}
	owner, err := source.Tree.AclList().AclState().OwnerPubKey()
	if err != nil {
		return manifest, err
	}
	if _, err = spacepayloads.ValidateSpaceHeader(space.SpaceHeaderWithId, owner, space.AclWithId.Payload, space.SpaceSettingsWithId.RawChange); err != nil {
		return
	}
	for _, id := range pending {
		if id == source.Tree.Id() || !source.Tree.HasChanges(id) {
			return manifest, fmt.Errorf("unknown pending change %s", id)
		}
	}
	expected, err := source.View()
	if err != nil {
		return manifest, err
	}
	if err = source.config.validateRecords(expected); err != nil {
		return
	}
	destination, err = filepath.Abs(destination)
	if err != nil {
		return
	}
	if _, err = os.Lstat(destination); err == nil {
		return manifest, os.ErrExist
	} else if !errors.Is(err, os.ErrNotExist) {
		return manifest, err
	}
	parent := filepath.Dir(destination)
	if err = os.MkdirAll(parent, 0700); err != nil {
		return
	}
	stage, err := os.MkdirTemp(parent, ".sync-migration-")
	if err != nil {
		return manifest, err
	}
	defer os.RemoveAll(stage)
	db, err := openNativeStore(ctx, filepath.Join(stage, "space.sqlite"))
	if err != nil {
		return manifest, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	storage, err := spacestorage.Create(ctx, db, space)
	if err != nil {
		return manifest, err
	}
	aclStorage, err := storage.AclStorage()
	if err != nil {
		return manifest, err
	}
	acl, err := list.BuildAclListWithIdentity(source.Keys, aclStorage, recordverifier.NewValidateFull())
	if err != nil {
		return manifest, err
	}
	for _, record := range source.Bundle.ACL[1:] {
		if err = ctx.Err(); err != nil {
			return
		}
		if err = acl.AddRawRecord(record); err != nil {
			return
		}
	}
	packet := source.Packet()
	tree, err := objecttree.ValidateRawTreeDefault(treestorage.TreeStorageCreatePayload{
		RootRawChange: source.Bundle.Root, Changes: packet.Changes, Heads: packet.Heads,
	}, storage, acl)
	if err != nil {
		return manifest, err
	}
	// ValidateRawTreeDefault builds an empty-data tree for serving peers. Rebuild
	// a decrypting tree to compare the application's complete projected state.
	tree, err = objecttree.BuildObjectTree(tree.Storage(), acl)
	if err != nil {
		return manifest, err
	}
	if err = verifyMigratedTree(ctx, source, tree, expected); err != nil {
		return
	}
	manifest = MigrationManifest{Version: 1, Status: "local-only", Namespace: source.config.Namespace, SpaceID: storage.Id(), TreeID: tree.Id(), Heads: append([]string{}, tree.Heads()...), Pending: append([]string{}, pending...)}
	if err = db.Flush(ctx, 0, anystore.FlushModeCheckpointFull); err != nil {
		return
	}
	err = db.Close()
	closed = true
	if err != nil {
		return
	}
	db, err = openNativeStore(ctx, filepath.Join(stage, "space.sqlite"))
	if err != nil {
		return
	}
	closed = false
	storage, err = spacestorage.New(ctx, manifest.SpaceID, db)
	if err != nil {
		return
	}
	aclStorage, err = storage.AclStorage()
	if err != nil {
		return
	}
	acl, err = list.BuildAclListWithIdentity(source.Keys, aclStorage, recordverifier.NewValidateFull())
	if err != nil {
		return
	}
	treeStorage, err := storage.TreeStorage(ctx, manifest.TreeID)
	if err != nil {
		return manifest, err
	}
	tree, err = objecttree.BuildObjectTree(treeStorage, acl)
	if err != nil {
		return
	}
	if err = verifyMigratedTree(ctx, source, tree, expected); err != nil {
		return
	}
	err = db.Close()
	closed = true
	if err != nil {
		return
	}
	if err = AtomicJSON(filepath.Join(stage, "manifest.json"), manifest); err != nil {
		return
	}
	if err = ctx.Err(); err != nil {
		return
	}
	// Reserve the destination to prevent overwriting an existing replica, even
	// when another migration races us. A failed rename removes only our empty dir.
	if err = os.Mkdir(destination, 0700); err != nil {
		return
	}
	if err = os.Rename(stage, filepath.Join(destination, "replica")); err != nil {
		_ = os.Remove(destination)
		return
	}
	published, err := os.Open(destination)
	if err != nil {
		return manifest, err
	}
	err = published.Sync()
	closeErr := published.Close()
	if err != nil {
		return manifest, err
	}
	if closeErr != nil {
		return manifest, closeErr
	}
	dir, err := os.Open(parent)
	if err != nil {
		return manifest, err
	}
	defer dir.Close()
	err = dir.Sync()
	return
}

func openNativeStore(ctx context.Context, path string) (anystore.DB, error) {
	return anystore.Open(ctx, path, &anystore.Config{
		ReadConnections:         2,
		SQLiteConnectionOptions: map[string]string{"synchronous": "FULL"},
		Durability:              anystore.DurabilityConfig{Sentinel: true},
	})
}

func verifyMigratedTree(ctx context.Context, source *Engine, tree objecttree.ObjectTree, expected View) error {
	wantHeads, gotHeads := slices.Clone(source.Tree.Heads()), slices.Clone(tree.Heads())
	slices.Sort(wantHeads)
	slices.Sort(gotHeads)
	if !slices.Equal(wantHeads, gotHeads) {
		return errors.New("migration heads mismatch")
	}
	for id, change := range source.Store.Changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		copied, err := tree.Storage().Get(ctx, id)
		if err != nil {
			return err
		}
		if !bytes.Equal(copied.RawChange, change.RawChange) {
			return errors.New("signed history changed during migration")
		}
	}
	projected := &Engine{config: source.config, Bundle: source.Bundle, Keys: source.Keys, Tree: tree, Store: &MemoryStorage{RootID: tree.Id()}}
	actual, err := projected.View()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, actual) {
		return errors.New("migration projection mismatch")
	}
	return nil
}

// StageMigration snapshots all pending IDs, not just one legacy network page.
// Lock ordering matches Close and Sync. Nothing in the active client is switched.
func (c *Client) StageMigration(ctx context.Context, destination string, space spacestorage.SpaceStorageCreatePayload) (MigrationManifest, error) {
	c.syncing.Lock()
	defer c.syncing.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.engine == nil {
		return MigrationManifest{}, NewError("sign_in_required", nil)
	}
	rows, err := c.db.SQL.QueryContext(ctx, "SELECT id FROM outbox ORDER BY id")
	if err != nil {
		return MigrationManifest{}, err
	}
	var pending []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return MigrationManifest{}, err
		}
		pending = append(pending, id)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return MigrationManifest{}, err
	}
	if closeErr != nil {
		return MigrationManifest{}, closeErr
	}
	return StageMigration(ctx, destination, c.engine, space, pending)
}
