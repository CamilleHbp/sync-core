package synccore

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/crypto"
)

// ReplayedMigration is a validated in-memory candidate, not an activated
// migration or a remote receipt. Old signed change IDs cannot survive new ACL
// admission. Changes maps them to newly signed changes, while application
// operation IDs, authors, sources, replacements and conflicts remain unchanged.
// Keep the original signed history (StageMigration) and all legacy outboxes
// until the application has independently verified replication and cutover.
type ReplayedMigration struct {
	Payload     treestorage.TreeStorageCreatePayload
	Changes     map[string]string
	SourceHeads []string
}

// ReplayMigration re-signs legacy transactions into a new, private-network-
// approved tree using their ORIGINAL account keys. It never impersonates an
// author with the owner's key. Missing signing keys or revoked authors block
// migration; callers must not export somebody else's keys to work around that.
// This is intended for the existing trusted gateway that already holds these
// application keys, or an explicitly provisioned migration process.
//
// The caller must keep source quiescent and archive it before activation. This
// function has no disk/network side effects and cannot acknowledge pending
// writes. It builds in isolated memory, verifies the entire projection, then
// returns a payload for Host.PutTree. No partial candidate is returned on error.
func ReplayMigration(ctx context.Context, source *Engine, root *treechangeproto.RawTreeChangeWithId, records []*consensusproto.RawRecordWithId, networkID string, signers map[string]crypto.PrivKey) (ReplayedMigration, error) {
	fail := ReplayedMigration{}
	if err := ctx.Err(); err != nil {
		return fail, err
	}
	if source == nil || source.Tree == nil || source.Store == nil || source.Keys == nil || root == nil || len(records) == 0 {
		return fail, errors.New("invalid migration input")
	}
	for _, record := range records {
		if record == nil {
			return fail, errors.New("invalid migration ACL record")
		}
	}
	if root.Id == source.Tree.Id() {
		return fail, errors.New("migration requires a new tree")
	}
	networkKey, err := crypto.DecodeNetworkId(networkID)
	if err != nil {
		return fail, err
	}
	// Independently verify every acceptance signature; never trust a caller's
	// pre-built ACL (which might have used a permissive/local-only verifier).
	aclStorage, err := list.NewInMemoryStorage(records[0].Id, records)
	if err != nil {
		return fail, err
	}
	acl, err := list.BuildAclListWithIdentity(source.Keys, aclStorage, recordverifier.New(networkKey))
	if err != nil {
		return fail, err
	}
	defer acl.Close(context.Background())
	if !source.Tree.AclList().AclState().Permissions(source.Keys.SignKey.GetPublic()).IsOwner() || !acl.AclState().Permissions(source.Keys.SignKey.GetPublic()).IsOwner() {
		return fail, list.ErrInsufficientPermissions
	}
	header, err := objecttree.UnmarshallRoot(root)
	if err != nil {
		return fail, err
	}
	if header.ChangeType != source.config.Namespace {
		return fail, NewError("namespace_mismatch", nil)
	}
	rootAuthor, err := crypto.UnmarshalEd25519PublicKeyProto(header.Identity)
	if err != nil {
		return fail, err
	}
	if !rootAuthor.Equals(source.Keys.SignKey.GetPublic()) {
		return fail, errors.New("migration root must be signed by the original owner")
	}
	expected, err := source.View()
	if err != nil {
		return fail, err
	}
	if err = source.config.validateRecords(expected); err != nil {
		return fail, err
	}
	type transaction struct {
		id, account string
		operations  []Operation
	}
	var transactions []transaction
	source.Tree.Lock()
	// View converted/validated all models above. Upstream iteration is causal,
	// retaining atomic signed batches rather than splitting required fields.
	err = source.Tree.IterateRoot(nil, func(ch *objecttree.Change) bool {
		if ch.Id == source.Tree.Id() {
			return true
		}
		ops, ok := ch.Model.([]Operation)
		if !ok {
			return false
		}
		transactions = append(transactions, transaction{ch.Id, ch.Identity.Account(), ops})
		return true
	})
	source.Tree.Unlock()
	if err != nil {
		return fail, err
	}
	if len(transactions) != len(source.Store.Changes)-1 {
		return fail, errors.New("migration source contains unprojected history")
	}
	// Validate every signer before creating even an in-memory destination change.
	for _, tx := range transactions {
		key := signers[tx.account]
		if key == nil || key.GetPublic().Account() != tx.account {
			return fail, fmt.Errorf("migration signing key unavailable for %s", tx.account)
		}
		if !acl.AclState().Permissions(key.GetPublic()).CanWrite() {
			return fail, list.ErrInsufficientPermissions
		}
	}
	storage := NewStorage(root)
	tree, err := objecttree.BuildObjectTree(storage, acl)
	if err != nil {
		return fail, err
	}
	defer tree.Close()
	bundle := source.Bundle
	bundle.Root = root
	candidate := &Engine{config: source.config, Bundle: bundle, Store: storage, Tree: tree, Keys: source.Keys}
	ops := map[string]Operation{}
	authors := map[string]string{}
	result := ReplayedMigration{Changes: map[string]string{}, SourceHeads: append([]string{}, source.Tree.Heads()...)}
	for _, tx := range transactions {
		if err = ctx.Err(); err != nil {
			return fail, err
		}
		fresh, err := prepareBatch(source.config, ops, authors, source.Bundle.Members[tx.account], tx.operations)
		if err != nil {
			return fail, fmt.Errorf("migration transaction %s: %w", tx.id, err)
		}
		tree.Lock()
		err = appendBatch(ctx, source.config, tree, signers[tx.account], fresh)
		heads := append([]string{}, tree.Heads()...)
		tree.Unlock()
		if err != nil {
			return fail, err
		}
		if len(heads) != 1 {
			return fail, errors.New("unexpected migration candidate heads")
		}
		result.Changes[tx.id] = heads[0]
	}
	actual, err := candidate.View()
	if err != nil {
		return fail, err
	}
	if !reflect.DeepEqual(expected, actual) {
		return fail, errors.New("migration replay changed application state")
	}
	if err = ctx.Err(); err != nil {
		return fail, err
	}
	packet := candidate.Packet()
	result.Payload = treestorage.TreeStorageCreatePayload{RootRawChange: root, Changes: packet.Changes, Heads: packet.Heads}
	if len(packet.Changes) == 0 {
		result.Payload, err = InitializeReplicaTree(ctx, source.config, root, acl, source.Keys)
		if err != nil {
			return fail, err
		}
	}
	return result, nil
}
