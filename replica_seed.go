package synccore

import (
	"context"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
)

// InitializeReplicaTree adds a signed, encrypted empty operation transaction to
// a newly created application root. Upstream deliberately does not replicate
// root-only trees. An empty workspace therefore needs this seed before it can
// be paired with another installation. No application records are fabricated.
// This builds an isolated payload, not a live tree or a remote acknowledgement.
func InitializeReplicaTree(ctx context.Context, config Config, root *treechangeproto.RawTreeChangeWithId, acl list.AclList, keys *accountdata.AccountKeys) (treestorage.TreeStorageCreatePayload, error) {
	var empty treestorage.TreeStorageCreatePayload
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := config.validate(); err != nil {
		return empty, err
	}
	if root == nil || acl == nil || keys == nil || keys.SignKey == nil {
		return empty, NewError("invalid_identity", nil)
	}
	header, err := objecttree.UnmarshallRoot(root)
	if err != nil {
		return empty, err
	}
	if header.ChangeType != config.Namespace {
		return empty, NewError("namespace_mismatch", nil)
	}
	storage := NewStorage(root)
	tree, err := objecttree.BuildObjectTree(storage, acl)
	if err != nil {
		return empty, err
	}
	defer tree.Close()
	tree.Lock()
	_, err = tree.AddContent(ctx, objecttree.SignableChangeContent{Data: []byte("[]"), DataType: config.Namespace, Key: keys.SignKey, ShouldBeEncrypted: true})
	tree.Unlock()
	if err != nil {
		return empty, err
	}
	candidate := Engine{Tree: tree, Store: storage}
	packet := candidate.Packet()
	return treestorage.TreeStorageCreatePayload{RootRawChange: root, Changes: packet.Changes, Heads: packet.Heads}, nil
}
