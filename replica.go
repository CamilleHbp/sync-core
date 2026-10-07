package synccore

import (
	"context"
	"errors"
	"maps"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
)

// Replica projects an upstream live tree without a cursor exchange, an
// application polling loop, or a second copy of the signed history. The Host
// owns the tree and database lifetime. Attach before exposing the tree to the
// application, and stop using the replica before closing its Host.
//
// Membership labels must come from the application's trusted workspace
// configuration, never from operation payloads. Upstream independently checks
// signing identity, ACL permissions and network acceptance on every change.
// This API does not migrate or activate a legacy Client.
type Replica struct {
	config  Config
	tree    synctree.SyncTree
	keys    *accountdata.AccountKeys
	user    string
	members map[string]string
	changes chan struct{}
}

// AttachReplica validates the existing projection and installs a pre-commit
// validator for subsequent remote updates. Initial remote imports must also be
// validated before application activation; this function does not roll back an
// import performed by a caller before attachment. It owns the tree's listener.
func AttachReplica(config Config, tree synctree.SyncTree, keys *accountdata.AccountKeys, user string, members map[string]string) (*Replica, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if tree == nil || keys == nil || keys.SignKey == nil || user == "" || members[keys.SignKey.GetPublic().Account()] != user {
		return nil, NewError("invalid_identity", nil)
	}
	r := &Replica{config: config, tree: tree, keys: keys, user: user, members: maps.Clone(members), changes: make(chan struct{}, 1)}
	tree.Lock()
	defer tree.Unlock()
	root, err := objecttree.UnmarshallRoot(tree.Header())
	if err != nil {
		return nil, err
	}
	if root.ChangeType != config.Namespace {
		return nil, NewError("namespace_mismatch", nil)
	}
	if _, err := r.viewLocked(tree); err != nil {
		return nil, err
	}
	tree.SetDeferredUpdater(false)
	tree.SetListener(r)
	r.signal()
	return r, nil
}

// Changes is a coalesced invalidation signal, not a delivery acknowledgement.
// On notification call View, which waits for the tree transaction to finish.
// Signals can be spurious after an upstream storage failure; never interpret
// them as proof that a remote node persisted a local write.
func (r *Replica) Changes() <-chan struct{} { return r.changes }
func (r *Replica) signal() {
	select {
	case r.changes <- struct{}{}:
	default:
	}
}
func (r *Replica) View() (View, error) {
	r.tree.Lock()
	defer r.tree.Unlock()
	return r.viewLocked(r.tree)
}
func (r *Replica) viewLocked(tree objecttree.ObjectTree) (View, error) {
	ops, authors, err := operationsLocked(r.config, tree, r.members)
	if err != nil {
		return View{}, err
	}
	view := Materialize(ops, authors, r.user)
	if err = r.config.validateRecords(view); err != nil {
		return View{}, err
	}
	return view, nil
}

// Apply commits an encrypted, signed upstream tree transaction locally and
// queues its live broadcast. Success is local durability, not server receipt.
// Replay of the same operation IDs and payloads is idempotent.
func (r *Replica) Apply(ctx context.Context, batch []Operation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.tree.Lock()
	defer r.tree.Unlock()
	ops, authors, err := operationsLocked(r.config, r.tree, r.members)
	if err != nil {
		return err
	}
	fresh, err := prepareBatch(r.config, ops, authors, r.user, batch)
	if err != nil {
		return err
	}
	if err = appendBatch(ctx, r.config, r.tree, r.keys.SignKey, fresh); err != nil {
		return err
	}
	if len(fresh) > 0 {
		r.signal()
	}
	return nil
}

// Update and Rebuild implement upstream's listener contract: the tree lock is
// already held. Returning a validation error rolls back an incremental remote
// change before it is persisted. Do not call these methods from app code.
func (r *Replica) Update(tree objecttree.ObjectTree) error {
	if tree.Id() != r.tree.Id() {
		return errors.New("wrong replica tree")
	}
	if _, err := r.viewLocked(tree); err != nil {
		return err
	}
	r.signal()
	return nil
}
func (r *Replica) Rebuild(tree objecttree.ObjectTree) error { return r.Update(tree) }
