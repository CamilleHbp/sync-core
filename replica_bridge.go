package synccore

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/util/crypto"
)

// Transaction retains the original atomic batch and its cryptographic author.
// Gateways use this only inside the authenticated workspace, never as unsigned
// input from a remote client.
type Transaction struct {
	Account    string
	Operations []Operation
}

func transactionsLocked(tree objecttree.ObjectTree) ([]Transaction, error) {
	var result []Transaction
	var invalid error
	err := tree.IterateRoot(func(ch *objecttree.Change, data []byte) (any, error) {
		if ch.Id == tree.Id() {
			return nil, nil
		}
		var batch []Operation
		if err := json.Unmarshal(data, &batch); err != nil {
			return nil, err
		}
		return batch, nil
	}, func(ch *objecttree.Change) bool {
		if ch.Id == tree.Id() {
			return true
		}
		batch, ok := ch.Model.([]Operation)
		if !ok {
			invalid = errors.New("invalid transaction")
			return false
		}
		result = append(result, Transaction{ch.Identity.Account(), batch})
		return true
	})
	return result, errors.Join(err, invalid)
}

// ReconcileLegacy is the trusted gateway compatibility bridge. It preserves
// operation identities and authors, not signed-change IDs. Original histories
// remain in both trees. Call with a quiescent legacy source and persist the
// returned engine atomically. Interrupted imports are safe to repeat.
func (r *Replica) ReconcileLegacy(ctx context.Context, source *Engine, bundles []Bundle) (*Engine, error) {
	if source.config.Namespace != r.config.Namespace {
		return nil, NewError("namespace_mismatch", nil)
	}
	sourceOps, sourceAuthors, err := source.Operations()
	if err != nil {
		return nil, err
	}
	source.Tree.Lock()
	incoming, err := transactionsLocked(source.Tree)
	source.Tree.Unlock()
	if err != nil {
		return nil, err
	}
	identities := map[string]Bundle{}
	keys := map[string]*accountdata.AccountKeys{}
	for _, b := range bundles {
		key, err := crypto.UnmarshalEd25519PrivateKeyProto(b.Key)
		if err != nil {
			return nil, err
		}
		id := key.GetPublic().Account()
		if r.members[id] != b.User || b.Root.Id != source.Bundle.Root.Id {
			return nil, NewError("identity_mismatch", nil)
		}
		identities[id] = b
		keys[id] = accountdata.New(key, key)
	}
	r.tree.Lock()
	defer r.tree.Unlock()
	ops, authors, err := operationsLocked(r.config, r.tree, r.members)
	if err != nil {
		return nil, err
	}
	for _, transaction := range incoming {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := keys[transaction.Account]
		if key == nil {
			return nil, NewError("invalid_identity", nil)
		}
		known, err := knownTransaction(transaction, ops, authors, r.members[transaction.Account])
		if err != nil {
			return nil, err
		}
		if known {
			continue
		}
		fresh, err := prepareBatch(r.config, ops, authors, r.members[transaction.Account], transaction.Operations)
		if err != nil {
			return nil, err
		}
		if err = appendBatch(ctx, r.config, r.tree, key.SignKey, fresh); err != nil {
			return nil, err
		}
	}
	outgoing, err := transactionsLocked(r.tree)
	if err != nil {
		return nil, err
	}
	result := source
	for _, transaction := range outgoing {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		b, ok := identities[transaction.Account]
		if !ok {
			return nil, NewError("invalid_identity", nil)
		}
		known, err := knownTransaction(transaction, sourceOps, sourceAuthors, b.User)
		if err != nil {
			return nil, err
		}
		if known {
			continue
		}
		if result == source {
			result, err = source.Clone()
			if err != nil {
				return nil, err
			}
		}
		writer, err := Open(source.config, b, result.Store)
		if err != nil {
			return nil, err
		}
		if err = writer.ApplyBatch(transaction.Operations); err != nil {
			return nil, err
		}
		result.Store = writer.Store
		for _, operation := range transaction.Operations {
			sourceOps[operation.ID] = operation
			sourceAuthors[operation.ID] = b.User
		}
	}
	if result == source {
		return source, nil
	}
	result, err = Open(source.config, source.Bundle, result.Store)
	return result, err
}

// A transaction already present with identical content and author needs no tree
// reconstruction or full projection validation. Check every operation even in
// mixed batches, so the fast path never hides a reused ID or changed author.
func knownTransaction(transaction Transaction, operations map[string]Operation, authors map[string]string, user string) (bool, error) {
	known := true
	for _, operation := range transaction.Operations {
		previous, exists := operations[operation.ID]
		if !exists {
			known = false
			continue
		}
		if !SameOperation(previous, operation) || authors[operation.ID] != user {
			return false, errors.New("operation ID reused")
		}
	}
	return known, nil
}
