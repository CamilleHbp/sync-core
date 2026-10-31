package synccore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"reflect"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/util/crypto"
)

// ApplyBatch durably appends a local transaction without reconstructing a tree.
// The engine must represent this database's committed revision. SQLite failure
// leaves both the signed tree and its projection unchanged.
func (d *Database) ApplyBatch(e *Engine, batch []Operation, local bool) error {
	return d.ApplyBatchAs(e, e.Bundle, batch, local)
}

// ApplyBatchAs signs as an already validated workspace member without cloning
// history. It cannot change the root, ACL or membership of the engine.
func (d *Database) ApplyBatchAs(e *Engine, bundle Bundle, batch []Operation, local bool) error {
	e.Tree.Lock()
	defer e.Tree.Unlock()
	if e.Tree.Storage() != e.Store {
		return errors.New("durable append requires the engine memory storage")
	}
	if bundle.Root == nil || bundle.Root.Id != e.Bundle.Root.Id || !bytes.Equal(bundle.Root.RawChange, e.Bundle.Root.RawChange) || !maps.Equal(bundle.Members, e.Bundle.Members) || !reflect.DeepEqual(bundle.ACL, e.Bundle.ACL) {
		return NewError("invalid_identity", nil)
	}
	key, err := crypto.UnmarshalEd25519PrivateKeyProto(bundle.Key)
	if err != nil {
		return err
	}
	if bundle.Members[key.GetPublic().Account()] != bundle.User {
		return NewError("invalid_identity", nil)
	}
	if _, err = e.cache.update(e.config, e.Tree, e.Bundle.Members, e.Bundle.User); err != nil {
		return err
	}
	fresh, err := e.cache.prepare(e.config, bundle.User, batch)
	if err != nil {
		return err
	}
	if len(fresh) == 0 {
		return nil
	}
	data, err := json.Marshal(fresh)
	if err != nil {
		return err
	}
	if len(data) > 200000 {
		return errors.New("transaction too large")
	}
	delta, authors := map[string]Operation{}, map[string]string{}
	for _, op := range fresh {
		delta[op.ID] = op
		authors[op.ID] = bundle.User
	}
	changed, _ := e.cache.project(delta, authors, bundle.User)
	_, err = e.Tree.AddContentWithValidator(context.Background(), objecttree.SignableChangeContent{Data: data, Key: key, ShouldBeEncrypted: true, DataType: e.config.Namespace}, func(change objecttree.StorageChange) (commitErr error) {
		defer func() {
			if commitErr != nil {
				commitErr = NewError("storage_unavailable", commitErr)
			}
		}()
		tx, err := d.SQL.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var stored []byte
		if err = tx.QueryRow("SELECT value FROM meta WHERE key='tree'").Scan(&stored); err != nil {
			return err
		}
		var committed MemoryStorage
		if err = json.Unmarshal(stored, &committed); err != nil {
			return err
		}
		if committed.RootID != e.Store.RootID || committed.Seq != e.Store.Seq || !reflect.DeepEqual(committed.HeadIDs, e.Store.HeadIDs) {
			return errors.New("engine is not at the committed database revision")
		}
		change.AddSeq = e.Store.Seq + 1
		raw, err := json.Marshal(change)
		if err != nil {
			return err
		}
		if _, err = tx.Exec("INSERT INTO changes VALUES(?,?,?)", change.Id, change.AddSeq, raw); err != nil {
			return err
		}
		for _, record := range changed {
			raw, err = json.Marshal(record)
			if err != nil {
				return err
			}
			if _, err = tx.Exec("INSERT INTO records VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET seq=excluded.seq,value=excluded.value", record.ID, change.AddSeq, raw); err != nil {
				return err
			}
		}
		next := *e.Store
		next.Changes = nil
		next.Seq = change.AddSeq
		next.HeadIDs = []string{change.Id}
		raw, err = json.Marshal(next)
		if err != nil {
			return err
		}
		if _, err = tx.Exec("UPDATE meta SET value=? WHERE key='tree'", raw); err != nil {
			return err
		}
		if local {
			if _, err = tx.Exec("INSERT INTO outbox VALUES(?)", change.Id); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	// Upstream appends to MemoryStorage only after the validator commits. Its
	// non-failing AddAll then publishes the signed head. Cache updates stay lazy.
	return err
}
