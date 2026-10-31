// Package synccore adapts upstream signed, encrypted any-sync trees to
// application operations. It supports legacy HTTPS replicas and native trees
// hosted on an explicitly configured private any-sync network.
package synccore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"sort"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/anyproto/lexid"
)

const MaxBatch = 200

type Bundle struct {
	User    string                               `json:"user"`
	Key     []byte                               `json:"key"`
	ACL     []*consensusproto.RawRecordWithId    `json:"acl"`
	Root    *treechangeproto.RawTreeChangeWithId `json:"root"`
	Members map[string]string                    `json:"members"` // public-key account -> display name
}
type Packet struct {
	RootID  string                                 `json:"rootId"`
	Heads   []string                               `json:"heads"`
	Changes []*treechangeproto.RawTreeChangeWithId `json:"changes"`
}
type Engine struct {
	cache  projectionCache
	config Config
	Bundle Bundle
	Store  *MemoryStorage
	Tree   objecttree.ObjectTree
	Keys   *accountdata.AccountKeys
}

func RandomID() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func Open(config Config, bundle Bundle, store *MemoryStorage) (*Engine, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if bundle.Root == nil || len(bundle.ACL) < 1 {
		return nil, errors.New("invalid bootstrap")
	}
	root, err := objecttree.UnmarshallRoot(bundle.Root)
	if err != nil || root.ChangeType != config.Namespace {
		return nil, NewError("namespace_mismatch", nil)
	}
	key, err := crypto.UnmarshalEd25519PrivateKeyProto(bundle.Key)
	if err != nil {
		return nil, err
	}
	if bundle.Members[key.GetPublic().Account()] != bundle.User {
		return nil, NewError("invalid_identity", nil)
	}
	keys := accountdata.New(key, key)
	ast, err := list.NewInMemoryStorage(bundle.ACL[0].Id, bundle.ACL)
	if err != nil {
		return nil, err
	}
	acl, err := list.BuildAclListWithIdentity(keys, ast, recordverifier.NewValidateFull())
	if err != nil {
		return nil, err
	}
	if store == nil {
		store = NewStorage(bundle.Root)
		v := store.Changes[bundle.Root.Id]
		v.OrderId = lexid.Must(lexid.CharsAllNoEscape, 4, 100).Next("")
		store.Changes[v.Id] = v
	}
	if store.RootID != bundle.Root.Id {
		return nil, errors.New("wrong workspace")
	}
	tree, err := objecttree.BuildObjectTree(store, acl)
	if err != nil {
		return nil, err
	}
	return &Engine{config: config, Bundle: bundle, Store: store, Tree: tree, Keys: keys}, nil
}
func (e *Engine) Clone() (*Engine, error) { return e.CloneAs(e.Bundle) }

// CloneAs preserves the validated projection while selecting an existing
// workspace member's signing identity. It never changes membership or history.
func (e *Engine) CloneAs(bundle Bundle) (*Engine, error) {
	e.Tree.Lock()
	defer e.Tree.Unlock()
	if bundle.Root == nil || bundle.Root.Id != e.Bundle.Root.Id || !maps.Equal(bundle.Members, e.Bundle.Members) {
		return nil, NewError("invalid_identity", nil)
	}
	if _, err := e.cache.update(e.config, e.Tree, e.Bundle.Members, e.Bundle.User); err != nil {
		return nil, err
	}
	st := *e.Store
	st.HeadIDs = append([]string{}, e.Store.HeadIDs...)
	st.Changes = maps.Clone(e.Store.Changes)
	next, err := Open(e.config, bundle, &st)
	if err != nil {
		return nil, err
	}
	next.cache = e.cache
	next.cache.heads = append([]string{}, e.cache.heads...)
	next.cache.seen = maps.Clone(e.cache.seen)
	next.cache.ops = maps.Clone(e.cache.ops)
	next.cache.authors = maps.Clone(e.cache.authors)
	next.cache.identities = maps.Clone(e.cache.identities)
	next.cache.kinds = maps.Clone(e.cache.kinds)
	next.cache.records = maps.Clone(e.cache.records)
	next.cache.view.User = bundle.User
	return next, nil
}

func (e *Engine) Packet() Packet {
	out := Packet{RootID: e.Tree.Id(), Heads: e.Tree.Heads(), Changes: []*treechangeproto.RawTreeChangeWithId{}}
	var cs []objecttree.StorageChange
	for id, ch := range e.Store.Changes {
		if id != e.Store.RootID {
			cs = append(cs, ch)
		}
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].OrderId < cs[j].OrderId })
	for _, ch := range cs {
		out.Changes = append(out.Changes, ch.RawTreeChangeWithId())
	}
	return out
}
func (e *Engine) Operations() (map[string]Operation, map[string]string, error) {
	e.Tree.Lock()
	defer e.Tree.Unlock()
	return operationsLocked(e.config, e.Tree, e.Bundle.Members)
}

// operationsLocked projects both legacy and native trees. Callers hold tree's
// lock, including upstream pre-commit validation callbacks.
func operationsLocked(config Config, tree objecttree.ObjectTree, members map[string]string) (map[string]Operation, map[string]string, error) {
	ops := map[string]Operation{}
	authors := map[string]string{}
	identities := map[string]string{}
	kinds := map[string]string{}
	var bad error
	err := tree.IterateRoot(func(ch *objecttree.Change, data []byte) (any, error) {
		if ch.Id == tree.Id() {
			return nil, nil
		}
		var batch []Operation
		if err := json.Unmarshal(data, &batch); err != nil {
			return nil, err
		}
		if len(batch) > MaxBatch {
			return nil, errors.New("batch too large")
		}
		for _, o := range batch {
			if err := config.validateOperation(o); err != nil {
				return nil, err
			}
		}
		return batch, nil
	}, func(ch *objecttree.Change) bool {
		if ch.Id == tree.Id() {
			return true
		}
		batch, ok := ch.Model.([]Operation)
		if !ok {
			bad = errors.New("invalid operation model")
			return false
		}
		if ch.DataType != config.Namespace || ch.IsSnapshot {
			bad = errors.New("unsupported change")
			return false
		}
		author, ok := members[ch.Identity.Account()]
		if !ok {
			bad = errors.New("unauthorized identity")
			return false
		}
		for _, o := range batch {
			if k := kinds[o.Record]; k != "" && k != o.Kind {
				bad = errors.New("record kind is immutable")
				return false
			}
			kinds[o.Record] = o.Kind
			if old, ok := ops[o.ID]; ok && (!SameOperation(old, o) || identities[o.ID] != ch.Identity.Account()) {
				bad = errors.New("operation ID reused")
				return false
			}
			ops[o.ID] = o
			authors[o.ID] = author
			identities[o.ID] = ch.Identity.Account()
		}
		return true
	})
	if err != nil {
		return nil, nil, err
	}
	if bad != nil {
		return nil, nil, bad
	}
	colors := map[string]int{}
	var visit func(string) error
	visit = func(id string) error {
		if colors[id] == 1 {
			return errors.New("cyclic replacement")
		}
		if colors[id] == 2 {
			return nil
		}
		colors[id] = 1
		o := ops[id]
		for _, p := range o.Replaces {
			old, ok := ops[p]
			if !ok || old.Record != o.Record || old.Kind != o.Kind || old.Field != o.Field {
				return errors.New("invalid replacement")
			}
			if err := visit(p); err != nil {
				return err
			}
		}
		colors[id] = 2
		return nil
	}
	for id := range ops {
		if err := visit(id); err != nil {
			return nil, nil, err
		}
	}
	return ops, authors, nil
}
func (e *Engine) Apply(o Operation) error { return e.ApplyBatch([]Operation{o}) }

// Merge validates identity, signatures and DAG structure through upstream code.
// Call on a clone and commit only after success: malformed batches never persist.
func (e *Engine) Merge(p Packet, expectedAccount string) error {
	if p.RootID != e.Tree.Id() || len(p.Changes) > MaxBatch || len(p.Heads) > 1000 {
		return errors.New("invalid sync packet")
	}
	builder := objecttree.NewChangeBuilder(crypto.NewKeyStorage(), e.Bundle.Root)
	pending := []*treechangeproto.RawTreeChangeWithId{}
	known := map[string]bool{}
	for id := range e.Store.Changes {
		known[id] = true
	}
	for _, raw := range p.Changes {
		if raw == nil || len(raw.RawChange) > 262144 {
			return errors.New("invalid change")
		}
		if known[raw.Id] {
			continue
		}
		ch, err := builder.Unmarshall(raw, true)
		if err != nil {
			return err
		}
		if ch.IsSnapshot || ch.IsDerived || ch.DataType != e.config.Namespace {
			return errors.New("unsupported change")
		}
		if expectedAccount != "" && ch.Identity.Account() != expectedAccount {
			return errors.New("wrong signing identity")
		}
		if _, ok := e.Bundle.Members[ch.Identity.Account()]; !ok {
			return errors.New("not a member")
		}
		known[raw.Id] = true
		pending = append(pending, raw)
	}

	for _, head := range p.Heads {
		if !known[head] {
			return errors.New("unknown head")
		}
	}
	for _, raw := range pending {
		ch, err := builder.Unmarshall(raw, true)
		if err != nil {
			return err
		}
		for _, prev := range ch.PreviousIds {
			if !known[prev] {
				return errors.New("missing parent")
			}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	e.Tree.Lock()
	_, err := e.Tree.AddRawChanges(context.Background(), objecttree.RawChangesPayload{NewHeads: p.Heads, RawChanges: pending})
	e.Tree.Unlock()
	if err != nil {
		return err
	}
	for _, raw := range pending {
		if !e.Tree.HasChanges(raw.Id) {
			return errors.New("unattached change")
		}
	}
	view, err := e.View()
	if err != nil {
		return err
	}
	return e.config.validateRecords(view)
}
func (e *Engine) View() (View, error) {
	e.Tree.Lock()
	defer e.Tree.Unlock()
	view, err := e.cache.update(e.config, e.Tree, e.Bundle.Members, e.Bundle.User)
	if err != nil {
		return View{}, err
	}
	return copyView(view), nil
}

func AtomicJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".sync-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
