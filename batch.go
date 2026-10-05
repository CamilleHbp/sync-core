package synccore

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/util/crypto"
	"sort"
)

// AncestorIDs returns the requested local changes and their dependency closure
// in insertion order, ready for bounded replay after a server restoration.
func (e *Engine) AncestorIDs(ids []string) ([]string, error) {
	queue := append([]string{}, ids...)
	seen := map[string]bool{e.Store.RootID: true}
	result := []string{}
	builder := objecttree.NewChangeBuilder(crypto.NewKeyStorage(), e.Bundle.Root)
	for len(queue) > 0 {
		id := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		stored, ok := e.Store.Changes[id]
		if !ok {
			return nil, errors.New("required ancestor is unavailable on this device")
		}
		change, err := builder.Unmarshall(stored.RawTreeChangeWithId(), true)
		if err != nil {
			return nil, err
		}
		queue = append(queue, change.PreviousIds...)
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return e.Store.Changes[result[i]].AddSeq < e.Store.Changes[result[j]].AddSeq })
	return result, nil
}

func (e *Engine) ApplyBatch(batch []Operation) error {
	if len(batch) > MaxBatch {
		return errors.New("batch too large")
	}
	ops, authors, err := e.Operations()
	if err != nil {
		return err
	}
	kinds := map[string]string{}
	for _, o := range ops {
		kinds[o.Record] = o.Kind
	}
	fresh := []Operation{}
	for _, o := range batch {
		if err = e.config.validateOperation(o); err != nil {
			return err
		}
		if old, ok := ops[o.ID]; ok {
			if !SameOperation(old, o) {
				return errors.New("operation ID reused")
			}
			continue
		}
		if kind := kinds[o.Record]; kind != "" && kind != o.Kind {
			return errors.New("record kind is immutable")
		}
		kinds[o.Record] = o.Kind
		for _, id := range o.Replaces {
			p, ok := ops[id]
			if !ok || p.Record != o.Record || p.Kind != o.Kind || p.Field != o.Field || p.ID == o.ID {
				return errors.New("invalid replaced version")
			}
		}
		ops[o.ID] = o
		authors[o.ID] = e.Bundle.User
		fresh = append(fresh, o)
	}
	if err = e.config.validateRecords(Materialize(ops, authors, e.Bundle.User)); err != nil {
		return err
	}
	e.Tree.Lock()
	defer e.Tree.Unlock()
	if len(fresh) > 0 {
		b, _ := json.Marshal(fresh)
		if len(b) > 200000 {
			return errors.New("transaction too large")
		}
		if _, err = e.Tree.AddContent(context.Background(), objecttree.SignableChangeContent{Data: b, Key: e.Keys.SignKey, ShouldBeEncrypted: true, DataType: e.config.Namespace}); err != nil {
			return err
		}
	}
	return nil
}
func (e *Engine) Missing(p Packet) ([]string, error) {
	if len(p.Changes) > MaxBatch {
		return nil, errors.New("batch too large")
	}
	known := map[string]bool{}
	for id := range e.Store.Changes {
		known[id] = true
	}
	for _, c := range p.Changes {
		if c == nil {
			return nil, errors.New("nil change")
		}
		known[c.Id] = true
	}
	builder := objecttree.NewChangeBuilder(crypto.NewKeyStorage(), e.Bundle.Root)
	need := []string{}
	seen := map[string]bool{}
	for _, c := range p.Changes {
		ch, err := builder.Unmarshall(c, true)
		if err != nil {
			return nil, err
		}
		for _, id := range ch.PreviousIds {
			if !known[id] && !seen[id] {
				need = append(need, id)
				seen[id] = true
			}
		}
	}
	return need, nil
}
