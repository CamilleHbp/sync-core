package synccore

import (
	"encoding/json"
	"errors"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"maps"
	"slices"
	"sort"
)

// projectionCache is guarded by the tree lock. Signed changes are immutable.
// A speculative pre-commit projection is only reusable while its heads remain
// attached; upstream rollback therefore invalidates it before a reader sees it.
type projectionCache struct {
	heads      []string
	seen       map[string]bool
	ops        map[string]Operation
	authors    map[string]string
	identities map[string]string
	kinds      map[string]string
	records    map[string]Record
	view       View
}

func (p *projectionCache) update(config Config, tree objecttree.ObjectTree, members map[string]string, user string) (View, error) {
	if p.seen != nil && slices.Equal(p.heads, tree.Heads()) {
		return p.view, nil
	}
	if p.seen != nil && !tree.HasChanges(p.heads...) {
		*p = projectionCache{}
	}
	if p.seen == nil {
		p.seen = map[string]bool{tree.Id(): true}
		p.ops = map[string]Operation{}
		p.authors = map[string]string{}
		p.identities = map[string]string{}
		p.kinds = map[string]string{}
		p.records = map[string]Record{}
	}
	pending := map[string]*objecttree.Change{}
	stack := append([]string{}, tree.Heads()...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if p.seen[id] || pending[id] != nil {
			continue
		}
		ch, err := tree.GetChange(id)
		if err != nil {
			return View{}, err
		}
		pending[id] = ch
		stack = append(stack, ch.PreviousIds...)
	}
	delta := map[string]Operation{}
	authors := map[string]string{}
	identities := map[string]string{}
	kinds := map[string]string{}
	for id, ch := range pending {
		if ch.DataType != config.Namespace || ch.IsSnapshot {
			return View{}, errors.New("unsupported change")
		}
		author, ok := members[ch.Identity.Account()]
		if !ok {
			return View{}, errors.New("unauthorized identity")
		}
		err := tree.IterateFrom(id, func(_ *objecttree.Change, data []byte) (any, error) {
			var batch []Operation
			err := json.Unmarshal(data, &batch)
			return batch, err
		}, func(*objecttree.Change) bool { return false })
		if err != nil {
			return View{}, err
		}
		batch, ok := ch.Model.([]Operation)
		if !ok {
			return View{}, errors.New("invalid operation model")
		}
		if len(batch) > MaxBatch {
			return View{}, errors.New("batch too large")
		}
		for _, op := range batch {
			if err := config.validateOperation(op); err != nil {
				return View{}, err
			}
			old, exists := p.ops[op.ID]
			identity := p.identities[op.ID]
			if !exists {
				old, exists = delta[op.ID]
				identity = identities[op.ID]
			}
			if exists {
				if !SameOperation(old, op) || identity != ch.Identity.Account() {
					return View{}, errors.New("operation ID reused")
				}
				continue
			}
			kind := p.kinds[op.Record]
			if kind == "" {
				kind = kinds[op.Record]
			}
			if kind != "" && kind != op.Kind {
				return View{}, errors.New("record kind is immutable")
			}
			delta[op.ID] = op
			authors[op.ID] = author
			identities[op.ID] = ch.Identity.Account()
			kinds[op.Record] = op.Kind
		}
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
		op, exists := delta[id]
		if !exists {
			return nil
		}
		colors[id] = 1
		for _, replacement := range op.Replaces {
			old, ok := delta[replacement]
			if !ok {
				old, ok = p.ops[replacement]
			}
			if !ok || old.Record != op.Record || old.Kind != op.Kind || old.Field != op.Field {
				return errors.New("invalid replacement")
			}
			if err := visit(replacement); err != nil {
				return err
			}
		}
		colors[id] = 2
		return nil
	}
	for id := range delta {
		if err := visit(id); err != nil {
			return View{}, err
		}
	}
	changed, next := p.project(delta, authors, user)
	if err := config.validateRecords(next); err != nil {
		return View{}, err
	}
	for id := range pending {
		p.seen[id] = true
	}
	maps.Copy(p.ops, delta)
	maps.Copy(p.authors, authors)
	maps.Copy(p.identities, identities)
	maps.Copy(p.kinds, kinds)
	maps.Copy(p.records, changed)
	p.heads = append([]string{}, tree.Heads()...)
	p.view = next
	return next, nil
}
func copyView(view View) View {
	out := view
	out.Records = make([]Record, len(view.Records))
	for i, record := range view.Records {
		record.Fields = maps.Clone(record.Fields)
		for field, versions := range record.Fields {
			versions = append([]Version{}, versions...)
			for j := range versions {
				if versions[j].Source != nil {
					source := *versions[j].Source
					versions[j].Source = &source
				}
			}
			record.Fields[field] = versions
		}
		out.Records[i] = record
	}
	return out
}

func (p *projectionCache) project(delta map[string]Operation, authors map[string]string, user string) (map[string]Record, View) {
	changed := map[string]Record{}
	superseded := map[string]bool{}
	for _, op := range delta {
		for _, id := range op.Replaces {
			superseded[id] = true
		}
	}
	for _, op := range delta {
		record, ok := changed[op.Record]
		if !ok {
			record = p.records[op.Record]
			record.ID = op.Record
			record.Kind = op.Kind
			record.Fields = maps.Clone(record.Fields)
			if record.Fields == nil {
				record.Fields = map[string][]Version{}
			}
		}
		versions := append([]Version{}, record.Fields[op.Field]...)
		if !superseded[op.ID] {
			versions = append(versions, Version{ID: op.ID, Value: op.Value, Author: authors[op.ID], Source: op.Source})
		}
		versions = slices.DeleteFunc(versions, func(v Version) bool { return superseded[v.ID] })
		sort.Slice(versions, func(i, j int) bool { return versions[i].ID < versions[j].ID })
		if len(versions) == 0 {
			delete(record.Fields, op.Field)
		} else {
			record.Fields[op.Field] = versions
		}
		changed[op.Record] = record
	}
	next := View{User: user, Changes: len(p.ops) + len(delta), Records: make([]Record, 0, len(p.records)+len(changed))}
	for id, record := range p.records {
		if replacement, ok := changed[id]; ok {
			record = replacement
		}
		next.Records = append(next.Records, record)
	}
	for id, record := range changed {
		if _, ok := p.records[id]; !ok {
			next.Records = append(next.Records, record)
		}
	}
	sort.Slice(next.Records, func(i, j int) bool { return next.Records[i].ID < next.Records[j].ID })

	return changed, next
}

func (p *projectionCache) prepare(config Config, user string, batch []Operation) ([]Operation, error) {
	if len(batch) > MaxBatch {
		return nil, errors.New("batch too large")
	}
	delta := map[string]Operation{}
	authors := map[string]string{}
	kinds := map[string]string{}
	fresh := []Operation{}
	for _, op := range batch {
		if err := config.validateOperation(op); err != nil {
			return nil, err
		}
		old, exists := p.ops[op.ID]
		if !exists {
			old, exists = delta[op.ID]
		}
		if exists {
			if !SameOperation(old, op) {
				return nil, errors.New("operation ID reused")
			}
			continue
		}
		kind := p.kinds[op.Record]
		if kind == "" {
			kind = kinds[op.Record]
		}
		if kind != "" && kind != op.Kind {
			return nil, errors.New("record kind is immutable")
		}
		for _, id := range op.Replaces {
			previous, ok := p.ops[id]
			if !ok {
				previous, ok = delta[id]
			}
			if !ok || previous.Record != op.Record || previous.Kind != op.Kind || previous.Field != op.Field || id == op.ID {
				return nil, errors.New("invalid replaced version")
			}
		}
		delta[op.ID] = op
		authors[op.ID] = user
		kinds[op.Record] = op.Kind
		fresh = append(fresh, op)
	}
	_, candidate := p.project(delta, authors, user)
	if err := config.validateRecords(candidate); err != nil {
		return nil, err
	}
	return fresh, nil
}
