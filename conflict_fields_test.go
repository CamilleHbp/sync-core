package synccore

import "testing"

func TestShoppingFieldConflictsConvergeAndResolve(t *testing.T) {
	a, bundles, list := seed(t)
	product, entry, price := RandomID(), RandomID(), RandomID()
	ops := []Operation{}
	add := func(id, kind string, values map[string]string) {
		for f, v := range values {
			ops = append(ops, Operation{ID: RandomID(), Record: id, Kind: kind, Field: f, Value: v, Replaces: []string{}})
		}
	}
	add(product, "product", map[string]string{"name": "Rice", "archived": "false"})
	add(price, "price", map[string]string{"product": product, "currency": "EUR", "amount": "2.00"})
	add(entry, "entry", map[string]string{"list": list, "product": product, "quantity": "1", "unit": "item", "done": "false"})
	must(t, a.ApplyBatch(ops))
	b, err := Open(testConfig, bundles[1], nil)
	must(t, err)
	must(t, b.Merge(a.Packet(), ""))
	fields := []struct{ id, kind, field, left, right string }{
		{entry, "entry", "quantity", "2", "3"},
		{entry, "entry", "done", "true", "false"},
		{price, "price", "amount", "2.10", "2.20"},
		{product, "product", "archived", "true", "false"},
	}
	for _, f := range fields {
		old := field(t, a, f.id, f.field).Fields[f.field][0].ID
		must(t, a.Apply(Operation{ID: RandomID(), Record: f.id, Kind: f.kind, Field: f.field, Value: f.left, Replaces: []string{old}}))
		must(t, b.Apply(Operation{ID: RandomID(), Record: f.id, Kind: f.kind, Field: f.field, Value: f.right, Replaces: []string{old}}))
	}
	must(t, a.Merge(b.Packet(), ""))
	must(t, b.Merge(a.Packet(), ""))
	for _, f := range fields {
		for _, replica := range []*Engine{a, b} {
			if !field(t, replica, f.id, f.field).Conflict(f.field) {
				t.Fatalf("lost %s conflict", f.field)
			}
		}
		versions := []string{}
		for _, v := range field(t, b, f.id, f.field).Fields[f.field] {
			versions = append(versions, v.ID)
		}
		must(t, b.Apply(Operation{ID: RandomID(), Record: f.id, Kind: f.kind, Field: f.field, Value: f.right, Replaces: versions}))
	}
	must(t, a.Merge(b.Packet(), ""))
	for _, f := range fields {
		for _, replica := range []*Engine{a, b} {
			r := field(t, replica, f.id, f.field)
			if r.Conflict(f.field) || r.Value(f.field) != f.right {
				t.Fatalf("resolution diverged for %s", f.field)
			}
		}
	}
}
