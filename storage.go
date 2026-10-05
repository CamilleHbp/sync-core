package synccore

import (
	"context"
	"fmt"
	"sort"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
)

// MemoryStorage implements the upstream object-tree runtime storage contract.
// Database commits signed changes and projections transactionally to SQLite.
// History is retained without pruning or snapshots.
type MemoryStorage struct {
	RootID   string                              `json:"rootId"`
	HeadIDs  []string                            `json:"heads"`
	Snapshot string                              `json:"snapshot"`
	Changes  map[string]objecttree.StorageChange `json:"changes"`
	Seq      uint64                              `json:"seq"`
}

func NewStorage(root *treechangeproto.RawTreeChangeWithId) *MemoryStorage {
	return &MemoryStorage{RootID: root.Id, HeadIDs: []string{root.Id}, Snapshot: root.Id,
		Changes: map[string]objecttree.StorageChange{root.Id: {Id: root.Id, RawChange: root.RawChange, TreeId: root.Id, OrderId: "a", SnapshotCounter: 1, ChangeSize: len(root.RawChange)}}}
}
func (s *MemoryStorage) Id() string { return s.RootID }
func (s *MemoryStorage) Root(ctx context.Context) (objecttree.StorageChange, error) {
	return s.Get(ctx, s.RootID)
}
func (s *MemoryStorage) Heads(context.Context) ([]string, error) {
	return append([]string{}, s.HeadIDs...), nil
}
func (s *MemoryStorage) CommonSnapshot(context.Context) (string, error) { return s.Snapshot, nil }
func (s *MemoryStorage) Has(_ context.Context, id string) (bool, error) {
	_, ok := s.Changes[id]
	return ok, nil
}
func (s *MemoryStorage) Get(_ context.Context, id string) (objecttree.StorageChange, error) {
	v, ok := s.Changes[id]
	if !ok {
		return v, fmt.Errorf("missing change %s", id)
	}
	return v, nil
}
func (s *MemoryStorage) GetAfterOrder(ctx context.Context, order string, fn objecttree.StorageIterator) error {
	var cs []objecttree.StorageChange
	for _, v := range s.Changes {
		if v.OrderId >= order {
			cs = append(cs, v)
		}
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].OrderId < cs[j].OrderId })
	for _, v := range cs {
		more, err := fn(ctx, v)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}
func (s *MemoryStorage) GetAfterAddSeq(ctx context.Context, seq uint64, fn objecttree.StorageIterator) error {
	var cs []objecttree.StorageChange
	for _, v := range s.Changes {
		if v.AddSeq > seq {
			cs = append(cs, v)
		}
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].AddSeq < cs[j].AddSeq })
	for _, v := range cs {
		more, err := fn(ctx, v)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}
func (s *MemoryStorage) AddAll(_ context.Context, cs []objecttree.StorageChange, heads []string, snapshot string) error {
	for _, v := range cs {
		if old, ok := s.Changes[v.Id]; ok {
			v.AddSeq = old.AddSeq
		} else {
			s.Seq++
			v.AddSeq = s.Seq
		}
		s.Changes[v.Id] = v
	}
	s.HeadIDs = append([]string{}, heads...)
	s.Snapshot = snapshot
	return nil
}
func (s *MemoryStorage) AddAllNoError(ctx context.Context, cs []objecttree.StorageChange, heads []string, snapshot string) error {
	return s.AddAll(ctx, cs, heads, snapshot)
}
func (s *MemoryStorage) Delete(context.Context) error {
	return fmt.Errorf("history deletion is disabled")
}
func (s *MemoryStorage) Close() error { return nil }
