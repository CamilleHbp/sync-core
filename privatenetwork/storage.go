package privatenetwork

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
)

// Each space has a separate upstream database. IDs never become path components.
type storageProvider struct {
	directory string
	mu        sync.Mutex
	spaces    map[string]spacestorage.SpaceStorage
}

func (s *storageProvider) Name() string        { return spacestorage.CName }
func (s *storageProvider) Init(*app.App) error { return os.MkdirAll(s.directory, 0700) }
func (s *storageProvider) path(id string) string {
	return filepath.Join(s.directory, fmt.Sprintf("%x", sha256.Sum256([]byte(id))), "space.sqlite")
}
func (s *storageProvider) SpaceExists(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.spaces[id] != nil {
		return true
	}
	_, err := os.Stat(s.path(id))
	return err == nil
}
func (s *storageProvider) WaitSpaceStorage(ctx context.Context, id string) (spacestorage.SpaceStorage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.spaces[id]; st != nil {
		return st, nil
	}
	if _, err := os.Stat(s.path(id)); os.IsNotExist(err) {
		return nil, spacestorage.ErrSpaceStorageMissing
	} else if err != nil {
		return nil, err
	}
	db, err := openSpaceDB(ctx, s.path(id))
	if err != nil {
		return nil, err
	}
	st, err := spacestorage.New(ctx, id, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.spaces[id] = st
	return st, nil
}
func openSpaceDB(ctx context.Context, path string) (anystore.DB, error) {
	return anystore.Open(ctx, path, &anystore.Config{ReadConnections: 2, SQLiteConnectionOptions: map[string]string{"synchronous": "FULL"}, Durability: anystore.DurabilityConfig{Sentinel: true}})
}
func (s *storageProvider) CreateSpaceStorage(ctx context.Context, payload spacestorage.SpaceStorageCreatePayload) (spacestorage.SpaceStorage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := payload.SpaceHeaderWithId.Id
	if s.spaces[id] != nil {
		return nil, spacestorage.ErrSpaceStorageExists
	}
	path := s.path(id)
	err := os.Mkdir(filepath.Dir(path), 0700)
	if os.IsExist(err) {
		return nil, spacestorage.ErrSpaceStorageExists
	}
	if err != nil {
		return nil, err
	}
	db, err := openSpaceDB(ctx, path)
	if err != nil {
		_ = os.RemoveAll(filepath.Dir(path))
		return nil, err
	}
	st, err := spacestorage.Create(ctx, db, payload)
	if err != nil {
		_ = db.Close()
		_ = os.RemoveAll(filepath.Dir(path))
		return nil, err
	}
	s.spaces[id] = st
	return st, nil
}
func (s *storageProvider) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result error
	for id, st := range s.spaces {
		if err := st.AnyStore().Close(); err != nil {
			result = err
		}
		delete(s.spaces, id)
	}
	return result
}
