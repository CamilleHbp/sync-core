package privatenetwork

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace"
	spaceconfig "github.com/anyproto/any-sync/commonspace/config"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/syncstatus"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/node/nodeclient"
	"github.com/anyproto/any-sync/util/syncqueues"
)

var ErrHostClosed = errors.New("private space host is closed")

// Host supplies application-owned persistence and tree lifetime management to
// upstream commonspace. Attach before starting Runtime.App. Close the Host
// before the Runtime. Callers must stop using returned trees before Close.
// No legacy replica is modified or implicitly enrolled by this API.
type Host struct {
	runtime *Runtime
	service commonspace.SpaceService
	storage *storageProvider
	streams streampool.StreamPool
	mu      sync.Mutex
	spaces  map[string]*spaceEntry
	closed  bool
	ctx     context.Context
	cancel  context.CancelFunc
}
type spaceEntry struct {
	ready   chan struct{}
	space   commonspace.Space
	err     error
	treesMu sync.Mutex
	builder objecttreebuilder.TreeBuilder
	trees   map[string]objecttree.ObjectTree
}

func AttachSpaces(r *Runtime, directory string) (*Host, error) {
	if r == nil || r.App == nil || directory == "" {
		return nil, errors.New("runtime and private space directory are required")
	}
	if r.App.Component(treemanager.CName) != nil {
		return nil, errors.New("space host already attached")
	}
	h := &Host{runtime: r, service: commonspace.New(), storage: &storageProvider{directory: filepath.Join(directory, "spaces"), spaces: map[string]spacestorage.SpaceStorage{}}, streams: streampool.New(), spaces: map[string]*spaceEntry{}}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	r.App.Register(h.storage).Register(h).Register(&peerProvider{host: h}).
		Register(&credentials{runtime: r}).Register(&streamHandler{host: h}).
		Register(h.streams).Register(nodeclient.New()).Register(syncqueues.New()).Register(h.service)
	return h, nil
}
func (h *Host) Name() string { return treemanager.CName }
func (h *Host) Init(a *app.App) error {
	return spacesyncproto.DRPCRegisterSpaceSync(a.MustComponent(server.CName).(server.DRPCServer), &spaceRPC{host: h})
}
func (h *Host) Run(context.Context) error { return nil }

// CreateSpace persists an owner-signed space locally. Admission and replication
// happen through upstream commonspace/credential services, not custom HTTP.
func (h *Host) CreateSpace(ctx context.Context, payload spacepayloads.SpaceCreatePayload) (commonspace.Space, error) {
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return nil, ErrHostClosed
	}
	id, err := h.service.CreateSpace(ctx, payload)
	if err != nil {
		return nil, err
	}
	return h.OpenSpace(ctx, id)
}
func (h *Host) OpenSpace(ctx context.Context, id string) (commonspace.Space, error) {
	if id == "" {
		return nil, errors.New("space identity is required")
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, ErrHostClosed
	}
	e := h.spaces[id]
	if e != nil {
		h.mu.Unlock()
		select {
		case <-e.ready:
			return e.space, e.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	e = &spaceEntry{ready: make(chan struct{}), trees: map[string]objecttree.ObjectTree{}}
	h.spaces[id] = e
	h.mu.Unlock()
	sp, err := h.service.NewSpace(ctx, id, commonspace.Deps{SyncStatus: syncstatus.NewNoOpSyncStatus(), TreeSyncer: &treeSyncer{host: h, entry: e, spaceID: id}})
	if err == nil {
		err = sp.Init(ctx)
	}
	if err != nil && sp != nil {
		_ = sp.Close()
		sp = nil
	}
	h.mu.Lock()
	e.space, e.err = sp, err
	close(e.ready)
	if err != nil {
		delete(h.spaces, id)
	}
	h.mu.Unlock()
	return sp, err
}

// Lookup never pulls an unsolicited remote space. Only explicitly opened
// spaces can be addressed by incoming network traffic.
func (h *Host) lookup(ctx context.Context, id string) (commonspace.Space, error) {
	h.mu.Lock()
	e := h.spaces[id]
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return nil, ErrHostClosed
	}
	if e == nil {
		return nil, spacestorage.ErrSpaceStorageMissing
	}
	select {
	case <-e.ready:
		return e.space, e.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (h *Host) treeEntry(spaceID string) (*spaceEntry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrHostClosed
	}
	e := h.spaces[spaceID]
	if e == nil {
		return nil, spacestorage.ErrSpaceStorageMissing
	}
	return e, nil
}
func (h *Host) GetTree(ctx context.Context, spaceID, treeID string) (objecttree.ObjectTree, error) {
	e, err := h.treeEntry(spaceID)
	if err != nil {
		return nil, err
	}
	e.treesMu.Lock()
	defer e.treesMu.Unlock()
	if tr := e.trees[treeID]; tr != nil {
		return tr, nil
	}
	if e.builder == nil {
		return nil, errors.New("space tree builder is not ready")
	}
	// Upstream requires an explicit source for non-local trees. Preserve the
	// sender supplied by incoming messages; application opens use private nodes.
	if _, err := peer.CtxPeerId(ctx); errors.Is(err, peer.ErrPeerIdNotFoundInContext) {
		ctx = peer.CtxWithPeerId(ctx, peer.CtxResponsiblePeers)
	}
	tr, err := e.builder.BuildTree(ctx, treeID, objecttreebuilder.BuildTreeOpts{})
	if err == nil {
		e.trees[treeID] = tr
	}
	return tr, err
}

// PutTree imports through upstream signature/ACL validation and starts live
// broadcasting. Listener callbacks run under the tree lock; they must not
// re-enter the tree. Use callbacks to signal application projection work.
func (h *Host) PutTree(ctx context.Context, spaceID string, payload treestorage.TreeStorageCreatePayload, listener updatelistener.UpdateListener) (objecttree.ObjectTree, error) {
	e, err := h.treeEntry(spaceID)
	if err != nil {
		return nil, err
	}
	e.treesMu.Lock()
	defer e.treesMu.Unlock()
	id := payload.RootRawChange.GetId()
	if id == "" {
		return nil, errors.New("tree root is required")
	}
	if e.trees[id] != nil {
		return nil, errors.New("tree already open")
	}
	if e.builder == nil {
		return nil, errors.New("space tree builder is not ready")
	}
	sp, err := h.lookup(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	// PutTree upstream creates only the root; use its full-history validator
	// so imports never silently discard the signed changes in the payload.
	validated, err := objecttree.ValidateRawTreeDefault(payload, sp.Storage(), sp.Acl())
	if err != nil {
		return nil, err
	}
	if err = validated.Close(); err != nil {
		return nil, err
	}
	tr, err := e.builder.BuildTree(ctx, id, objecttreebuilder.BuildTreeOpts{Listener: listener})
	if err == nil {
		e.trees[id] = tr
	}
	return tr, err
}
func (h *Host) ValidateAndPutTree(ctx context.Context, spaceID string, payload treestorage.TreeStorageCreatePayload) error {
	_, err := h.PutTree(ctx, spaceID, payload, nil)
	return err
}
func (h *Host) MarkTreeDeleted(ctx context.Context, spaceID, treeID string) error {
	// Upstream's deletion manager owns the tombstone. Marking is not physical
	// removal; deletion workers later call DeleteTree after their safety checks.
	return nil
}
func (h *Host) DeleteTree(ctx context.Context, spaceID, treeID string) error {
	e, err := h.treeEntry(spaceID)
	if err != nil {
		return err
	}
	tr, err := h.GetTree(ctx, spaceID, treeID)
	if err != nil {
		return err
	}
	e.treesMu.Lock()
	defer e.treesMu.Unlock()
	if err = tr.Delete(); err != nil {
		return err
	}
	delete(e.trees, treeID)
	return tr.Close()
}

// Reconnect is for connectivity/resume events, not a timer. Normal changes use
// persistent upstream streams. Upstream anti-entropy remains a safety net.
func (h *Host) Reconnect(ctx context.Context) error {
	h.mu.Lock()
	ids := make([]string, 0, len(h.spaces))
	for id := range h.spaces {
		ids = append(ids, id)
	}
	h.mu.Unlock()
	var result error
	for _, id := range ids {
		sp, err := h.lookup(ctx, id)
		if err == nil {
			err = sp.SyncHeads(ctx)
		}
		result = errors.Join(result, err)
	}
	return result
}
func (h *Host) Close(ctx context.Context) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.cancel()
	entries := make([]*spaceEntry, 0, len(h.spaces))
	for _, e := range h.spaces {
		entries = append(entries, e)
	}
	h.mu.Unlock()
	var result error
	for _, e := range entries {
		<-e.ready
		if e.space != nil {
			result = errors.Join(result, e.space.Close())
		}
		e.treesMu.Lock()
		for _, tr := range e.trees {
			result = errors.Join(result, tr.Close())
		}
		e.treesMu.Unlock()
	}
	return errors.Join(result, h.storage.close())
}
func (c *runtimeConfig) GetSpace() spaceconfig.Config {
	return spaceconfig.Config{SyncPeriod: 600, KeepTreeDataInMemory: true}
}
func (c *runtimeConfig) GetStreamConfig() streampool.StreamConfig {
	return streampool.StreamConfig{SendQueueSize: 256, DialQueueWorkers: 4, DialQueueSize: 64}
}
