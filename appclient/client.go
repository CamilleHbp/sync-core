// Package appclient binds legacy pairing to native private-network replicas.
// It never falls back to writing a legacy replica after native activation.
package appclient

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	core "github.com/CamilleHbp/sync-core"
	"github.com/CamilleHbp/sync-core/privateclient"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/util/crypto"
)

// Provision is delivered only by an authenticated, TLS-pinned application
// homeserver. It contains public network metadata, never service private keys.
type Provision struct {
	NetworkConfig []byte                         `json:"networkConfig"`
	NetworkID     string                         `json:"networkId"`
	Owner         string                         `json:"owner"`
	Workspace     privateclient.SignedConnection `json:"workspace"`
}
type activation struct {
	Provision       Provision `json:"provision"`
	LegacyWorkspace string    `json:"legacyWorkspace"`
}
type Client struct {
	checking         sync.Mutex
	health           *core.SyncTracker
	upgradeAttempted bool
	mu               sync.RWMutex
	legacy           *core.Client
	native           *privateclient.Client
	config           core.Config
	directory        string
	events           chan struct{}
	ctx              context.Context
	cancel           context.CancelFunc
	watchCancel      context.CancelFunc
	closed           bool
}

func New(directory string, config core.Config) (*Client, error) {
	legacy, err := core.NewClient(directory, config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{health: core.NewSyncTracker(filepath.Join(directory, "sync-health.json")), legacy: legacy, config: config, directory: directory, events: make(chan struct{}, 1), ctx: ctx, cancel: cancel}
	data, err := os.ReadFile(filepath.Join(directory, "native-active.json"))
	if err == nil {
		var saved activation
		if err = json.Unmarshal(data, &saved); err == nil {
			var state core.ClientView
			state, err = legacy.View()
			if err == nil && state.Workspace != saved.LegacyWorkspace {
				err = core.NewError("identity_mismatch", nil)
			}
			if err == nil {
				err = c.openNative(ctx, saved.Provision)
			}
		}
	} else if os.IsNotExist(err) {
		err = nil
	}
	if err != nil {
		c.Close()
		return nil, err
	}
	c.startWatch()
	return c, nil
}
func (c *Client) signal() {
	select {
	case c.events <- struct{}{}:
	default:
	}
}
func (c *Client) Changes() <-chan struct{} { return c.events }
func (c *Client) Done() <-chan struct{}    { return c.ctx.Done() }
func (c *Client) startWatch() {
	if c.watchCancel != nil {
		c.watchCancel()
	}
	ctx, cancel := context.WithCancel(c.ctx)
	c.watchCancel = cancel
	native := c.native
	go func() {
		if native != nil {
			for {
				select {
				case <-ctx.Done():
					return
				case <-native.Changes():
					c.signal()
				case <-native.Done():
					return
				}
			}
		}
		_ = c.legacy.WatchUpdates(ctx, c.signal)
	}()
}
func (c *Client) openNative(ctx context.Context, p Provision) error {
	source, err := c.legacy.Snapshot()
	if err != nil {
		return err
	}
	peer, err := accountdata.NewRandom()
	if err != nil {
		return err
	}
	key, err := crypto.UnmarshalEd25519PrivateKeyProto(source.Bundle.Key)
	if err != nil {
		return err
	}
	native, err := privateclient.Open(ctx, filepath.Join(c.directory, "native"), c.config, p.NetworkConfig, p.NetworkID, p.Owner, accountdata.New(peer.PeerKey, key), p.Workspace)
	if err != nil {
		return err
	}
	c.native = native
	return nil
}
func (c *Client) Connect(ctx context.Context, origin, code string) error {
	c.health.Invalidate()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("client closed")
	}
	if c.native != nil {
		return c.native.Reconnect(ctx)
	}
	if err := c.legacy.Connect(ctx, origin, code); err != nil {
		return err
	}
	c.startWatch()
	c.signal()
	return c.upgrade(ctx)
}

// upgrade is attempted on pairing and once per process startup, never periodically. It fails closed:
// all old operations must be present before the durable activation marker.
func (c *Client) upgrade(ctx context.Context) error {
	var p Provision
	if err := c.legacy.Provision(ctx, &p); err != nil {
		if core.ErrorCode(err) == "not_found" {
			return nil
		}
		return err
	}
	if p.NetworkID == "" {
		return nil
	}
	if err := p.Workspace.Verify(c.config.Namespace, p.NetworkID, p.Owner); err != nil {
		return err
	}
	if err := c.legacy.Sync(ctx); err != nil {
		return err
	}
	view, err := c.legacy.View()
	if err != nil {
		return err
	}
	if view.Pending != 0 {
		return core.NewError("sync_incomplete", nil)
	}
	source, err := c.legacy.Snapshot()
	if err != nil {
		return err
	}
	if err = c.openNative(ctx, p); err != nil {
		return err
	}
	err = c.native.VerifyLegacy(source)
	if err == nil {
		err = core.AtomicJSON(filepath.Join(c.directory, "native-active.json"), activation{p, view.Workspace})
	}
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = c.native.Close(closeCtx)
		c.native = nil
		return err
	}
	c.startWatch()
	c.signal()
	return nil
}
func (c *Client) View() (core.ClientView, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return core.ClientView{}, errors.New("client closed")
	}
	old, err := c.legacy.View()
	if err != nil || c.native == nil {
		old.SyncStatus = c.health.Snapshot(old)
		return old, err
	}
	view, err := c.native.View()
	if err != nil {
		return old, err
	}
	old.Records = view.Records
	// -1 means that node durability is not measured, not "zero pending".
	old.Pending = -1
	old.SyncStatus = c.health.Snapshot(old)
	if old.SyncStatus.Confirmed {
		old.Pending = 0
	}
	return old, nil
}
func (c *Client) Apply(ctx context.Context, ops []core.Operation) error {
	c.health.Invalidate()
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return errors.New("client closed")
	}
	var err error
	if c.native != nil {
		err = c.native.Apply(ctx, ops)
	} else {
		err = c.legacy.Apply(ops)
	}
	return err
}
func (c *Client) syncTransport(ctx context.Context) error {
	c.mu.Lock()
	if !c.closed && c.native == nil && !c.upgradeAttempted {
		c.upgradeAttempted = true
		if err := c.upgrade(ctx); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.mu.Unlock()
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return errors.New("client closed")
	}
	if c.native != nil {
		return c.native.Reconnect(ctx)
	}
	return c.legacy.Sync(ctx)
}
func (c *Client) Close() error {
	c.cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var err error
	if c.native != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		err = c.native.Close(ctx)
	}
	return errors.Join(err, c.legacy.Close())
}

// Sync reconnects transport and checks the server's durable checkpoint. A live
// native peer alone never establishes server confirmation.
func (c *Client) Sync(ctx context.Context) error { return c.check(ctx, true) }

// Check refreshes confirmation without repeatedly reconnecting a live native
// transport. Foreground clients may call this periodically; legacy clients sync.
func (c *Client) Check(ctx context.Context) error { return c.check(ctx, false) }

func (c *Client) check(ctx context.Context, reconnect bool) error {
	c.checking.Lock()
	defer c.checking.Unlock()
	generation := c.health.Begin()
	c.mu.RLock()
	native := c.native != nil
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return errors.New("client closed")
	}
	var err error
	if reconnect || !native {
		err = c.syncTransport(ctx)
	}
	view, viewErr := c.View()
	if err == nil {
		err = viewErr
	}
	// syncTransport may have activated the native replica on this call.
	c.mu.RLock()
	native = c.native != nil
	c.mu.RUnlock()
	confirmed, supported := view.Pending == 0, true
	if err == nil && native {
		var checkpoint core.CheckpointResponse
		checkpoint, supported, err = c.legacy.Checkpoint(ctx)
		// Read again after HTTP: intervening remote updates invalidate the proof.
		if err == nil {
			view, err = c.View()
		}
		confirmed = supported && err == nil && checkpoint.Digest == core.RecordsDigest(view.Records)
	}
	c.health.Complete(view, generation, confirmed, supported, err)
	return err
}
