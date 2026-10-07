// Package privateclient provides an application-facing, push-driven client for
// an explicitly trusted self-hosted any-sync network. It has no HTTPS exchange
// loop, public-network fallback, or implicit legacy migration.
package privateclient

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	core "github.com/CamilleHbp/sync-core"
	"github.com/CamilleHbp/sync-core/privatenetwork"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/util/crypto"
)

// Connection is public workspace metadata. Deliver it over the application's
// authenticated pairing channel; neither its owner nor its network ID is a
// trust anchor until independently approved by the caller. No private keys,
// tokens, or network service credentials belong here.
type Connection struct {
	Version   int               `json:"version"`
	Namespace string            `json:"namespace"`
	NetworkID string            `json:"networkId"`
	SpaceID   string            `json:"spaceId"`
	TreeID    string            `json:"treeId"`
	Owner     string            `json:"owner"`
	Members   map[string]string `json:"members"`
}
type SignedConnection struct {
	Connection Connection `json:"connection"`
	Signature  []byte     `json:"signature"`
}

// SignConnection is for the provisioning owner. Open also verifies that this
// signer actually owns the network-accepted ACL and signed the selected tree.
func SignConnection(connection Connection, key crypto.PrivKey) (SignedConnection, error) {
	if key == nil || key.GetPublic().Account() != connection.Owner {
		return SignedConnection{}, core.NewError("invalid_identity", nil)
	}
	if err := connection.validate(); err != nil {
		return SignedConnection{}, err
	}
	connection.Members = maps.Clone(connection.Members)
	data, err := json.Marshal(connection)
	if err != nil {
		return SignedConnection{}, err
	}
	signature, err := key.Sign(data)
	return SignedConnection{Connection: connection, Signature: signature}, err
}
func (c Connection) validate() error {
	if c.Version != 1 || strings.TrimSpace(c.Namespace) == "" || c.SpaceID == "" || c.TreeID == "" || len(c.Members) == 0 || len(c.Members) > 100 || c.Members[c.Owner] == "" {
		return core.NewError("invalid_workspace", nil)
	}
	if _, err := crypto.DecodeNetworkId(c.NetworkID); err != nil {
		return core.NewError("invalid_workspace", err)
	}
	for account, label := range c.Members {
		if _, err := crypto.DecodeAccountAddress(account); err != nil {
			return core.NewError("invalid_identity", err)
		}
		if strings.TrimSpace(label) == "" || len(label) > 256 {
			return core.NewError("invalid_identity", nil)
		}
	}
	return nil
}
func (s SignedConnection) Verify(namespace, networkID, owner string) error {
	if namespace != s.Connection.Namespace || networkID == "" || networkID != s.Connection.NetworkID || owner == "" || owner != s.Connection.Owner {
		return core.NewError("identity_mismatch", nil)
	}
	if err := s.Connection.validate(); err != nil {
		return err
	}
	public, err := crypto.DecodeAccountAddress(owner)
	if err != nil {
		return core.NewError("invalid_identity", err)
	}
	data, err := json.Marshal(s.Connection)
	if err != nil {
		return err
	}
	valid, err := public.Verify(data, s.Signature)
	if err != nil || !valid {
		return core.NewError("invalid_identity", err)
	}
	return nil
}

type Client struct {
	mu      sync.RWMutex
	runtime *privatenetwork.Runtime
	host    *privatenetwork.Host
	replica *core.Replica
	changes <-chan struct{}
	done    chan struct{}
	closed  bool
}

// Open pins this directory to the account, application and signed connection.
// Keys come from the application's own key storage and are never persisted by
// this wrapper. networkID and owner must come from trusted provisioning, not be
// copied unconditionally from an untrusted SignedConnection.
func Open(ctx context.Context, directory string, config core.Config, networkConfig []byte, networkID, owner string, keys *accountdata.AccountKeys, signed SignedConnection) (client *Client, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if config.ValidateOperation == nil || strings.TrimSpace(config.Namespace) == "" {
		return nil, core.NewError("invalid_config", nil)
	}
	if err = signed.Verify(config.Namespace, networkID, owner); err != nil {
		return nil, err
	}
	if keys == nil || keys.SignKey == nil || keys.PeerKey == nil || keys.PeerId != keys.PeerKey.GetPublic().PeerId() {
		return nil, core.NewError("invalid_identity", nil)
	}
	account := keys.SignKey.GetPublic().Account()
	if signed.Connection.Members[account] == "" {
		return nil, core.NewError("invalid_identity", nil)
	}
	if _, err = privatenetwork.ParseConfig(networkConfig, networkID); err != nil {
		return nil, err
	}
	if directory == "" {
		return nil, core.NewError("invalid_config", nil)
	}
	// Snapshot mutable caller maps/slices before retaining or using them.
	raw, err := json.Marshal(signed)
	if err != nil {
		return nil, err
	}
	signed = SignedConnection{}
	if err = json.Unmarshal(raw, &signed); err != nil {
		return nil, err
	}
	binding := struct {
		Account   string           `json:"account"`
		Workspace SignedConnection `json:"workspace"`
	}{account, signed}
	if err = pinBinding(directory, binding); err != nil {
		return nil, err
	}
	runtime, err := privatenetwork.New(networkConfig, networkID, directory, keys)
	if err != nil {
		return nil, err
	}
	host, err := privatenetwork.AttachSpaces(runtime, directory)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			err = errors.Join(err, host.Close(closeCtx), runtime.Close(closeCtx))
		}
	}()
	if err = runtime.App.Start(ctx); err != nil {
		return nil, err
	}
	connection := signed.Connection
	space, err := host.OpenSpace(ctx, connection.SpaceID)
	if err != nil {
		return nil, err
	}
	space.Acl().RLock()
	actualOwner, ownerErr := space.Acl().AclState().OwnerPubKey()
	permission := space.Acl().AclState().Permissions(keys.SignKey.GetPublic())
	space.Acl().RUnlock()
	if ownerErr != nil || actualOwner == nil || actualOwner.Account() != owner || permission.NoPermissions() {
		return nil, core.NewError("invalid_identity", ownerErr)
	}
	tree, err := host.GetTree(ctx, connection.SpaceID, connection.TreeID)
	if err != nil {
		return nil, err
	}
	root, err := objecttree.UnmarshallRoot(tree.Header())
	if err != nil {
		return nil, err
	}
	rootKey, err := crypto.UnmarshalEd25519PublicKeyProto(root.Identity)
	if err != nil {
		return nil, err
	}
	if root.SpaceId != connection.SpaceID || !rootKey.Equals(actualOwner) || root.ChangeType != config.Namespace {
		return nil, core.NewError("identity_mismatch", nil)
	}
	live, ok := tree.(synctree.SyncTree)
	if !ok {
		return nil, errors.New("upstream live tree required")
	}
	replica, err := core.AttachReplica(config, live, keys, connection.Members[account], connection.Members)
	if err != nil {
		return nil, err
	}
	// Space.Init already starts upstream's initial reconciliation. Do not make
	// opening a persisted replica depend on reaching a peer: its local view and
	// queued edits must remain available offline. Reconnect exposes explicit
	// connectivity/resume recovery without gating local application startup.
	client = &Client{runtime: runtime, host: host, replica: replica, changes: replica.Changes(), done: make(chan struct{})}
	success = true
	return client, nil
}

func (c *Client) ReconcileLegacy(ctx context.Context, source *core.Engine, bundles []core.Bundle) (*core.Engine, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, privatenetwork.ErrHostClosed
	}
	return c.replica.ReconcileLegacy(ctx, source, bundles)
}

func (c *Client) VerifyLegacy(source *core.Engine) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return privatenetwork.ErrHostClosed
	}
	return c.replica.VerifyLegacy(source)
}

func (c *Client) Changes() <-chan struct{} { return c.changes }
func (c *Client) Done() <-chan struct{}    { return c.done }
func (c *Client) View() (core.View, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return core.View{}, privatenetwork.ErrHostClosed
	}
	return c.replica.View()
}
func (c *Client) Apply(ctx context.Context, operations []core.Operation) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return privatenetwork.ErrHostClosed
	}
	return c.replica.Apply(ctx, operations)
}

// Reconnect is called by connectivity/resume events, never a polling timer.
func (c *Client) Reconnect(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return privatenetwork.ErrHostClosed
	}
	return c.host.Reconnect(ctx)
}
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.done)
	return errors.Join(c.host.Close(ctx), c.runtime.Close(ctx))
}

// Publish a complete binding without replacing any existing binding, including
// when two Open calls race. Interrupted publication fails closed on next open.
func pinBinding(directory string, binding any) error {
	data, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	path := filepath.Join(directory, "connection.json")
	compare := func() error {
		existing, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(existing) != string(data) {
			return core.NewError("identity_mismatch", nil)
		}
		return nil
	}
	if _, err = os.Lstat(path); err == nil {
		return compare()
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.CreateTemp(directory, ".connection-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = publishBinding(f.Name(), path); os.IsExist(err) {
		return compare()
	} else if err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
