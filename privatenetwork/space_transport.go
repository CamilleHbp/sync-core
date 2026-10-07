package privatenetwork

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/credentialprovider"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/treesyncer"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/peermanager"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/streampool/streamhandler"
	"storj.io/drpc"
)

type credentials struct{ runtime *Runtime }

func (*credentials) Name() string        { return credentialprovider.CName }
func (*credentials) Init(*app.App) error { return nil }
func (c *credentials) GetCredential(ctx context.Context, header *spacesyncproto.RawSpaceHeaderWithId) ([]byte, error) {
	receipt, err := c.runtime.Coordinator.SpaceSign(ctx, coordinatorclient.SpaceSignPayload{SpaceId: header.Id, SpaceHeader: header.RawHeader})
	if err != nil {
		return nil, err
	}
	return receipt.MarshalVT()
}

type peerProvider struct{ host *Host }

func (*peerProvider) Name() string        { return peermanager.CName }
func (*peerProvider) Init(*app.App) error { return nil }
func (p *peerProvider) NewPeerManager(_ context.Context, spaceID string) (peermanager.PeerManager, error) {
	return &spacePeers{host: p.host, spaceID: spaceID}, nil
}

type spacePeers struct {
	host    *Host
	spaceID string
}

func (*spacePeers) Name() string        { return peermanager.CName }
func (*spacePeers) Init(*app.App) error { return nil }
func (p *spacePeers) GetResponsiblePeers(ctx context.Context) ([]peer.Peer, error) {
	var peers []peer.Peer
	var result error
	for _, id := range p.host.runtime.Nodes.NodeIds(p.spaceID) {
		remote, err := p.host.runtime.Pool.Get(ctx, id)
		if err != nil {
			result = errors.Join(result, err)
		} else {
			peers = append(peers, remote)
		}
	}
	if len(peers) > 0 {
		return peers, nil
	}
	if result == nil {
		result = errors.New("no private space peers")
	}
	return nil, result
}
func (p *spacePeers) GetNodePeers(ctx context.Context) ([]peer.Peer, error) {
	return p.GetResponsiblePeers(ctx)
}
func (p *spacePeers) BroadcastMessage(ctx context.Context, msg drpc.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.host.streams.Send(p.host.ctx, msg, p.GetResponsiblePeers)
}
func (p *spacePeers) SendMessage(ctx context.Context, id string, msg drpc.Message) error {
	if !slices.Contains(p.host.runtime.Nodes.NodeIds(p.spaceID), id) {
		return errors.New("peer is not responsible for private space")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.host.streams.Send(p.host.ctx, msg, func(ctx context.Context) ([]peer.Peer, error) {
		remote, err := p.host.runtime.Pool.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		return []peer.Peer{remote}, nil
	})
}
func (p *spacePeers) KeepAlive(ctx context.Context) {
	// Subscription is required even for an idle replica that has never written.
	payload, err := (&spacesyncproto.SpaceSubscription{SpaceIds: []string{p.spaceID}, Action: spacesyncproto.SpaceSubscriptionAction_Subscribe}).MarshalVT()
	if err == nil {
		_ = p.BroadcastMessage(ctx, &spacesyncproto.ObjectSyncMessage{Payload: payload})
	}
}

type streamHandler struct{ host *Host }

func (*streamHandler) Name() string        { return streamhandler.CName }
func (*streamHandler) Init(*app.App) error { return nil }
func (s *streamHandler) OpenStream(ctx context.Context, p peer.Peer) (drpc.Stream, []string, int, error) {
	ctx = peer.CtxWithPeerId(s.host.ctx, p.Id())
	conn, err := p.AcquireDrpcConn(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	stream, err := spacesyncproto.NewDRPCSpaceSyncClient(conn).ObjectSyncStream(ctx)
	if err != nil {
		p.ReleaseDrpcConn(ctx, conn)
		return nil, nil, 0, err
	}
	// Acquired connections belong to this stream until it terminates.
	context.AfterFunc(stream.Context(), func() { p.ReleaseDrpcConn(context.Background(), conn) })
	// Subscriptions belong to a stream, not to the peer connection. Reopening
	// an idle stream must restore them even when no local writes are queued.
	s.host.mu.Lock()
	ids := make([]string, 0, len(s.host.spaces))
	for id := range s.host.spaces {
		if slices.Contains(s.host.runtime.Nodes.NodeIds(id), p.Id()) {
			ids = append(ids, id)
		}
	}
	s.host.mu.Unlock()
	if len(ids) > 0 {
		payload, marshalErr := (&spacesyncproto.SpaceSubscription{SpaceIds: ids, Action: spacesyncproto.SpaceSubscriptionAction_Subscribe}).MarshalVT()
		if marshalErr != nil {
			_ = stream.Close()
			return nil, nil, 0, marshalErr
		}
		if err = stream.Send(&spacesyncproto.ObjectSyncMessage{Payload: payload}); err != nil {
			_ = stream.Close()
			return nil, nil, 0, err
		}
	}
	return stream, nil, 256, nil
}
func (s *streamHandler) NewReadMessage() drpc.Message { return &objectmessages.HeadUpdate{} }
func (s *streamHandler) HandleMessage(ctx context.Context, id string, msg drpc.Message) error {
	update, ok := msg.(*objectmessages.HeadUpdate)
	if !ok || update.SpaceId() == "" {
		return errors.New("unexpected private space message")
	}
	if err := s.host.authorize(ctx, update.SpaceId()); err != nil {
		return err
	}
	sp, err := s.host.lookup(ctx, update.SpaceId())
	if err != nil {
		return err
	}
	return sp.HandleMessage(ctx, update)
}
func (h *Host) authorize(ctx context.Context, spaceID string) error {
	id, err := peer.CtxPeerId(ctx)
	if err != nil {
		return err
	}
	if !slices.Contains(h.runtime.Nodes.NodeIds(spaceID), id) {
		return errors.New("untrusted private space peer")
	}
	return nil
}

type treeSyncer struct {
	host    *Host
	entry   *spaceEntry
	spaceID string
	stopped atomic.Bool
}

func (*treeSyncer) Name() string { return treesyncer.CName }
func (t *treeSyncer) Init(a *app.App) error {
	t.entry.treesMu.Lock()
	defer t.entry.treesMu.Unlock()
	t.entry.builder = a.MustComponent(objecttreebuilder.CName).(objecttreebuilder.TreeBuilderComponent)
	return nil
}
func (*treeSyncer) Run(context.Context) error     { return nil }
func (t *treeSyncer) Close(context.Context) error { t.StopSync(); return nil }
func (t *treeSyncer) StartSync()                  { t.stopped.Store(false) }
func (t *treeSyncer) StopSync()                   { t.stopped.Store(true) }
func (t *treeSyncer) ShouldSync(string) bool      { return !t.stopped.Load() }
func (t *treeSyncer) SyncAll(ctx context.Context, p peer.Peer, existing, missing []string) error {
	if t.stopped.Load() {
		return nil
	}
	ctx = peer.CtxWithPeerId(ctx, p.Id())
	var result error
	for _, id := range append(slices.Clone(missing), existing...) {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		tr, err := t.host.GetTree(ctx, t.spaceID, id)
		if err == nil {
			st, ok := tr.(synctree.SyncTree)
			if !ok {
				err = errors.New("tree does not support upstream synchronization")
			} else {
				err = st.SyncWithPeer(ctx, p)
			}
		}
		result = errors.Join(result, err)
	}
	return result
}

type spaceRPC struct {
	spacesyncproto.DRPCSpaceSyncUnimplementedServer
	host *Host
}

func (r *spaceRPC) HeadSync(ctx context.Context, req *spacesyncproto.HeadSyncRequest) (*spacesyncproto.HeadSyncResponse, error) {
	if err := r.host.authorize(ctx, req.SpaceId); err != nil {
		return nil, err
	}
	sp, err := r.host.lookup(ctx, req.SpaceId)
	if err != nil {
		return nil, err
	}
	return sp.HandleRangeRequest(ctx, req)
}
func (r *spaceRPC) ObjectSyncRequestStream(req *spacesyncproto.ObjectSyncMessage, stream spacesyncproto.DRPCSpaceSync_ObjectSyncRequestStreamStream) error {
	ctx := stream.Context()
	if err := r.host.authorize(ctx, req.SpaceId); err != nil {
		return err
	}
	sp, err := r.host.lookup(ctx, req.SpaceId)
	if err != nil {
		return err
	}
	return sp.HandleStreamSyncRequest(ctx, req, stream)
}
func (r *spaceRPC) ObjectSyncStream(stream spacesyncproto.DRPCSpaceSync_ObjectSyncStreamStream) error {
	return r.host.streams.ReadStream(stream, 256)
}
func (r *spaceRPC) StoreDiff(ctx context.Context, req *spacesyncproto.StoreDiffRequest) (*spacesyncproto.StoreDiffResponse, error) {
	if err := r.host.authorize(ctx, req.SpaceId); err != nil {
		return nil, err
	}
	sp, err := r.host.lookup(ctx, req.SpaceId)
	if err != nil {
		return nil, err
	}
	return sp.KeyValue().HandleStoreDiffRequest(ctx, req)
}
func (r *spaceRPC) StoreElements(stream spacesyncproto.DRPCSpaceSync_StoreElementsStream) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	ctx := stream.Context()
	if err = r.host.authorize(ctx, req.SpaceId); err != nil {
		return err
	}
	sp, err := r.host.lookup(ctx, req.SpaceId)
	if err != nil {
		return err
	}
	return sp.KeyValue().HandleStoreElementsRequest(ctx, stream)
}
