// Package privatenetwork assembles upstream any-sync transport components for
// an explicitly configured self-hosted network. It has no built-in endpoints.
package privatenetwork

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/nodeconfsource"
	"github.com/anyproto/any-sync/net/peerservice"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/net/secureservice"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/nodeconfstore"
	"github.com/anyproto/any-sync/util/crypto"
	libpeer "github.com/libp2p/go-libp2p/core/peer"
	"gopkg.in/yaml.v3"
)

// ParseConfig requires a separately supplied trust anchor. The caller obtains
// both the configuration and expected network identity from its administrator,
// never from an untrusted pairing link or a public-network default.
func ParseConfig(data []byte, expectedNetworkID string) (nodeconf.Configuration, error) {
	var c nodeconf.Configuration
	if _, err := crypto.DecodeNetworkId(expectedNetworkID); err != nil {
		return c, errors.New("a valid private network identity is required")
	}
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		return c, fmt.Errorf("invalid private network configuration: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, errors.New("expected exactly one network configuration")
	}
	if c.NetworkId != expectedNetworkID {
		return c, errors.New("private network identity mismatch")
	}
	seen := map[string]bool{}
	types := map[nodeconf.NodeType]bool{}
	for _, n := range c.Nodes {
		if _, err := libpeer.Decode(n.PeerId); err != nil || seen[n.PeerId] {
			return c, errors.New("invalid or duplicate private network peer")
		}
		seen[n.PeerId] = true
		if len(n.Addresses) == 0 {
			return c, errors.New("private network peer has no address")
		}
		for _, address := range n.Addresses {
			// Upstream uses bare host:port for yamux in generated configs.
			if !strings.Contains(address, "://") {
				address = "yamux://" + address
			}
			u, err := url.Parse(address)
			if err != nil || (u.Scheme != "yamux" && u.Scheme != "quic") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
				return c, errors.New("invalid private network transport address")
			}
			host, port, err := net.SplitHostPort(u.Host)
			number, portErr := strconv.Atoi(port)
			if err != nil || host == "" || portErr != nil || number < 1 || number > 65535 {
				return c, errors.New("private network address requires a host and valid port")
			}
		}
		for _, typ := range n.Types {
			types[typ] = true
		}
	}
	for _, typ := range []nodeconf.NodeType{nodeconf.NodeTypeCoordinator, nodeconf.NodeTypeConsensus, nodeconf.NodeTypeTree} {
		if !types[typ] {
			return c, fmt.Errorf("private network is missing a %s peer", typ)
		}
	}
	return c, nil
}

// Runtime owns transport and topology discovery, not application synchronization.
// Application spaces attach their upstream sync services to App before Start.
// Close spaces before closing the runtime. Account keys remain caller-owned.
type Runtime struct {
	App         *app.App
	Pool        pool.Service
	Nodes       nodeconf.Service
	Coordinator coordinatorclient.CoordinatorClient
}

// New validates trust before creating any components or persistent state. It
// does not dial until App.Start is called. No public bootstrap is ever added.
func New(data []byte, expectedNetworkID, directory string, keys *accountdata.AccountKeys) (*Runtime, error) {
	c, err := ParseConfig(data, expectedNetworkID)
	if err != nil {
		return nil, err
	}
	if keys == nil || keys.PeerKey == nil || keys.SignKey == nil || keys.PeerId != keys.PeerKey.GetPublic().PeerId() {
		return nil, errors.New("valid private network account keys are required")
	}
	if directory == "" {
		return nil, errors.New("private network state directory is required")
	}
	if err = os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	r := &Runtime{App: new(app.App), Pool: pool.New(), Nodes: nodeconf.New(), Coordinator: coordinatorclient.New()}
	r.App.Register(&runtimeConfig{network: c, directory: directory}).
		Register(&account{keys: keys}).
		Register(&pinnedStore{NodeConfStore: nodeconfstore.New(), networkID: expectedNetworkID}).
		Register(r.Nodes).
		Register(r.Coordinator).
		Register(nodeconfsource.New()).
		Register(server.New()).
		Register(r.Pool).
		Register(secureservice.New()).
		Register(yamux.New()).
		Register(quic.New()).
		Register(peerservice.New())
	return r, nil
}

func (r *Runtime) Close(ctx context.Context) error { return r.App.Close(ctx) }

// Pin both cached and newly discovered topology. Upstream authenticates the
// coordinator, but its topology store does not itself enforce network identity.
type pinnedStore struct {
	nodeconfstore.NodeConfStore
	networkID string
}

// Upstream nodeconf.Init does not consistently propagate GetLast errors.
// Reject an invalid cache during this earlier component initialization, before
// nodeconf can install it or any peer transports start.
func (s *pinnedStore) Init(a *app.App) error {
	if err := s.NodeConfStore.Init(a); err != nil {
		return err
	}
	_, err := s.GetLast(context.Background(), s.networkID)
	if errors.Is(err, nodeconf.ErrConfigurationNotFound) {
		return nil
	}
	return err
}

func (s *pinnedStore) validate(c nodeconf.Configuration) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	_, err = ParseConfig(data, s.networkID)
	return err
}

func (s *pinnedStore) GetLast(ctx context.Context, id string) (nodeconf.Configuration, error) {
	if id != s.networkID {
		return nodeconf.Configuration{}, errors.New("private network identity mismatch")
	}
	c, err := s.NodeConfStore.GetLast(ctx, id)
	if err == nil {
		err = s.validate(c)
	}
	return c, err
}

func (s *pinnedStore) SaveLast(ctx context.Context, c nodeconf.Configuration) error {
	if err := s.validate(c); err != nil {
		return err
	}
	return s.NodeConfStore.SaveLast(ctx, c)
}

type account struct{ keys *accountdata.AccountKeys }

func (a *account) Name() string                      { return accountservice.CName }
func (a *account) Init(*app.App) error               { return nil }
func (a *account) Account() *accountdata.AccountKeys { return a.keys }

type runtimeConfig struct {
	network   nodeconf.Configuration
	directory string
}

func (c *runtimeConfig) Name() string                        { return "config" }
func (c *runtimeConfig) Init(*app.App) error                 { return nil }
func (c *runtimeConfig) GetNodeConf() nodeconf.Configuration { return c.network }
func (c *runtimeConfig) GetNodeConfStorePath() string        { return filepath.Join(c.directory, "network") }
func (c *runtimeConfig) GetDrpc() rpc.Config {
	return rpc.Config{Stream: rpc.StreamConfig{MaxMsgSizeMb: 4}}
}
func (c *runtimeConfig) GetSecureService() secureservice.Config {
	return secureservice.Config{RequireClientAuth: true}
}
func (c *runtimeConfig) GetYamux() yamux.Config {
	return yamux.Config{WriteTimeoutSec: 15, DialTimeoutSec: 10, KeepAlivePeriodSec: 30}
}
func (c *runtimeConfig) GetQuic() quic.Config {
	return quic.Config{WriteTimeoutSec: 15, DialTimeoutSec: 10, CloseTimeoutSec: 5, KeepAlivePeriodSec: 30}
}
