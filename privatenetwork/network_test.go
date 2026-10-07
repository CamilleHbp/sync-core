package privatenetwork

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/nodeconf"
	"gopkg.in/yaml.v3"
)

func testConfiguration(t *testing.T) ([]byte, string) {
	t.Helper()
	keys, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	c := nodeconf.Configuration{Id: "test", NetworkId: keys.SignKey.GetPublic().Network(), Nodes: []nodeconf.Node{{
		PeerId: keys.PeerId, Addresses: []string{"yamux://127.0.0.1:1"},
		Types: []nodeconf.NodeType{nodeconf.NodeTypeTree, nodeconf.NodeTypeConsensus, nodeconf.NodeTypeCoordinator},
	}}}
	data, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return data, c.NetworkId
}

func TestConfigurationRequiresExplicitTrust(t *testing.T) {
	data, id := testConfiguration(t)
	if _, err := ParseConfig(data, id); err != nil {
		t.Fatal(err)
	}
	_, otherID := testConfiguration(t)
	for _, badID := range []string{"", "invalid", otherID} {
		if _, err := ParseConfig(data, badID); err == nil {
			t.Fatal("accepted wrong trust anchor")
		}
	}
	for _, data := range [][]byte{nil, []byte("{}"), append(append([]byte{}, data...), []byte("\n---\n{}\n")...), append(append([]byte{}, data...), []byte("\nunknown: true\n")...)} {
		if _, err := ParseConfig(data, id); err == nil {
			t.Fatal("accepted invalid configuration")
		}
	}
}

func TestConfigurationRejectsInvalidPeers(t *testing.T) {
	data, id := testConfiguration(t)
	for _, mutate := range []func(*nodeconf.Configuration){
		func(c *nodeconf.Configuration) { c.Nodes = nil },
		func(c *nodeconf.Configuration) { c.Nodes = append(c.Nodes, c.Nodes[0]) },
		func(c *nodeconf.Configuration) { c.Nodes[0].PeerId = "invalid" },
		func(c *nodeconf.Configuration) { c.Nodes[0].Addresses = nil },
		func(c *nodeconf.Configuration) { c.Nodes[0].Addresses = []string{"https://example.invalid"} },
		func(c *nodeconf.Configuration) {
			c.Nodes[0].Addresses = []string{"yamux://user:password@localhost:123"}
		},
		func(c *nodeconf.Configuration) { c.Nodes[0].Addresses = []string{"yamux://localhost"} },
		func(c *nodeconf.Configuration) { c.Nodes[0].Addresses = []string{"yamux://localhost:0"} },
		func(c *nodeconf.Configuration) { c.Nodes[0].Addresses = []string{"yamux://localhost:65536"} },
		func(c *nodeconf.Configuration) { c.Nodes[0].Types = []nodeconf.NodeType{nodeconf.NodeTypeTree} },
	} {
		var c nodeconf.Configuration
		if err := yaml.Unmarshal(data, &c); err != nil {
			t.Fatal(err)
		}
		mutate(&c)
		bad, err := yaml.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseConfig(bad, id); err == nil {
			t.Fatal("accepted invalid peer configuration")
		}
	}
}

func TestRejectBeforeCreatingState(t *testing.T) {
	data, _ := testConfiguration(t)
	dir := filepath.Join(t.TempDir(), "state")
	if _, err := New(data, "wrong-network", dir, nil); err == nil {
		t.Fatal("accepted wrong network")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("created state before validating trust")
	}
}

func TestTopologyRemainsPinned(t *testing.T) {
	data, id := testConfiguration(t)
	foreign, _ := testConfiguration(t)
	keys, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	r, err := New(data, id, dir, keys)
	if err != nil {
		t.Fatal(err)
	}
	s := r.App.Component(nodeconf.CNameStore).(*pinnedStore)
	if err = s.Init(r.App); err != nil {
		t.Fatal(err)
	}
	var wrong nodeconf.Configuration
	if err = yaml.Unmarshal(foreign, &wrong); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveLast(context.Background(), wrong); err == nil {
		t.Fatal("accepted foreign topology update")
	}
	// A corrupt/stale cache must not override the explicitly selected network.
	if err = os.WriteFile(filepath.Join(dir, "network", id+".yml"), foreign, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetLast(context.Background(), id); err == nil {
		t.Fatal("accepted foreign cached topology")
	}
}

func TestRuntimeRejectsPoisonedCache(t *testing.T) {
	data, id := testConfiguration(t)
	_, foreignID := testConfiguration(t)
	var poisoned nodeconf.Configuration
	if err := yaml.Unmarshal(data, &poisoned); err != nil {
		t.Fatal(err)
	}
	// Keep all coordinator peers identical: upstream's address merge must not
	// accidentally be the only thing enforcing the independent network pin.
	poisoned.NetworkId = foreignID
	cache, err := yaml.Marshal(poisoned)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = os.MkdirAll(filepath.Join(dir, "network"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "network", id+".yml"), cache, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := New(data, id, dir, keys)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = r.App.Start(ctx)
	if err == nil {
		_ = r.Close(context.Background())
		t.Fatal("runtime started with a foreign cached network identity")
	}
}

// This opt-in test uses only a separately provisioned private development stack
// and newly generated synthetic account keys. It does not read user accounts.
func TestPrivateNetworkHandshake(t *testing.T) {
	path := os.Getenv("SYNC_CORE_PRIVATE_CONFIG")
	if path == "" {
		t.Skip("set SYNC_CORE_PRIVATE_CONFIG and SYNC_CORE_PRIVATE_NETWORK_ID for a self-hosted integration test")
	}
	id := os.Getenv("SYNC_CORE_PRIVATE_NETWORK_ID")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := accountdata.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(data, id, t.TempDir(), keys)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = r.App.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	response, err := r.Coordinator.NetworkConfiguration(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if response.NetworkId != id {
		t.Fatal("coordinator returned another network")
	}
	// Also authenticate every configured tree node; no HTTP gateway is involved.
	for _, n := range r.Nodes.Configuration().Nodes {
		if n.HasType(nodeconf.NodeTypeTree) {
			p, err := r.Pool.Get(ctx, n.PeerId)
			if err != nil {
				t.Fatal(err)
			}
			if p.Id() != n.PeerId {
				t.Fatal("peer identity mismatch")
			}
		}
	}
}
