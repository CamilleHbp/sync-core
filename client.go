package synccore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type identity struct {
	NativeAvailable bool     `json:"nativeAvailable,omitempty"`
	Origin          string   `json:"origin"`
	Token           string   `json:"token"`
	Bundle          Bundle   `json:"bundle"`
	Cursor          uint64   `json:"cursor"`
	Recovery        []string `json:"recovery,omitempty"`
}
type Client struct {
	mu      sync.Mutex
	syncing sync.Mutex
	config  Config
	db      *Database
	engine  *Engine
	state   identity
	http    *http.Client
}
type ClientView struct {
	Authenticated bool     `json:"authenticated"`
	User          string   `json:"user"`
	Workspace     string   `json:"workspace"`
	Origin        string   `json:"origin"`
	Pending       int      `json:"pending"`
	Records       []Record `json:"records"`
}

func NewClient(directory string, config Config) (*Client, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	db, err := OpenDatabase(filepath.Join(directory, "sync.sqlite"))
	if err != nil {
		return nil, err
	}
	c := &Client{config: config, db: db, http: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	err = db.Get("identity", &c.state)
	if errors.Is(err, sql.ErrNoRows) {
		return c, nil
	}
	if err == nil {
		var store *MemoryStorage
		store, err = db.Load()
		if err == nil {
			c.engine, err = Open(config, c.state.Bundle, store)
		}
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return c, nil
}
func (c *Client) Close() error {
	c.syncing.Lock()
	defer c.syncing.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.Close()
}
func validOrigin(origin string) (string, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", NewError("invalid_origin", nil)
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1")) {
		return "", NewError("https_required", nil)
	}
	return strings.TrimSuffix(u.String(), "/"), nil
}
func (c *Client) request(ctx context.Context, origin, token, route string, in, out any) error {
	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, "POST", origin+"/api/v1/"+route, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Sync-Namespace", c.config.Namespace)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return NewError("offline", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
	if err != nil {
		return NewError("offline", err)
	}
	if len(data) > 8<<20 {
		return NewError("response_too_large", nil)
	}
	if response.StatusCode != http.StatusOK {
		var problem struct {
			Code string `json:"code"`
		}
		json.Unmarshal(data, &problem)
		if problem.Code == "" {
			switch response.StatusCode {
			case 401, 403:
				problem.Code = "sign_in_required"
			case 404:
				problem.Code = "not_found"
			case 429:
				problem.Code = "rate_limited"
			default:
				problem.Code = "server_error"
			}
		}
		return NewError(problem.Code, nil)
	}
	if err = json.Unmarshal(data, out); err != nil {
		return NewError("invalid_response", err)
	}
	return nil
}

// Connect pins the replica to its origin, namespace, account, and workspace.
// Reauthentication never discards offline changes, even after session expiry.
func (c *Client) Connect(ctx context.Context, origin, code string) error {
	normalized, err := validOrigin(origin)
	if err != nil {
		return err
	}
	c.syncing.Lock()
	defer c.syncing.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.engine != nil && c.state.Origin != normalized {
		return NewError("identity_mismatch", nil)
	}
	var result struct {
		Token  string `json:"token"`
		Native bool   `json:"native"`
		Bundle Bundle `json:"bundle"`
	}
	if err = c.request(ctx, normalized, "", "login", map[string]string{"code": code}, &result); err != nil {
		return err
	}
	if result.Token == "" {
		return NewError("invalid_response", nil)
	}
	if c.engine != nil && (result.Bundle.Root == nil || c.state.Bundle.Root.Id != result.Bundle.Root.Id || c.state.Bundle.User != result.Bundle.User || !bytes.Equal(c.state.Bundle.Key, result.Bundle.Key)) {
		return NewError("identity_mismatch", nil)
	}
	var store *MemoryStorage
	if c.engine != nil {
		store = c.engine.Store
	}
	engine, err := Open(c.config, result.Bundle, store)
	if err != nil {
		return err
	}
	next := c.state
	next.Origin = normalized
	next.Token = result.Token
	next.Bundle = result.Bundle
	next.NativeAvailable = result.Native
	if err = c.db.Commit(engine, map[string]any{"identity": next}, false, nil); err != nil {
		return err
	}
	c.state = next
	c.engine = engine
	return nil
}
func (c *Client) View() (ClientView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := ClientView{Records: []Record{}}
	if c.engine == nil {
		return v, nil
	}
	view, err := c.engine.View()
	if err != nil {
		return v, err
	}
	return ClientView{Authenticated: true, User: c.state.Bundle.User, Workspace: c.state.Bundle.Root.Id, Origin: c.state.Origin, Pending: c.db.PendingCount(), Records: view.Records}, nil
}

// Apply acknowledges only after signed changes, projection and outbox are durable.
// Stable operation IDs make replay after a lost acknowledgement idempotent.
func (c *Client) Apply(operations []Operation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.engine == nil {
		return NewError("sign_in_required", nil)
	}
	engine, err := c.engine.Clone()
	if err != nil {
		return err
	}
	if err = engine.ApplyBatch(operations); err != nil {
		return err
	}
	if err = c.db.Commit(engine, nil, true, nil); err != nil {
		return err
	}
	c.engine = engine
	return nil
}

// Sync permits edits during HTTP and commits each accepted page with its cursor.
func (c *Client) Sync(ctx context.Context) error {
	c.syncing.Lock()
	defer c.syncing.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.engine == nil {
		return NewError("sign_in_required", nil)
	}
	for round := 0; round < 1000; round++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids, err := c.db.Pending()
		if err != nil {
			return err
		}
		if len(c.state.Recovery) > 0 {
			ids = c.state.Recovery
			if len(ids) > MaxBatch {
				ids = ids[:MaxBatch]
			}
		}
		size, take := 0, 0
		for _, id := range ids {
			n := len(c.engine.Store.Changes[id].RawChange)
			if size+n > 1048576 && take > 0 {
				break
			}
			size += n
			take++
		}
		ids = ids[:take]
		packet, err := c.engine.PacketIDs(ids)
		if err != nil {
			return err
		}
		request := SyncRequest{Packet: packet, Cursor: c.state.Cursor}
		origin, token := c.state.Origin, c.state.Token
		var response SyncResponse
		c.mu.Unlock()
		err = c.request(ctx, origin, token, "sync", request, &response)
		c.mu.Lock()
		if err != nil {
			return err
		}
		if len(response.Need) > 0 {
			recovery, err := c.engine.AncestorIDs(response.Need)
			if err != nil {
				return err
			}
			if len(recovery) == 0 {
				return NewError("invalid_ancestor", nil)
			}
			next := c.state
			next.Recovery = recovery
			next.Cursor = 0
			if err = c.db.Put("identity", next); err != nil {
				return err
			}
			c.state = next
			continue
		}
		engine, err := c.engine.Clone()
		if err != nil {
			return err
		}
		need, err := engine.Missing(response.Packet)
		if err != nil {
			return err
		}
		if len(need) > 0 {
			if err = c.fetchAncestors(ctx, engine, need, 0); err != nil {
				return err
			}
		}
		if err = engine.Merge(response.Packet, ""); err != nil {
			return err
		}
		next := c.state
		next.Cursor = response.Cursor
		sent := map[string]bool{}
		for _, id := range ids {
			sent[id] = true
		}
		ack := map[string]bool{}
		for _, id := range response.Ack {
			if !sent[id] {
				return NewError("invalid_ack", nil)
			}
			ack[id] = true
		}
		next.Recovery = nil
		for _, id := range c.state.Recovery {
			if !ack[id] {
				next.Recovery = append(next.Recovery, id)
			}
		}
		if err = c.db.Commit(engine, map[string]any{"identity": next}, false, response.Ack); err != nil {
			return err
		}
		c.engine = engine
		c.state = next
		if !response.More && c.db.PendingCount() == 0 && len(next.Recovery) == 0 {
			return nil
		}
	}
	return NewError("sync_incomplete", nil)
}
func (c *Client) fetchAncestors(ctx context.Context, e *Engine, ids []string, depth int) error {
	if depth > 1000 {
		return NewError("history_limit", nil)
	}
	for len(ids) > 0 {
		n := len(ids)
		if n > MaxBatch {
			n = MaxBatch
		}
		part := ids[:n]
		ids = ids[n:]
		var response SyncResponse
		if err := c.request(ctx, c.state.Origin, c.state.Token, "sync", SyncRequest{Cursor: c.state.Cursor, Need: part}, &response); err != nil {
			return err
		}
		missing, err := e.Missing(response.Packet)
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			if err = c.fetchAncestors(ctx, e, missing, depth+1); err != nil {
				return err
			}
		}
		if err = e.Merge(response.Packet, ""); err != nil {
			return err
		}
	}
	return nil
}
