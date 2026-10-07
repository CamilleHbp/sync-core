package synccore

import (
	"bufio"
	"context"
	"net/http"
	"strings"
	"time"
)

// WatchUpdates consumes authenticated push invalidations. Only a broken stream
// is retried; an idle, healthy connection causes no synchronization requests.
// callback must return promptly. Cancellation closes the HTTP stream.
func (c *Client) WatchUpdates(ctx context.Context, callback func()) error {
	c.mu.Lock()
	origin, token, namespace := c.state.Origin, c.state.Token, c.config.Namespace
	c.mu.Unlock()
	if token == "" {
		return NewError("sign_in_required", nil)
	}
	return WatchEvents(ctx, origin, token, namespace, callback)
}

func WatchEvents(ctx context.Context, origin, token, namespace string, callback func()) error {
	origin, err := validOrigin(origin)
	if err != nil {
		return err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	backoff := time.Second
	for ctx.Err() == nil {
		req, err := http.NewRequestWithContext(ctx, "GET", origin+"/api/v1/events", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Sync-Namespace", namespace)
		req.Header.Set("Accept", "text/event-stream")
		started := time.Now()
		res, err := client.Do(req)
		if err == nil {
			if res.StatusCode == 401 || res.StatusCode == 403 {
				res.Body.Close()
				return NewError("sign_in_required", nil)
			}
			if res.StatusCode == 200 && strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
				scan := bufio.NewScanner(res.Body)
				scan.Buffer(make([]byte, 4096), 64<<10)
				event := ""
				for scan.Scan() {
					line := scan.Text()
					if strings.HasPrefix(line, "event:") {
						event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
					}
					if line == "" {
						if event == "change" || event == "revoked" {
							callback()
						}
						if event == "revoked" {
							res.Body.Close()
							return nil
						}
						event = ""
					}
				}
			}
			res.Body.Close()
		}
		if time.Since(started) > 30*time.Second {
			backoff = time.Second
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	return ctx.Err()
}

// Provision requests native-network metadata only from the pinned authenticated
// homeserver. The caller still verifies the owner's signature and network pin.
func (c *Client) Provision(ctx context.Context, out any) error {
	c.mu.Lock()
	origin, token := c.state.Origin, c.state.Token
	c.mu.Unlock()
	if token == "" {
		return NewError("sign_in_required", nil)
	}
	return c.request(ctx, origin, token, "native", map[string]string{}, out)
}

// Snapshot returns an independent legacy replica, including its original keys.
// Keep it private; it is for verified migration, not for JSON/UI responses.
func (c *Client) Snapshot() (*Engine, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.engine == nil {
		return nil, NewError("sign_in_required", nil)
	}
	return c.engine.Clone()
}
