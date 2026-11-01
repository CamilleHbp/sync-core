package synccore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// Checkpoints reveal no task contents, credentials, or device details. The
// authenticated server must calculate its digest from a durably committed view.
type CheckpointRequest struct {
	Nonce string `json:"nonce"`
}
type CheckpointResponse struct {
	Nonce     string `json:"nonce"`
	Namespace string `json:"namespace"`
	Workspace string `json:"workspace"`
	Digest    string `json:"digest"`
}

// RecordsDigest compares causal versions (including conflicts and deletions),
// not just visible values. Canonical ordering does not mutate the caller's view.
func RecordsDigest(records []Record) string {
	canonical := make([]Record, 0, len(records))
	for _, record := range records {
		fields := make(map[string][]Version, len(record.Fields))
		for field, versions := range record.Fields {
			copyVersions := append([]Version{}, versions...)
			sort.Slice(copyVersions, func(i, j int) bool { return copyVersions[i].ID < copyVersions[j].ID })
			fields[field] = copyVersions
		}
		canonical = append(canonical, Record{ID: record.ID, Kind: record.Kind, Fields: fields})
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ID < canonical[j].ID })
	raw, _ := json.Marshal(canonical)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// NewCheckpoint belongs behind the app server's existing authentication and
// namespace checks, after it has persisted the native-to-server projection.
func NewCheckpoint(request CheckpointRequest, namespace, workspace string, records []Record) (CheckpointResponse, error) {
	if len(request.Nonce) != 48 {
		return CheckpointResponse{}, NewError("invalid_request", nil)
	}
	if _, err := hex.DecodeString(request.Nonce); err != nil {
		return CheckpointResponse{}, NewError("invalid_request", err)
	}
	return CheckpointResponse{request.Nonce, namespace, workspace, RecordsDigest(records)}, nil
}

// Checkpoint uses the paired origin and authenticated session. Unsupported old
// servers return supported=false; callers must never interpret that as synced.
func (c *Client) Checkpoint(ctx context.Context) (CheckpointResponse, bool, error) {
	c.mu.Lock()
	if c.engine == nil {
		c.mu.Unlock()
		return CheckpointResponse{}, false, NewError("sign_in_required", nil)
	}
	origin, token, workspace := c.state.Origin, c.state.Token, c.state.Bundle.Root.Id
	namespace := c.config.Namespace
	c.mu.Unlock()
	request := CheckpointRequest{Nonce: RandomID()}
	var response CheckpointResponse
	err := c.request(ctx, origin, token, "checkpoint", request, &response)
	if ErrorCode(err) == "not_found" {
		return response, false, nil
	}
	if err != nil {
		return response, false, err
	}
	if response.Nonce != request.Nonce || response.Namespace != namespace || response.Workspace != workspace || len(response.Digest) != 64 {
		return response, false, NewError("invalid_response", nil)
	}
	if _, err = hex.DecodeString(response.Digest); err != nil {
		return response, false, NewError("invalid_response", err)
	}
	return response, true, nil
}
