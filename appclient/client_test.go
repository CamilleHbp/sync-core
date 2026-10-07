package appclient

import (
	"context"
	"encoding/json"
	"fmt"
	core "github.com/CamilleHbp/sync-core"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPrivatePairingAndGateway(t *testing.T) {
	path := os.Getenv("SYNC_CORE_PRIVATE_CONFIG")
	if path == "" {
		t.Skip("private integration network required")
	}
	network, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	id := os.Getenv("SYNC_CORE_PRIVATE_NETWORK_ID")
	cfg := core.Config{Namespace: "sync-core.appclient-test", ValidateOperation: func(core.Operation) error { return nil }}
	bundles, err := core.NewWorkspace(cfg, []string{"Owner"})
	if err != nil {
		t.Fatal(err)
	}
	source, err := core.Open(cfg, bundles[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	operation := core.Operation{ID: core.RandomID(), Record: core.RandomID(), Kind: "task", Field: "title", Value: "Original"}
	if err = source.Apply(operation); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	provision, err := Prepare(ctx, cfg, source, bundles, network, id, filepath.Join(t.TempDir(), "migration"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]any{"token": "synthetic", "bundle": bundles[0], "native": true})
		case "/api/v1/native":
			json.NewEncoder(w).Encode(provision)
		case "/api/v1/sync":
			json.NewEncoder(w).Encode(source.Page(0))
		case "/api/v1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "event: change\ndata: {}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	directory := t.TempDir()
	client, err := New(directory, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { client.Close() }()
	if err = client.Connect(ctx, server.URL, "synthetic"); err != nil {
		t.Fatal(err)
	}
	if client.native == nil {
		t.Fatal("did not activate native client")
	}
	edit := operation
	edit.ID = core.RandomID()
	edit.Value = "Native edit"
	edit.Replaces = []string{operation.ID}
	if err = client.Apply(ctx, []core.Operation{edit}); err != nil {
		t.Fatal(err)
	}
	merged, err := client.native.ReconcileLegacy(ctx, source, bundles)
	if err != nil {
		t.Fatal(err)
	}
	view, err := merged.View()
	if err != nil || view.Records[0].Value("title") != "Native edit" {
		t.Fatalf("gateway lost native edit: %v", err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	client, err = New(directory, cfg)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := client.View()
	if err != nil || restored.Records[0].Value("title") != "Native edit" {
		t.Fatalf("native restart lost data: %v", err)
	}
}
