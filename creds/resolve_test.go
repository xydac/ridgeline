package creds_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xydac/ridgeline/creds"
)

func TestResolveRefs_Replaces(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	if err := cs.Put(ctx, "umami_main", []byte("sekret-key")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg := map[string]any{
		"base_url":    "https://stats.example.com",
		"website_id":  "abc-123",
		"api_key_ref": "umami_main",
	}
	warns, err := creds.ResolveRefs(ctx, cs, cfg)
	if err != nil {
		t.Fatalf("ResolveRefs: %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	if _, ok := cfg["api_key_ref"]; ok {
		t.Fatalf("api_key_ref should have been deleted")
	}
	if got := cfg["api_key"]; got != "sekret-key" {
		t.Fatalf("api_key = %v, want sekret-key", got)
	}
	if got := cfg["base_url"]; got != "https://stats.example.com" {
		t.Fatalf("base_url unexpectedly changed: %v", got)
	}
}

func TestResolveRefs_MissingCred(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	cfg := map[string]any{"api_key_ref": "umami_main"}
	_, err := creds.ResolveRefs(context.Background(), cs, cfg)
	if err == nil {
		t.Fatal("want error for missing credential")
	}
	if !strings.Contains(err.Error(), "umami_main") {
		t.Fatalf("error should name missing credential: %v", err)
	}
	if _, ok := cfg["api_key_ref"]; !ok {
		t.Fatalf("cfg must be left unchanged on error, api_key_ref missing")
	}
}

func TestResolveRefs_NoRefsIsNoop(t *testing.T) {
	cfg := map[string]any{"base_url": "x", "page_size": 50}
	warns, err := creds.ResolveRefs(context.Background(), nil, cfg)
	if err != nil {
		t.Fatalf("ResolveRefs: %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	if cfg["base_url"] != "x" || cfg["page_size"] != 50 {
		t.Fatalf("cfg unexpectedly mutated: %v", cfg)
	}
}

func TestResolveRefs_NilCfg(t *testing.T) {
	warns, err := creds.ResolveRefs(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("nil cfg should be no-op: %v", err)
	}
	if warns != nil {
		t.Fatalf("nil cfg must not warn: %v", warns)
	}
}

func TestResolveRefs_RejectsNonStringRef(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	cfg := map[string]any{"api_key_ref": 123}
	if _, err := creds.ResolveRefs(context.Background(), cs, cfg); err == nil {
		t.Fatal("want error for non-string ref")
	}
}

func TestResolveRefs_RejectsEmptyRef(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	cfg := map[string]any{"api_key_ref": "   "}
	if _, err := creds.ResolveRefs(context.Background(), cs, cfg); err == nil {
		t.Fatal("want error for empty ref")
	}
}

func TestResolveRefs_RefWithoutStore(t *testing.T) {
	cfg := map[string]any{"api_key_ref": "x"}
	if _, err := creds.ResolveRefs(context.Background(), nil, cfg); err == nil {
		t.Fatal("want error when a ref is present but store is nil")
	}
}

func TestResolveRefs_CollisionWarns(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	if err := cs.Put(ctx, "k", []byte("from-store")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg := map[string]any{"api_key": "inline-value", "api_key_ref": "k"}
	warns, err := creds.ResolveRefs(ctx, cs, cfg)
	if err != nil {
		t.Fatalf("ResolveRefs: %v", err)
	}
	if len(warns) != 1 || !strings.Contains(warns[0], "api_key") {
		t.Fatalf("expected collision warning, got %v", warns)
	}
	if cfg["api_key"] != "from-store" {
		t.Fatalf("ref should win over literal: %v", cfg["api_key"])
	}
}

func TestResolveRefs_LoneUnderscoreRefIsIgnored(t *testing.T) {
	// An input key that is exactly "_ref" strips to "" which cannot
	// be set on the config. Guard against that edge.
	cs, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	if err := cs.Put(ctx, "anything", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cfg := map[string]any{"_ref": "anything"}
	warns, err := creds.ResolveRefs(ctx, cs, cfg)
	if err != nil {
		t.Fatalf("ResolveRefs: %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	if _, ok := cfg["_ref"]; !ok {
		t.Fatalf("_ref should be left untouched: %v", cfg)
	}
}
