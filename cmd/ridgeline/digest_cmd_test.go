package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xydac/ridgeline/memory"
)

// TestPostDigestWebhook verifies that postDigestWebhook POSTs valid JSON and
// returns no error on a 200 response.
func TestPostDigestWebhook(t *testing.T) {
	var got memory.DigestJSON
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type: got %q, want application/json", ct)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &got); err != nil {
			t.Errorf("unmarshal body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := &memory.DigestJSON{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Since:       "7d",
		Sections: []memory.DigestSectionJSON{
			{Title: "This Week", Content: "all good"},
		},
	}
	if err := postDigestWebhook(srv.URL, d); err != nil {
		t.Fatalf("postDigestWebhook: %v", err)
	}
	if got.Since != "7d" {
		t.Errorf("webhook body since: got %q, want %q", got.Since, "7d")
	}
	if len(got.Sections) != 1 || got.Sections[0].Title != "This Week" {
		t.Errorf("webhook body sections: %v", got.Sections)
	}
}

// TestPostDigestWebhook_NonOK verifies that a non-2xx response returns an error.
func TestPostDigestWebhook_NonOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	d := &memory.DigestJSON{Since: "7d"}
	if err := postDigestWebhook(srv.URL, d); err == nil {
		t.Error("expected error for 500 response, got nil")
	}
}
