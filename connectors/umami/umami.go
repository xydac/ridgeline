package umami

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xydac/ridgeline/connectors"
)

// Name is the connector name registered with the connectors package.
const Name = "umami"

// Header used to carry an Umami v2 API key.
const APIKeyHeader = "x-umami-api-key"

// DefaultPageSize is the page size when the config omits page_size.
// Umami's own default is 10; we ask for more per round-trip because
// analytics syncs are typically bulk.
const DefaultPageSize = 100

// MaxPageSize is Umami's published upper bound.
const MaxPageSize = 1000

// DefaultMaxPages caps how many pages a single Extract call fetches,
// so a runaway query stays within the per-sync timebox. Raise this in
// config for a bulk backfill.
const DefaultMaxPages = 10

// StreamEvents is the sole stream currently supported.
const StreamEvents = "events"

// cursorKey holds the RFC 3339 high-water mark for this connector.
const cursorKey = "last_created_at"

func init() {
	connectors.Register(New())
}

// Connector is an Umami analytics connector. A single Connector value
// is safe to reuse across concurrent Extract calls; the HTTP client is
// shared and has no mutable state.
type Connector struct {
	// Client is the http.Client used for API calls. Defaults to
	// http.DefaultClient on first use; tests may set a custom client.
	Client *http.Client
	// Now overrides the clock used for the default endAt timestamp.
	// Tests set this to pin responses; nil means time.Now.
	Now func() time.Time
}

// New returns a ready-to-register Connector.
func New() *Connector { return &Connector{} }

// Spec returns the connector's self-description.
func (c *Connector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{
		Name:        Name,
		DisplayName: "Umami",
		Description: "Incremental events feed from a self-hosted Umami analytics install.",
		Version:     "0.1.0",
		AuthType:    connectors.AuthAPIKey,
		AuthConfig:  &connectors.AuthConfig{KeyFields: []string{"api_key"}},
		Streams: []connectors.StreamSpec{{
			Name:        StreamEvents,
			Description: "Page views and custom events for the configured website, newest first.",
			SyncModes:   []connectors.SyncMode{connectors.Incremental, connectors.FullRefresh},
			DefaultCron: "0 * * * *",
		}},
	}
}

// knownConfigKeys enumerates every config key this connector reads.
// Kept together so Validate's unknown-key check and the code that
// actually consumes the values stay in sync.
var knownConfigKeys = []string{"base_url", "website_id", "api_key", "page_size", "max_pages"}

// Validate checks that the required fields are present and numeric
// bounds make sense, and reports typo'd keys up front. It does not
// reach out to the API.
func (c *Connector) Validate(_ context.Context, cfg connectors.ConnectorConfig) error {
	if err := connectors.CheckUnknownKeys(cfg, knownConfigKeys...); err != nil {
		return fmt.Errorf("umami: %w", err)
	}
	if strings.TrimSpace(cfg.String("base_url")) == "" {
		return fmt.Errorf("umami: base_url must not be empty")
	}
	if strings.TrimSpace(cfg.String("website_id")) == "" {
		return fmt.Errorf("umami: website_id must not be empty")
	}
	if strings.TrimSpace(cfg.String("api_key")) == "" {
		return fmt.Errorf("umami: api_key must not be empty (set api_key_ref to a stored credential)")
	}
	if ps := cfg.Int("page_size", DefaultPageSize); ps <= 0 || ps > MaxPageSize {
		return fmt.Errorf("umami: page_size must be in 1..%d (got %d)", MaxPageSize, ps)
	}
	if mp := cfg.Int("max_pages", DefaultMaxPages); mp <= 0 {
		return fmt.Errorf("umami: max_pages must be > 0 (got %d)", mp)
	}
	return nil
}

// Discover reports the events stream as available. Because the
// configured website_id may currently have zero events, Available is
// only a claim that the endpoint is reachable; a concrete record
// count is not cheap to compute and would double every sync.
func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
	spec := c.Spec()
	out := make([]connectors.DiscoveredStream, 0, len(spec.Streams))
	for _, ss := range spec.Streams {
		out = append(out, connectors.DiscoveredStream{StreamSpec: ss, Available: true})
	}
	return &connectors.Catalog{Streams: out}, nil
}

// Extract pulls the events stream from Umami and closes the returned
// channel when done. Each call walks pages from 1 upward until the
// API returns a short page, max_pages is reached, or ctx is cancelled.
// A StateMsg carrying the newest createdAt seen is emitted after the
// stream completes so a crash before the state persists costs at most
// one sync's worth of re-fetched records.
func (c *Connector) Extract(ctx context.Context, cfg connectors.ConnectorConfig, streams []connectors.Stream, state connectors.State) (<-chan connectors.Message, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.String("base_url")), "/")
	websiteID := strings.TrimSpace(cfg.String("website_id"))
	pageSize := cfg.Int("page_size", DefaultPageSize)
	maxPages := cfg.Int("max_pages", DefaultMaxPages)
	if baseURL == "" || websiteID == "" {
		return nil, fmt.Errorf("umami: base_url and website_id are required")
	}
	if pageSize <= 0 || pageSize > MaxPageSize {
		return nil, fmt.Errorf("umami: page_size out of range: %d", pageSize)
	}
	if maxPages <= 0 {
		return nil, fmt.Errorf("umami: max_pages must be > 0 (got %d)", maxPages)
	}
	auth, err := newAuthorizer(cfg)
	if err != nil {
		return nil, err
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}

	ch := make(chan connectors.Message, 64)
	go func() {
		defer close(ch)
		for _, s := range streams {
			if s.Name != StreamEvents {
				sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelWarn,
					fmt.Sprintf("umami: unknown stream %q (only %q is supported)", s.Name, StreamEvents)))
				continue
			}
			since := parseCursor(state)
			highWater := since
			endAt := now().UTC()
			startAt := sinceForRequest(since)
			for page := 1; page <= maxPages; page++ {
				events, err := c.fetchPage(ctx, baseURL, websiteID, auth, startAt, endAt, page, pageSize)
				if err != nil {
					sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelError,
						fmt.Sprintf("umami %s: %v", s.Name, err)))
					break
				}
				if len(events) == 0 {
					break
				}
				progressed := false
				for _, ev := range events {
					rec, err := recordFromEvent(s.Name, ev)
					if err != nil {
						sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelWarn,
							fmt.Sprintf("umami %s: %v", s.Name, err)))
						continue
					}
					if !rec.Timestamp.After(since) {
						// Umami's default ordering returns newest first; a
						// record at-or-before `since` means we have crossed
						// into already-seen territory. Skip it and keep
						// walking; pagination stops naturally once the
						// page is all old records.
						continue
					}
					if rec.Timestamp.After(highWater) {
						highWater = rec.Timestamp
					}
					progressed = true
					if !sendMessage(ctx, ch, connectors.RecordMessage(s.Name, rec)) {
						return
					}
				}
				if len(events) < pageSize {
					// Short page means we have walked past the last row
					// the server had to offer, regardless of how the
					// server echoes its pageSize field.
					break
				}
				if !progressed {
					// Every record on this full page was at-or-before the
					// cursor; the rest must be older still.
					break
				}
			}
			next := connectors.State{cursorKey: highWater.UTC().Format(time.RFC3339Nano)}
			if !sendMessage(ctx, ch, connectors.StateMessage(next)) {
				return
			}
		}
	}()
	return ch, nil
}

// parseCursor decodes the persisted RFC 3339 timestamp. A missing or
// malformed value yields the zero time so the first sync pulls the
// default lookback window.
func parseCursor(state connectors.State) time.Time {
	if state == nil {
		return time.Time{}
	}
	raw, ok := state[cursorKey].(string)
	if !ok || raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// sinceForRequest picks the startAt value passed to Umami. The endpoint
// requires startAt, so on a first sync we fall back to a 30-day
// lookback; subsequent syncs use the stored cursor.
func sinceForRequest(since time.Time) time.Time {
	if since.IsZero() {
		return time.Now().UTC().Add(-30 * 24 * time.Hour)
	}
	return since
}

// fetchPage issues one GET against /api/websites/{id}/events and
// returns the raw events on the page. The caller detects a short page
// by comparing len(events) to the requested pageSize.
func (c *Connector) fetchPage(ctx context.Context, baseURL, websiteID string, auth authorizer, startAt, endAt time.Time, page, pageSize int) ([]map[string]any, error) {
	u, err := url.Parse(baseURL + "/api/websites/" + url.PathEscape(websiteID) + "/events")
	if err != nil {
		return nil, fmt.Errorf("parse base_url: %w", err)
	}
	q := u.Query()
	q.Set("startAt", strconv.FormatInt(startAt.UnixMilli(), 10))
	q.Set("endAt", strconv.FormatInt(endAt.UnixMilli(), 10))
	q.Set("page", strconv.Itoa(page))
	q.Set("pageSize", strconv.Itoa(pageSize))
	q.Set("orderBy", "createdAt")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if err := auth.decorate(req); err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "ridgeline/0.0.0-dev (+https://github.com/xydac/ridgeline)")

	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("umami %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var payload struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return payload.Data, nil
}

// recordFromEvent turns a raw event object into a Record. Timestamp
// comes from createdAt, which Umami emits as RFC 3339.
func recordFromEvent(stream string, ev map[string]any) (connectors.Record, error) {
	raw, ok := ev["createdAt"].(string)
	if !ok || raw == "" {
		return connectors.Record{}, fmt.Errorf("event missing createdAt: %v", ev["id"])
	}
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return connectors.Record{}, fmt.Errorf("event createdAt %q: %w", raw, err)
	}
	return connectors.Record{
		Stream:    stream,
		Timestamp: ts.UTC(),
		Data:      ev,
	}, nil
}

// sendMessage posts m on ch unless ctx is cancelled first. It reports
// whether the send succeeded; false means the goroutine should stop.
func sendMessage(ctx context.Context, ch chan<- connectors.Message, m connectors.Message) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- m:
		return true
	}
}
