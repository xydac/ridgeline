package hackernews

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
const Name = "hackernews"

// DefaultBaseURL is the public Algolia HN search endpoint.
const DefaultBaseURL = "https://hn.algolia.com/api/v1"

// DefaultHitsPerPage is the default page size. Algolia's own default is
// 20; we request more per round-trip because HN syncs are typically bulk.
const DefaultHitsPerPage = 50

// MaxHitsPerPage is Algolia's published upper bound.
const MaxHitsPerPage = 1000

// DefaultMaxPages caps how many pages a single Extract call fetches,
// so a runaway query does not wander away from the timebox. Users can
// raise this in their config for a bulk backfill.
const DefaultMaxPages = 5

// Stream names.
const (
	StreamStories  = "stories"
	StreamComments = "comments"
)

// cursorKey returns the state key used to persist the high-water mark
// for stream. The value stored is the largest created_at_i seen.
func cursorKey(stream string) string { return "since_" + stream }

func init() {
	connectors.Register(New())
}

// Connector is a Hacker News search connector. A single Connector value
// is safe to reuse across concurrent Extract calls; the HTTP client is
// shared and has no mutable state.
type Connector struct {
	// Client is the http.Client used for API calls. Defaults to
	// http.DefaultClient on first use; tests may set a custom client
	// (e.g. with a short Timeout).
	Client *http.Client
}

// New returns a ready-to-register Connector.
func New() *Connector { return &Connector{} }

// Spec returns the connector's self-description.
func (c *Connector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{
		Name:        Name,
		DisplayName: "Hacker News",
		Description: "Incremental search over Hacker News stories and comments via the public Algolia API.",
		Version:     "0.1.0",
		AuthType:    connectors.AuthNone,
		Streams: []connectors.StreamSpec{
			{
				Name:        StreamStories,
				Description: "Stories matching the configured query, newest first.",
				SyncModes:   []connectors.SyncMode{connectors.Incremental, connectors.FullRefresh},
				DefaultCron: "0 * * * *",
			},
			{
				Name:        StreamComments,
				Description: "Comments matching the configured query, newest first.",
				SyncModes:   []connectors.SyncMode{connectors.Incremental, connectors.FullRefresh},
				DefaultCron: "0 * * * *",
			},
		},
	}
}

// Validate checks that a query is configured. It does not reach out to
// the API; the Algolia endpoint is public and has no credential to
// verify here.
func (c *Connector) Validate(_ context.Context, cfg connectors.ConnectorConfig) error {
	if strings.TrimSpace(cfg.String("query")) == "" {
		return fmt.Errorf("hackernews: query must not be empty")
	}
	return nil
}

// Discover reports both streams as available. Because the query might
// currently match no records, Available is not a claim of non-empty
// results; it only asserts the endpoint is reachable via the interface.
func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
	spec := c.Spec()
	out := make([]connectors.DiscoveredStream, 0, len(spec.Streams))
	for _, ss := range spec.Streams {
		out = append(out, connectors.DiscoveredStream{StreamSpec: ss, Available: true})
	}
	return &connectors.Catalog{Streams: out}, nil
}

// Extract fetches records for each requested stream and closes the
// returned channel when done. Each stream yields up to
// hits_per_page * max_pages records per call, newest first; a StateMsg
// follows each stream carrying the updated high-water mark.
//
// Pagination stops when the API returns fewer hits than requested, when
// max_pages is reached, or when ctx is cancelled.
func (c *Connector) Extract(ctx context.Context, cfg connectors.ConnectorConfig, streams []connectors.Stream, state connectors.State) (<-chan connectors.Message, error) {
	query := strings.TrimSpace(cfg.String("query"))
	if query == "" {
		return nil, fmt.Errorf("hackernews: query must not be empty")
	}
	hitsPerPage := cfg.Int("hits_per_page", DefaultHitsPerPage)
	if hitsPerPage <= 0 || hitsPerPage > MaxHitsPerPage {
		return nil, fmt.Errorf("hackernews: hits_per_page must be in 1..%d (got %d)", MaxHitsPerPage, hitsPerPage)
	}
	maxPages := cfg.Int("max_pages", DefaultMaxPages)
	if maxPages <= 0 {
		return nil, fmt.Errorf("hackernews: max_pages must be > 0 (got %d)", maxPages)
	}
	baseURL := cfg.String("base_url")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}

	ch := make(chan connectors.Message, 64)
	go func() {
		defer close(ch)
		for _, s := range streams {
			tag, err := tagForStream(s.Name)
			if err != nil {
				sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelWarn, err.Error()))
				continue
			}
			since := sinceFromState(state, s.Name)
			highWater := since
			pages := 0
			// upper is the exclusive upper bound for the next page.
			// Zero means "no upper bound" so the first request pulls
			// the freshest items; it advances backward in time as we
			// walk pages. The lower bound (since) is always applied
			// when non-zero, so re-runs do not re-fetch already-seen
			// records.
			var upper int64
			for pages < maxPages {
				hits, err := c.fetchPage(ctx, baseURL, query, tag, since, upper, hitsPerPage)
				if err != nil {
					sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelError, fmt.Sprintf("hackernews %s: %v", s.Name, err)))
					break
				}
				if len(hits) == 0 {
					break
				}
				for _, h := range hits {
					rec, err := recordFromHit(s.Name, h)
					if err != nil {
						sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelWarn, fmt.Sprintf("hackernews %s: %v", s.Name, err)))
						continue
					}
					if rec.Timestamp.Unix() > highWater {
						highWater = rec.Timestamp.Unix()
					}
					if !sendMessage(ctx, ch, connectors.RecordMessage(s.Name, rec)) {
						return
					}
				}
				pages++
				if len(hits) < hitsPerPage {
					break
				}
				// Algolia sorts newest-first, so the last hit carries
				// the oldest timestamp on the page. Walk backwards by
				// using that as the exclusive upper bound for the next
				// page (created_at_i < oldestOnPage). The since lower
				// bound is re-applied on every call so we never cross
				// back into already-seen territory; we still stop early
				// when the page is fully older than since to spare the
				// API a guaranteed-empty round trip.
				oldest := oldestCreatedAt(hits)
				if since > 0 && oldest <= since {
					break
				}
				upper = oldest
			}
			next := connectors.State{cursorKey(s.Name): highWater}
			if !sendMessage(ctx, ch, connectors.StateMessage(next)) {
				return
			}
		}
	}()
	return ch, nil
}

// tagForStream maps a stream name to the Algolia tag that filters that
// record kind.
func tagForStream(name string) (string, error) {
	switch name {
	case StreamStories:
		return "story", nil
	case StreamComments:
		return "comment", nil
	}
	return "", fmt.Errorf("hackernews: unknown stream %q", name)
}

// sinceFromState extracts the persisted high-water mark for stream.
// Missing, wrong-typed, or negative values yield 0 (full history).
func sinceFromState(state connectors.State, stream string) int64 {
	if state == nil {
		return 0
	}
	switch v := state[cursorKey(stream)].(type) {
	case int64:
		if v < 0 {
			return 0
		}
		return v
	case int:
		if v < 0 {
			return 0
		}
		return int64(v)
	case float64:
		if v < 0 {
			return 0
		}
		return int64(v)
	case json.Number:
		n, err := v.Int64()
		if err != nil || n < 0 {
			return 0
		}
		return n
	}
	return 0
}

// fetchPage issues one search_by_date call. The lowerExclusive bound
// (created_at_i>since) is always applied when non-zero so re-runs
// never re-fetch already-seen records. The upperExclusive bound
// (created_at_i<upper) is applied when non-zero to walk pages
// backwards in time.
func (c *Connector) fetchPage(ctx context.Context, baseURL, query, tag string, lowerExclusive, upperExclusive int64, hitsPerPage int) ([]map[string]any, error) {
	u, err := url.Parse(strings.TrimRight(baseURL, "/") + "/search_by_date")
	if err != nil {
		return nil, fmt.Errorf("parse base_url: %w", err)
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("tags", tag)
	q.Set("hitsPerPage", strconv.Itoa(hitsPerPage))
	var filters []string
	if lowerExclusive > 0 {
		filters = append(filters, "created_at_i>"+strconv.FormatInt(lowerExclusive, 10))
	}
	if upperExclusive > 0 {
		filters = append(filters, "created_at_i<"+strconv.FormatInt(upperExclusive, 10))
	}
	if len(filters) > 0 {
		q.Set("numericFilters", strings.Join(filters, ","))
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
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
		return nil, fmt.Errorf("algolia %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var payload struct {
		Hits []map[string]any `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return payload.Hits, nil
}

// recordFromHit turns an Algolia hit into a Record. Timestamp comes
// from created_at_i, which is the authoritative unix epoch field on
// every hit.
func recordFromHit(stream string, hit map[string]any) (connectors.Record, error) {
	ts, ok := createdAt(hit)
	if !ok {
		return connectors.Record{}, fmt.Errorf("hit missing created_at_i: %v", hit["objectID"])
	}
	return connectors.Record{
		Stream:    stream,
		Timestamp: time.Unix(ts, 0).UTC(),
		Data:      hit,
	}, nil
}

// createdAt extracts the unix timestamp from a hit, tolerating the
// several numeric shapes encoding/json can decode into.
func createdAt(hit map[string]any) (int64, bool) {
	switch v := hit["created_at_i"].(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// oldestCreatedAt returns the smallest created_at_i on a newest-first
// page. Hits without a valid timestamp are skipped; if none has one,
// zero is returned and the caller stops paginating.
func oldestCreatedAt(hits []map[string]any) int64 {
	var oldest int64
	for _, h := range hits {
		ts, ok := createdAt(h)
		if !ok {
			continue
		}
		if oldest == 0 || ts < oldest {
			oldest = ts
		}
	}
	return oldest
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
