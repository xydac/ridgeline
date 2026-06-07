package posthog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/xydac/ridgeline/connectors"
)

// Name is the connector name registered with the connectors package.
const Name = "posthog"

// DefaultBaseURL is the PostHog cloud endpoint used when base_url is empty.
const DefaultBaseURL = "https://app.posthog.com"

// DefaultLookbackDays is the initial backfill window when no cursor exists.
const DefaultLookbackDays = 30

// pageLimit is the number of events requested per API page.
const pageLimit = 100

// StreamEvents is the individual events stream.
const StreamEvents = "events"

// CursorKey holds the last successfully fetched event timestamp (RFC3339).
// Exported so tests can assert cursor advancement without hardcoding the string.
const CursorKey = "last_timestamp"

func init() {
	connectors.Register(New())
}

// Connector is a PostHog analytics connector. A single Connector value is
// safe to reuse across concurrent Extract calls.
type Connector struct {
	// Client is the http.Client used for API calls. Defaults to
	// http.DefaultClient; tests may inject a custom client.
	Client *http.Client
	// Now overrides the clock. Tests set this to pin the current time;
	// nil means time.Now.
	Now func() time.Time
}

// New returns a ready-to-register Connector.
func New() *Connector { return &Connector{} }

// Spec returns the connector's self-description.
func (c *Connector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{
		Name:        Name,
		DisplayName: "PostHog",
		Description: "Individual analytics events from PostHog (cloud or self-hosted).",
		Version:     "0.1.0",
		AuthType:    connectors.AuthAPIKey,
		AuthConfig:  &connectors.AuthConfig{KeyFields: []string{"api_key"}},
		Streams: []connectors.StreamSpec{{
			Name:        StreamEvents,
			Description: "Individual events with typed timestamp, event name, and distinct_id.",
			SyncModes:   []connectors.SyncMode{connectors.Incremental, connectors.FullRefresh},
			DefaultCron: "0 * * * *",
			Schema: connectors.Schema{Columns: []connectors.Column{
				{Name: "timestamp", Type: connectors.Timestamp, Key: true},
				{Name: "event", Type: connectors.String},
				{Name: "distinct_id", Type: connectors.String},
			}},
		}},
	}
}

// knownConfigKeys enumerates every config key this connector reads.
var knownConfigKeys = []string{
	"base_url", "project_id", "api_key", "lookback_days",
}

// Validate checks required fields and reports unknown keys.
func (c *Connector) Validate(_ context.Context, cfg connectors.ConnectorConfig) error {
	if err := connectors.CheckUnknownKeys(cfg, knownConfigKeys...); err != nil {
		return fmt.Errorf("posthog: %w", err)
	}
	if strings.TrimSpace(cfg.String("project_id")) == "" {
		return fmt.Errorf("posthog: project_id must not be empty")
	}
	if strings.TrimSpace(cfg.String("api_key")) == "" {
		return fmt.Errorf("posthog: api_key must not be empty (set api_key_ref to a stored credential)")
	}
	return nil
}

// Discover reports the events stream as available.
func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
	spec := c.Spec()
	out := make([]connectors.DiscoveredStream, 0, len(spec.Streams))
	for _, ss := range spec.Streams {
		out = append(out, connectors.DiscoveredStream{StreamSpec: ss, Available: true})
	}
	return &connectors.Catalog{Streams: out}, nil
}

// Extract fetches events from PostHog and closes the returned channel when
// done. Each call fetches events with a timestamp after the stored cursor
// using the /api/projects/{project_id}/events/ endpoint and follows all
// pagination links.
func (c *Connector) Extract(ctx context.Context, cfg connectors.ConnectorConfig, streams []connectors.Stream, state connectors.State) (<-chan connectors.Message, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.String("base_url")), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	projectID := strings.TrimSpace(cfg.String("project_id"))
	apiKey := strings.TrimSpace(cfg.String("api_key"))
	lookbackDays := cfg.Int("lookback_days", DefaultLookbackDays)

	if projectID == "" || apiKey == "" {
		return nil, fmt.Errorf("posthog: project_id and api_key are required")
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
					fmt.Sprintf("posthog: unknown stream %q (only %q is supported)", s.Name, StreamEvents)))
				continue
			}

			cursor := parseCursor(state)
			var after time.Time
			if cursor.IsZero() {
				after = now().UTC().Add(-time.Duration(lookbackDays) * 24 * time.Hour)
			} else {
				// Start one nanosecond past the cursor to avoid re-fetching
				// the last seen event.
				after = cursor.Add(time.Nanosecond)
			}

			client := c.Client
			if client == nil {
				client = http.DefaultClient
			}

			events, err := c.fetchAllEvents(ctx, client, baseURL, projectID, apiKey, after)
			if err != nil {
				sendMessage(ctx, ch, connectors.ErrorMessage(fmt.Errorf("posthog %s: %w", s.Name, err)))
				return
			}

			if len(events) == 0 {
				if !sendMessage(ctx, ch, connectors.StateMessage(state)) {
					return
				}
				continue
			}

			// Events arrive newest-first from the API; reverse for
			// chronological emit order.
			for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
				events[i], events[j] = events[j], events[i]
			}

			highWater := cursor
			for _, ev := range events {
				if ev.Timestamp.After(highWater) {
					highWater = ev.Timestamp
				}
				rec := connectors.Record{
					Stream:    s.Name,
					Timestamp: ev.Timestamp,
					Data: map[string]any{
						"timestamp":   ev.Timestamp.UTC().Format(time.RFC3339),
						"event":       ev.Event,
						"distinct_id": ev.DistinctID,
					},
				}
				if !sendMessage(ctx, ch, connectors.RecordMessage(s.Name, rec)) {
					return
				}
			}

			var newState connectors.State
			if highWater.IsZero() {
				newState = connectors.State{}
			} else {
				newState = connectors.State{CursorKey: highWater.UTC().Format(time.RFC3339Nano)}
			}
			if !sendMessage(ctx, ch, connectors.StateMessage(newState)) {
				return
			}
		}
	}()
	return ch, nil
}

// event is one entry in the PostHog events API response.
type event struct {
	Timestamp  time.Time
	Event      string
	DistinctID string
}

// rawEvent is the JSON shape PostHog returns for each event.
type rawEvent struct {
	ID         string `json:"id"`
	Event      string `json:"event"`
	DistinctID string `json:"distinct_id"`
	Timestamp  string `json:"timestamp"`
}

// fetchAllEvents retrieves all event pages starting from after and
// following the "next" cursor links in the response.
func (c *Connector) fetchAllEvents(ctx context.Context, client *http.Client, baseURL, projectID, apiKey string, after time.Time) ([]event, error) {
	endpoint := fmt.Sprintf("%s/api/projects/%s/events/", baseURL, projectID)

	q := url.Values{}
	q.Set("after", after.UTC().Format(time.RFC3339Nano))
	q.Set("limit", fmt.Sprintf("%d", pageLimit))
	nextURL := endpoint + "?" + q.Encode()

	var all []event
	for nextURL != "" {
		batch, next, err := c.fetchPage(ctx, client, nextURL, apiKey)
		if err != nil {
			return nil, err
		}
		all = append(all, batch...)
		nextURL = next
	}
	return all, nil
}

// fetchPage retrieves a single page from url and returns the events and the
// next-page URL (empty string when there are no more pages).
func (c *Connector) fetchPage(ctx context.Context, client *http.Client, pageURL, apiKey string) ([]event, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "ridgeline/0.0.0-dev (+https://github.com/xydac/ridgeline)")

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, "", fmt.Errorf("401 Unauthorized: %s", strings.TrimSpace(string(body)))
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, "", fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Results []rawEvent `json:"results"`
		Next    *string    `json:"next"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, "", fmt.Errorf("decode response: %w", err)
	}

	out := make([]event, 0, len(payload.Results))
	for _, raw := range payload.Results {
		ts, err := time.Parse(time.RFC3339Nano, raw.Timestamp)
		if err != nil {
			// Try without nanoseconds.
			ts, err = time.Parse(time.RFC3339, raw.Timestamp)
			if err != nil {
				return nil, "", fmt.Errorf("parse timestamp %q: %w", raw.Timestamp, err)
			}
		}
		out = append(out, event{
			Timestamp:  ts.UTC(),
			Event:      raw.Event,
			DistinctID: raw.DistinctID,
		})
	}

	var next string
	if payload.Next != nil {
		next = *payload.Next
	}
	return out, next, nil
}

// parseCursor decodes the persisted RFC3339 timestamp from state.
func parseCursor(state connectors.State) time.Time {
	if state == nil {
		return time.Time{}
	}
	raw, ok := state[CursorKey].(string)
	if !ok || raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t, err = time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}
		}
	}
	return t.UTC()
}

// sendMessage posts m on ch unless ctx is cancelled first.
func sendMessage(ctx context.Context, ch chan<- connectors.Message, m connectors.Message) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- m:
		return true
	}
}
