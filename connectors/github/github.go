package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/xydac/ridgeline/connectors"
)

// Name is the connector name registered with the connectors package.
const Name = "github"

// DefaultBaseURL is the GitHub REST API endpoint used when base_url is empty.
const DefaultBaseURL = "https://api.github.com"

// StreamViews is the daily repository views stream.
const StreamViews = "views"

// StreamClones is the daily repository clones stream.
const StreamClones = "clones"

func init() {
	connectors.Register(New())
}

// Connector is a GitHub repository traffic connector. A single Connector
// value is safe to reuse across concurrent Extract calls.
type Connector struct {
	// Client is the http.Client used for API calls. Defaults to
	// http.DefaultClient; tests may inject a custom client.
	Client *http.Client
	// Now overrides the clock. Tests set this to pin the current date;
	// nil means time.Now.
	Now func() time.Time
}

// New returns a ready-to-register Connector.
func New() *Connector { return &Connector{} }

var trafficSchema = connectors.Schema{Columns: []connectors.Column{
	{Name: "date", Type: connectors.Timestamp, Key: true},
	{Name: "count", Type: connectors.Int},
	{Name: "uniques", Type: connectors.Int},
}}

// Spec returns the connector's self-description.
func (c *Connector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{
		Name:        Name,
		DisplayName: "GitHub Repository Traffic",
		Description: "Daily views and clones for a GitHub repository (requires push access).",
		Version:     "0.1.0",
		AuthType:    connectors.AuthAPIKey,
		AuthConfig:  &connectors.AuthConfig{KeyFields: []string{"api_token"}},
		Streams: []connectors.StreamSpec{
			{
				Name:        StreamViews,
				Description: "Daily unique visitors and total views for the repository.",
				SyncModes:   []connectors.SyncMode{connectors.Incremental},
				DefaultCron: "0 2 * * *",
				Schema:      trafficSchema,
			},
			{
				Name:        StreamClones,
				Description: "Daily unique cloners and total clones for the repository.",
				SyncModes:   []connectors.SyncMode{connectors.Incremental},
				DefaultCron: "0 2 * * *",
				Schema:      trafficSchema,
			},
		},
	}
}

var knownConfigKeys = []string{
	"owner", "repo", "api_token", "base_url",
}

// Validate checks required fields and reports unknown keys.
func (c *Connector) Validate(_ context.Context, cfg connectors.ConnectorConfig) error {
	if err := connectors.CheckUnknownKeys(cfg, knownConfigKeys...); err != nil {
		return fmt.Errorf("github: %w", err)
	}
	if strings.TrimSpace(cfg.String("owner")) == "" {
		return fmt.Errorf("github: owner must not be empty")
	}
	if strings.TrimSpace(cfg.String("repo")) == "" {
		return fmt.Errorf("github: repo must not be empty")
	}
	if strings.TrimSpace(cfg.String("api_token")) == "" {
		return fmt.Errorf("github: api_token must not be empty (set api_token_ref to a stored credential)")
	}
	return nil
}

// Discover reports both traffic streams as available.
func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
	spec := c.Spec()
	out := make([]connectors.DiscoveredStream, 0, len(spec.Streams))
	for _, ss := range spec.Streams {
		out = append(out, connectors.DiscoveredStream{StreamSpec: ss, Available: true})
	}
	return &connectors.Catalog{Streams: out}, nil
}

// Extract fetches traffic data from GitHub and closes the returned channel
// when done. Each call fetches up to the last 14 days of daily data and
// emits only records newer than the per-stream cursor stored in state.
func (c *Connector) Extract(ctx context.Context, cfg connectors.ConnectorConfig, streams []connectors.Stream, state connectors.State) (<-chan connectors.Message, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.String("base_url")), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	owner := strings.TrimSpace(cfg.String("owner"))
	repo := strings.TrimSpace(cfg.String("repo"))
	apiToken := strings.TrimSpace(cfg.String("api_token"))

	if owner == "" || repo == "" || apiToken == "" {
		return nil, fmt.Errorf("github: owner, repo, and api_token are required")
	}

	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}

	ch := make(chan connectors.Message, 64)
	go func() {
		defer close(ch)
		currentState := copyState(state)

		for _, s := range streams {
			switch s.Name {
			case StreamViews, StreamClones:
			default:
				sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelWarn,
					fmt.Sprintf("github: unknown stream %q (known: %q, %q)", s.Name, StreamViews, StreamClones)))
				continue
			}

			cursorKey := s.Name + "_cursor"
			cursor := parseCursor(currentState, cursorKey)

			entries, err := c.fetchTraffic(ctx, client, baseURL, owner, repo, apiToken, s.Name)
			if err != nil {
				sendMessage(ctx, ch, connectors.ErrorMessage(fmt.Errorf("github %s: %w", s.Name, err)))
				return
			}

			highWater := cursor
			for _, entry := range entries {
				if !entry.Timestamp.After(cursor) {
					continue
				}
				if entry.Timestamp.After(highWater) {
					highWater = entry.Timestamp
				}
				rec := connectors.Record{
					Stream:    s.Name,
					Timestamp: entry.Timestamp,
					Data: map[string]any{
						"date":    entry.Timestamp.Format("2006-01-02"),
						"count":   entry.Count,
						"uniques": entry.Uniques,
					},
				}
				if !sendMessage(ctx, ch, connectors.RecordMessage(s.Name, rec)) {
					return
				}
			}

			if !highWater.IsZero() {
				currentState[cursorKey] = highWater.UTC().Format(time.RFC3339)
			}
			if !sendMessage(ctx, ch, connectors.StateMessage(currentState)) {
				return
			}
		}
	}()
	return ch, nil
}

// trafficEntry is one daily row from the GitHub traffic API.
type trafficEntry struct {
	Timestamp time.Time
	Count     int64
	Uniques   int64
}

// rawEntry is the JSON shape GitHub returns for each day in the traffic lists.
type rawEntry struct {
	Timestamp string `json:"timestamp"`
	Count     int64  `json:"count"`
	Uniques   int64  `json:"uniques"`
}

func (c *Connector) fetchTraffic(ctx context.Context, client *http.Client, baseURL, owner, repo, apiToken, stream string) ([]trafficEntry, error) {
	var listKey string
	switch stream {
	case StreamViews:
		listKey = "views"
	case StreamClones:
		listKey = "clones"
	default:
		return nil, fmt.Errorf("unknown stream %q", stream)
	}

	url := fmt.Sprintf("%s/repos/%s/%s/traffic/%s", baseURL, owner, repo, stream)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "ridgeline/0.0.0-dev (+https://github.com/xydac/ridgeline)")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("401 Unauthorized: %s", parseGitHubMsg(body))
	}
	if resp.StatusCode == http.StatusForbidden {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("403 Forbidden (push access required): %s", parseGitHubMsg(body))
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var payload map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	raw, ok := payload[listKey]
	if !ok {
		return nil, nil
	}

	var rows []rawEntry
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, fmt.Errorf("decode %s list: %w", listKey, err)
	}

	out := make([]trafficEntry, 0, len(rows))
	for _, row := range rows {
		ts, err := time.Parse(time.RFC3339, row.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("parse timestamp %q: %w", row.Timestamp, err)
		}
		out = append(out, trafficEntry{
			Timestamp: ts.UTC(),
			Count:     row.Count,
			Uniques:   row.Uniques,
		})
	}
	return out, nil
}

// parseGitHubMsg extracts the "message" field from a GitHub API error
// body. If the body is not valid JSON or has no message field, the raw
// body is returned trimmed of whitespace so the caller always gets a
// single-line error string.
func parseGitHubMsg(body []byte) string {
	var e struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &e); err == nil && e.Message != "" {
		return e.Message
	}
	return strings.TrimSpace(string(body))
}

// parseCursor reads the per-stream cursor from state and returns zero time
// if absent or unparseable.
func parseCursor(state connectors.State, key string) time.Time {
	if state == nil {
		return time.Time{}
	}
	raw, ok := state[key].(string)
	if !ok || raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// copyState returns a shallow copy of state so mutations during a run do
// not affect the caller's original map.
func copyState(state connectors.State) connectors.State {
	out := connectors.State{}
	for k, v := range state {
		out[k] = v
	}
	return out
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
