package plausible

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
const Name = "plausible"

// DefaultBaseURL is the Plausible cloud endpoint used when base_url is empty.
const DefaultBaseURL = "https://plausible.io"

// DefaultLookbackDays is the initial backfill window when no cursor exists.
const DefaultLookbackDays = 30

// StreamTimeseries is the daily aggregate metrics stream.
const StreamTimeseries = "timeseries"

// cursorKey holds the last successfully fetched date (YYYY-MM-DD).
const cursorKey = "last_date"

func init() {
	connectors.Register(New())
}

// Connector is a Plausible Analytics connector. A single Connector value
// is safe to reuse across concurrent Extract calls.
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

// Spec returns the connector's self-description.
func (c *Connector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{
		Name:        Name,
		DisplayName: "Plausible Analytics",
		Description: "Daily aggregate metrics from Plausible Analytics (cloud or self-hosted).",
		Version:     "0.1.0",
		AuthType:    connectors.AuthAPIKey,
		AuthConfig:  &connectors.AuthConfig{KeyFields: []string{"api_token"}},
		Streams: []connectors.StreamSpec{{
			Name:        StreamTimeseries,
			Description: "Daily visitors, pageviews, bounce rate, and visit duration for the configured site.",
			SyncModes:   []connectors.SyncMode{connectors.Incremental, connectors.FullRefresh},
			DefaultCron: "0 1 * * *",
			Schema: connectors.Schema{Columns: []connectors.Column{
				{Name: "date", Type: connectors.Timestamp, Key: true},
				{Name: "visitors", Type: connectors.Int},
				{Name: "pageviews", Type: connectors.Int},
				{Name: "bounce_rate", Type: connectors.Float},
				{Name: "visit_duration", Type: connectors.Float},
			}},
		}},
	}
}

// knownConfigKeys enumerates every config key this connector reads.
var knownConfigKeys = []string{
	"base_url", "site_id", "api_token", "lookback_days",
}

// Validate checks required fields and reports unknown keys.
func (c *Connector) Validate(_ context.Context, cfg connectors.ConnectorConfig) error {
	if err := connectors.CheckUnknownKeys(cfg, knownConfigKeys...); err != nil {
		return fmt.Errorf("plausible: %w", err)
	}
	if strings.TrimSpace(cfg.String("site_id")) == "" {
		return fmt.Errorf("plausible: site_id must not be empty")
	}
	if strings.TrimSpace(cfg.String("api_token")) == "" {
		return fmt.Errorf("plausible: api_token must not be empty (set api_token_ref to a stored credential)")
	}
	return nil
}

// Discover reports the timeseries stream as available.
func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
	spec := c.Spec()
	out := make([]connectors.DiscoveredStream, 0, len(spec.Streams))
	for _, ss := range spec.Streams {
		out = append(out, connectors.DiscoveredStream{StreamSpec: ss, Available: true})
	}
	return &connectors.Catalog{Streams: out}, nil
}

// Extract fetches the timeseries stream from Plausible and closes the
// returned channel when done. Each call fetches dates from (cursor+1 day)
// through yesterday using the /api/v1/stats/timeseries endpoint.
func (c *Connector) Extract(ctx context.Context, cfg connectors.ConnectorConfig, streams []connectors.Stream, state connectors.State) (<-chan connectors.Message, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.String("base_url")), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	siteID := strings.TrimSpace(cfg.String("site_id"))
	apiToken := strings.TrimSpace(cfg.String("api_token"))
	lookbackDays := cfg.Int("lookback_days", DefaultLookbackDays)

	if siteID == "" || apiToken == "" {
		return nil, fmt.Errorf("plausible: site_id and api_token are required")
	}

	now := c.Now
	if now == nil {
		now = time.Now
	}

	ch := make(chan connectors.Message, 64)
	go func() {
		defer close(ch)
		for _, s := range streams {
			if s.Name != StreamTimeseries {
				sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelWarn,
					fmt.Sprintf("plausible: unknown stream %q (only %q is supported)", s.Name, StreamTimeseries)))
				continue
			}

			today := now().UTC().Truncate(24 * time.Hour)
			yesterday := today.Add(-24 * time.Hour)

			cursor := parseCursor(state)
			var startDate time.Time
			if cursor.IsZero() {
				startDate = today.Add(-time.Duration(lookbackDays) * 24 * time.Hour)
			} else {
				startDate = cursor.Add(24 * time.Hour)
			}

			if !startDate.Before(today) {
				// Already up to date through yesterday.
				if !sendMessage(ctx, ch, connectors.StateMessage(state)) {
					return
				}
				continue
			}

			endDate := yesterday
			if endDate.Before(startDate) {
				endDate = startDate
			}

			rows, err := c.fetchTimeseries(ctx, baseURL, siteID, apiToken, startDate, endDate)
			if err != nil {
				sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelError,
					fmt.Sprintf("plausible %s: %v", s.Name, err)))
				newState := connectors.State{}
				if cursor != (time.Time{}) {
					newState[cursorKey] = cursor.Format("2006-01-02")
				}
				sendMessage(ctx, ch, connectors.StateMessage(newState))
				continue
			}

			highWater := cursor
			for _, row := range rows {
				rec, err := recordFromRow(s.Name, row)
				if err != nil {
					sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelWarn,
						fmt.Sprintf("plausible %s: %v", s.Name, err)))
					continue
				}
				if rec.Timestamp.After(highWater) {
					highWater = rec.Timestamp
				}
				if !sendMessage(ctx, ch, connectors.RecordMessage(s.Name, rec)) {
					return
				}
			}

			var newState connectors.State
			if highWater.IsZero() {
				newState = connectors.State{}
			} else {
				newState = connectors.State{cursorKey: highWater.UTC().Format("2006-01-02")}
			}
			if !sendMessage(ctx, ch, connectors.StateMessage(newState)) {
				return
			}
		}
	}()
	return ch, nil
}

// timeseriesRow is one entry in the /api/v1/stats/timeseries response.
type timeseriesRow struct {
	Date          string  `json:"date"`
	Visitors      int64   `json:"visitors"`
	Pageviews     int64   `json:"pageviews"`
	BounceRate    float64 `json:"bounce_rate"`
	VisitDuration float64 `json:"visit_duration"`
}

func (c *Connector) fetchTimeseries(ctx context.Context, baseURL, siteID, apiToken string, start, end time.Time) ([]timeseriesRow, error) {
	u, err := url.Parse(baseURL + "/api/v1/stats/timeseries")
	if err != nil {
		return nil, fmt.Errorf("parse base_url: %w", err)
	}
	q := u.Query()
	q.Set("site_id", siteID)
	q.Set("period", "custom")
	q.Set("date", start.Format("2006-01-02")+","+end.Format("2006-01-02"))
	q.Set("metrics", "visitors,pageviews,bounce_rate,visit_duration")
	q.Set("interval", "date")
	u.RawQuery = q.Encode()

	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "ridgeline/0.0.0-dev (+https://github.com/xydac/ridgeline)")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("401 Unauthorized: %s", strings.TrimSpace(string(body)))
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Results []timeseriesRow `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return payload.Results, nil
}

// recordFromRow converts a timeseries row to a connector Record.
func recordFromRow(stream string, row timeseriesRow) (connectors.Record, error) {
	ts, err := time.Parse("2006-01-02", row.Date)
	if err != nil {
		return connectors.Record{}, fmt.Errorf("parse date %q: %w", row.Date, err)
	}
	return connectors.Record{
		Stream:    stream,
		Timestamp: ts.UTC(),
		Data: map[string]any{
			"date":           row.Date,
			"visitors":       row.Visitors,
			"pageviews":      row.Pageviews,
			"bounce_rate":    row.BounceRate,
			"visit_duration": row.VisitDuration,
		},
	}, nil
}

// parseCursor decodes the persisted YYYY-MM-DD date string.
func parseCursor(state connectors.State) time.Time {
	if state == nil {
		return time.Time{}
	}
	raw, ok := state[cursorKey].(string)
	if !ok || raw == "" {
		return time.Time{}
	}
	t, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}
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
