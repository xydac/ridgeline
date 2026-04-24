package gsc

import (
	"bytes"
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
const Name = "gsc"

// StreamSearchAnalytics is the sole stream currently supported.
const StreamSearchAnalytics = "search_analytics"

// cursorKey holds the YYYY-MM-DD high-water mark for this connector.
const cursorKey = "last_date"

// dateLayout is the date format Google Search Console accepts and emits.
const dateLayout = "2006-01-02"

// Defaults tuned for the hourly-cadence, single-site case. Bulk
// backfills should raise row_limit and max_pages in config.
const (
	DefaultRowLimit      = 1000
	MaxRowLimit          = 25000
	DefaultMaxPages      = 10
	DefaultLookbackDays  = 28
	DefaultEndOffsetDays = 2
)

// defaultDimensions is used when the config omits `dimensions`. Matches
// what a first-time user exploring keyword performance would most
// likely want.
var defaultDimensions = []string{"date", "query", "page"}

// validDimensions enumerates every dimension the Search Analytics API
// accepts. The set is closed and rarely changes.
var validDimensions = map[string]struct{}{
	"date":             {},
	"query":            {},
	"page":             {},
	"country":          {},
	"device":           {},
	"searchAppearance": {},
}

// defaultAPIBase is the Search Console API root. Exposed as a package
// variable so tests can redirect it at an httptest server.
var defaultAPIBase = "https://searchconsole.googleapis.com"

func init() {
	connectors.Register(New())
}

// Connector is a Google Search Console connector. It is safe to reuse
// a single value across concurrent Extract calls; per-call state lives
// in the authorizer created inside Extract.
type Connector struct {
	// Client is the http.Client used for API and token calls. Defaults
	// to http.DefaultClient on first use; tests may set a custom client.
	Client *http.Client
	// Now overrides the clock used for date-window math and token
	// expiry. Tests pin this to produce deterministic responses.
	Now func() time.Time
	// APIBase and TokenURL let tests redirect to an httptest server.
	// Empty strings fall back to the Google production endpoints.
	APIBase  string
	TokenURL string
}

// New returns a ready-to-register Connector.
func New() *Connector { return &Connector{} }

// Spec returns the connector's self-description.
func (c *Connector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{
		Name:        Name,
		DisplayName: "Google Search Console",
		Description: "Daily Search Analytics rows for a configured property, fetched via the webmasters/v3 API.",
		Version:     "0.1.0",
		AuthType:    connectors.AuthOAuth2,
		AuthConfig: &connectors.AuthConfig{
			TokenURL:  defaultTokenURL,
			Scopes:    []string{"https://www.googleapis.com/auth/webmasters.readonly"},
			KeyFields: []string{"client_id", "client_secret", "refresh_token"},
		},
		Streams: []connectors.StreamSpec{{
			Name:        StreamSearchAnalytics,
			Description: "Search Analytics rows bucketed by the configured dimensions.",
			SyncModes:   []connectors.SyncMode{connectors.Incremental, connectors.FullRefresh},
			DefaultCron: "0 * * * *",
		}},
	}
}

// knownConfigKeys enumerates every config key this connector reads.
var knownConfigKeys = []string{
	"site_url",
	"client_id", "client_secret", "refresh_token",
	"dimensions",
	"row_limit", "max_pages",
	"lookback_days", "end_offset_days",
}

// Validate checks the static shape of cfg: required fields present,
// numeric bounds sane, dimension names in the known set. It does not
// call the API.
func (c *Connector) Validate(_ context.Context, cfg connectors.ConnectorConfig) error {
	if err := connectors.CheckUnknownKeys(cfg, knownConfigKeys...); err != nil {
		return fmt.Errorf("gsc: %w", err)
	}
	if strings.TrimSpace(cfg.String("site_url")) == "" {
		return fmt.Errorf("gsc: site_url must not be empty")
	}
	for _, field := range []string{"client_id", "client_secret", "refresh_token"} {
		if strings.TrimSpace(cfg.String(field)) == "" {
			return fmt.Errorf("gsc: %s must not be empty (set %s_ref to a stored credential)", field, field)
		}
	}
	for _, d := range dimensionsFromConfig(cfg) {
		if _, ok := validDimensions[d]; !ok {
			return fmt.Errorf("gsc: unknown dimension %q (valid: date, query, page, country, device, searchAppearance)", d)
		}
	}
	if rl := cfg.Int("row_limit", DefaultRowLimit); rl <= 0 || rl > MaxRowLimit {
		return fmt.Errorf("gsc: row_limit must be in 1..%d (got %d)", MaxRowLimit, rl)
	}
	if mp := cfg.Int("max_pages", DefaultMaxPages); mp <= 0 {
		return fmt.Errorf("gsc: max_pages must be > 0 (got %d)", mp)
	}
	if lb := cfg.Int("lookback_days", DefaultLookbackDays); lb <= 0 {
		return fmt.Errorf("gsc: lookback_days must be > 0 (got %d)", lb)
	}
	if eo := cfg.Int("end_offset_days", DefaultEndOffsetDays); eo < 0 {
		return fmt.Errorf("gsc: end_offset_days must be >= 0 (got %d)", eo)
	}
	return nil
}

// Discover reports the search_analytics stream as available. A real
// liveness probe would cost a token exchange plus an API round-trip
// every time the orchestrator refreshes; that is deferred until a
// concrete failure mode motivates the cost.
func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
	spec := c.Spec()
	out := make([]connectors.DiscoveredStream, 0, len(spec.Streams))
	for _, ss := range spec.Streams {
		out = append(out, connectors.DiscoveredStream{StreamSpec: ss, Available: true})
	}
	return &connectors.Catalog{Streams: out}, nil
}

// Extract queries the Search Analytics endpoint for rows between the
// cursor (exclusive) and today - end_offset_days (inclusive). The
// returned channel is closed when extraction is complete. On a first
// sync the lookback window starts at today - lookback_days.
func (c *Connector) Extract(ctx context.Context, cfg connectors.ConnectorConfig, streams []connectors.Stream, state connectors.State) (<-chan connectors.Message, error) {
	siteURL := strings.TrimSpace(cfg.String("site_url"))
	if siteURL == "" {
		return nil, fmt.Errorf("gsc: site_url is required")
	}
	dims := dimensionsFromConfig(cfg)
	rowLimit := cfg.Int("row_limit", DefaultRowLimit)
	maxPages := cfg.Int("max_pages", DefaultMaxPages)
	lookback := cfg.Int("lookback_days", DefaultLookbackDays)
	endOffset := cfg.Int("end_offset_days", DefaultEndOffsetDays)
	if rowLimit <= 0 || rowLimit > MaxRowLimit {
		return nil, fmt.Errorf("gsc: row_limit out of range: %d", rowLimit)
	}
	if maxPages <= 0 {
		return nil, fmt.Errorf("gsc: max_pages must be > 0 (got %d)", maxPages)
	}
	if lookback <= 0 {
		return nil, fmt.Errorf("gsc: lookback_days must be > 0 (got %d)", lookback)
	}
	if endOffset < 0 {
		return nil, fmt.Errorf("gsc: end_offset_days must be >= 0 (got %d)", endOffset)
	}
	hasDateDim := false
	for _, d := range dims {
		if d == "date" {
			hasDateDim = true
			break
		}
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	auth, err := newTokenAuth(cfg, state, c.Client, now)
	if err != nil {
		return nil, err
	}
	if c.TokenURL != "" {
		auth.tokenURL = c.TokenURL
	}
	apiBase := strings.TrimRight(c.APIBase, "/")
	if apiBase == "" {
		apiBase = defaultAPIBase
	}

	ch := make(chan connectors.Message, 64)
	go func() {
		defer close(ch)
		for _, s := range streams {
			if s.Name != StreamSearchAnalytics {
				sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelWarn,
					fmt.Sprintf("gsc: unknown stream %q (only %q is supported)", s.Name, StreamSearchAnalytics)))
				continue
			}
			cursor := parseCursor(state)
			startDate, endDate, ok := computeWindow(cursor, now(), lookback, endOffset)
			if !ok {
				// Nothing to pull: the available window is entirely at
				// or behind the cursor. Emit a state checkpoint so the
				// cached access token, if freshly obtained, still lands.
				sendMessage(ctx, ch, connectors.StateMessage(stateWithAuth(auth, cursor)))
				continue
			}
			highWater := cursor
			for page := 0; page < maxPages; page++ {
				startRow := page * rowLimit
				rows, err := c.fetchPage(ctx, apiBase, siteURL, auth, startDate, endDate, dims, rowLimit, startRow)
				if auth.consumeDirty() {
					if !sendMessage(ctx, ch, connectors.StateMessage(stateWithAuth(auth, highWater))) {
						return
					}
				}
				if err != nil {
					sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelError,
						fmt.Sprintf("gsc %s: %v", s.Name, err)))
					break
				}
				if len(rows) == 0 {
					break
				}
				for _, row := range rows {
					rec, err := recordFromRow(s.Name, dims, row, startDate, endDate, hasDateDim)
					if err != nil {
						sendMessage(ctx, ch, connectors.LogMessage(connectors.LevelWarn,
							fmt.Sprintf("gsc %s: %v", s.Name, err)))
						continue
					}
					if rec.Timestamp.After(highWater) {
						highWater = rec.Timestamp
					}
					if !sendMessage(ctx, ch, connectors.RecordMessage(s.Name, rec)) {
						return
					}
				}
				if len(rows) < rowLimit {
					break
				}
			}
			if !sendMessage(ctx, ch, connectors.StateMessage(stateWithAuth(auth, highWater))) {
				return
			}
		}
	}()
	return ch, nil
}

// dimensionsFromConfig returns the user-configured dimensions, falling
// back to the default list when the key is missing.
func dimensionsFromConfig(cfg connectors.ConnectorConfig) []string {
	d := cfg.StringSlice("dimensions")
	if len(d) == 0 {
		return defaultDimensions
	}
	return d
}

// stateWithAuth builds a State carrying the cursor plus any
// auth-managed fields.
func stateWithAuth(auth *tokenAuth, highWater time.Time) connectors.State {
	s := connectors.State{}
	if !highWater.IsZero() {
		s[cursorKey] = highWater.UTC().Format(dateLayout)
	}
	auth.applyTo(s)
	return s
}

// parseCursor decodes the persisted YYYY-MM-DD cursor. A missing or
// malformed value yields the zero time.
func parseCursor(state connectors.State) time.Time {
	if state == nil {
		return time.Time{}
	}
	raw, ok := state[cursorKey].(string)
	if !ok || raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(dateLayout, raw)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// computeWindow returns [startDate, endDate] for the next query. The
// third return is false when the window is empty (nothing new to pull).
func computeWindow(cursor, now time.Time, lookback, endOffset int) (time.Time, time.Time, bool) {
	endDate := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -endOffset)
	var startDate time.Time
	if cursor.IsZero() {
		startDate = endDate.AddDate(0, 0, -lookback+1)
	} else {
		startDate = cursor.AddDate(0, 0, 1)
	}
	if startDate.After(endDate) {
		return time.Time{}, time.Time{}, false
	}
	return startDate, endDate, true
}

// apiRow is the raw Search Analytics row shape.
type apiRow struct {
	Keys        []string `json:"keys"`
	Clicks      float64  `json:"clicks"`
	Impressions float64  `json:"impressions"`
	CTR         float64  `json:"ctr"`
	Position    float64  `json:"position"`
}

// fetchPage issues one POST against
// /webmasters/v3/sites/{siteUrl}/searchAnalytics/query and returns
// the rows on that page. A 401 triggers exactly one forced re-auth
// plus retry; a second 401 surfaces the original body.
func (c *Connector) fetchPage(ctx context.Context, apiBase, siteURL string, auth *tokenAuth, start, end time.Time, dims []string, rowLimit, startRow int) ([]apiRow, error) {
	reqURL := apiBase + "/webmasters/v3/sites/" + url.PathEscape(siteURL) + "/searchAnalytics/query"
	body, err := json.Marshal(map[string]any{
		"startDate":  start.Format(dateLayout),
		"endDate":    end.Format(dateLayout),
		"dimensions": dims,
		"rowLimit":   rowLimit,
		"startRow":   startRow,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	do := func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		if err := auth.decorate(req); err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "ridgeline/0.0.0-dev (+https://github.com/xydac/ridgeline)")
		return client.Do(req)
	}
	resp, err := do()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		origStatus := resp.Status
		origBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if rerr := auth.reauth(ctx); rerr != nil {
			return nil, fmt.Errorf("gsc %s: %s", origStatus, strings.TrimSpace(string(origBody)))
		}
		resp, err = do()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusUnauthorized {
			resp.Body.Close()
			return nil, fmt.Errorf("gsc %s: %s", origStatus, strings.TrimSpace(string(origBody)))
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("gsc %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var payload struct {
		Rows []apiRow `json:"rows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return payload.Rows, nil
}

// recordFromRow turns one API row into a Record. The data map flattens
// each dimension onto its name (so `keys: ["2026-04-20","foo","/bar"]`
// with dims ["date","query","page"] becomes fields date/query/page)
// and carries the four metric fields. When `date` is a dimension, the
// row's date is used as the Record timestamp; otherwise the window's
// endDate is used so the cursor still advances on each run.
func recordFromRow(stream string, dims []string, row apiRow, start, end time.Time, hasDateDim bool) (connectors.Record, error) {
	data := map[string]any{
		"clicks":      row.Clicks,
		"impressions": row.Impressions,
		"ctr":         row.CTR,
		"position":    row.Position,
	}
	if len(row.Keys) != len(dims) {
		return connectors.Record{}, fmt.Errorf("row keys length %d != dimensions length %d", len(row.Keys), len(dims))
	}
	var ts time.Time
	for i, d := range dims {
		data[d] = row.Keys[i]
		if d == "date" {
			t, err := time.Parse(dateLayout, row.Keys[i])
			if err != nil {
				return connectors.Record{}, fmt.Errorf("row date %q: %w", row.Keys[i], err)
			}
			ts = t.UTC()
		}
	}
	if !hasDateDim {
		ts = end
	}
	_ = start
	return connectors.Record{Stream: stream, Timestamp: ts, Data: data}, nil
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
