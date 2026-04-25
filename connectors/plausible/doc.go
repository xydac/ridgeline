// Package plausible is the native Ridgeline connector for Plausible
// Analytics (https://plausible.io or self-hosted).
//
// Authentication uses a Plausible API token. Create one at
// Settings -> API Tokens in your Plausible dashboard, then store it with
// `ridgeline creds put <name>` and reference it from ridgeline.yaml as
// `api_token_ref: <name>`.
//
// The connector exposes a "timeseries" stream that fetches daily aggregate
// metrics from /api/v1/stats/timeseries. Each record carries one day of
// data with visitors, pageviews, bounce_rate, and visit_duration columns.
// Syncs are incremental: the cursor is the last date successfully fetched.
//
// Config keys:
//
//	site_id      domain registered in Plausible (e.g. "example.com") (required)
//	api_token    Plausible API token (or use api_token_ref)            (required)
//	base_url     override for self-hosted installs                     (optional, default https://plausible.io)
//	lookback_days initial backfill window when no cursor exists        (optional, default 30)
package plausible
