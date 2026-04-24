// Package gsc is the native Ridgeline connector for Google Search
// Console (https://search.google.com/search-console), pulling daily
// Search Analytics rows via the webmasters/v3 API.
//
// Auth is OAuth 2.0 with a bring-your-own refresh token. The full
// browser + PKCE flow is not yet wired into the binary, so today the
// user obtains a refresh token out of band (for example with the
// Google OAuth Playground or the `oauth2l` CLI) and stores it in the
// credential store alongside the OAuth client id and secret. The
// connector exchanges the refresh token for short-lived access tokens
// at sync time and caches the access token in the per-connector state
// map so a typical run makes one token call per hour.
//
// The sole stream today is "search_analytics", returned by
// POST /webmasters/v3/sites/{siteUrl}/searchAnalytics/query. Rows are
// bucketed by the configured dimensions (default: date, query, page)
// and the incremental cursor is a YYYY-MM-DD date stored under
// `last_date`. Subsequent syncs request startDate = last_date + 1 day.
//
// Google embargoes the most recent ~2 days of data, so each sync
// queries up to (today - end_offset_days) inclusive (default 2).
//
// Config keys:
//
//	site_url            sc-domain:example.com or https://example.com/ (required)
//	client_id           OAuth client id     (required; set via client_id_ref)
//	client_secret       OAuth client secret (required; set via client_secret_ref)
//	refresh_token       long-lived refresh token (required; set via refresh_token_ref)
//	dimensions          []string of GSC dimensions (optional; default [date,query,page])
//	row_limit           rows per page, 1..25000 (optional; default 1000)
//	max_pages           page cap per extract (optional; default 10)
//	lookback_days       initial lookback on first sync (optional; default 28)
//	end_offset_days     days before today to stop (optional; default 2)
package gsc
