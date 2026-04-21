// Package umami is the native Ridgeline connector for self-hosted
// Umami analytics (https://umami.is).
//
// The connector authenticates with a per-installation API key set in
// the Umami UI (Settings -> API Keys). The key is sent on every request
// in the x-umami-api-key header. Store the key with
// `ridgeline creds put <name>` and reference it from ridgeline.yaml as
// `api_key_ref: <name>`; the sync command resolves the ref to the
// plaintext key before this connector ever sees the config.
//
// Today the connector exposes one stream, "events", pulled from
// /api/websites/{website_id}/events. Records are paginated in chunks
// of page_size (default 100, max 1000). Each extract caps at max_pages
// (default 10) so a runaway query stays bounded; raise max_pages in
// config for a bulk backfill.
//
// Config keys:
//
//	base_url     https://stats.example.com  (required, no trailing /)
//	website_id   UUID of the website        (required)
//	api_key      Umami API key              (required; set via api_key_ref)
//	page_size    records per request        (optional; default 100, max 1000)
//	max_pages    page cap per extract       (optional; default 10)
//
// The incremental cursor is a RFC 3339 createdAt timestamp stored under
// `last_created_at`; subsequent syncs request `startAt = last_created_at`
// and emit only records strictly newer than the cursor.
package umami
