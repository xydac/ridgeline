// Package hackernews is a native connector for Hacker News, backed by
// the public Algolia HN Search API. No authentication is required.
//
// Streams:
//
//	stories  - posts matching the configured query, newest first
//	comments - comments matching the configured query, newest first
//
// Config keys:
//
//	query          (string, required): Algolia search query.
//	hits_per_page  (int, optional):    results per API call. Default 50, max 1000.
//	max_pages      (int, optional):    safety cap on pagination per sync.
//	                                   Default 5, so a single sync fetches up
//	                                   to hits_per_page * max_pages records.
//	base_url       (string, optional): override the API endpoint. Tests
//	                                   set this to an httptest server. The
//	                                   default points at the real Algolia
//	                                   HN search API.
//
// The connector is incremental. On each run it records the most recent
// created_at_i seen per stream into connector state and, on the next
// run, asks Algolia for only records strictly newer than that cursor.
// Re-running against unchanged data writes zero records.
//
// Records emitted on the stories stream have the shape:
//
//	Timestamp = created_at
//	Data      = raw Algolia hit (objectID, title, url, author, points,
//	            num_comments, story_text, created_at, created_at_i, ...)
//
// Records emitted on the comments stream follow the same pattern with
// the Algolia comment-shaped hit.
//
// The connector registers itself under the name "hackernews" on import.
package hackernews
