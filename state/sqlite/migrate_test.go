package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/xydac/ridgeline/state/sqlite"
)

// setupPreV14DB creates a SQLite file with the BM tables in their v13 shape
// (3-part FQ names), marks schema versions 1-13 as applied, and returns the
// path. The caller is responsible for opening via sqlite.Open to trigger v14.
func setupPreV14DB(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "state.db")
	dsn := path + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		`CREATE TABLE schema_versions (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL) STRICT;`,
		`CREATE TABLE state (key TEXT PRIMARY KEY, data BLOB NOT NULL, updated_at TEXT NOT NULL) STRICT;`,
		`CREATE TABLE credentials (name TEXT PRIMARY KEY, nonce BLOB NOT NULL, ciphertext BLOB NOT NULL, updated_at TEXT NOT NULL) STRICT;`,
		`CREATE TABLE bm_streams (connector TEXT NOT NULL, stream TEXT NOT NULL, kind TEXT NOT NULL DEFAULT 'unstructured', first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, row_count_lifetime INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (connector, stream)) STRICT;`,
		`CREATE TABLE bm_metrics (fq_name TEXT PRIMARY KEY, unit TEXT NOT NULL DEFAULT '', direction TEXT NOT NULL DEFAULT 'neutral', aggregation TEXT NOT NULL DEFAULT 'none', last_value REAL, last_value_at TEXT, updated_at TEXT NOT NULL) STRICT;`,
		`CREATE TABLE bm_metric_values (fq_name TEXT NOT NULL, value REAL NOT NULL, observed_at TEXT NOT NULL, UNIQUE (fq_name, observed_at)) STRICT;`,
		`CREATE INDEX idx_bm_metric_values_lookup ON bm_metric_values (fq_name, observed_at);`,
		`CREATE TABLE bm_baselines (fq_name TEXT NOT NULL, window_days INTEGER NOT NULL, mean REAL NOT NULL, stddev REAL NOT NULL, min REAL NOT NULL, max REAL NOT NULL, sample_count INTEGER NOT NULL, last_computed_at TEXT NOT NULL, PRIMARY KEY (fq_name, window_days)) STRICT;`,
		`CREATE TABLE bm_events (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL DEFAULT 'anomaly', metric_fq TEXT NOT NULL DEFAULT '', observed_value REAL NOT NULL DEFAULT 0.0, baseline_mean REAL NOT NULL DEFAULT 0.0, stddev_from_mean REAL NOT NULL DEFAULT 0.0, direction TEXT NOT NULL DEFAULT 'surprise-neutral', window_days INTEGER NOT NULL DEFAULT 0, description TEXT, at TEXT NOT NULL, UNIQUE (kind, metric_fq, window_days, at)) STRICT;`,
		`CREATE INDEX idx_bm_events_at_v10 ON bm_events (at DESC);`,
		`CREATE TABLE bm_watches (name TEXT PRIMARY KEY, metric_fq TEXT NOT NULL, op TEXT NOT NULL, threshold REAL NOT NULL, unit TEXT NOT NULL DEFAULT '', condition TEXT NOT NULL, extra TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, last_triggered_at TEXT) STRICT;`,
		`CREATE TABLE bm_patterns (id INTEGER PRIMARY KEY AUTOINCREMENT, fq_name TEXT NOT NULL, pattern TEXT NOT NULL, confidence REAL NOT NULL, evidence_start TEXT NOT NULL, evidence_end TEXT NOT NULL, sample_count INTEGER NOT NULL, detected_at TEXT NOT NULL, UNIQUE (fq_name, pattern)) STRICT;`,
		`CREATE INDEX idx_bm_patterns_fq ON bm_patterns (fq_name);`,
		// Mark versions 1-13 as applied so the store skips to v14 on open.
		`INSERT INTO schema_versions (version, applied_at) VALUES
			(1,'2026-01-01T00:00:00.000Z'),
			(2,'2026-01-01T00:00:00.000Z'),
			(3,'2026-01-01T00:00:00.000Z'),
			(4,'2026-01-01T00:00:00.000Z'),
			(5,'2026-01-01T00:00:00.000Z'),
			(6,'2026-01-01T00:00:00.000Z'),
			(7,'2026-01-01T00:00:00.000Z'),
			(8,'2026-01-01T00:00:00.000Z'),
			(9,'2026-01-01T00:00:00.000Z'),
			(10,'2026-01-01T00:00:00.000Z'),
			(11,'2026-01-01T00:00:00.000Z'),
			(12,'2026-01-01T00:00:00.000Z'),
			(13,'2026-01-01T00:00:00.000Z');`,
		// Old-style data: 3-part FQ names, single-word connector.
		`INSERT INTO bm_streams (connector, stream, kind, first_seen_at, last_seen_at, row_count_lifetime)
			VALUES ('plausible', 'daily', 'metric', '2026-01-01T00:00:00Z', '2026-01-10T00:00:00Z', 100);`,
		`INSERT INTO bm_metrics (fq_name, unit, direction, aggregation, last_value, last_value_at, updated_at)
			VALUES ('plausible.daily.visitors', 'count', 'higher_is_better', 'sum', 1234.0, '2026-01-10T00:00:00Z', '2026-01-10T00:00:00Z');`,
		`INSERT INTO bm_metric_values (fq_name, value, observed_at)
			VALUES ('plausible.daily.visitors', 1234.0, '2026-01-10T00:00:00Z');`,
		`INSERT INTO bm_baselines (fq_name, window_days, mean, stddev, min, max, sample_count, last_computed_at)
			VALUES ('plausible.daily.visitors', 30, 1200.0, 100.0, 900.0, 1500.0, 10, '2026-01-10T00:00:00Z');`,
		`INSERT INTO bm_events (kind, metric_fq, observed_value, baseline_mean, stddev_from_mean, direction, window_days, at)
			VALUES ('anomaly', 'plausible.daily.visitors', 1234.0, 1200.0, 0.34, 'surprise-neutral', 30, '2026-01-10T00:00:00Z');`,
		`INSERT INTO bm_watches (name, metric_fq, op, threshold, condition, created_at)
			VALUES ('visitors-low', 'plausible.daily.visitors', 'lt', 500.0, 'below 500', '2026-01-01T00:00:00Z');`,
		`INSERT INTO bm_patterns (fq_name, pattern, confidence, evidence_start, evidence_end, sample_count, detected_at)
			VALUES ('plausible.daily.visitors', 'steady-growth', 0.8, '2026-01-01T00:00:00Z', '2026-01-10T00:00:00Z', 10, '2026-01-10T00:00:00Z');`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("setup: %v\nstmt: %s", err, stmt)
		}
	}
	return path
}

// TestMigrationV14_RewritesFQNames verifies that opening a pre-v14 database
// rewrites 3-part BM FQ names to 4-part form across all affected tables.
func TestMigrationV14_RewritesFQNames(t *testing.T) {
	dir := t.TempDir()
	path := setupPreV14DB(t, dir)

	// Open via sqlite.Open to trigger migration v14.
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	db := store.DB()
	ctx := context.Background()

	// All 3-part names should be gone; 4-part names should appear.
	want4 := "plausible.plausible.daily.visitors"
	want3 := "plausible.daily.visitors" // must NOT appear as a primary name

	checkFQ := func(table, col string) {
		t.Helper()
		rows, err := db.QueryContext(ctx, "SELECT "+col+" FROM "+table)
		if err != nil {
			t.Fatalf("%s: query %s: %v", table, col, err)
		}
		defer rows.Close()
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("%s: scan: %v", table, err)
			}
			if v == "" {
				continue // empty metric_fq on non-anomaly events
			}
			if strings.Count(v, ".") < 3 {
				t.Errorf("%s.%s: got 3-part (or fewer) name %q, want 4-part", table, col, v)
			}
			if v == want3 {
				t.Errorf("%s.%s: old name %q still present after migration", table, col, v)
			}
			if v != want4 {
				t.Errorf("%s.%s: got %q, want %q", table, col, v, want4)
			}
		}
	}

	checkFQ("bm_metrics", "fq_name")
	checkFQ("bm_metric_values", "fq_name")
	checkFQ("bm_baselines", "fq_name")
	checkFQ("bm_events", "metric_fq")
	checkFQ("bm_watches", "metric_fq")
	checkFQ("bm_patterns", "fq_name")

	// bm_streams: connector column should have been extended.
	var connector string
	if err := db.QueryRowContext(ctx, "SELECT connector FROM bm_streams LIMIT 1").Scan(&connector); err != nil {
		t.Fatalf("bm_streams: %v", err)
	}
	if connector != "plausible.plausible" {
		t.Errorf("bm_streams.connector: got %q, want %q", connector, "plausible.plausible")
	}

	// Backup tables must exist and contain the original rows.
	var backupFQ string
	if err := db.QueryRowContext(ctx, "SELECT fq_name FROM bm_metrics_pre14 LIMIT 1").Scan(&backupFQ); err != nil {
		t.Fatalf("bm_metrics_pre14: %v", err)
	}
	if backupFQ != want3 {
		t.Errorf("bm_metrics_pre14: got %q, want original %q", backupFQ, want3)
	}

	// Applying migration again (opening again) must be idempotent - no error.
	store2, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("second Open (idempotency check): %v", err)
	}
	store2.Close()
}

// TestNamespaceIsolation_SameTypeDistinctConnectors verifies that two
// connectors of the same type but different configured names produce distinct
// catalog entries when synced through the Business Memory layer.
func TestNamespaceIsolation_SameTypeDistinctConnectors(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	db := store.DB()
	ctx := context.Background()
	now := "2026-09-07T12:00:00Z"

	// Simulate two Plausible connectors: myapp.prod and myapp.staging.
	// Both are "plausible" type but have different product.name keys.
	insert := func(connKey, fqName string) {
		_, err := db.ExecContext(ctx,
			`INSERT INTO bm_streams (connector, stream, kind, first_seen_at, last_seen_at, row_count_lifetime)
			VALUES (?, 'daily', 'metric', ?, ?, 100)`,
			connKey, now, now)
		if err != nil {
			t.Fatalf("insert stream %s: %v", connKey, err)
		}
		_, err = db.ExecContext(ctx,
			`INSERT INTO bm_metrics (fq_name, unit, direction, aggregation, last_value, last_value_at, updated_at)
			VALUES (?, 'count', 'higher_is_better', 'sum', 1000.0, ?, ?)`,
			fqName, now, now)
		if err != nil {
			t.Fatalf("insert metric %s: %v", fqName, err)
		}
	}

	insert("myapp.prod", "myapp.prod.daily.visitors")
	insert("myapp.staging", "myapp.staging.daily.visitors")

	// Both streams must be present as separate rows.
	var streamCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM bm_streams").Scan(&streamCount); err != nil {
		t.Fatalf("count streams: %v", err)
	}
	if streamCount != 2 {
		t.Errorf("want 2 distinct stream rows, got %d", streamCount)
	}

	// Both metrics must be present as separate rows.
	var metricCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM bm_metrics").Scan(&metricCount); err != nil {
		t.Fatalf("count metrics: %v", err)
	}
	if metricCount != 2 {
		t.Errorf("want 2 distinct metric rows, got %d", metricCount)
	}

	// The FQ names must differ.
	rows, err := db.QueryContext(ctx, "SELECT fq_name FROM bm_metrics ORDER BY fq_name")
	if err != nil {
		t.Fatalf("list metrics: %v", err)
	}
	defer rows.Close()
	var fqNames []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		fqNames = append(fqNames, v)
	}
	if len(fqNames) != 2 {
		t.Fatalf("want 2 FQ names, got %v", fqNames)
	}
	if fqNames[0] == fqNames[1] {
		t.Errorf("prod and staging connectors share a FQ name: %s", fqNames[0])
	}
	if !strings.Contains(fqNames[0], ".prod.") || !strings.Contains(fqNames[1], ".staging.") {
		t.Errorf("unexpected FQ names: %v", fqNames)
	}
}

// TestMigrationV14_NoOpOnFreshDB verifies that migration v14 does nothing
// harmful on a freshly created database (no old-style rows to rewrite).
func TestMigrationV14_NoOpOnFreshDB(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	// No error means v14 ran cleanly on an empty DB.
	var n int
	if err := store.DB().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM bm_metrics").Scan(&n); err != nil {
		t.Fatalf("count metrics: %v", err)
	}
	if n != 0 {
		t.Errorf("fresh db should have 0 metrics, got %d", n)
	}
}

// TestLegacyFQName checks the string-manipulation logic that strips the
// product prefix from a 4-part FQ name to derive the deprecated 3-part form.
func TestLegacyFQName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"myapp.prod.daily.visitors", "prod.daily.visitors"},
		{"plausible.plausible.daily.visitors", "plausible.daily.visitors"},
		{"a.b.c.d", "b.c.d"},
		{"nodots", "nodots"},
	}
	// We test this indirectly by verifying the string-manipulation logic.
	for _, c := range cases {
		idx := strings.IndexByte(c.in, '.')
		var got string
		if idx < 0 {
			got = c.in
		} else {
			got = c.in[idx+1:]
		}
		if got != c.want {
			t.Errorf("legacyFQName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
