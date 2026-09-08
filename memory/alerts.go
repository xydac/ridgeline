package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// AlertChannel is one registered delivery endpoint from bm_alert_channels.
type AlertChannel struct {
	ID              int64
	Name            string
	Kind            string // "webhook", "stderr", "file"
	Target          string // URL, "", or file path
	CreatedAt       time.Time
	LastDeliveredAt *time.Time
}

// AlertPayload is the JSON envelope sent to every delivery channel.
type AlertPayload struct {
	EventID     int64     `json:"event_id"`
	Kind        string    `json:"kind"`
	MetricFQ    string    `json:"metric_fq"`
	Description string    `json:"description"`
	Direction   string    `json:"direction"`
	Value       float64   `json:"observed_value"`
	At          time.Time `json:"at"`
}

// AddAlertChannel registers a delivery channel. name must be unique.
// kind is one of "webhook", "stderr", "file". target is the URL or file path
// (ignored for stderr). Returns the new channel's ID.
func (c *Catalog) AddAlertChannel(ctx context.Context, name, kind, target string) (int64, error) {
	kind = strings.ToLower(kind)
	switch kind {
	case "webhook", "stderr", "file":
	default:
		return 0, fmt.Errorf("alerts config: unknown channel kind %q; valid kinds: webhook, stderr, file", kind)
	}
	if name == "" {
		return 0, fmt.Errorf("alerts config: name is required")
	}
	if kind == "webhook" && target == "" {
		return 0, fmt.Errorf("alerts config: webhook channel requires a target URL")
	}
	if kind == "file" && target == "" {
		return 0, fmt.Errorf("alerts config: file channel requires a target file path")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := c.db.ExecContext(ctx, `
INSERT INTO bm_alert_channels (name, kind, target, extra, created_at)
VALUES (?, ?, ?, '{}', ?)`, name, kind, target, now)
	if err != nil {
		return 0, fmt.Errorf("alerts config: %w", err)
	}
	return res.LastInsertId()
}

// ListAlertChannels returns all registered channels ordered by creation time.
func (c *Catalog) ListAlertChannels(ctx context.Context) ([]AlertChannel, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT id, name, kind, target, created_at, last_delivered_at
FROM bm_alert_channels ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("alerts list: %w", err)
	}
	defer rows.Close()
	var out []AlertChannel
	for rows.Next() {
		var ch AlertChannel
		var createdStr string
		var lastStr *string
		if err := rows.Scan(&ch.ID, &ch.Name, &ch.Kind, &ch.Target, &createdStr, &lastStr); err != nil {
			return nil, err
		}
		ch.CreatedAt, _ = time.Parse(time.RFC3339, createdStr)
		if lastStr != nil {
			t, _ := time.Parse(time.RFC3339, *lastStr)
			ch.LastDeliveredAt = &t
		}
		out = append(out, ch)
	}
	return out, rows.Err()
}

// DeleteAlertChannel removes a channel by name. Returns an error if not found.
func (c *Catalog) DeleteAlertChannel(ctx context.Context, name string) error {
	res, err := c.db.ExecContext(ctx, `DELETE FROM bm_alert_channels WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("alerts remove: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("alerts remove: no channel named %q", name)
	}
	return nil
}

// UndeliveredEvents returns events from bm_events (within the given window)
// that have not yet been delivered to channelID.
func (c *Catalog) UndeliveredEvents(ctx context.Context, since time.Duration, channelID int64) ([]EventRow, error) {
	cutoff := time.Now().UTC().Add(-since).Format(time.RFC3339)
	rows, err := c.db.QueryContext(ctx, `
SELECT e.id, e.kind, e.metric_fq, e.observed_value, e.baseline_mean,
       e.stddev_from_mean, e.direction, e.window_days, e.description, e.at
FROM bm_events e
WHERE e.at >= ?
  AND NOT EXISTS (
      SELECT 1 FROM bm_alert_deliveries d
      WHERE d.event_id = e.id AND d.channel_id = ?
  )
ORDER BY e.at ASC`, cutoff, channelID)
	if err != nil {
		return nil, fmt.Errorf("alerts run: query undelivered: %w", err)
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		var ev EventRow
		var atStr string
		if err := rows.Scan(&ev.ID, &ev.Kind, &ev.MetricFQ, &ev.ObservedValue,
			&ev.BaselineMean, &ev.StddevFromMean, &ev.Direction,
			&ev.WindowDays, &ev.Description, &atStr); err != nil {
			return nil, err
		}
		ev.At, _ = time.Parse(time.RFC3339, atStr)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// RecordDelivery marks event eventID as delivered to channel channelID.
// Idempotent: silently succeeds if the delivery was already recorded.
func (c *Catalog) RecordDelivery(ctx context.Context, eventID, channelID int64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := c.db.ExecContext(ctx, `
INSERT OR IGNORE INTO bm_alert_deliveries (event_id, channel_id, delivered_at)
VALUES (?, ?, ?)`, eventID, channelID, now)
	if err != nil {
		return fmt.Errorf("record delivery: %w", err)
	}
	_, _ = c.db.ExecContext(ctx, `
UPDATE bm_alert_channels SET last_delivered_at = ? WHERE id = ?`, now, channelID)
	return nil
}

// DeliverToChannel sends a single event to a single channel. It does NOT
// record the delivery -- the caller is responsible for calling RecordDelivery.
// Returns an error if delivery failed; caller decides whether to skip or abort.
func DeliverToChannel(ch AlertChannel, ev EventRow) error {
	payload := AlertPayload{
		EventID:     ev.ID,
		Kind:        ev.Kind,
		MetricFQ:    ev.MetricFQ,
		Description: ev.Description,
		Direction:   ev.Direction,
		Value:       ev.ObservedValue,
		At:          ev.At,
	}

	switch ch.Kind {
	case "webhook":
		return deliverWebhook(ch.Target, payload)
	case "stderr":
		data, _ := json.Marshal(payload)
		fmt.Fprintf(os.Stderr, "%s\n", data)
		return nil
	case "file":
		return deliverFile(ch.Target, payload)
	default:
		return fmt.Errorf("unknown channel kind %q", ch.Kind)
	}
}

func deliverWebhook(url string, payload AlertPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("webhook: marshal: %w", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(data)) //nolint:noctx
	if err != nil {
		return fmt.Errorf("webhook: post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook: server returned %d", resp.StatusCode)
	}
	return nil
}

func deliverFile(path string, payload AlertPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("file: marshal: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("file: open %q: %w", path, err)
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%s\n", data)
	return err
}

// SyntheticEvent returns a test event suitable for use with DeliverToChannel.
// The event has ID 0 and is not stored in the database.
func SyntheticEvent() EventRow {
	return EventRow{
		ID:          0,
		Kind:        "test",
		MetricFQ:    "myapp.demo.pageviews.visitors",
		Description: "synthetic test alert from ridgeline alerts test",
		Direction:   "none",
		At:          time.Now().UTC(),
	}
}
