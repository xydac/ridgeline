// Package git provides a connector that reads local git repository commits
// into the Business Memory event timeline. Each commit is emitted as a
// stream record (for DuckDB queries) and as a bm_events entry (for explain
// correlation).
package git

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/xydac/ridgeline/connectors"
)

// Name is the connector name registered with the connectors package.
const Name = "git"

// StreamCommits is the only stream; each record is one git commit.
const StreamCommits = "commits"

// cursorKey stores the last commit hash seen so repeated syncs are incremental.
const cursorKey = "last_hash"

func init() {
	connectors.Register(New())
}

// Connector reads commits from a local git repository.
type Connector struct{}

// New returns a ready-to-register Connector.
func New() *Connector { return &Connector{} }

// Spec returns the connector's self-description.
func (c *Connector) Spec() connectors.ConnectorSpec {
	return connectors.ConnectorSpec{
		Name:        Name,
		DisplayName: "Git",
		Description: "Reads commits from a local git repository into the Business Memory event timeline.",
		Version:     "0.1.0",
		AuthType:    connectors.AuthNone,
		Streams: []connectors.StreamSpec{
			{
				Name:        StreamCommits,
				Description: "One record per git commit, newest first.",
				Kind:        connectors.Event,
				SyncModes:   []connectors.SyncMode{connectors.Incremental},
				DefaultCron: "0 * * * *",
				Schema: connectors.Schema{Columns: []connectors.Column{
					{Name: "hash", Type: connectors.String},
					{Name: "author_email", Type: connectors.String},
					{Name: "subject", Type: connectors.String},
				}},
			},
		},
	}
}

// Validate checks that the configured path is a valid git repository.
func (c *Connector) Validate(ctx context.Context, cfg connectors.ConnectorConfig) error {
	path, err := repoPath(cfg)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "git", "-C", path, "rev-parse", "--git-dir")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git: %s is not a git repository: %s", path, strings.TrimSpace(string(out)))
	}
	return nil
}

// Discover returns the static stream catalog.
func (c *Connector) Discover(_ context.Context, _ connectors.ConnectorConfig) (*connectors.Catalog, error) {
	spec := c.Spec()
	cat := &connectors.Catalog{}
	for _, s := range spec.Streams {
		cat.Streams = append(cat.Streams, connectors.DiscoveredStream{StreamSpec: s, Available: true})
	}
	return cat, nil
}

// Extract reads commits from the configured repo, emitting records for all
// commits newer than the cursor. The cursor is the last commit hash seen; on
// first run, all commits are fetched.
func (c *Connector) Extract(ctx context.Context, cfg connectors.ConnectorConfig, streams []connectors.Stream, state connectors.State) (<-chan connectors.Message, error) {
	path, err := repoPath(cfg)
	if err != nil {
		return nil, err
	}

	wantCommits := false
	for _, s := range streams {
		if s.Name == StreamCommits {
			wantCommits = true
		}
	}
	if !wantCommits {
		ch := make(chan connectors.Message)
		close(ch)
		return ch, nil
	}

	lastHash := state.String(cursorKey, "")
	commits, err := readCommits(ctx, path, lastHash)
	if err != nil {
		return nil, err
	}

	ch := make(chan connectors.Message, len(commits)+1)
	go func() {
		defer close(ch)
		if len(commits) == 0 {
			return
		}
		newCursor := connectors.State{cursorKey: commits[0].hash}
		for _, commit := range commits {
			select {
			case <-ctx.Done():
				return
			default:
			}
			ch <- connectors.RecordMessage(StreamCommits, connectors.Record{
				Timestamp: commit.at,
				Data: map[string]any{
					"hash":         commit.hash,
					"author_email": commit.authorEmail,
					"subject":      commit.subject,
				},
			})
		}
		ch <- connectors.StateMessage(newCursor)
	}()
	return ch, nil
}

// EmitEvents implements connectors.EventEmitter. It returns one EventRecord
// per commit not yet recorded (same cursor as Extract). The sync pipeline
// calls this after Extract to insert commits into bm_events.
func (c *Connector) EmitEvents(ctx context.Context, cfg connectors.ConnectorConfig, state connectors.State) ([]connectors.EventRecord, error) {
	path, err := repoPath(cfg)
	if err != nil {
		return nil, err
	}
	lastHash := state.String(cursorKey, "")
	commits, err := readCommits(ctx, path, lastHash)
	if err != nil {
		return nil, err
	}
	out := make([]connectors.EventRecord, 0, len(commits))
	for _, commit := range commits {
		out = append(out, connectors.EventRecord{
			Hash:        commit.hash,
			Kind:        "commit",
			Description: commit.subject,
			At:          commit.at,
		})
	}
	return out, nil
}

// commitInfo holds parsed fields from a single git log entry.
type commitInfo struct {
	hash        string
	authorEmail string
	subject     string
	at          time.Time
}

// readCommits returns commits in the repo at path, newest first, stopping
// when lastHash is encountered (exclusive). If lastHash is empty, all
// commits are returned.
func readCommits(ctx context.Context, path, lastHash string) ([]commitInfo, error) {
	args := []string{"-C", path, "log", "--format=%H\t%ae\t%aI\t%s", "--no-merges", "HEAD"}
	out, err := exec.CommandContext(ctx, "git", args...).Output()
	if err != nil {
		if len(out) == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("git log: %w", err)
	}

	var commits []commitInfo
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 4)
		if len(parts) != 4 {
			continue
		}
		hash, email, dateStr, subject := parts[0], parts[1], parts[2], parts[3]
		if hash == lastHash {
			break
		}
		at, err := time.Parse(time.RFC3339, dateStr)
		if err != nil {
			continue
		}
		commits = append(commits, commitInfo{
			hash:        hash,
			authorEmail: email,
			subject:     subject,
			at:          at.UTC(),
		})
	}
	return commits, scanner.Err()
}

// repoPath extracts the required "path" config key.
func repoPath(cfg connectors.ConnectorConfig) (string, error) {
	s := cfg.String("path")
	if s == "" {
		return "", fmt.Errorf("git: 'path' is required in connector config")
	}
	return s, nil
}
