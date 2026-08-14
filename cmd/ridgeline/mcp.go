package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	ridgelinememory "github.com/xydac/ridgeline/memory"
)

// mcpMetric is the JSON shape returned by the list_metrics tool.
type mcpMetric struct {
	FQName      string   `json:"fq_name"`
	Unit        string   `json:"unit"`
	Direction   string   `json:"direction"`
	Aggregation string   `json:"aggregation"`
	LastValue   *float64 `json:"last_value,omitempty"`
	LastValueAt *string  `json:"last_value_at,omitempty"`
}

// buildMCPServer constructs and returns the ridgeline MCP server backed by cat.
// Extracted from runMCP to allow testing the tool handlers independently.
func buildMCPServer(cat *ridgelinememory.Catalog, version string) *server.MCPServer {
	s := server.NewMCPServer(
		"ridgeline",
		version,
		server.WithToolCapabilities(true),
	)

	s.AddTool(
		mcp.NewTool("list_metrics",
			mcp.WithDescription("List all metrics tracked in the Business Memory catalog. Returns metric name, unit, directionality, aggregation hint, and last observed value."),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			rows, err := cat.ListMetrics(ctx)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("list_metrics: %v", err)), nil
			}
			out := make([]mcpMetric, len(rows))
			for i, r := range rows {
				m := mcpMetric{
					FQName:      r.FQName,
					Unit:        r.Unit,
					Direction:   r.Direction,
					Aggregation: r.Aggregation,
					LastValue:   r.LastValue,
				}
				if r.LastValueAt != nil {
					ts := r.LastValueAt.Format(time.RFC3339)
					m.LastValueAt = &ts
				}
				out[i] = m
			}
			b, err := json.Marshal(out)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("list_metrics: marshal: %v", err)), nil
			}
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("explain",
			mcp.WithDescription("Return a structured narrative for a metric from the Business Memory catalog, covering current value, baseline comparison, prior-period trend, and anomalies."),
			mcp.WithString("metric_fq",
				mcp.Required(),
				mcp.Description("Fully-qualified metric name (e.g. plausible.daily.visitors)."),
			),
			mcp.WithString("since",
				mcp.Description("Time window to analyze (e.g. 7d, 30d, 24h). Defaults to 7d."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			fqName, err := req.RequireString("metric_fq")
			if err != nil {
				return mcp.NewToolResultError("explain: metric_fq is required"), nil
			}
			sinceStr := req.GetString("since", "7d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("explain: invalid since %q: %v", sinceStr, err)), nil
			}
			data, err := cat.ExplainMetric(ctx, fqName, since)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("explain: %v", err)), nil
			}
			b, err := json.Marshal(ridgelinememory.ToExplainJSON(data))
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("explain: marshal: %v", err)), nil
			}
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	return s
}

// runMCP handles `ridgeline mcp --config PATH`.
//
// It starts a Model Context Protocol server over stdio, exposing two tools:
//
//	list_metrics -- return all metrics in the Business Memory catalog
//	explain      -- return a structured narrative for a metric
//
// The server reads JSON-RPC messages from stdin and writes responses to stdout.
// All diagnostic output goes to stderr so it does not corrupt the transport.
func runMCP(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: ridgeline mcp --config PATH")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Run a Model Context Protocol server over stdio.")
		fmt.Fprintln(fs.Output(), "Exposes two tools to the connected agent:")
		fmt.Fprintln(fs.Output(), "  list_metrics  -- return all metrics in the Business Memory catalog")
		fmt.Fprintln(fs.Output(), "  explain       -- return a narrative for a metric over a time window")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Wire ridgeline into Claude Desktop by adding an entry under")
		fmt.Fprintln(fs.Output(), "\"mcpServers\" in claude_desktop_config.json.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, os.Stdout, args)
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("mcp: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	s := buildMCPServer(cat, Version)
	errLog := log.New(os.Stderr, "ridgeline-mcp: ", 0)
	return server.ServeStdio(s, server.WithErrorLogger(errLog))
}
