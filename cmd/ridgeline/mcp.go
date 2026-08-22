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

	s.AddTool(
		mcp.NewTool("investigate",
			mcp.WithDescription("Produce a cross-source causal narrative for a metric: anomalies, events correlated by temporal proximity (deploys, commits, notes), and Pearson correlation against sibling metrics."),
			mcp.WithString("metric_fq",
				mcp.Required(),
				mcp.Description("Fully-qualified metric name (e.g. plausible.daily.visitors)."),
			),
			mcp.WithString("since",
				mcp.Description("Time window to analyze (e.g. 14d, 30d). Defaults to 14d."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			fqName, err := req.RequireString("metric_fq")
			if err != nil {
				return mcp.NewToolResultError("investigate: metric_fq is required"), nil
			}
			sinceStr := req.GetString("since", "14d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("investigate: invalid since %q: %v", sinceStr, err)), nil
			}
			data, err := cat.InvestigateMetric(ctx, fqName, since)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("investigate: %v", err)), nil
			}
			b, err := json.Marshal(ridgelinememory.ToInvestigateJSON(data))
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("investigate: marshal: %v", err)), nil
			}
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("compare",
			mcp.WithDescription("Compare two metrics side-by-side over the same time window. Returns baseline, anomalies, and a verdict (both-improved, diverged, both-regressed, or unchanged) for each metric, plus shared correlated events and a confidence score."),
			mcp.WithString("metric_a",
				mcp.Required(),
				mcp.Description("First fully-qualified metric name (e.g. plausible.daily.visitors)."),
			),
			mcp.WithString("metric_b",
				mcp.Required(),
				mcp.Description("Second fully-qualified metric name (e.g. plausible.daily.pageviews)."),
			),
			mcp.WithString("since",
				mcp.Description("Time window for both metrics (e.g. 7d, 30d). Defaults to 7d."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			metricA, err := req.RequireString("metric_a")
			if err != nil {
				return mcp.NewToolResultError("compare: metric_a is required"), nil
			}
			metricB, err := req.RequireString("metric_b")
			if err != nil {
				return mcp.NewToolResultError("compare: metric_b is required"), nil
			}
			sinceStr := req.GetString("since", "7d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("compare: invalid since %q: %v", sinceStr, err)), nil
			}
			data, err := cat.CompareMetrics(ctx, metricA, metricB, since)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("compare: %v", err)), nil
			}
			b, err := json.Marshal(ridgelinememory.ToCompareJSON(data))
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("compare: marshal: %v", err)), nil
			}
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	s.AddTool(
		mcp.NewTool("summarize",
			mcp.WithDescription("Return a ranked overview of all tracked metrics, ordered by directionality-weighted deviation from baseline. Surfaces the most notable metrics (surprise-bad events first) across all connectors. Use this to answer 'what should I focus on this week?'"),
			mcp.WithString("since",
				mcp.Description("Time window to analyze (e.g. 7d, 30d). Defaults to 7d."),
			),
			mcp.WithNumber("top",
				mcp.Description("Maximum number of metrics to return. Defaults to 5."),
			),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			sinceStr := req.GetString("since", "7d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("summarize: invalid since %q: %v", sinceStr, err)), nil
			}
			topK := int(req.GetFloat("top", 5))
			if topK <= 0 {
				topK = 5
			}
			data, err := cat.SummarizeAll(ctx, since, topK)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("summarize: %v", err)), nil
			}
			b, err := json.Marshal(ridgelinememory.ToSummaryJSON(data))
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("summarize: marshal: %v", err)), nil
			}
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	return s
}

// runMCP handles `ridgeline mcp --config PATH`.
//
// It starts a Model Context Protocol server over stdio, exposing five tools:
//
//	list_metrics -- return all metrics in the Business Memory catalog
//	explain      -- structured narrative for one metric over a time window
//	investigate  -- causal narrative: anomalies, correlated events, sibling correlation
//	compare      -- pairwise comparison of two metrics over the same window
//	summarize    -- ranked overview of all tracked metrics (what to focus on)
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
		fmt.Fprintln(fs.Output(), "Exposes five tools to the connected agent:")
		fmt.Fprintln(fs.Output(), "  list_metrics  -- return all metrics in the Business Memory catalog")
		fmt.Fprintln(fs.Output(), "  explain       -- return a narrative for a metric over a time window")
		fmt.Fprintln(fs.Output(), "  investigate   -- causal narrative with correlated events and sibling metrics")
		fmt.Fprintln(fs.Output(), "  compare       -- pairwise comparison of two metrics over the same window")
		fmt.Fprintln(fs.Output(), "  summarize     -- ranked overview of all tracked metrics")
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
	if fs.NArg() > 0 {
		return usageErrorf("mcp: unexpected argument %q", fs.Arg(0))
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
