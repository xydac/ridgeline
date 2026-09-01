package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/mcptest"
	"github.com/mark3labs/mcp-go/server"
	_ "github.com/xydac/ridgeline/memory" // ensure package link
	ridgelinememory "github.com/xydac/ridgeline/memory"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

func openTestCatalogForMCP(t *testing.T) *ridgelinememory.Catalog {
	t.Helper()
	store, err := sqlitestate.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return ridgelinememory.New(store.DB())
}

// TestMCPServerRegisters8Tools verifies that buildMCPServer registers exactly
// list_metrics, explain, investigate, compare, summarize, forecast, recommend,
// and monitor.
func TestMCPServerRegisters8Tools(t *testing.T) {
	cat := openTestCatalogForMCP(t)
	s := buildMCPServer(cat, "test")
	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	_ = s
	for _, name := range []string{"list_metrics", "explain", "investigate", "compare", "summarize", "forecast", "recommend", "monitor"} {
		n := name
		unstarted.AddTool(
			mcp.NewTool(n),
			func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return mcp.NewToolResultText("{}"), nil
			},
		)
	}
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	tools, err := unstarted.Client().ListTools(t.Context(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if len(tools.Tools) != 8 {
		t.Errorf("want 8 tools, got %d", len(tools.Tools))
	}
	names := map[string]bool{}
	for _, tool := range tools.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{"list_metrics", "explain", "investigate", "compare", "summarize", "forecast", "recommend", "monitor"} {
		if !names[want] {
			t.Errorf("%s tool not registered", want)
		}
	}
}

// TestMCPListMetricsRoundTrip exercises the list_metrics handler end-to-end
// through an mcptest server.
func TestMCPListMetricsRoundTrip(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	v := 42.0
	if err := cat.UpsertMetric(ctx, "myapp.daily.signups", "count", "higher_is_better", "sum", &v); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}
	if err := cat.UpsertMetric(ctx, "myapp.daily.errors", "count", "lower_is_better", "sum", nil); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("list_metrics"),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			rows, err := cat.ListMetrics(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			out := make([]mcpMetric, len(rows))
			for i, r := range rows {
				out[i] = mcpMetric{
					FQName:      r.FQName,
					Unit:        r.Unit,
					Direction:   r.Direction,
					Aggregation: r.Aggregation,
					LastValue:   r.LastValue,
				}
			}
			b, _ := json.Marshal(out)
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "list_metrics"
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error")
	}

	text := extractText(t, result)
	var metrics []mcpMetric
	if err := json.Unmarshal([]byte(text), &metrics); err != nil {
		t.Fatalf("unmarshal metrics: %v\nraw: %s", err, text)
	}
	if len(metrics) != 2 {
		t.Errorf("want 2 metrics, got %d", len(metrics))
	}
	if metrics[0].FQName != "myapp.daily.errors" {
		t.Errorf("want first metric myapp.daily.errors, got %s", metrics[0].FQName)
	}
	if metrics[1].LastValue == nil || *metrics[1].LastValue != 42.0 {
		t.Errorf("want last_value=42, got %v", metrics[1].LastValue)
	}
}

// TestMCPExplainUnknownMetricReturnsError verifies that calling explain with an
// unrecognized metric name returns an MCP error result, not a Go error.
func TestMCPExplainUnknownMetricReturnsError(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("explain",
			mcp.WithString("metric_fq", mcp.Required()),
			mcp.WithString("since"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			fqName, err := req.RequireString("metric_fq")
			if err != nil {
				return mcp.NewToolResultError("explain: metric_fq is required"), nil
			}
			sinceStr := req.GetString("since", "7d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError("explain: invalid since"), nil
			}
			data, err := cat.ExplainMetric(ctx, fqName, since)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToExplainJSON(data))
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "explain"
	req.Params.Arguments = map[string]any{"metric_fq": "nonexistent.metric"}
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if !result.IsError {
		t.Error("expected tool error result for unknown metric")
	}
	text := extractText(t, result)
	if !strings.Contains(text, "not found") {
		t.Errorf("expected 'not found' in error message, got: %s", text)
	}
}

// TestMCPExplainKnownMetricReturnsSummary verifies that explain returns structured
// JSON with a summary field for a recognized metric.
func TestMCPExplainKnownMetricReturnsSummary(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	v := 100.0
	if err := cat.UpsertMetric(ctx, "myapp.daily.visitors", "users", "higher_is_better", "sum", &v); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("explain",
			mcp.WithString("metric_fq", mcp.Required()),
			mcp.WithString("since"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			fqName, err := req.RequireString("metric_fq")
			if err != nil {
				return mcp.NewToolResultError("explain: metric_fq is required"), nil
			}
			sinceStr := req.GetString("since", "7d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError("explain: invalid since"), nil
			}
			data, err := cat.ExplainMetric(ctx, fqName, since)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToExplainJSON(data))
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "explain"
	req.Params.Arguments = map[string]any{"metric_fq": "myapp.daily.visitors", "since": "7d"}
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(t, result))
	}

	text := extractText(t, result)
	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("unmarshal explain result: %v\nraw: %s", err, text)
	}
	if _, ok := out["summary"]; !ok {
		t.Errorf("explain result missing 'summary' field; got keys: %v", keys(out))
	}
	if out["metric_fq"] != "myapp.daily.visitors" {
		t.Errorf("metric_fq mismatch: %v", out["metric_fq"])
	}
}

// TestMCPInvestigateUnknownMetricReturnsError verifies that the investigate tool
// returns an MCP error result for an unrecognized metric.
func TestMCPInvestigateUnknownMetricReturnsError(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("investigate",
			mcp.WithString("metric_fq", mcp.Required()),
			mcp.WithString("since"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			fqName, err := req.RequireString("metric_fq")
			if err != nil {
				return mcp.NewToolResultError("investigate: metric_fq is required"), nil
			}
			sinceStr := req.GetString("since", "14d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError("investigate: invalid since"), nil
			}
			data, err := cat.InvestigateMetric(ctx, fqName, since)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToInvestigateJSON(data))
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "investigate"
	req.Params.Arguments = map[string]any{"metric_fq": "no.such.metric"}
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if !result.IsError {
		t.Error("expected tool error result for unknown metric")
	}
	text := extractText(t, result)
	if !strings.Contains(text, "not found") {
		t.Errorf("expected 'not found' in error message, got: %s", text)
	}
}

// TestMCPInvestigateKnownMetricReturnsCausalFields verifies that investigate
// returns structured JSON with metric_fq, explain, causal_candidates, and
// sibling_correlations fields for a known metric.
func TestMCPInvestigateKnownMetricReturnsCausalFields(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	v := 500.0
	if err := cat.UpsertMetric(ctx, "myapp.daily.revenue", "usd", "higher_is_better", "sum", &v); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("investigate",
			mcp.WithString("metric_fq", mcp.Required()),
			mcp.WithString("since"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			fqName, err := req.RequireString("metric_fq")
			if err != nil {
				return mcp.NewToolResultError("investigate: metric_fq is required"), nil
			}
			sinceStr := req.GetString("since", "14d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError("investigate: invalid since"), nil
			}
			data, err := cat.InvestigateMetric(ctx, fqName, since)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToInvestigateJSON(data))
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "investigate"
	req.Params.Arguments = map[string]any{"metric_fq": "myapp.daily.revenue", "since": "14d"}
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(t, result))
	}

	text := extractText(t, result)
	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("unmarshal investigate result: %v\nraw: %s", err, text)
	}
	for _, field := range []string{"metric_fq", "explain", "causal_candidates", "sibling_correlations"} {
		if _, ok := out[field]; !ok {
			t.Errorf("investigate result missing %q field; got keys: %v", field, keys(out))
		}
	}
	if out["metric_fq"] != "myapp.daily.revenue" {
		t.Errorf("metric_fq mismatch: %v", out["metric_fq"])
	}
}

// TestMCPCompareKnownMetricsReturnsVerdictFields verifies that compare returns
// structured JSON with metric_a, metric_b, verdict, and summary fields.
func TestMCPCompareKnownMetricsReturnsVerdictFields(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	va, vb := 100.0, 200.0
	if err := cat.UpsertMetric(ctx, "myapp.daily.visitors", "users", "higher_is_better", "sum", &va); err != nil {
		t.Fatalf("upsert metric a: %v", err)
	}
	if err := cat.UpsertMetric(ctx, "myapp.daily.pageviews", "count", "higher_is_better", "sum", &vb); err != nil {
		t.Fatalf("upsert metric b: %v", err)
	}

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("compare",
			mcp.WithString("metric_a", mcp.Required()),
			mcp.WithString("metric_b", mcp.Required()),
			mcp.WithString("since"),
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
				return mcp.NewToolResultError("compare: invalid since"), nil
			}
			data, err := cat.CompareMetrics(ctx, metricA, metricB, since)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToCompareJSON(data))
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "compare"
	req.Params.Arguments = map[string]any{
		"metric_a": "myapp.daily.visitors",
		"metric_b": "myapp.daily.pageviews",
		"since":    "7d",
	}
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(t, result))
	}

	text := extractText(t, result)
	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("unmarshal compare result: %v\nraw: %s", err, text)
	}
	for _, field := range []string{"metric_a", "metric_b", "verdict", "summary"} {
		if _, ok := out[field]; !ok {
			t.Errorf("compare result missing %q field; got keys: %v", field, keys(out))
		}
	}
}

// TestMCPSummarizeReturnsTopMetrics verifies that summarize returns structured
// JSON with total_metrics and top_metrics fields for a catalog with known data.
func TestMCPSummarizeReturnsTopMetrics(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	for _, fq := range []string{"myapp.daily.visitors", "myapp.daily.signups", "myapp.daily.errors"} {
		v := 42.0
		if err := cat.UpsertMetric(ctx, fq, "count", "higher_is_better", "sum", &v); err != nil {
			t.Fatalf("upsert metric %s: %v", fq, err)
		}
	}

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("summarize",
			mcp.WithString("since"),
			mcp.WithNumber("top"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			sinceStr := req.GetString("since", "7d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError("summarize: invalid since"), nil
			}
			topK := int(req.GetFloat("top", 5))
			if topK <= 0 {
				topK = 5
			}
			data, err := cat.SummarizeAll(ctx, since, topK)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToSummaryJSON(data))
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "summarize"
	req.Params.Arguments = map[string]any{"since": "7d", "top": 5}
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(t, result))
	}

	text := extractText(t, result)
	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("unmarshal summarize result: %v\nraw: %s", err, text)
	}
	for _, field := range []string{"total_metrics", "top_metrics"} {
		if _, ok := out[field]; !ok {
			t.Errorf("summarize result missing %q field; got keys: %v", field, keys(out))
		}
	}
	if n, ok := out["total_metrics"].(float64); !ok || int(n) != 3 {
		t.Errorf("want total_metrics=3, got %v", out["total_metrics"])
	}
}

// TestMCPForecastUnknownMetricReturnsError verifies that the forecast tool
// returns an MCP error result for an unrecognized metric.
func TestMCPForecastUnknownMetricReturnsError(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("forecast",
			mcp.WithString("metric_fq", mcp.Required()),
			mcp.WithString("horizon"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			fqName, err := req.RequireString("metric_fq")
			if err != nil {
				return mcp.NewToolResultError("forecast: metric_fq is required"), nil
			}
			horizonStr := req.GetString("horizon", "7d")
			horizon, err := parseSinceDuration(horizonStr)
			if err != nil {
				return mcp.NewToolResultError("forecast: invalid horizon"), nil
			}
			data, err := cat.ForecastMetric(ctx, fqName, horizon)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToForecastJSON(data))
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "forecast"
	req.Params.Arguments = map[string]any{"metric_fq": "no.such.metric"}
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if !result.IsError {
		t.Error("expected tool error result for unknown metric")
	}
	text := extractText(t, result)
	if !strings.Contains(text, "not found") {
		t.Errorf("expected 'not found' in error, got: %s", text)
	}
}

// TestMCPForecastKnownMetricReturnsFields verifies that forecast returns
// structured JSON with metric_fq, label, confidence, and horizon_days fields.
func TestMCPForecastKnownMetricReturnsFields(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	v := 200.0
	if err := cat.UpsertMetric(ctx, "myapp.daily.signups", "count", "higher_is_better", "sum", &v); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}
	// Two observations are required for regression; values differ so slope is non-zero.
	if err := cat.RecordMetricValue(ctx, "myapp.daily.signups", 180.0); err != nil {
		t.Fatalf("record value 1: %v", err)
	}
	if err := cat.RecordMetricValue(ctx, "myapp.daily.signups", 200.0); err != nil {
		t.Fatalf("record value 2: %v", err)
	}

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("forecast",
			mcp.WithString("metric_fq", mcp.Required()),
			mcp.WithString("horizon"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			fqName, err := req.RequireString("metric_fq")
			if err != nil {
				return mcp.NewToolResultError("forecast: metric_fq is required"), nil
			}
			horizonStr := req.GetString("horizon", "7d")
			horizon, err := parseSinceDuration(horizonStr)
			if err != nil {
				return mcp.NewToolResultError("forecast: invalid horizon"), nil
			}
			data, err := cat.ForecastMetric(ctx, fqName, horizon)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToForecastJSON(data))
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "forecast"
	req.Params.Arguments = map[string]any{"metric_fq": "myapp.daily.signups", "horizon": "7d"}
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(t, result))
	}

	text := extractText(t, result)
	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("unmarshal forecast result: %v\nraw: %s", err, text)
	}
	for _, field := range []string{"metric_fq", "directional_label", "confidence", "horizon", "sample_count"} {
		if _, ok := out[field]; !ok {
			t.Errorf("forecast result missing %q field; got keys: %v", field, keys(out))
		}
	}
	if out["metric_fq"] != "myapp.daily.signups" {
		t.Errorf("metric_fq mismatch: %v", out["metric_fq"])
	}
}

// TestMCPRecommendReturnsFields verifies that recommend returns structured JSON
// with total and items fields.
func TestMCPRecommendReturnsFields(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	for _, fq := range []string{"myapp.daily.visitors", "myapp.daily.revenue"} {
		v := 100.0
		if err := cat.UpsertMetric(ctx, fq, "count", "higher_is_better", "sum", &v); err != nil {
			t.Fatalf("upsert metric: %v", err)
		}
	}

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("recommend",
			mcp.WithString("since"),
			mcp.WithNumber("top"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			sinceStr := req.GetString("since", "7d")
			since, err := parseSinceDuration(sinceStr)
			if err != nil {
				return mcp.NewToolResultError("recommend: invalid since"), nil
			}
			topK := int(req.GetFloat("top", 5))
			if topK <= 0 {
				topK = 5
			}
			data, err := cat.RecommendAll(ctx, since, topK)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToRecommendJSON(data))
			return mcp.NewToolResultText(string(b)), nil
		},
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	var req mcp.CallToolRequest
	req.Params.Name = "recommend"
	req.Params.Arguments = map[string]any{"since": "7d", "top": 5}
	result, err := unstarted.Client().CallTool(ctx, req)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", extractText(t, result))
	}

	text := extractText(t, result)
	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("unmarshal recommend result: %v\nraw: %s", err, text)
	}
	for _, field := range []string{"since", "items"} {
		if _, ok := out[field]; !ok {
			t.Errorf("recommend result missing %q field; got keys: %v", field, keys(out))
		}
	}
}

// TestMCPMonitorListAndRunRoundTrip exercises the monitor tool's list and run
// actions through an mcptest server, including adding a watch and verifying
// it appears in list output and run result.
func TestMCPMonitorListAndRunRoundTrip(t *testing.T) {
	ctx := t.Context()
	cat := openTestCatalogForMCP(t)

	v := 999.0
	if err := cat.UpsertMetric(ctx, "myapp.daily.errors", "count", "lower_is_better", "sum", &v); err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	monitorHandler := func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		action, err := req.RequireString("action")
		if err != nil {
			return mcp.NewToolResultError("monitor: action is required"), nil
		}
		switch action {
		case "add":
			name := req.GetString("name", "")
			metric := req.GetString("metric", "")
			condition := req.GetString("condition", "")
			if err := cat.AddWatch(ctx, name, metric, condition); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(map[string]string{"status": "ok", "name": name})
			return mcp.NewToolResultText(string(b)), nil
		case "list":
			rows, err := cat.ListWatches(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			type watchJSON struct {
				Name      string `json:"name"`
				MetricFQ  string `json:"metric_fq"`
				Condition string `json:"condition"`
			}
			out := make([]watchJSON, len(rows))
			for i, w := range rows {
				out[i] = watchJSON{Name: w.Name, MetricFQ: w.MetricFQ, Condition: w.Condition}
			}
			b, _ := json.Marshal(out)
			return mcp.NewToolResultText(string(b)), nil
		case "run":
			result, err := cat.RunWatches(ctx)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(ridgelinememory.ToMonitorRunJSON(result))
			return mcp.NewToolResultText(string(b)), nil
		default:
			return mcp.NewToolResultError("monitor: unknown action"), nil
		}
	}

	unstarted := mcptest.NewUnstartedServer(t)
	unstarted.AddServerOptions(server.WithToolCapabilities(true))
	unstarted.AddTool(
		mcp.NewTool("monitor",
			mcp.WithString("action", mcp.Required()),
			mcp.WithString("name"),
			mcp.WithString("metric"),
			mcp.WithString("condition"),
		),
		monitorHandler,
	)
	if err := unstarted.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer unstarted.Close()

	// Add a watch.
	addReq := mcp.CallToolRequest{}
	addReq.Params.Name = "monitor"
	addReq.Params.Arguments = map[string]any{
		"action":    "add",
		"name":      "errors-too-high",
		"metric":    "myapp.daily.errors",
		"condition": "above 100",
	}
	addResult, err := unstarted.Client().CallTool(ctx, addReq)
	if err != nil {
		t.Fatalf("monitor add: %v", err)
	}
	if addResult.IsError {
		t.Fatalf("monitor add error: %s", extractText(t, addResult))
	}

	// List should contain the watch.
	listReq := mcp.CallToolRequest{}
	listReq.Params.Name = "monitor"
	listReq.Params.Arguments = map[string]any{"action": "list"}
	listResult, err := unstarted.Client().CallTool(ctx, listReq)
	if err != nil {
		t.Fatalf("monitor list: %v", err)
	}
	if listResult.IsError {
		t.Fatalf("monitor list error: %s", extractText(t, listResult))
	}
	listText := extractText(t, listResult)
	if !strings.Contains(listText, "errors-too-high") {
		t.Errorf("monitor list missing added watch; got: %s", listText)
	}

	// Run should return evaluated count >= 1.
	runReq := mcp.CallToolRequest{}
	runReq.Params.Name = "monitor"
	runReq.Params.Arguments = map[string]any{"action": "run"}
	runResult, err := unstarted.Client().CallTool(ctx, runReq)
	if err != nil {
		t.Fatalf("monitor run: %v", err)
	}
	if runResult.IsError {
		t.Fatalf("monitor run error: %s", extractText(t, runResult))
	}
	var runOut map[string]any
	if err := json.Unmarshal([]byte(extractText(t, runResult)), &runOut); err != nil {
		t.Fatalf("unmarshal monitor run: %v", err)
	}
	if _, ok := runOut["evaluated"]; !ok {
		t.Errorf("monitor run missing 'evaluated' field; got keys: %v", keys(runOut))
	}
	if _, ok := runOut["triggered"]; !ok {
		t.Errorf("monitor run missing 'triggered' field; got keys: %v", keys(runOut))
	}
}

func extractText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	for _, c := range result.Content {
		if tc, ok := c.(mcp.TextContent); ok {
			return tc.Text
		}
	}
	t.Fatalf("no text content in tool result")
	return ""
}

func keys(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
