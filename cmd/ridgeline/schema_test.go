package main

import (
	"bytes"
	"strings"
	"testing"

	// side-effect imports to register connectors
	_ "github.com/xydac/ridgeline/connectors/github"
	_ "github.com/xydac/ridgeline/connectors/gsc"
	_ "github.com/xydac/ridgeline/connectors/hackernews"
	_ "github.com/xydac/ridgeline/connectors/plausible"
	_ "github.com/xydac/ridgeline/connectors/posthog"
	_ "github.com/xydac/ridgeline/connectors/umami"
)

func TestRunSchema_PlausibleAll(t *testing.T) {
	var buf bytes.Buffer
	if err := runSchema([]string{"plausible"}, &buf); err != nil {
		t.Fatalf("runSchema plausible: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"kind:       metric",
		"stream:     timeseries",
		"visitors",
		"higher_is_better",
		"bounce_rate",
		"lower_is_better",
		"unit=%",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

func TestRunSchema_SpecificStream(t *testing.T) {
	var buf bytes.Buffer
	if err := runSchema([]string{"github.views"}, &buf); err != nil {
		t.Fatalf("runSchema github.views: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "kind:       metric") {
		t.Errorf("missing metric kind in:\n%s", out)
	}
	if !strings.Contains(out, "higher_is_better") {
		t.Errorf("missing directionality in:\n%s", out)
	}
}

func TestRunSchema_UnknownConnector(t *testing.T) {
	var buf bytes.Buffer
	err := runSchema([]string{"nonexistent"}, &buf)
	if err == nil {
		t.Fatal("expected error for unknown connector, got nil")
	}
	if !strings.Contains(err.Error(), "no connector named") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunSchema_UnknownStream(t *testing.T) {
	var buf bytes.Buffer
	err := runSchema([]string{"plausible.nosuchstream"}, &buf)
	if err == nil {
		t.Fatal("expected error for unknown stream, got nil")
	}
	if !strings.Contains(err.Error(), "no stream named") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunSchema_MissingArg(t *testing.T) {
	var buf bytes.Buffer
	err := runSchema([]string{}, &buf)
	if err == nil {
		t.Fatal("expected error for missing arg, got nil")
	}
}

func TestRunSchema_EventAndUnstructuredKinds(t *testing.T) {
	for _, tc := range []struct {
		arg      string
		wantKind string
	}{
		{"posthog.events", "event"},
		{"umami.events", "event"},
		{"hackernews.stories", "unstructured"},
	} {
		var buf bytes.Buffer
		if err := runSchema([]string{tc.arg}, &buf); err != nil {
			t.Fatalf("runSchema %s: %v", tc.arg, err)
		}
		if !strings.Contains(buf.String(), "kind:       "+tc.wantKind) {
			t.Errorf("runSchema %s: missing kind %q in:\n%s", tc.arg, tc.wantKind, buf.String())
		}
	}
}
