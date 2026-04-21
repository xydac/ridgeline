package testsrc_test

import (
	"context"
	"testing"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/testsrc"
)

func TestRegistered(t *testing.T) {
	t.Parallel()
	if _, ok := connectors.Get(testsrc.Name); !ok {
		t.Fatalf("testsrc connector not registered")
	}
}

func TestSpec(t *testing.T) {
	t.Parallel()
	c := testsrc.New()
	spec := c.Spec()
	if spec.Name != testsrc.Name {
		t.Errorf("Name = %q, want %q", spec.Name, testsrc.Name)
	}
	if len(spec.Streams) != 2 {
		t.Errorf("streams = %d, want 2", len(spec.Streams))
	}
}

func TestDiscover(t *testing.T) {
	t.Parallel()
	c := testsrc.New()
	cat, err := c.Discover(context.Background(), connectors.ConnectorConfig{})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(cat.Streams) != 2 {
		t.Fatalf("Streams = %d, want 2", len(cat.Streams))
	}
	for _, s := range cat.Streams {
		if !s.Available {
			t.Errorf("stream %s should be available", s.Name)
		}
	}
}

func TestExtract_DefaultRecords(t *testing.T) {
	t.Parallel()
	c := testsrc.New()
	streams := []connectors.Stream{{Name: "pages"}, {Name: "events"}}
	ch, err := c.Extract(context.Background(), connectors.ConnectorConfig{}, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var records, states int
	for m := range ch {
		switch m.Type {
		case connectors.RecordMsg:
			records++
		case connectors.StateMsg:
			states++
		}
	}
	if records != testsrc.DefaultRecords*2 {
		t.Errorf("records = %d, want %d", records, testsrc.DefaultRecords*2)
	}
	if states != 2 {
		t.Errorf("states = %d, want 2", states)
	}
}

func TestExtract_ConfigRecords(t *testing.T) {
	t.Parallel()
	c := testsrc.New()
	streams := []connectors.Stream{{Name: "pages"}}
	ch, err := c.Extract(context.Background(), connectors.ConnectorConfig{"records": 3}, streams, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	var records int
	for m := range ch {
		if m.Type == connectors.RecordMsg {
			records++
		}
	}
	if records != 3 {
		t.Errorf("records = %d, want 3", records)
	}
}

func TestExtract_NegativeRejected(t *testing.T) {
	t.Parallel()
	c := testsrc.New()
	_, err := c.Extract(context.Background(), connectors.ConnectorConfig{"records": -1}, nil, nil)
	if err == nil {
		t.Fatal("expected error for negative records")
	}
}

func TestExtract_ContextCancel(t *testing.T) {
	t.Parallel()
	c := testsrc.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch, err := c.Extract(ctx, connectors.ConnectorConfig{"records": 1000}, []connectors.Stream{{Name: "pages"}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	for range ch {
		// drain
	}
}
