package connectors

import (
	"context"
	"reflect"
	"testing"
)

type fakeConnector struct {
	name string
}

func (f *fakeConnector) Spec() ConnectorSpec {
	return ConnectorSpec{Name: f.name, Version: "0.0.1", AuthType: AuthNone}
}
func (f *fakeConnector) Validate(context.Context, ConnectorConfig) error { return nil }
func (f *fakeConnector) Discover(context.Context, ConnectorConfig) (*Catalog, error) {
	return &Catalog{}, nil
}
func (f *fakeConnector) Extract(context.Context, ConnectorConfig, []Stream, State) (<-chan Message, error) {
	ch := make(chan Message)
	close(ch)
	return ch, nil
}

func TestRegisterAndGet(t *testing.T) {
	t.Cleanup(reset)
	reset()

	Register(&fakeConnector{name: "alpha"})
	Register(&fakeConnector{name: "bravo"})

	got, ok := Get("alpha")
	if !ok || got.Spec().Name != "alpha" {
		t.Fatalf("Get(alpha) = (%v, %v); want connector with name alpha", got, ok)
	}
	if _, ok := Get("missing"); ok {
		t.Fatalf("Get(missing) returned ok; want not found")
	}

	if want, got := []string{"alpha", "bravo"}, List(); !reflect.DeepEqual(want, got) {
		t.Fatalf("List() = %v; want %v", got, want)
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	t.Cleanup(reset)
	reset()

	Register(&fakeConnector{name: "dup"})
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on duplicate Register")
		}
	}()
	Register(&fakeConnector{name: "dup"})
}

func TestRegisterNilPanics(t *testing.T) {
	t.Cleanup(reset)
	reset()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on nil Register")
		}
	}()
	Register(nil)
}

func TestRegisterEmptyNamePanics(t *testing.T) {
	t.Cleanup(reset)
	reset()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on empty-name Register")
		}
	}()
	Register(&fakeConnector{name: ""})
}
