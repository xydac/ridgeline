package external_test

import (
	"context"
	"testing"

	"github.com/xydac/ridgeline/connectors"
	"github.com/xydac/ridgeline/connectors/external"
)

func TestRegistered(t *testing.T) {
	t.Parallel()
	if _, ok := connectors.Get(external.Name); !ok {
		t.Fatalf("external connector not registered")
	}
}

func TestSpec(t *testing.T) {
	t.Parallel()
	spec := external.New().Spec()
	if spec.Name != external.Name {
		t.Errorf("Name = %q, want %q", spec.Name, external.Name)
	}
	if spec.AuthType != connectors.AuthNone {
		t.Errorf("AuthType = %v, want AuthNone", spec.AuthType)
	}
	if spec.Version == "" {
		t.Error("Version must not be empty")
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	c := external.New()
	cases := []struct {
		name    string
		cfg     connectors.ConnectorConfig
		wantErr bool
	}{
		{"empty", connectors.ConnectorConfig{}, true},
		{"whitespace-command", connectors.ConnectorConfig{"command": "   "}, true},
		{"valid", connectors.ConnectorConfig{"command": "/bin/echo"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.Validate(context.Background(), tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate(%v) err=%v, wantErr=%v", tc.cfg, err, tc.wantErr)
			}
		})
	}
}

func TestDiscover(t *testing.T) {
	t.Parallel()
	c := external.New()
	cat, err := c.Discover(context.Background(), connectors.ConnectorConfig{"command": "/bin/echo"})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if cat == nil {
		t.Fatal("Discover returned nil catalog")
	}
}
