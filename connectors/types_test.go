package connectors

import "testing"

func TestAuthTypeString(t *testing.T) {
	cases := map[AuthType]string{
		AuthNone:     "none",
		AuthAPIKey:   "api_key",
		AuthOAuth2:   "oauth2",
		AuthJWT:      "jwt",
		AuthBasic:    "basic",
		AuthType(99): "unknown",
	}
	for in, want := range cases {
		if got := in.String(); got != want {
			t.Errorf("AuthType(%d).String() = %q; want %q", in, got, want)
		}
	}
}

func TestSyncModeString(t *testing.T) {
	cases := map[SyncMode]string{
		FullRefresh:  "full_refresh",
		Incremental:  "incremental",
		CDC:          "cdc",
		SyncMode(42): "unknown",
	}
	for in, want := range cases {
		if got := in.String(); got != want {
			t.Errorf("SyncMode(%d).String() = %q; want %q", in, got, want)
		}
	}
}

func TestColumnTypeString(t *testing.T) {
	cases := map[ColumnType]string{
		String:        "string",
		Int:           "int",
		Float:         "float",
		Bool:          "bool",
		Timestamp:     "timestamp",
		JSON:          "json",
		ColumnType(8): "unknown",
	}
	for in, want := range cases {
		if got := in.String(); got != want {
			t.Errorf("ColumnType(%d).String() = %q; want %q", in, got, want)
		}
	}
}

func TestStateString(t *testing.T) {
	s := State{"cursor": "abc", "count": 42}
	if got := s.String("cursor", ""); got != "abc" {
		t.Errorf("State.String(cursor) = %q; want abc", got)
	}
	if got := s.String("missing", "fallback"); got != "fallback" {
		t.Errorf("State.String(missing) = %q; want fallback", got)
	}
	if got := s.String("count", "fallback"); got != "fallback" {
		t.Errorf("State.String(count) = %q; want fallback (non-string)", got)
	}
}

func TestConnectorConfigAccessors(t *testing.T) {
	cfg := ConnectorConfig{
		"name":        "acme",
		"keywords":    []any{"foo", "bar"},
		"explicit":    []string{"baz"},
		"batch_size":  float64(50),
		"int_value":   int(7),
		"int64_value": int64(11),
	}
	if got := cfg.String("name"); got != "acme" {
		t.Errorf("String(name) = %q; want acme", got)
	}
	if got := cfg.String("missing"); got != "" {
		t.Errorf("String(missing) = %q; want empty", got)
	}
	if got := cfg.StringSlice("keywords"); len(got) != 2 || got[0] != "foo" || got[1] != "bar" {
		t.Errorf("StringSlice(keywords) = %v; want [foo bar]", got)
	}
	if got := cfg.StringSlice("explicit"); len(got) != 1 || got[0] != "baz" {
		t.Errorf("StringSlice(explicit) = %v; want [baz]", got)
	}
	if got := cfg.StringSlice("missing"); got != nil {
		t.Errorf("StringSlice(missing) = %v; want nil", got)
	}
	if got := cfg.Int("batch_size", 0); got != 50 {
		t.Errorf("Int(batch_size) = %d; want 50", got)
	}
	if got := cfg.Int("int_value", 0); got != 7 {
		t.Errorf("Int(int_value) = %d; want 7", got)
	}
	if got := cfg.Int("int64_value", 0); got != 11 {
		t.Errorf("Int(int64_value) = %d; want 11", got)
	}
	if got := cfg.Int("missing", 99); got != 99 {
		t.Errorf("Int(missing, 99) = %d; want 99", got)
	}
}
