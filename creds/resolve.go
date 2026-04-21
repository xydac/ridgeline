package creds

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// RefSuffix is the config-key suffix that marks a secret reference.
// A key "api_key_ref" with value "umami_main" asks ResolveRefs to read
// credential "umami_main" from the store and set the sibling key
// "api_key" to its plaintext value.
const RefSuffix = "_ref"

// ResolveRefs walks cfg and, for every entry whose key ends in
// RefSuffix, loads the named credential and stores the plaintext under
// the sibling key with the suffix stripped. The original _ref entry is
// removed so connectors never see it.
//
// The value at a *_ref key must be a non-empty string. Any other shape
// or a missing credential returns an error that names both the config
// key and the credential; the whole cfg is left unchanged on error.
//
// cfg is modified in place. Keys without the suffix are untouched. If
// cfg already holds the stripped key and its _ref sibling, the _ref
// wins: the explicit reference is the newer convention and the literal
// value is treated as a user typo. The stripped key's prior value is
// overwritten and a warning is returned alongside the resolved cfg via
// the second return so the CLI can surface it. An absent sibling
// collision produces a nil warning.
//
// Passing a nil Store with a cfg that has no *_ref keys is a no-op and
// returns nil.
func ResolveRefs(ctx context.Context, store *Store, cfg map[string]any) (warnings []string, err error) {
	if len(cfg) == 0 {
		return nil, nil
	}
	// Snapshot keys in sorted order so error messages are stable.
	keys := make([]string, 0, len(cfg))
	for k := range cfg {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	resolved := make(map[string]string, 4)
	var warns []string
	for _, k := range keys {
		if !strings.HasSuffix(k, RefSuffix) || k == RefSuffix {
			continue
		}
		name, ok := cfg[k].(string)
		if !ok {
			return nil, fmt.Errorf("creds: %s must be a string credential name", k)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("creds: %s must not be empty", k)
		}
		if store == nil {
			return nil, fmt.Errorf("creds: %s references %q but no credential store is configured", k, name)
		}
		plain, err := store.Get(ctx, name)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf("creds: %s references %q which is not in the credential store", k, name)
			}
			return nil, fmt.Errorf("creds: resolve %s: %w", k, err)
		}
		bare := strings.TrimSuffix(k, RefSuffix)
		if _, collision := cfg[bare]; collision {
			warns = append(warns, fmt.Sprintf("%s overrides %s from the config", k, bare))
		}
		resolved[bare] = string(plain)
	}

	for k := range cfg {
		if strings.HasSuffix(k, RefSuffix) && k != RefSuffix {
			delete(cfg, k)
		}
	}
	for bare, plain := range resolved {
		cfg[bare] = plain
	}
	return warns, nil
}
