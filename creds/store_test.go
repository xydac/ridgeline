package creds_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xydac/ridgeline/creds"
	"github.com/xydac/ridgeline/state/sqlite"
)

func newTestStore(t *testing.T) (*creds.Store, func()) {
	t.Helper()
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	key, err := creds.NewRandomKey()
	if err != nil {
		t.Fatalf("NewRandomKey: %v", err)
	}
	cs, err := creds.New(st.DB(), key)
	if err != nil {
		t.Fatalf("creds.New: %v", err)
	}
	return cs, func() { _ = st.Close() }
}

func TestRoundTrip(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()

	want := []byte("super-secret-token")
	if err := cs.Put(ctx, "gsc_token", want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := cs.Get(ctx, "gsc_token")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("round trip: want %q got %q", want, got)
	}
}

func TestGet_NotFound(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	_, err := cs.Get(context.Background(), "missing")
	if !errors.Is(err, creds.ErrNotFound) {
		t.Fatalf("Get missing: want ErrNotFound, got %v", err)
	}
}

func TestPut_OverwritesWithNewNonce(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	if err := cs.Put(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("Put1: %v", err)
	}
	if err := cs.Put(ctx, "k", []byte("v2")); err != nil {
		t.Fatalf("Put2: %v", err)
	}
	got, err := cs.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("after overwrite: want v2, got %q", got)
	}
}

func TestDelete(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	if err := cs.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := cs.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := cs.Get(ctx, "k"); !errors.Is(err, creds.ErrNotFound) {
		t.Fatalf("Get after delete: want ErrNotFound, got %v", err)
	}
	// Delete of a missing key surfaces ErrNotFound so the CLI can
	// exit non-zero on a typo instead of reporting a silent success.
	if err := cs.Delete(ctx, "k"); !errors.Is(err, creds.ErrNotFound) {
		t.Fatalf("Delete missing: want ErrNotFound, got %v", err)
	}
}

func TestNamesSorted(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	for _, n := range []string{"c", "a", "b"} {
		if err := cs.Put(ctx, n, []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", n, err)
		}
	}
	names, err := cs.Names(ctx)
	if err != nil {
		t.Fatalf("Names: %v", err)
	}
	if len(names) != 3 || names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Fatalf("Names: got %v", names)
	}
}

func TestNew_RejectsBadKey(t *testing.T) {
	st, _ := sqlite.Open(":memory:")
	defer st.Close()
	if _, err := creds.New(st.DB(), nil); err == nil {
		t.Fatalf("New nil key: want error")
	}
	if _, err := creds.New(st.DB(), []byte("too-short")); err == nil {
		t.Fatalf("New short key: want error")
	}
	if _, err := creds.New(nil, make([]byte, creds.KeySize)); err == nil {
		t.Fatalf("New nil db: want error")
	}
}

func TestGet_WrongKeyFails(t *testing.T) {
	// Encrypt with key A, try to decrypt with key B on the same DB.
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	keyA, _ := creds.NewRandomKey()
	csA, err := creds.New(st.DB(), keyA)
	if err != nil {
		t.Fatalf("New A: %v", err)
	}
	if err := csA.Put(ctx, "k", []byte("plain")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	keyB, _ := creds.NewRandomKey()
	csB, err := creds.New(st.DB(), keyB)
	if err != nil {
		t.Fatalf("New B: %v", err)
	}
	if _, err := csB.Get(ctx, "k"); err == nil {
		t.Fatalf("Get with wrong key: want error, got nil")
	}
}

func TestGet_TamperedCiphertextFails(t *testing.T) {
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer st.Close()
	key, _ := creds.NewRandomKey()
	cs, err := creds.New(st.DB(), key)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := cs.Put(ctx, "k", []byte("plain")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Flip every byte of the ciphertext so authentication fails. We
	// read, mutate in Go, and write back as a BLOB, because SQLite
	// STRICT mode refuses the SQL concat || trick (it returns TEXT).
	var ct []byte
	if err := st.DB().QueryRowContext(ctx, `SELECT ciphertext FROM credentials WHERE name='k'`).Scan(&ct); err != nil {
		t.Fatalf("read ct: %v", err)
	}
	for i := range ct {
		ct[i] ^= 0xff
	}
	if _, err := st.DB().ExecContext(ctx, `UPDATE credentials SET ciphertext = ? WHERE name='k'`, ct); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	if _, err := cs.Get(ctx, "k"); err == nil {
		t.Fatalf("Get after tamper: want error, got nil")
	}
}

func TestConcurrentPutGet(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := cs.Put(ctx, "shared", []byte("x")); err != nil {
				t.Errorf("Put %d: %v", i, err)
			}
			if _, err := cs.Get(ctx, "shared"); err != nil {
				t.Errorf("Get %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestEmptyNameRejected(t *testing.T) {
	cs, cleanup := newTestStore(t)
	defer cleanup()
	ctx := context.Background()
	if err := cs.Put(ctx, "", []byte("v")); err == nil {
		t.Fatalf("Put empty: want error")
	}
	if _, err := cs.Get(ctx, ""); err == nil {
		t.Fatalf("Get empty: want error")
	}
	if err := cs.Delete(ctx, ""); err == nil {
		t.Fatalf("Delete empty: want error")
	}
}

func TestKeyFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "key")
	key, _ := creds.NewRandomKey()
	if err := creds.WriteKeyFile(path, key); err != nil {
		t.Fatalf("WriteKeyFile: %v", err)
	}
	got, err := creds.KeyFromFile(path)
	if err != nil {
		t.Fatalf("KeyFromFile: %v", err)
	}
	if string(got) != string(key) {
		t.Fatalf("key round trip mismatch")
	}
}

func TestKeyFromFile_RejectsWrongLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	// Only 16 bytes of hex = 8 bytes of key, not 32.
	if err := creds.WriteKeyFile(path, make([]byte, creds.KeySize)); err != nil {
		t.Fatalf("WriteKeyFile: %v", err)
	}
	// Manually truncate the file to simulate a hand-damaged key.
	if err := os.WriteFile(path, []byte("deadbeef\n"), 0o600); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if _, err := creds.KeyFromFile(path); err == nil {
		t.Fatalf("KeyFromFile short: want error")
	}
}
