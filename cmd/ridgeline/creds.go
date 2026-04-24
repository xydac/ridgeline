package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/creds"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

// runCreds implements `ridgeline creds` with four verbs:
//
//	list                 list stored credential names, one per line
//	put   NAME           read a secret from stdin, encrypt, store it
//	get   NAME           decrypt and print the secret to stdout
//	rm    NAME           delete the credential (no-op if missing)
//	oauth PROVIDER ...   run the provider's OAuth browser flow and store
//	                     the resulting credentials
//
// Every verb takes --config PATH, which resolves state_path and
// key_path. Both files are created on first use: the SQLite database
// via the usual migrations, the key file via a fresh random key
// written with mode 0600. A future sync that references a `*_ref` key
// will then find the credentials the CLI put there.
func runCreds(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		credsUsage(stdout)
		return nil
	}
	verb, rest := args[0], args[1:]
	switch verb {
	case "help", "--help", "-h":
		credsUsage(stdout)
		return nil
	case "list":
		return credsList(ctx, rest, stdout)
	case "put":
		return credsPut(ctx, rest, stdin, stderr)
	case "get":
		return credsGet(ctx, rest, stdout)
	case "rm":
		return credsRm(ctx, rest)
	case "oauth":
		return credsOAuth(ctx, rest, stdout, stderr)
	}
	return fmt.Errorf("unknown creds verb %q (known: list, put, get, rm, oauth)", verb)
}

func credsUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  ridgeline creds list  --config PATH")
	fmt.Fprintln(w, "  ridgeline creds put   --config PATH NAME        # reads secret from stdin")
	fmt.Fprintln(w, "  ridgeline creds get   --config PATH NAME        # writes plaintext to stdout")
	fmt.Fprintln(w, "  ridgeline creds rm    --config PATH NAME")
	fmt.Fprintln(w, "  ridgeline creds oauth gsc --config PATH --client-id ID --client-secret SEC [--name PREFIX]")
}

// credsFlags parses --config out of args and returns the loaded config
// alongside the remaining positional arguments. Errors here are the
// same flavor as runSync: missing --config is a hard error.
func credsFlags(verb string, args []string) (*config.File, []string, bool, error) {
	fs := flag.NewFlagSet("creds "+verb, flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	help, err := parseSubcommandFlags(fs, args)
	if err != nil {
		return nil, nil, false, err
	}
	if help {
		return nil, nil, true, nil
	}
	if *cfgPath == "" {
		return nil, nil, false, fmt.Errorf("--config PATH is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return nil, nil, false, err
	}
	return cfg, fs.Args(), false, nil
}

// openCreds opens (or creates) the state db and the key file, and
// returns a creds.Store sharing the db handle. The caller must Close
// the returned *sqlitestate.Store. A missing key file is created with
// a freshly generated random key; on the first `creds put` a user
// therefore needs no setup step beyond writing the yaml.
func openCreds(cfg *config.File) (*creds.Store, *sqlitestate.Store, error) {
	store, err := sqlitestate.Open(cfg.StatePath)
	if err != nil {
		return nil, nil, err
	}
	key, err := creds.KeyFromFile(cfg.KeyPath)
	if errors.Is(err, os.ErrNotExist) {
		key, err = creds.NewRandomKey()
		if err != nil {
			store.Close()
			return nil, nil, err
		}
		if err := creds.WriteKeyFile(cfg.KeyPath, key); err != nil {
			store.Close()
			return nil, nil, err
		}
	} else if err != nil {
		store.Close()
		return nil, nil, err
	}
	cs, err := creds.New(store.DB(), key)
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	return cs, store, nil
}

func credsList(ctx context.Context, args []string, stdout io.Writer) error {
	cfg, rest, help, err := credsFlags("list", args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if len(rest) != 0 {
		return fmt.Errorf("list: unexpected argument %q", rest[0])
	}
	cs, store, err := openCreds(cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	names, err := cs.Names(ctx)
	if err != nil {
		return err
	}
	for _, n := range names {
		fmt.Fprintln(stdout, n)
	}
	return nil
}

func credsPut(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer) error {
	cfg, rest, help, err := credsFlags("put", args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if len(rest) != 1 {
		return fmt.Errorf("put: exactly one NAME argument is required")
	}
	name := rest[0]
	if isInteractive(stdin) {
		fmt.Fprintf(stderr, "Enter secret for %q, end with EOF (Ctrl+D):\n", name)
	}
	raw, err := io.ReadAll(stdin)
	if err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}
	// Trim a single trailing newline so `echo foo | creds put` stores
	// exactly "foo". A user wanting a trailing newline can pipe it with
	// printf.
	raw = bytes.TrimSuffix(raw, []byte("\n"))
	raw = bytes.TrimSuffix(raw, []byte("\r"))
	if len(raw) == 0 {
		return fmt.Errorf("put: refused to store an empty secret for %q", name)
	}

	cs, store, err := openCreds(cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := cs.Put(ctx, name, raw); err != nil {
		return err
	}
	fmt.Fprintf(stderr, "stored credential %q (%d bytes)\n", name, len(raw))
	return nil
}

func credsGet(ctx context.Context, args []string, stdout io.Writer) error {
	cfg, rest, help, err := credsFlags("get", args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if len(rest) != 1 {
		return fmt.Errorf("get: exactly one NAME argument is required")
	}
	name := rest[0]
	cs, store, err := openCreds(cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	plain, err := cs.Get(ctx, name)
	if err != nil {
		return err
	}
	if _, err := stdout.Write(plain); err != nil {
		return err
	}
	// Match `echo`: append a trailing newline when the plaintext has
	// none, so piping into $(...) yields the expected string without
	// mangling a shell prompt.
	if len(plain) == 0 || plain[len(plain)-1] != '\n' {
		fmt.Fprintln(stdout)
	}
	return nil
}

func credsRm(ctx context.Context, args []string) error {
	cfg, rest, help, err := credsFlags("rm", args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if len(rest) != 1 {
		return fmt.Errorf("rm: exactly one NAME argument is required")
	}
	cs, store, err := openCreds(cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := cs.Delete(ctx, rest[0]); err != nil {
		if errors.Is(err, creds.ErrNotFound) {
			return fmt.Errorf("rm: credential %q does not exist", rest[0])
		}
		return err
	}
	return nil
}

// isInteractive reports whether r is an attached terminal. Used to
// decide whether to print the "Enter secret" prompt. We avoid a
// dedicated tty package by reading os.Stdin's mode directly; any
// other io.Reader type is treated as non-interactive.
func isInteractive(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
