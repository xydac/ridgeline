package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/creds"
	sqlitestate "github.com/xydac/ridgeline/state/sqlite"
)

// validateCredName rejects names that contain path separators, dot-dot
// sequences, or whitespace. These characters could be exploited to
// construct unexpected filesystem paths in credential-backed connectors
// and make error messages ambiguous.
func validateCredName(name string) error {
	if name == "" {
		return fmt.Errorf("credential name must not be empty")
	}
	if strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return fmt.Errorf("credential name %q must not contain path separators", name)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("credential name %q must not contain '..'", name)
	}
	for _, r := range name {
		if unicode.IsSpace(r) {
			return fmt.Errorf("credential name %q must not contain whitespace", name)
		}
	}
	return nil
}

// errCreds strips a leading "creds: " prefix from errors returned by the
// creds package so that main.go's single "creds: " wrap does not double it.
func errCreds(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(strings.TrimPrefix(err.Error(), "creds: "))
}

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
		credsUsage(stderr)
		return usageErrorf("a verb is required (list, put, get, rm, oauth)")
	}
	verb, rest := args[0], args[1:]
	switch verb {
	case "help", "--help", "-h":
		credsUsage(stdout)
		return nil
	case "init":
		return credsInit(ctx, rest, stdout, stderr)
	case "list":
		return credsList(ctx, rest, stdout)
	case "put":
		return credsPut(ctx, rest, stdin, stdout, stderr)
	case "get":
		return credsGet(ctx, rest, stdout)
	case "rm":
		return credsRm(ctx, rest, stdout)
	case "oauth":
		return credsOAuth(ctx, rest, stdin, stdout, stderr)
	}
	return usageErrorf("unknown creds verb %q (known: init, list, put, get, rm, oauth)", verb)
}

func credsUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  ridgeline creds init  --config PATH [--force-new-key]   # create or reset the credential store")
	fmt.Fprintln(w, "  ridgeline creds list  --config PATH")
	fmt.Fprintln(w, "  ridgeline creds put   --config PATH [--raw] NAME        # reads secret from stdin")
	fmt.Fprintln(w, "  ridgeline creds get   --config PATH [--raw] NAME        # writes plaintext to stdout")
	fmt.Fprintln(w, "  ridgeline creds rm    --config PATH NAME")
	fmt.Fprintln(w, "  ridgeline creds oauth gsc --config PATH --client-id ID (--client-secret SEC | --client-secret-file PATH | --client-secret-stdin) [--name PREFIX]")
}

// credsInit implements `ridgeline creds init`. Without --force-new-key it
// initializes the credential store and key file for the first time; it errors
// when the key file already exists to avoid accidental re-init. With
// --force-new-key it truncates all stored credentials and replaces the key
// file, making all prior secrets unrecoverable. This is the only supported
// path for migrating to a new key after losing the original key file.
func credsInit(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if cfgVal, rest, found := prescanStringFlag("config", args); found {
		args = append([]string{"--config", cfgVal}, rest...)
	}
	fs := flag.NewFlagSet("creds init", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	forceNewKey := fs.Bool("force-new-key", false, "DESTRUCTIVE: wipe all stored credentials and replace the key file")
	yes := fs.Bool("yes", false, "confirm the destructive operation requested by --force-new-key")
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: ridgeline creds init --config PATH [--force-new-key --yes]")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Initialize the credential store and key file for the first time.")
		fmt.Fprintln(w, "Pass --force-new-key --yes to wipe all existing credentials and replace the key (DESTRUCTIVE).")
		fmt.Fprintln(w, "The --yes flag is required with --force-new-key to prevent accidental use.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if *cfgPath == "" {
		return fmt.Errorf("--config PATH is required")
	}
	cfg, err := config.LoadCreds(*cfgPath)
	if err != nil {
		return err
	}

	sqlStore, err := sqlitestate.Open(cfg.StatePath)
	if err != nil {
		return err
	}
	defer sqlStore.Close()

	if *forceNewKey {
		if !*yes {
			fmt.Fprintf(stderr, "WARNING: --force-new-key will permanently delete all credentials in %s\n", cfg.StatePath)
			fmt.Fprintf(stderr, "         and replace the encryption key at %s.\n", cfg.KeyPath)
			fmt.Fprintln(stderr, "         Any other config sharing this key_path will have its credentials made unrecoverable.")
			fmt.Fprintln(stderr, "Re-run with --force-new-key --yes to confirm.")
			return fmt.Errorf("--force-new-key requires --yes to confirm")
		}
		// Wipe all credentials then replace the key file.
		if _, err := sqlStore.DB().ExecContext(ctx, `DELETE FROM credentials`); err != nil {
			return fmt.Errorf("creds init: wipe credentials: %w", err)
		}
		key, err := creds.NewRandomKey()
		if err != nil {
			return err
		}
		if err := creds.WriteKeyFile(cfg.KeyPath, key); err != nil {
			return err
		}
		fmt.Fprintln(stderr, "credential store wiped and new key written to", cfg.KeyPath)
		return nil
	}

	// Normal init: fail fast if the key file already exists.
	if _, err := os.Stat(cfg.KeyPath); err == nil {
		return fmt.Errorf("key file already exists at %s; use `ridgeline creds list` to verify the store, or pass --force-new-key --yes to replace it (DESTRUCTIVE)", cfg.KeyPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("creds init: stat key file: %w", err)
	}
	key, err := creds.NewRandomKey()
	if err != nil {
		return err
	}
	if err := creds.WriteKeyFile(cfg.KeyPath, key); err != nil {
		return err
	}
	fmt.Fprintln(stderr, "credential store initialized; key written to", cfg.KeyPath)
	return nil
}

// credsFlags parses --config out of args and returns the loaded config
// alongside the remaining positional arguments. Errors here are the
// same flavor as runSync: missing --config is a hard error.
//
// --config is honored regardless of position relative to NAME positionals
// by prescanning for it before handing off to flag.FlagSet.
//
// stdout is used as the destination for help output so --help exits 0
// with usage printed to the right writer.
func credsFlags(verb string, args []string, stdout io.Writer) (*config.File, []string, bool, error) {
	// Pre-scan so --config is honored whether the user writes it before or
	// after the NAME positional. flag.FlagSet stops at the first non-flag
	// token, so without this a flag-after-positional is silently dropped.
	if cfgVal, rest, found := prescanStringFlag("config", args); found {
		args = append([]string{"--config", cfgVal}, rest...)
	}
	fs := flag.NewFlagSet("creds "+verb, flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintf(w, "Usage: ridgeline creds %s --config PATH", verb)
		if verb != "list" {
			fmt.Fprint(w, " NAME")
		}
		fmt.Fprintln(w)
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if err != nil {
		return nil, nil, false, err
	}
	if help {
		return nil, nil, true, nil
	}
	if *cfgPath == "" {
		return nil, nil, false, fmt.Errorf("--config PATH is required")
	}
	cfg, err := config.LoadCreds(*cfgPath)
	if err != nil {
		return nil, nil, false, err
	}
	return cfg, fs.Args(), false, nil
}

// openCreds opens (or creates) the state db and the key file, and
// returns a creds.Store sharing the db handle. The caller must Close
// the returned *sqlitestate.Store. A missing key file is created with
// a freshly generated random key ONLY when the credential store is
// empty; if existing secrets are present, openCreds returns an error
// rather than orphaning them behind an incompatible key.
func openCreds(cfg *config.File) (*creds.Store, *sqlitestate.Store, error) {
	store, err := sqlitestate.Open(cfg.StatePath)
	if err != nil {
		return nil, nil, err
	}
	key, err := creds.KeyFromFile(cfg.KeyPath)
	if errors.Is(err, os.ErrNotExist) {
		// Refuse to mint a fresh key when existing secrets would become
		// unrecoverable. An empty store is safe to auto-init (fresh-machine flow).
		var count int
		if qErr := store.DB().QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM credentials`).Scan(&count); qErr == nil && count > 0 {
			store.Close()
			return nil, nil, fmt.Errorf(
				"credential store exists but key file missing at %s; "+
					"refusing to mint a replacement key (would orphan %d existing secret(s)); "+
					"either restore the key or run `ridgeline creds init --force-new-key` to wipe the store",
				cfg.KeyPath, count)
		}
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
	cfg, rest, help, err := credsFlags("list", args, stdout)
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

func credsPut(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	// Pre-scan so --config is honored whether the user writes it before or
	// after the NAME positional.
	if cfgVal, rest, found := prescanStringFlag("config", args); found {
		args = append([]string{"--config", cfgVal}, rest...)
	}
	fs := flag.NewFlagSet("creds put", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	rawMode := fs.Bool("raw", false, "store bytes verbatim without stripping a trailing newline")
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: ridgeline creds put --config PATH [--raw] NAME")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Reads the secret from stdin, encrypts it, and stores it under NAME.")
		fmt.Fprintln(w, "Pass --raw to preserve trailing newlines verbatim (default: strip one).")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if *cfgPath == "" {
		return fmt.Errorf("--config PATH is required")
	}
	rest := fs.Args()
	if len(rest) != 1 {
		return fmt.Errorf("put: exactly one NAME argument is required")
	}
	name := rest[0]
	if err := validateCredName(name); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	if isInteractive(stdin) {
		fmt.Fprintf(stderr, "Enter secret for %q, end with EOF (Ctrl+D):\n", name)
	}
	input, err := io.ReadAll(stdin)
	if err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}
	secret := input
	if !*rawMode {
		// Strip a single trailing newline so `echo foo | creds put` stores
		// exactly "foo". Pass --raw to preserve trailing bytes verbatim.
		secret = bytes.TrimSuffix(secret, []byte("\n"))
		secret = bytes.TrimSuffix(secret, []byte("\r"))
	}
	if len(secret) == 0 {
		return fmt.Errorf("put: refused to store an empty secret for %q", name)
	}

	cfg, err := config.LoadCreds(*cfgPath)
	if err != nil {
		return err
	}
	cs, store, err := openCreds(cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	action := "stored"
	warnUndecryptable := false
	if _, getErr := cs.Get(ctx, name); getErr == nil {
		action = "replaced"
	} else if !errors.Is(getErr, creds.ErrNotFound) {
		// A record exists but cannot be decrypted (key mismatch or corruption).
		action = "replaced"
		warnUndecryptable = true
	}
	if err := cs.Put(ctx, name, secret); err != nil {
		return errCreds(err)
	}
	if warnUndecryptable {
		fmt.Fprintf(stderr, "warning: replaced an undecryptable record for %q\n", name)
	}
	fmt.Fprintf(stderr, "%s credential %q (%d bytes)\n", action, name, len(secret))
	return nil
}

func credsGet(ctx context.Context, args []string, stdout io.Writer) error {
	// Pre-scan so --config is honored whether the user writes it before or
	// after the NAME positional.
	if cfgVal, rest, found := prescanStringFlag("config", args); found {
		args = append([]string{"--config", cfgVal}, rest...)
	}
	fs := flag.NewFlagSet("creds get", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	rawMode := fs.Bool("raw", false, "write secret bytes verbatim without appending a trailing newline")
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Usage: ridgeline creds get --config PATH [--raw] NAME")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Decrypts and writes the secret to stdout.")
		fmt.Fprintln(w, "Without --raw a trailing newline is appended when the secret lacks one,")
		fmt.Fprintln(w, "matching the behavior of echo so the value is shell-friendly.")
		fmt.Fprintln(w, "Pass --raw to retrieve the exact bytes as stored.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if *cfgPath == "" {
		return fmt.Errorf("--config PATH is required")
	}
	rest := fs.Args()
	if len(rest) != 1 {
		return fmt.Errorf("get: exactly one NAME argument is required")
	}
	name := rest[0]
	if err := validateCredName(name); err != nil {
		return fmt.Errorf("get: %w", err)
	}
	cfg, err := config.LoadCreds(*cfgPath)
	if err != nil {
		return err
	}
	cs, store, err := openCreds(cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	plain, err := cs.Get(ctx, name)
	if err != nil {
		if errors.Is(err, creds.ErrNotFound) {
			return fmt.Errorf("get: credential %q does not exist", name)
		}
		return errCreds(err)
	}
	if _, err := stdout.Write(plain); err != nil {
		return err
	}
	if !*rawMode {
		// Match `echo`: append a trailing newline when the plaintext has
		// none, so piping into $(...) yields the expected string without
		// mangling a shell prompt.
		if len(plain) == 0 || plain[len(plain)-1] != '\n' {
			fmt.Fprintln(stdout)
		}
	}
	return nil
}

func credsRm(ctx context.Context, args []string, stdout io.Writer) error {
	cfg, rest, help, err := credsFlags("rm", args, stdout)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if len(rest) != 1 {
		return fmt.Errorf("rm: exactly one NAME argument is required")
	}
	if err := validateCredName(rest[0]); err != nil {
		return fmt.Errorf("rm: %w", err)
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
