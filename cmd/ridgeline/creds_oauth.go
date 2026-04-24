package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/xydac/ridgeline/config"
	"github.com/xydac/ridgeline/connectors/gsc"
)

// credsOAuth dispatches `ridgeline creds oauth PROVIDER ...` to the
// provider-specific browser flow. The only supported provider today
// is `gsc`; unknown names return a usage error rather than a
// silent no-op.
func credsOAuth(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("oauth: provider required (known: gsc)")
	}
	provider, rest := args[0], args[1:]
	switch provider {
	case "help", "--help", "-h":
		fmt.Fprintln(stdout, "Usage: ridgeline creds oauth gsc --config PATH --client-id ID --client-secret SEC [--name PREFIX] [--listen ADDR]")
		return nil
	case "gsc":
		return credsOAuthGSC(ctx, rest, stdout, stderr)
	}
	return fmt.Errorf("oauth: unknown provider %q (known: gsc)", provider)
}

// credsOAuthGSC runs the Google Search Console PKCE flow and stores
// three credentials keyed off the --name prefix: `<prefix>_client_id`,
// `<prefix>_client_secret`, and `<prefix>_refresh_token`. After a
// successful run it prints the yaml snippet a user should paste into
// their ridgeline.yaml under the gsc connector config so the `*_ref`
// lookups resolve on the next sync.
func credsOAuthGSC(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("creds oauth gsc", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	name := fs.String("name", "gsc", "prefix used to key the stored credentials")
	clientID := fs.String("client-id", "", "Google OAuth 2.0 client id (desktop app type)")
	clientSecret := fs.String("client-secret", "", "Google OAuth 2.0 client secret")
	listen := fs.String("listen", "127.0.0.1:0", "local callback listener address")
	authURL := fs.String("auth-url", "", "override OAuth authorization endpoint (default: Google)")
	tokenURL := fs.String("token-url", "", "override OAuth token endpoint (default: Google)")
	help, err := parseSubcommandFlags(fs, args)
	if err != nil {
		return err
	}
	if help {
		return nil
	}
	if err := rejectExtraArgs(fs); err != nil {
		return err
	}
	if *cfgPath == "" {
		return fmt.Errorf("--config PATH is required")
	}
	if *clientID == "" || *clientSecret == "" {
		return fmt.Errorf("--client-id and --client-secret are required")
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	cs, store, err := openCreds(cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	fmt.Fprintln(stderr, "starting local callback listener; a Google sign-in URL will be printed")
	res, err := gsc.RunPKCEFlow(ctx, gsc.PKCEConfig{
		ClientID:     *clientID,
		ClientSecret: *clientSecret,
		Listen:       *listen,
		AuthURL:      *authURL,
		TokenURL:     *tokenURL,
		OnAuthURL: func(u string) {
			fmt.Fprintf(stderr, "\nOpen this URL in a browser, sign in with the Google account that owns the property, and grant read access:\n\n  %s\n\n", u)
		},
	})
	if err != nil {
		return err
	}

	keys := []struct {
		suffix string
		value  string
	}{
		{"client_id", *clientID},
		{"client_secret", *clientSecret},
		{"refresh_token", res.RefreshToken},
	}
	for _, k := range keys {
		if err := cs.Put(ctx, *name+"_"+k.suffix, []byte(k.value)); err != nil {
			return fmt.Errorf("store %s_%s: %w", *name, k.suffix, err)
		}
	}

	fmt.Fprintf(stderr, "stored %s_client_id, %s_client_secret, %s_refresh_token\n", *name, *name, *name)
	fmt.Fprintln(stdout, "")
	fmt.Fprintln(stdout, "Add this under your gsc connector config in ridgeline.yaml:")
	fmt.Fprintf(stdout, "  client_id_ref: %s_client_id\n", *name)
	fmt.Fprintf(stdout, "  client_secret_ref: %s_client_secret\n", *name)
	fmt.Fprintf(stdout, "  refresh_token_ref: %s_refresh_token\n", *name)
	return nil
}
