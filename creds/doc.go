// Package creds implements an at-rest encrypted credential store
// backed by the state/sqlite database.
//
// Each credential is sealed with AES-256-GCM using a 32-byte key the
// caller supplies. A fresh random nonce is generated on every Put and
// stored alongside the ciphertext, so re-encrypting the same plaintext
// never produces the same blob twice. Tampered ciphertext or nonce
// both yield a decryption error with no partial plaintext.
//
// The key lifecycle is the caller's problem. Typical deployments
// derive the key from a password via scrypt or argon2, or read it from
// an OS keyring. The helpers KeyFromFile and WriteKeyFile provide a
// convenience for file-based deployment while we wait for the TUI to
// own interactive unlock.
//
// Usage sketch:
//
//	st, _ := sqlite.Open("~/.ridgeline/ridgeline.db")
//	defer st.Close()
//	key, _ := creds.KeyFromFile("~/.ridgeline/key")
//	cs, _ := creds.New(st.DB(), key)
//	_ = cs.Put(ctx, "gsc_oauth_token", []byte("ya29...."))
//	tok, _ := cs.Get(ctx, "gsc_oauth_token")
package creds
