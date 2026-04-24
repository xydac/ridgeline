package main

import (
	"errors"
	"flag"
	"fmt"
)

// parseSubcommandFlags parses args into fs and normalizes two cross-cutting
// behaviors every subcommand wants:
//
//   - `--help` / `-h`: the stdlib flag package prints usage and returns
//     flag.ErrHelp. We convert that into (true, nil) so the caller can
//     return nil and the user sees usage plus a zero exit, not the
//     "sync: flag: help requested" footgun that plagued the first few
//     releases.
//   - any other flag.Parse error: propagated unchanged.
//
// The bool return is true when the help path fired, so callers that need
// to skip their normal work can branch on it.
func parseSubcommandFlags(fs *flag.FlagSet, args []string) (helpRequested bool, err error) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// rejectExtraArgs returns a descriptive error if fs has any positional
// arguments left after Parse. Most subcommands use flags only; this keeps
// typos like `ridgeline status --config x.yaml extra garbage` from being
// silently ignored.
func rejectExtraArgs(subcommand string, fs *flag.FlagSet) error {
	if fs.NArg() == 0 {
		return nil
	}
	return fmt.Errorf("%s: unexpected argument %q", subcommand, fs.Arg(0))
}
