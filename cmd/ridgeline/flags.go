package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
)

// parseSubcommandFlags parses args into fs and normalizes two cross-cutting
// behaviors every subcommand wants:
//
//   - `--help` / `-h`: the stdlib flag package prints usage to stdout (via
//     fs.SetOutput) and returns flag.ErrHelp. We convert that into
//     (true, nil) so the caller exits 0 with no trailing error line.
//   - any other flag.Parse error: propagated unchanged.
//
// stdout must be the destination for help output (typically os.Stdout or the
// injected writer from the caller). The function calls fs.SetOutput(stdout)
// before parsing so the Usage function and flag error messages go to the
// right place.
//
// The bool return is true when the help path fired; callers should return nil
// immediately so main.go exits 0.
func parseSubcommandFlags(fs *flag.FlagSet, stdout io.Writer, args []string) (helpRequested bool, err error) {
	fs.SetOutput(stdout)
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
// silently ignored. The returned error carries no subcommand prefix
// because main.go already wraps the result with one.
func rejectExtraArgs(fs *flag.FlagSet) error {
	if fs.NArg() == 0 {
		return nil
	}
	return fmt.Errorf("unexpected argument %q", fs.Arg(0))
}
