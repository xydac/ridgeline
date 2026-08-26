package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

// usageError is returned by subcommand handlers when the caller invoked
// the CLI incorrectly (missing required verb, unknown option). main.go
// exits 2 for this error class - the conventional usage-error sentinel.
// Regular runtime failures (IO errors, missing config) exit 1.
type usageError struct {
	msg string
}

func (e *usageError) Error() string { return e.msg }

func usageErrorf(format string, a ...any) error {
	return &usageError{msg: fmt.Sprintf(format, a...)}
}

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
		return false, usageErrorf("%s", err)
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

// liftFlags reorders args so that all flag tokens come before positional tokens.
// This lets subcommands accept positional arguments in any position relative to
// flags (e.g. `ridgeline explain metric.name --since 7d` works as well as
// `ridgeline explain --since 7d metric.name`), since flag.FlagSet stops parsing
// at the first non-flag token.
//
// fs must already have all its flags registered before this is called.
// The double-dash "--" sentinel is honoured: everything after it is treated as
// a positional and placed at the end of the reordered slice.
func liftFlags(fs *flag.FlagSet, args []string) []string {
	var flagTokens, positionals []string
	i := 0
	for i < len(args) {
		tok := args[i]
		if tok == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(tok, "-") {
			positionals = append(positionals, tok)
			i++
			continue
		}
		flagTokens = append(flagTokens, tok)
		// If the flag is in --name=value form it already carries its value.
		if strings.Contains(tok, "=") {
			i++
			continue
		}
		// Look up the flag to see whether it takes a value argument.
		// Bool flags do not consume a following token; all others do.
		name := strings.TrimLeft(tok, "-")
		f := fs.Lookup(name)
		isBool := false
		if f != nil {
			if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok {
				isBool = bf.IsBoolFlag()
			}
		}
		if !isBool && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			// Next token is this flag's value.
			flagTokens = append(flagTokens, args[i+1])
			i += 2
			continue
		}
		i++
	}
	return append(flagTokens, positionals...)
}

// prescanStringFlag extracts every occurrence of --name VALUE or --name=VALUE
// from args, wherever they appear (including after positional tokens), and
// returns the last value found together with a copy of args with those tokens
// removed. This lets callers honor a flag regardless of its position relative
// to positional arguments, working around flag.FlagSet's stop-at-first-
// non-flag behavior.
//
// If the flag does not appear, found is false and rest == args (no copy made).
func prescanStringFlag(name string, args []string) (val string, rest []string, found bool) {
	prefix := "--" + name + "="
	flag2 := "--" + name
	out := args[:0:0] // share backing array only if we mutate
	i := 0
	for i < len(args) {
		a := args[i]
		if a == flag2 && i+1 < len(args) {
			val = args[i+1]
			found = true
			i += 2
			continue
		}
		if strings.HasPrefix(a, prefix) {
			val = strings.TrimPrefix(a, prefix)
			found = true
			i++
			continue
		}
		out = append(out, a)
		i++
	}
	if !found {
		return "", args, false
	}
	return val, out, true
}
