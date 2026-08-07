package main

import (
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/xydac/ridgeline/connectors"
)

func runSchema(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("schema", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.Usage = func() {
		fmt.Fprintln(out, "usage: ridgeline schema <connector>[.<stream>]")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Print semantic metadata for a connector's streams.")
		fmt.Fprintln(out, "When <stream> is omitted, all streams for the connector are shown.")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Examples:")
		fmt.Fprintln(out, "  ridgeline schema plausible")
		fmt.Fprintln(out, "  ridgeline schema plausible.timeseries")
		fmt.Fprintln(out, "  ridgeline schema github.views")
	}
	if err := fs.Parse(args); err != nil {
		return &usageError{msg: err.Error()}
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return &usageError{msg: "connector name required"}
	}

	arg := fs.Arg(0)
	connectorName, streamFilter, _ := strings.Cut(arg, ".")

	conn, ok := connectors.Get(connectorName)
	if !ok {
		return fmt.Errorf("no connector named %q (run 'ridgeline status' to see registered connectors)", connectorName)
	}

	spec := conn.Spec()
	printed := 0
	for _, ss := range spec.Streams {
		if streamFilter != "" && ss.Name != streamFilter {
			continue
		}
		printStreamSpec(out, connectorName, ss)
		printed++
	}
	if printed == 0 {
		return fmt.Errorf("connector %q has no stream named %q", connectorName, streamFilter)
	}
	return nil
}

func printStreamSpec(out io.Writer, connector string, ss connectors.StreamSpec) {
	fmt.Fprintf(out, "connector:  %s\n", connector)
	fmt.Fprintf(out, "stream:     %s\n", ss.Name)
	fmt.Fprintf(out, "kind:       %s\n", ss.Kind)
	if ss.Description != "" {
		fmt.Fprintf(out, "description: %s\n", ss.Description)
	}
	if len(ss.Schema.Columns) == 0 {
		fmt.Fprintln(out, "columns:    (no typed schema declared)")
		fmt.Fprintln(out)
		return
	}
	fmt.Fprintln(out, "columns:")
	maxName := 4
	for _, c := range ss.Schema.Columns {
		if len(c.Name) > maxName {
			maxName = len(c.Name)
		}
	}
	for _, c := range ss.Schema.Columns {
		flags := c.Type.String()
		if c.Key {
			flags += ", key"
		}
		if c.Semantics != nil {
			flags += ", " + c.Semantics.Direction.String()
			flags += ", " + c.Semantics.Aggregation.String()
			if c.Semantics.Unit != "" {
				flags += ", unit=" + c.Semantics.Unit
			}
		}
		fmt.Fprintf(out, "  %-*s  %s\n", maxName, c.Name, flags)
	}
	fmt.Fprintln(out)
}
