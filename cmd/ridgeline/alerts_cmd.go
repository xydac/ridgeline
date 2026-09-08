package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/xydac/ridgeline/memory"
)

// runAlerts handles `ridgeline alerts <subcommand>`.
func runAlerts(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		printAlertsUsage(stdout)
		return nil
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "config":
		return runAlertsConfig(ctx, rest, stdout)
	case "run":
		return runAlertsRun(ctx, rest, stdout)
	case "list":
		return runAlertsList(ctx, rest, stdout)
	case "test":
		return runAlertsTest(ctx, rest, stdout)
	case "rm":
		return runAlertsRm(ctx, rest, stdout)
	default:
		printAlertsUsage(stdout)
		return usageErrorf("alerts: unknown subcommand %q", sub)
	}
}

// runAlertsConfig handles `ridgeline alerts config <kind> <target> --config PATH --name NAME`.
func runAlertsConfig(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("alerts config", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	name := fs.String("name", "", "channel name (default: kind)")
	fs.Usage = func() {
		fmt.Fprintln(stdout, "Usage: ridgeline alerts config <kind> <target> --config PATH [--name NAME]")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "Register a delivery channel. kind is one of: webhook, stderr, file")
		fmt.Fprintln(stdout, "  webhook   target is a URL that receives POST application/json")
		fmt.Fprintln(stdout, "  stderr    target is ignored; events are written to stderr as JSON lines")
		fmt.Fprintln(stdout, "  file      target is a file path; events are appended as JSON lines")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "Examples:")
		fmt.Fprintln(stdout, "  ridgeline alerts config webhook https://hooks.example.com/alerts --config ridgeline.yaml")
		fmt.Fprintln(stdout, "  ridgeline alerts config file /var/log/ridgeline-alerts.jsonl --config ridgeline.yaml")
		fmt.Fprintln(stdout, "  ridgeline alerts config stderr '' --config ridgeline.yaml --name my-stderr")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return usageErrorf("alerts config: channel kind required")
	}
	kind := fs.Arg(0)
	target := ""
	if fs.NArg() >= 2 {
		target = fs.Arg(1)
	}
	if *cfgPath == "" {
		return usageErrorf("alerts config: --config is required")
	}
	chName := *name
	if chName == "" {
		chName = kind
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	id, err := cat.AddAlertChannel(ctx, chName, kind, target)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "registered channel %q (id=%d, kind=%s)\n", chName, id, kind)
	return nil
}

// runAlertsRun handles `ridgeline alerts run --config PATH [--since DUR]`.
// It evaluates bm_watches AND surfaces recent anomaly events, then delivers
// all new events to every registered channel exactly once (idempotent).
func runAlertsRun(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("alerts run", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	sinceStr := fs.String("since", "60m", "look-back window for events (e.g. 30m, 2h, 24h)")
	fs.Usage = func() {
		fmt.Fprintln(stdout, "Usage: ridgeline alerts run --config PATH [--since DURATION]")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "Evaluate all watch rules, then deliver any new events (from the look-back")
		fmt.Fprintln(stdout, "window) to every registered channel exactly once. Idempotent: running")
		fmt.Fprintln(stdout, "twice delivers nothing the second time.")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "Suitable for cron: */5 * * * * ridgeline alerts run --config ridgeline.yaml")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("alerts run: --config is required")
	}
	since, err := parseSinceDuration(*sinceStr)
	if err != nil {
		return usageErrorf("alerts run: invalid --since %q: %v", *sinceStr, err)
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	// First, evaluate all watch rules so their events land in bm_events.
	_, _ = cat.RunWatches(ctx)

	channels, err := cat.ListAlertChannels(ctx)
	if err != nil {
		return fmt.Errorf("alerts run: %w", err)
	}
	if len(channels) == 0 {
		fmt.Fprintln(stdout, "no channels registered; use 'ridgeline alerts config' to add one")
		return nil
	}

	totalDelivered := 0
	totalFailed := 0

	for _, ch := range channels {
		events, err := cat.UndeliveredEvents(ctx, since, ch.ID)
		if err != nil {
			fmt.Fprintf(stdout, "channel %q: fetch events: %v\n", ch.Name, err)
			continue
		}
		for _, ev := range events {
			if deliverErr := memory.DeliverToChannel(ch, ev); deliverErr != nil {
				fmt.Fprintf(stdout, "channel %q: deliver event %d: %v\n", ch.Name, ev.ID, deliverErr)
				totalFailed++
				// Do not record as delivered; will retry on next run.
				continue
			}
			if recErr := cat.RecordDelivery(ctx, ev.ID, ch.ID); recErr != nil {
				fmt.Fprintf(stdout, "channel %q: record delivery %d: %v\n", ch.Name, ev.ID, recErr)
			}
			totalDelivered++
		}
	}

	fmt.Fprintf(stdout, "alerts run: delivered %d, failed %d\n", totalDelivered, totalFailed)
	return nil
}

// runAlertsList handles `ridgeline alerts list --config PATH`.
func runAlertsList(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("alerts list", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		fmt.Fprintln(stdout, "Usage: ridgeline alerts list --config PATH")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "List registered alert channels and their last delivery timestamp.")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if *cfgPath == "" {
		return usageErrorf("alerts list: --config is required")
	}

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	channels, err := cat.ListAlertChannels(ctx)
	if err != nil {
		return fmt.Errorf("alerts list: %w", err)
	}
	if len(channels) == 0 {
		fmt.Fprintln(stdout, "no channels registered")
		return nil
	}

	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tKIND\tTARGET\tLAST DELIVERED")
	for _, ch := range channels {
		last := "never"
		if ch.LastDeliveredAt != nil {
			last = ch.LastDeliveredAt.UTC().Format(time.RFC3339)
		}
		target := ch.Target
		if target == "" {
			target = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", ch.Name, ch.Kind, target, last)
	}
	return tw.Flush()
}

// runAlertsTest handles `ridgeline alerts test <channel-name> --config PATH`.
func runAlertsTest(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("alerts test", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		fmt.Fprintln(stdout, "Usage: ridgeline alerts test <channel-name> --config PATH")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "Fire a synthetic test event to the named channel. Delivery is NOT recorded")
		fmt.Fprintln(stdout, "in bm_alert_deliveries, so it does not affect idempotency.")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return usageErrorf("alerts test: channel name required")
	}
	if *cfgPath == "" {
		return usageErrorf("alerts test: --config is required")
	}
	chName := fs.Arg(0)

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	channels, err := cat.ListAlertChannels(ctx)
	if err != nil {
		return fmt.Errorf("alerts test: %w", err)
	}
	var target *memory.AlertChannel
	for i := range channels {
		if channels[i].Name == chName {
			target = &channels[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("alerts test: no channel named %q (use 'ridgeline alerts list' to see channels)", chName)
	}

	ev := memory.SyntheticEvent()
	if err := memory.DeliverToChannel(*target, ev); err != nil {
		return fmt.Errorf("alerts test: delivery failed: %w", err)
	}
	fmt.Fprintf(stdout, "test event delivered to channel %q (%s)\n", chName, target.Kind)
	return nil
}

// runAlertsRm handles `ridgeline alerts rm <channel-name> --config PATH`.
func runAlertsRm(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("alerts rm", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to ridgeline.yaml")
	fs.Usage = func() {
		fmt.Fprintln(stdout, "Usage: ridgeline alerts rm <channel-name> --config PATH")
		fmt.Fprintln(stdout, "")
		fmt.Fprintln(stdout, "Remove a registered alert channel by name.")
		fs.PrintDefaults()
	}
	help, err := parseSubcommandFlags(fs, stdout, liftFlags(fs, args))
	if help || err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return usageErrorf("alerts rm: channel name required")
	}
	if *cfgPath == "" {
		return usageErrorf("alerts rm: --config is required")
	}
	chName := fs.Arg(0)

	cat, store, err := openCatalogFromConfig(*cfgPath)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := cat.DeleteAlertChannel(ctx, chName); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "removed channel %q\n", chName)
	return nil
}

func printAlertsUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: ridgeline alerts <subcommand> [flags]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Subcommands:")
	fmt.Fprintln(w, "  config <kind> <target>  register a delivery channel (webhook, stderr, file)")
	fmt.Fprintln(w, "  run                     evaluate watches and deliver new events to all channels")
	fmt.Fprintln(w, "  list                    list registered channels and last delivery time")
	fmt.Fprintln(w, "  test <channel>          fire a synthetic test event to a channel")
	fmt.Fprintln(w, "  rm <channel>            remove a channel")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "All subcommands require --config PATH.")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Cron example (every 5 minutes):")
	fmt.Fprintln(w, "  */5 * * * * ridgeline alerts run --config /etc/ridgeline/ridgeline.yaml")
}

var (
	_ = time.Duration(0) // keep time import live on this file
	_ = os.Stderr
)
