package main

import (
	"flag"
	"reflect"
	"testing"
)

func TestLiftFlags(t *testing.T) {
	makeFS := func() *flag.FlagSet {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.String("config", "", "")
		fs.String("since", "7d", "")
		fs.Bool("json", false, "")
		return fs
	}

	cases := []struct {
		name  string
		input []string
		want  []string
	}{
		{
			name:  "flags already before positionals",
			input: []string{"--config", "r.yaml", "--since", "7d", "metric.name"},
			want:  []string{"--config", "r.yaml", "--since", "7d", "metric.name"},
		},
		{
			name:  "positional before flags",
			input: []string{"metric.name", "--config", "r.yaml", "--since", "7d"},
			want:  []string{"--config", "r.yaml", "--since", "7d", "metric.name"},
		},
		{
			name:  "bool flag does not consume positional",
			input: []string{"metric.name", "--json", "--config", "r.yaml"},
			want:  []string{"--json", "--config", "r.yaml", "metric.name"},
		},
		{
			name:  "double-dash sentinel honoured",
			input: []string{"--config", "r.yaml", "--", "metric.name", "--since", "7d"},
			want:  []string{"--config", "r.yaml", "metric.name", "--since", "7d"},
		},
		{
			name:  "flag=value form handled",
			input: []string{"metric.name", "--config=r.yaml"},
			want:  []string{"--config=r.yaml", "metric.name"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := makeFS()
			got := liftFlags(fs, tc.input)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("liftFlags(%v) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}
