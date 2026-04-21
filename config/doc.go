// Package config loads and validates ridgeline.yaml, the declarative
// configuration file that describes which products a user tracks and
// which connectors and sinks each product uses.
//
// The schema intentionally starts small. It covers enough today to
// run a pipeline against the in-repo testsrc connector with a
// JSON-lines sink, driven entirely by config. The schema will grow
// to cover enrichers, transforms, schedules, and external connectors
// as those subsystems land.
//
// Example:
//
//	version: 1
//	state_path: ~/.ridgeline/ridgeline.db
//	key_path: ~/.ridgeline/key
//	products:
//	  myapp:
//	    connectors:
//	      - name: demo
//	        type: testsrc
//	        config:
//	          records: 5
//	        streams:
//	          - pages
//	          - events
//	        sink:
//	          type: jsonl
//	          options:
//	            dir: ./out
//
// Paths beginning with "~/" are expanded to the user's home directory
// on Load.
package config
