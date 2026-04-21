// Package config loads and validates ridgeline.yaml, the declarative
// configuration file that describes which products a user tracks and
// which connectors and sinks each product uses.
//
// The schema intentionally starts small. Cycle 3 ships enough to run
// a dry-run pipeline against the in-repo testsrc connector with a
// JSON-lines sink, driven entirely by config. Future cycles will
// extend the schema to cover enrichers, transforms, schedules, and
// external connectors.
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
