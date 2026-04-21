// Package protocol defines the JSON-lines wire protocol that lets
// external connectors, enrichers, and sinks be written in any language.
//
// Each message is a single line of JSON terminated by '\n'. Lines must
// be UTF-8 encoded. There is no length prefix and no framing other
// than the newline; readers are expected to use a buffered scanner
// with a generous max line size (the Decoder defaults to 1 MiB).
//
// Direction:
//
//	ridgeline -> external process (stdin):
//	  Command messages: spec, validate, discover, extract, enrich
//
//	external process -> ridgeline (stdout):
//	  Output messages:  SPEC, RECORD, STATE, LOG, SCHEMA, DONE, ERROR
//
//	external process -> ridgeline (stderr):
//	  Unstructured logs, surfaced verbatim in the orchestrator's UI.
//
// Type names use uppercase for output messages and lowercase for
// commands, matching the convention in the product spec. The full
// grammar lives in docs/protocol.md.
//
// Encoders and Decoders in this package are NOT safe for concurrent
// use by multiple goroutines. The orchestrator wraps each external
// process with one Encoder for stdin and one Decoder for stdout.
package protocol
