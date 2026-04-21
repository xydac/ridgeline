// Package external is the runner for connectors written as separate
// executables in any language. The runner spawns a child process and
// drives it via the JSON-lines protocol defined in package protocol.
//
// On every Extract call the runner:
//
//  1. Spawns the configured command with its args, env, and working dir.
//  2. Writes a single "extract" Command on the child's stdin (followed by
//     a close), naming the requested streams and passing the connector
//     config and the persisted incremental state.
//  3. Reads JSON-lines Outputs from the child's stdout and translates
//     them into connectors.Message values: RECORD, STATE, LOG, and
//     SCHEMA pass through; ERROR fails the run; DONE and stdout EOF end
//     the stream cleanly.
//  4. Forwards anything the child writes to stderr as warn-level log
//     messages tagged with the connector name.
//  5. Waits for the child to exit. A non-zero exit code that is not
//     preceded by an explicit ERROR is reported via the log channel.
//
// Cancelling the context handed to Extract kills the child process and
// drains its pipes, so a stuck connector cannot block the orchestrator.
package external
