package protocol

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// DefaultMaxLineBytes is the default maximum size of one JSON line
// the Decoder will accept. Lines larger than this cause Read to
// return an error.
const DefaultMaxLineBytes = 1 << 20 // 1 MiB

// Decoder reads Outputs from an external process's stdout. Lines
// that exceed MaxLineBytes are rejected; this guards the orchestrator
// against runaway producers.
//
// Decoder is NOT safe for concurrent use.
type Decoder struct {
	scanner *bufio.Scanner
	// Strict causes Read to fail on unknown JSON fields. Default false:
	// the orchestrator accepts forward-compatible additions silently.
	Strict bool
}

// NewDecoder returns a Decoder that reads from r with the default
// max line size.
func NewDecoder(r io.Reader) *Decoder {
	return NewDecoderSize(r, DefaultMaxLineBytes)
}

// NewDecoderSize returns a Decoder that reads from r with maxLine as
// the per-line cap. bufio.Scanner uses max(cap(initial), maxLine) as
// the actual limit, so initialBuf is sized below maxLine.
func NewDecoderSize(r io.Reader, maxLine int) *Decoder {
	s := bufio.NewScanner(r)
	initial := 64 * 1024
	if initial > maxLine {
		initial = maxLine
	}
	s.Buffer(make([]byte, 0, initial), maxLine)
	return &Decoder{scanner: s}
}

// Read returns the next Output from the stream. At end of input it
// returns io.EOF. Blank lines are skipped silently to be forgiving
// of producers that print extra newlines.
func (d *Decoder) Read() (Output, error) {
	for d.scanner.Scan() {
		line := d.scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var out Output
		dec := json.NewDecoder(bytesReader(line))
		if d.Strict {
			dec.DisallowUnknownFields()
		}
		if err := dec.Decode(&out); err != nil {
			return Output{}, fmt.Errorf("protocol: decode line: %w", err)
		}
		if out.Type == "" {
			return Output{}, errors.New("protocol: message missing required field \"type\"")
		}
		return out, nil
	}
	if err := d.scanner.Err(); err != nil {
		// bufio.Scanner returns ErrTooLong when a line exceeds maxLine.
		return Output{}, fmt.Errorf("protocol: scan: %w", err)
	}
	return Output{}, io.EOF
}

// bytesReader is a tiny helper so we don't pull in bytes just for one type.
func bytesReader(b []byte) *bytesReadOnly { return &bytesReadOnly{b: b} }

type bytesReadOnly struct {
	b []byte
	i int
}

func (r *bytesReadOnly) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
