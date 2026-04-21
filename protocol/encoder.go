package protocol

import (
	"bufio"
	"encoding/json"
	"io"
)

// Encoder writes Commands to an external process's stdin as
// JSON-lines. It buffers writes; callers must call Flush when they
// need to guarantee the bytes have been handed to the underlying
// writer.
//
// Encoder is NOT safe for concurrent use.
type Encoder struct {
	bw  *bufio.Writer
	enc *json.Encoder
}

// NewEncoder returns an Encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	// json.Encoder always appends '\n', which is exactly what we want.
	enc.SetEscapeHTML(false)
	return &Encoder{bw: bw, enc: enc}
}

// Write encodes cmd as one JSON line and writes it to the underlying
// stream. It does not flush.
func (e *Encoder) Write(cmd Command) error {
	return e.enc.Encode(cmd)
}

// Flush flushes any buffered bytes to the underlying writer.
func (e *Encoder) Flush() error {
	return e.bw.Flush()
}
