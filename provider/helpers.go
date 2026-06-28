package provider

import (
	"bytes"
	"encoding/json"
	"io"
)

// newJSONReaderWithStream takes a raw request body and ensures the
// "stream" field is set to true before forwarding upstream.
func newJSONReaderWithStream(body []byte) io.Reader {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		// If we can't parse, just pass through.
		return bytes.NewReader(body)
	}
	m["stream"] = true
	out, err := json.Marshal(m)
	if err != nil {
		return bytes.NewReader(body)
	}
	return bytes.NewReader(out)
}
