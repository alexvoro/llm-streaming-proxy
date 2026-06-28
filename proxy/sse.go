package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ChatCompletionChunk matches the OpenAI streaming response format.
type ChatCompletionChunk struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
}

type Choice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// ChatCompletionRequest matches the OpenAI request format.
type ChatCompletionRequest struct {
	Model    string    `json:"model,omitempty"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// NewChunk creates a ChatCompletionChunk with the given content token.
func NewChunk(model, content string) ChatCompletionChunk {
	return ChatCompletionChunk{
		ID:      fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano()),
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []Choice{
			{
				Index: 0,
				Delta: Delta{Content: content},
			},
		},
	}
}

// NewDoneChunk creates a final chunk with finish_reason "stop".
func NewDoneChunk(model string) ChatCompletionChunk {
	stop := "stop"
	return ChatCompletionChunk{
		ID:      fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano()),
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []Choice{
			{
				Index:        0,
				Delta:        Delta{},
				FinishReason: &stop,
			},
		},
	}
}

// MarshalChunkToSSE serializes a chunk to an SSE data line.
func MarshalChunkToSSE(chunk ChatCompletionChunk) ([]byte, error) {
	data, err := json.Marshal(chunk)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteString("data: ")
	buf.Write(data)
	buf.WriteString("\n\n")
	return buf.Bytes(), nil
}

// SSEWriter writes SSE events to an http.ResponseWriter with proper
// headers and flushing.
type SSEWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

// NewSSEWriter initializes an SSE response. Returns an error if the
// ResponseWriter does not support flushing.
func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("response writer does not support flushing")
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	return &SSEWriter{w: w, flusher: flusher}, nil
}

// WriteEvent writes a raw SSE data line and flushes.
func (sw *SSEWriter) WriteEvent(data []byte) error {
	if _, err := sw.w.Write(data); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

// WriteDone writes the [DONE] sentinel and flushes.
func (sw *SSEWriter) WriteDone() error {
	if _, err := sw.w.Write([]byte("data: [DONE]\n\n")); err != nil {
		return err
	}
	sw.flusher.Flush()
	return nil
}

// ReadSSEStream reads an SSE stream from an io.Reader and sends parsed
// data payloads on the returned channel. The channel is closed when the
// stream ends or an error occurs. Respects context cancellation.
func ReadSSEStream(r io.Reader) <-chan StreamChunk {
	ch := make(chan StreamChunk, 16)

	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()

			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			payload := strings.TrimPrefix(line, "data: ")

			if payload == "[DONE]" {
				ch <- StreamChunk{Done: true}
				return
			}

			ch <- StreamChunk{Data: []byte(payload)}
		}
		if err := scanner.Err(); err != nil {
			ch <- StreamChunk{Err: err}
		}
	}()

	return ch
}
