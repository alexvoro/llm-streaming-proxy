package proxy

import (
	"bytes"
	"strings"
	"testing"
)

func TestReadSSEStream(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantChunks int
		wantDone   bool
	}{
		{
			name:       "basic chunks",
			input:      "data: {\"content\":\"hello\"}\n\ndata: {\"content\":\"world\"}\n\ndata: [DONE]\n\n",
			wantChunks: 2,
			wantDone:   true,
		},
		{
			name:       "ignores non-data lines",
			input:      "event: message\ndata: {\"content\":\"hello\"}\n\n: comment\ndata: [DONE]\n\n",
			wantChunks: 1,
			wantDone:   true,
		},
		{
			name:       "empty stream",
			input:      "",
			wantChunks: 0,
			wantDone:   false,
		},
		{
			name:       "stream without done",
			input:      "data: {\"content\":\"hello\"}\n\n",
			wantChunks: 1,
			wantDone:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			ch := ReadSSEStream(reader)

			var chunks []StreamChunk
			for chunk := range ch {
				chunks = append(chunks, chunk)
			}

			dataChunks := 0
			gotDone := false
			for _, c := range chunks {
				if c.Done {
					gotDone = true
				} else if c.Err == nil {
					dataChunks++
				}
			}

			if dataChunks != tt.wantChunks {
				t.Errorf("got %d data chunks, want %d", dataChunks, tt.wantChunks)
			}
			if gotDone != tt.wantDone {
				t.Errorf("got done=%v, want %v", gotDone, tt.wantDone)
			}
		})
	}
}

func TestMarshalChunkToSSE(t *testing.T) {
	chunk := NewChunk("test-model", "hello")
	data, err := MarshalChunkToSSE(chunk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !bytes.HasPrefix(data, []byte("data: ")) {
		t.Error("SSE data should start with 'data: '")
	}
	if !bytes.HasSuffix(data, []byte("\n\n")) {
		t.Error("SSE data should end with '\\n\\n'")
	}
	if !bytes.Contains(data, []byte(`"content":"hello"`)) {
		t.Error("SSE data should contain the content")
	}
}

func TestNewChunk(t *testing.T) {
	chunk := NewChunk("gpt-4", "token")
	if chunk.Model != "gpt-4" {
		t.Errorf("model = %q, want %q", chunk.Model, "gpt-4")
	}
	if chunk.Object != "chat.completion.chunk" {
		t.Errorf("object = %q, want %q", chunk.Object, "chat.completion.chunk")
	}
	if len(chunk.Choices) != 1 {
		t.Fatalf("choices length = %d, want 1", len(chunk.Choices))
	}
	if chunk.Choices[0].Delta.Content != "token" {
		t.Errorf("content = %q, want %q", chunk.Choices[0].Delta.Content, "token")
	}
}

func TestNewDoneChunk(t *testing.T) {
	chunk := NewDoneChunk("gpt-4")
	if len(chunk.Choices) != 1 {
		t.Fatalf("choices length = %d, want 1", len(chunk.Choices))
	}
	if chunk.Choices[0].FinishReason == nil {
		t.Fatal("finish_reason should not be nil")
	}
	if *chunk.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason = %q, want %q", *chunk.Choices[0].FinishReason, "stop")
	}
}
