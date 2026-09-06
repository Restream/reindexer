package reindexer

import (
	"encoding/json"
	"testing"

	"github.com/restream/reindexer/v5/bindings"
)

func TestEmbedderConfigProtocolOptionsJSONRoundTrip(t *testing.T) {
	cfg := bindings.EmbedderConfig{
		URL:             "http://localhost/v1/embeddings",
		ProtocolOptions: &bindings.EmbeddingProtocolOptionsOpenAI{Model: "local", FieldsFormat: "join"},
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal embedder config: %v", err)
	}

	var decoded bindings.EmbedderConfig
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal embedder config: %v", err)
	}
	opts, ok := decoded.ProtocolOptions.(*bindings.EmbeddingProtocolOptionsOpenAI)
	if !ok {
		t.Fatalf("unexpected protocol options type %T", decoded.ProtocolOptions)
	}
	if opts.ProtocolType != bindings.EmbeddingProtocolOpenAI || opts.Model != "local" || opts.FieldsFormat != "join" {
		t.Fatalf("unexpected OpenAI options: %+v", opts)
	}
}

func TestEmbedderConfigProtocolOptionsJSONValidation(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{name: "missing type", json: `{"URL":"http://localhost","protocol":{"model":"local"}}`},
		{name: "empty protocol", json: `{"URL":"http://localhost","protocol":{}}`},
		{name: "unknown type", json: `{"URL":"http://localhost","protocol":{"type":"grpc"}}`},
		{name: "OpenAI without model", json: `{"URL":"http://localhost","protocol":{"type":"openai"}}`},
		{name: "RX with model", json: `{"URL":"http://localhost","protocol":{"type":"rx","model":"local"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var cfg bindings.EmbedderConfig
			if err := json.Unmarshal([]byte(test.json), &cfg); err == nil {
				t.Fatal("expected unmarshal error")
			}
		})
	}
}

func TestEmbedderConfigRejectsInvalidProtocolOptionsOnMarshal(t *testing.T) {
	tests := []bindings.IEmbeddingProtocolOptions{
		&bindings.EmbeddingProtocolOptionsOpenAI{},
		&bindings.EmbeddingProtocolOptionsOpenAI{ProtocolType: bindings.EmbeddingProtocolRX, Model: "local"},
		&bindings.EmbeddingProtocolOptionsRX{ProtocolType: bindings.EmbeddingProtocolOpenAI},
	}
	for _, opts := range tests {
		cfg := bindings.EmbedderConfig{URL: "http://localhost", ProtocolOptions: opts}
		if _, err := json.Marshal(cfg); err == nil {
			t.Fatalf("expected marshal error for %#v", opts)
		}
	}
}

func TestEmbedderConfigWithoutProtocolDefaultsToRX(t *testing.T) {
	var cfg bindings.EmbedderConfig
	if err := json.Unmarshal([]byte(`{"URL":"http://localhost"}`), &cfg); err != nil {
		t.Fatalf("unmarshal embedder config: %v", err)
	}
	if cfg.ProtocolOptions != nil {
		t.Fatalf("unexpected protocol options: %#v", cfg.ProtocolOptions)
	}
}
