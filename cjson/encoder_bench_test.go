package cjson

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type benchEncoderNestedDoc struct {
	Code   int    `json:"code"`
	Label  string `json:"label"`
	Active bool   `json:"active"`
}

type benchEncoderTypicalDoc struct {
	ID        int                   `json:"id"`
	UUID      string                `json:"uuid" reindex:"uuid,,uuid"`
	Name      string                `json:"name"`
	Category  string                `json:"category"`
	Count     int64                 `json:"count"`
	Score     float64               `json:"score"`
	Enabled   bool                  `json:"enabled"`
	Tags      []string              `json:"tags"`
	Values    []int                 `json:"values"`
	Vector    []float32             `json:"vector"`
	Nested    benchEncoderNestedDoc `json:"nested"`
	Optional  string                `json:"optional,omitempty"`
	SkipEmpty []int                 `json:"skip_empty,omitempty"`
}

type benchEncoderStringDoc struct {
	ID     int      `json:"id"`
	Title  string   `json:"title"`
	Body   string   `json:"body"`
	Tags   []string `json:"tags"`
	Status string   `json:"status"`
}

type benchEncoderSliceDoc struct {
	ID     int       `json:"id"`
	Values []int     `json:"values"`
	Vector []float32 `json:"vector"`
}

func newBenchEncoderTypicalDoc() benchEncoderTypicalDoc {
	vector := make([]float32, 64)
	for i := range vector {
		vector[i] = float32(i) / 10
	}
	return benchEncoderTypicalDoc{
		ID:       42,
		UUID:     "550e8400-e29b-41d4-a716-446655440000",
		Name:     "cjson encoder benchmark document",
		Category: "perf",
		Count:    123456789,
		Score:    42.75,
		Enabled:  true,
		Tags:     []string{"go", "cjson", "encoder", "hot-path"},
		Values:   []int{1, 8, 64, 512, 4096, 8192, -42, -8192},
		Vector:   vector,
		Nested: benchEncoderNestedDoc{
			Code:   7,
			Label:  "nested payload",
			Active: true,
		},
	}
}

func benchmarkCJSONEncodeRaw(b *testing.B, doc any) {
	state := NewState()
	enc := state.NewEncoder()
	ser := NewPoolSerializer()
	if err := enc.EncodeRaw(doc, ser); err != nil {
		b.Fatalf("warm cjson encoder: %v", err)
	}
	ser.Close()

	b.ReportAllocs()
	for b.Loop() {
		ser := NewPoolSerializer()
		if err := enc.EncodeRaw(doc, ser); err != nil {
			b.Fatalf("encode cjson: %v", err)
		}
		benchBytesSink = ser.Bytes()
		ser.Close()
	}
}

func BenchmarkCJSONEncoderTypicalDocument(b *testing.B) {
	benchmarkCJSONEncodeRaw(b, newBenchEncoderTypicalDoc())
}

func BenchmarkGobEncoderTypicalDocument(b *testing.B) {
	doc := newBenchEncoderTypicalDoc()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(doc); err != nil {
		b.Fatalf("warm gob encoder: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		if err := enc.Encode(doc); err != nil {
			b.Fatalf("encode gob: %v", err)
		}
		benchBytesSink = buf.Bytes()
	}
}

func BenchmarkCJSONEncoderFocused(b *testing.B) {
	b.Run("nested", func(b *testing.B) {
		benchmarkCJSONEncodeRaw(b, benchEncoderNestedDoc{Code: 42, Label: "nested", Active: true})
	})
	b.Run("uuid", func(b *testing.B) {
		benchmarkCJSONEncodeRaw(b, struct {
			UUID string `json:"uuid" reindex:"uuid,,uuid"`
		}{UUID: "550e8400-e29b-41d4-a716-446655440000"})
	})
	b.Run("string-heavy", func(b *testing.B) {
		benchmarkCJSONEncodeRaw(b, benchEncoderStringDoc{
			ID:     1,
			Title:  "The Art of Query Planning",
			Body:   "abcdefghijklmnopqrstuvwxyz-0123456789-cjson-encoder",
			Tags:   []string{"alpha", "beta", "gamma", "delta", "epsilon"},
			Status: "published",
		})
	})
	b.Run("slice-heavy", func(b *testing.B) {
		vector := make([]float32, 256)
		for i := range vector {
			vector[i] = float32(i) / 100
		}
		benchmarkCJSONEncodeRaw(b, benchEncoderSliceDoc{
			ID:     1,
			Values: []int{1, 8, 64, 512, 4096, 8192, -42, -8192, 16384, -16384},
			Vector: vector,
		})
	})
}

func BenchmarkCJSONEncoderArgumentShape(b *testing.B) {
	state := NewState()
	enc := state.NewEncoder()
	doc := benchSliceDocF32{ID: 1, Values: make([]float32, 256)}
	for i := range doc.Values {
		doc.Values[i] = float32(i) / 10
	}
	preboxed := any(doc)

	ser := NewPoolSerializer()
	if err := enc.EncodeRaw(preboxed, ser); err != nil {
		b.Fatalf("warm cjson encoder: %v", err)
	}
	ser.Close()

	b.Run("value-direct", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ser := NewPoolSerializer()
			if err := enc.EncodeRaw(doc, ser); err != nil {
				b.Fatalf("encode cjson value: %v", err)
			}
			benchBytesSink = ser.Bytes()
			ser.Close()
		}
	})

	b.Run("value-preboxed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ser := NewPoolSerializer()
			if err := enc.EncodeRaw(preboxed, ser); err != nil {
				b.Fatalf("encode cjson preboxed value: %v", err)
			}
			benchBytesSink = ser.Bytes()
			ser.Close()
		}
	})

	b.Run("pointer", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ser := NewPoolSerializer()
			if err := enc.EncodeRaw(&doc, ser); err != nil {
				b.Fatalf("encode cjson pointer: %v", err)
			}
			benchBytesSink = ser.Bytes()
			ser.Close()
		}
	})
}

func BenchmarkParseUuid(b *testing.B) {
	for _, uuid := range []string{
		"550e8400-e29b-41d4-a716-446655440000",
		"550e8400e29b41d4a716446655440000",
		"00000000-0000-0000-0000-000000000000",
	} {
		b.Run(uuid, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				parsed, err := ParseUuid(uuid)
				if err != nil {
					b.Fatal(err)
				}
				benchUint64Sink ^= parsed[0] ^ parsed[1]
			}
		})
	}
}

func TestParseUuidMatchesSlowPath(t *testing.T) {
	for _, uuid := range []string{
		"",
		"550e8400-e29b-41d4-a716-446655440000",
		"550e8400e29b41d4a716446655440000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
		"00000000-0000-0000-0000-000000000000",
		"550e8400-e29b-41d4-2716-446655440000",
		"550e8400-e29b-41d4-a716-44665544000x",
		"550e8400-e29b41d4-a716-446655440000",
		"550e8400-e29b-41d4-a716-44665544000",
	} {
		t.Run(uuid, func(t *testing.T) {
			got, gotErr := ParseUuid(uuid)
			want, wantErr := parseUuidSlow(uuid)
			assert.Equal(t, want, got)
			if wantErr == nil {
				assert.NoError(t, gotErr)
			} else if assert.Error(t, gotErr) {
				assert.Equal(t, wantErr.Error(), gotErr.Error())
			}
		})
	}
}

func TestCJSONEncoderTypicalDocumentStableWire(t *testing.T) {
	state := NewState()
	enc := state.NewEncoder()
	doc := newBenchEncoderTypicalDoc()

	ser1 := NewPoolSerializer()
	if err := enc.EncodeRaw(doc, ser1); err != nil {
		t.Fatalf("first encode: %v", err)
	}
	wire1 := append([]byte(nil), ser1.Bytes()...)
	ser1.Close()

	ser2 := NewPoolSerializer()
	if err := enc.EncodeRaw(doc, ser2); err != nil {
		t.Fatalf("second encode: %v", err)
	}
	wire2 := append([]byte(nil), ser2.Bytes()...)
	ser2.Close()

	if !bytes.Equal(wire1, wire2) {
		t.Fatalf("EncodeRaw produced unstable wire bytes: len1=%d len2=%d", len(wire1), len(wire2))
	}

	dec := state.NewDecoder(benchEncoderTypicalDoc{}, nil)
	defer dec.Finalize()
	var decoded benchEncoderTypicalDoc
	if err := dec.Decode(wire2, &decoded); err != nil {
		t.Fatalf("decode encoded document: %v", err)
	}
	if !reflect.DeepEqual(doc, decoded) {
		t.Fatalf("EncodeRaw/Decode mismatch:\nwant: %#v\n got: %#v", doc, decoded)
	}
}
