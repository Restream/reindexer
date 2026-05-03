package cjson

import "testing"

type benchSliceDocF32 struct {
	ID     int       `json:"id"`
	Values []float32 `json:"values"`
}

type benchSliceDocF64 struct {
	ID     int       `json:"id"`
	Values []float64 `json:"values"`
}

func BenchmarkEncoderEncodeSliceFloat32(b *testing.B) {
	state := NewState()
	enc := state.NewEncoder()
	doc := benchSliceDocF32{ID: 1, Values: make([]float32, 1536)}
	for i := range doc.Values {
		doc.Values[i] = float32(i) / 10
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(doc.Values) * 4))

	for b.Loop() {
		ser := NewPoolSerializer()
		if err := enc.EncodeRaw(doc, ser); err != nil {
			b.Fatalf("encode float32 slice: %v", err)
		}
		benchBytesSink = ser.Bytes()
		ser.Close()
	}
}

func BenchmarkEncoderEncodeSliceFloat64(b *testing.B) {
	state := NewState()
	enc := state.NewEncoder()
	doc := benchSliceDocF64{ID: 1, Values: make([]float64, 1536)}
	for i := range doc.Values {
		doc.Values[i] = float64(i) / 10
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(doc.Values) * 8))

	for b.Loop() {
		ser := NewPoolSerializer()
		if err := enc.EncodeRaw(doc, ser); err != nil {
			b.Fatalf("encode float64 slice: %v", err)
		}
		benchBytesSink = ser.Bytes()
		ser.Close()
	}
}

func BenchmarkDecoderDecodeSliceFloat32(b *testing.B) {
	state := NewState()
	enc := state.NewEncoder()
	src := benchSliceDocF32{ID: 1, Values: make([]float32, 1536)}
	for i := range src.Values {
		src.Values[i] = float32(i) / 10
	}

	wireSer := NewPoolSerializer()
	if err := enc.EncodeRaw(src, wireSer); err != nil {
		b.Fatalf("prepare float32 payload: %v", err)
	}
	wire := append([]byte(nil), wireSer.Bytes()...)
	wireSer.Close()

	dec := state.NewDecoder(&benchSliceDocF32{}, nil)
	var dst benchSliceDocF32

	b.ReportAllocs()
	b.SetBytes(int64(len(src.Values) * 4))

	for b.Loop() {
		dst = benchSliceDocF32{}
		if err := dec.Decode(wire, &dst); err != nil {
			b.Fatalf("decode float32 slice: %v", err)
		}
	}
	benchUint64Sink = uint64(len(dst.Values))
}

func TestDecoderDecodeSliceFloat32Reuse(t *testing.T) {
	state := NewState()
	enc := state.NewEncoder()
	src := benchSliceDocF32{ID: 1, Values: []float32{1.25, 2.5, 3.75}}

	wireSer := NewPoolSerializer()
	if err := enc.EncodeRaw(src, wireSer); err != nil {
		t.Fatalf("prepare float32 payload: %v", err)
	}
	wire := append([]byte(nil), wireSer.Bytes()...)
	wireSer.Close()

	dec := state.NewDecoder(&benchSliceDocF32{}, nil)
	dst := benchSliceDocF32{Values: make([]float32, 0, len(src.Values))}
	initialCap := cap(dst.Values)
	if err := dec.Decode(wire, &dst); err != nil {
		t.Fatalf("decode float32 slice: %v", err)
	}
	if cap(dst.Values) != initialCap {
		t.Fatalf("expected slice capacity reuse: got %d, want %d", cap(dst.Values), initialCap)
	}
	if dst.ID != src.ID || len(dst.Values) != len(src.Values) {
		t.Fatalf("unexpected decode result: %+v", dst)
	}
	for i := range src.Values {
		if dst.Values[i] != src.Values[i] {
			t.Fatalf("unexpected value at %d: got %v, want %v", i, dst.Values[i], src.Values[i])
		}
	}
}

func BenchmarkDecoderDecodeSliceFloat32Reuse(b *testing.B) {
	state := NewState()
	enc := state.NewEncoder()
	src := benchSliceDocF32{ID: 1, Values: make([]float32, 1536)}
	for i := range src.Values {
		src.Values[i] = float32(i) / 10
	}

	wireSer := NewPoolSerializer()
	if err := enc.EncodeRaw(src, wireSer); err != nil {
		b.Fatalf("prepare float32 payload: %v", err)
	}
	wire := append([]byte(nil), wireSer.Bytes()...)
	wireSer.Close()

	dec := state.NewDecoder(&benchSliceDocF32{}, nil)
	dst := benchSliceDocF32{Values: make([]float32, 0, len(src.Values))}

	b.ReportAllocs()
	b.SetBytes(int64(len(src.Values) * 4))

	for b.Loop() {
		dst.ID = 0
		dst.Values = dst.Values[:0]
		if err := dec.Decode(wire, &dst); err != nil {
			b.Fatalf("decode float32 slice: %v", err)
		}
	}
	benchUint64Sink = uint64(len(dst.Values))
}

func BenchmarkDecoderDecodeSliceFloat64(b *testing.B) {
	state := NewState()
	enc := state.NewEncoder()
	src := benchSliceDocF64{ID: 1, Values: make([]float64, 1536)}
	for i := range src.Values {
		src.Values[i] = float64(i) / 10
	}

	wireSer := NewPoolSerializer()
	if err := enc.EncodeRaw(src, wireSer); err != nil {
		b.Fatalf("prepare float64 payload: %v", err)
	}
	wire := append([]byte(nil), wireSer.Bytes()...)
	wireSer.Close()

	dec := state.NewDecoder(&benchSliceDocF64{}, nil)
	var dst benchSliceDocF64

	b.ReportAllocs()
	b.SetBytes(int64(len(src.Values) * 8))

	for b.Loop() {
		dst = benchSliceDocF64{}
		if err := dec.Decode(wire, &dst); err != nil {
			b.Fatalf("decode float64 slice: %v", err)
		}
	}
	benchUint64Sink = uint64(len(dst.Values))
}

func BenchmarkDecoderDecodeSliceFloat64Reuse(b *testing.B) {
	state := NewState()
	enc := state.NewEncoder()
	src := benchSliceDocF64{ID: 1, Values: make([]float64, 1536)}
	for i := range src.Values {
		src.Values[i] = float64(i) / 10
	}

	wireSer := NewPoolSerializer()
	if err := enc.EncodeRaw(src, wireSer); err != nil {
		b.Fatalf("prepare float64 payload: %v", err)
	}
	wire := append([]byte(nil), wireSer.Bytes()...)
	wireSer.Close()

	dec := state.NewDecoder(&benchSliceDocF64{}, nil)
	dst := benchSliceDocF64{Values: make([]float64, 0, len(src.Values))}

	b.ReportAllocs()
	b.SetBytes(int64(len(src.Values) * 8))

	for b.Loop() {
		dst.ID = 0
		dst.Values = dst.Values[:0]
		if err := dec.Decode(wire, &dst); err != nil {
			b.Fatalf("decode float64 slice: %v", err)
		}
	}
	benchUint64Sink = uint64(len(dst.Values))
}
