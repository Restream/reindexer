package cjson

import (
	"fmt"
	"sort"
	"testing"
)

type benchBase64BytesDoc struct {
	ID   int    `json:"id"`
	Data []byte `json:"data"`
}

var cjsonBase64PayloadSizes = []int{0, 1, 2, 3, 8, 16, 24, 32, 64, 128, 192, 512, 1024, 4096}

func makeCJSONBase64Payload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*31 + 17)
	}
	return payload
}

func makeCJSONBase64RealShapePayloads() [][]byte {
	words := []string{
		"able_ant", "balanced_bear", "direct_dane", "enabled_emu",
		"faithful_fish", "great_goat", "humane_hare", "logical_lark",
		"modern_mole", "optimal_orca", "rapid_ray", "welcome_wren",
	}
	payloads := make([][]byte, len(words))
	for i, word := range words {
		payloads[i] = []byte(word)
	}
	return payloads
}

func BenchmarkCJSONBase64Encode(b *testing.B) {
	state := NewState()
	enc := state.NewEncoder()

	for _, size := range cjsonBase64PayloadSizes {
		payload := makeCJSONBase64Payload(size)
		doc := benchBase64BytesDoc{ID: size, Data: payload}

		ser := NewPoolSerializer()
		if err := enc.EncodeRaw(doc, ser); err != nil {
			b.Fatalf("warm encode size %d: %v", size, err)
		}
		ser.Close()

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				ser := NewPoolSerializer()
				if err := enc.EncodeRaw(doc, ser); err != nil {
					b.Fatalf("encode size %d: %v", size, err)
				}
				benchBytesSink = ser.Bytes()
				ser.Close()
			}
		})
	}
}

func BenchmarkCJSONBase64Decode(b *testing.B) {
	state := NewState()
	enc := state.NewEncoder()
	dec := state.NewDecoder(benchBase64BytesDoc{}, nil)
	defer dec.Finalize()

	for _, size := range cjsonBase64PayloadSizes {
		payload := makeCJSONBase64Payload(size)
		doc := benchBase64BytesDoc{ID: size, Data: payload}
		ser := NewPoolSerializer()
		if err := enc.EncodeRaw(doc, ser); err != nil {
			b.Fatalf("prepare decode size %d: %v", size, err)
		}
		wire := append([]byte(nil), ser.Bytes()...)
		ser.Close()

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				var out benchBase64BytesDoc
				if err := dec.Decode(wire, &out); err != nil {
					b.Fatalf("decode size %d: %v", size, err)
				}
				benchBytesSink = out.Data
			}
		})
	}
}

func BenchmarkCJSONBase64RealShape(b *testing.B) {
	payloads := makeCJSONBase64RealShapePayloads()
	state := NewState()
	enc := state.NewEncoder()
	dec := state.NewDecoder(benchBase64BytesDoc{}, nil)
	defer dec.Finalize()

	wires := make([][]byte, len(payloads))
	for i, payload := range payloads {
		ser := NewPoolSerializer()
		if err := enc.EncodeRaw(benchBase64BytesDoc{ID: i, Data: payload}, ser); err != nil {
			b.Fatalf("prepare real-shape %d: %v", i, err)
		}
		wires[i] = append([]byte(nil), ser.Bytes()...)
		ser.Close()
	}

	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for i, payload := range payloads {
				ser := NewPoolSerializer()
				if err := enc.EncodeRaw(benchBase64BytesDoc{ID: i, Data: payload}, ser); err != nil {
					b.Fatalf("encode real-shape %d: %v", i, err)
				}
				benchBytesSink = ser.Bytes()
				ser.Close()
			}
		}
	})

	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for i, wire := range wires {
				var out benchBase64BytesDoc
				if err := dec.Decode(wire, &out); err != nil {
					b.Fatalf("decode real-shape %d: %v", i, err)
				}
				benchBytesSink = out.Data
			}
		}
	})
}

func BenchmarkCJSONBase64PayloadReport(b *testing.B) {
	payloads := append([][]byte(nil), makeCJSONBase64RealShapePayloads()...)
	for _, size := range cjsonBase64PayloadSizes {
		payloads = append(payloads, makeCJSONBase64Payload(size))
	}

	lengths := make([]int, len(payloads))
	encodedLengths := make([]int, len(payloads))
	modCounts := [3]int{}
	buckets := [4]int{}
	for i, payload := range payloads {
		l := len(payload)
		lengths[i] = l
		encodedLengths[i] = ((l + 2) / 3) * 4
		modCounts[l%3]++
		switch {
		case l < 16:
			buckets[0]++
		case l <= 128:
			buckets[1]++
		case l <= 191:
			buckets[2]++
		default:
			buckets[3]++
		}
	}
	sort.Ints(lengths)
	sort.Ints(encodedLengths)

	count := float64(len(payloads))
	b.ReportAllocs()
	for b.Loop() {
		for _, payload := range payloads {
			benchBytesSink = payload
		}
	}

	b.ReportMetric(count, "payloads")
	b.ReportMetric(float64(lengths[0]), "raw_min")
	b.ReportMetric(float64(lengths[len(lengths)/2]), "raw_p50")
	b.ReportMetric(float64(lengths[(len(lengths)*90)/100]), "raw_p90")
	b.ReportMetric(float64(lengths[(len(lengths)*99)/100]), "raw_p99")
	b.ReportMetric(float64(lengths[len(lengths)-1]), "raw_max")
	b.ReportMetric(float64(encodedLengths[len(encodedLengths)/2]), "enc_p50")
	b.ReportMetric(float64(encodedLengths[len(encodedLengths)-1]), "enc_max")
	b.ReportMetric(float64(modCounts[0])*100/count, "mod0_pct")
	b.ReportMetric(float64(modCounts[1])*100/count, "mod1_pct")
	b.ReportMetric(float64(modCounts[2])*100/count, "mod2_pct")
	b.ReportMetric(float64(buckets[0])*100/count, "lt16_pct")
	b.ReportMetric(float64(buckets[1])*100/count, "16_128_pct")
	b.ReportMetric(float64(buckets[2])*100/count, "129_191_pct")
	b.ReportMetric(float64(buckets[3])*100/count, "ge192_pct")
}
