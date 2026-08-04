package reindexer

import "testing"

var benchQueryLen int

func BenchmarkQueryWhereExpressionsFieldValues(b *testing.B) {
	left := Field{Name: "id"}
	right := Values{Values: []any{1}}

	b.ReportAllocs()
	for b.Loop() {
		q := newQuery(nil, "bench", nil)
		q.WhereExpressions(left, EQ, right)
		benchQueryLen = len(q.ser.Bytes())
		q.close()
	}
}

func BenchmarkQueryWhereExpressionsFunctions(b *testing.B) {
	left := FlatArrayLen{Field: "tags"}
	right := Now{TimeUnit: Sec}

	b.ReportAllocs()
	for b.Loop() {
		q := newQuery(nil, "bench", nil)
		q.WhereExpressions(left, EQ, right)
		benchQueryLen = len(q.ser.Bytes())
		q.close()
	}
}
