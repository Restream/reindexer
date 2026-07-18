package reindexer

import (
	"reflect"
	"testing"
)

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

func BenchmarkQueryWhereExpressionsFunctionsLegacySlices(b *testing.B) {
	left := legacyFlatArrayLen{Field: "tags"}
	right := legacyNow{TimeUnit: Sec}

	b.ReportAllocs()
	for b.Loop() {
		q := newQuery(nil, "bench", nil)
		legacyWhereExpressionsFunctions(q, left, EQ, right)
		benchQueryLen = len(q.ser.Bytes())
		q.close()
	}
}

func legacyWhereExpressionsFunctions(q *Query, left legacyFlatArrayLen, condition int, right legacyNow) {
	q.ser.PutVarCUInt(queryExpressions)
	legacySerializeFunctionExpression(q, left)
	q.ser.PutVarCUInt(q.nextOp)
	q.ser.PutVarCUInt(condition)
	legacySerializeFunctionExpression(q, right)
	q.whereEntriesCount++
	q.nextOp = opAND
}

type legacyFlatArrayLen struct {
	Field string
}

func (f legacyFlatArrayLen) Type() int {
	return expressionTypeExpression
}

func (f legacyFlatArrayLen) FunctionType() int {
	return functionFlatArrayLen
}

func (f legacyFlatArrayLen) Fields() []string {
	return []string{f.Field}
}

func (f legacyFlatArrayLen) Args() []any {
	return []any{}
}

type legacyNow struct {
	TimeUnit TimeUnit
}

func (n legacyNow) Type() int {
	return expressionTypeExpression
}

func (n legacyNow) FunctionType() int {
	return functionNow
}

func (n legacyNow) Fields() []string {
	return []string{}
}

func (n legacyNow) Args() []any {
	return []any{n.TimeUnit}
}

type legacyFunctionExpression interface {
	Type() int
	FunctionType() int
	Fields() []string
	Args() []any
}

func legacySerializeFunctionExpression(q *Query, fn legacyFunctionExpression) {
	q.ser.PutVarCUInt(fn.Type())
	legacySerializeFunction(q, fn)
}

func legacySerializeFunction(q *Query, fn legacyFunctionExpression) {
	q.ser.PutVarCUInt(len(fn.Fields()))
	for _, field := range fn.Fields() {
		q.ser.PutVString(field)
	}
	q.ser.PutVarCUInt(len(fn.Args()))
	for _, arg := range fn.Args() {
		if err := q.ser.PutValue(reflect.ValueOf(arg)); err != nil {
			panic(err)
		}
	}
	q.ser.PutVarCUInt(fn.FunctionType())
}
