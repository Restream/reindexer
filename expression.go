package reindexer

import (
	"reflect"

	"github.com/restream/reindexer/v5/cjson"
)

type TimeUnit string

const (
	Sec  TimeUnit = "sec"
	Msec TimeUnit = "msec"
	Usec TimeUnit = "usec"
	Nsec TimeUnit = "nsec"
)

// IExpression is a serializable operand for WhereExpressions: a field reference, literal values,
// a nested query, or a built-in function (see Field, Values, SubQuery, FlatArrayLen, Now, etc.).
type IExpression interface {
	Type() int
	Serialize(ser *cjson.Serializer)
}

// ==================== IExpressions implemetations list begins here ====================

// Field names an indexed field for use as the left or right side of an expression condition.
type Field struct {
	Name string
}

// Values is a list of literal values serialized as the right-hand side of an expression (or other supported positions).
type Values struct {
	Values []any
}

// SubQuery embeds another Query’s serialized body as an expression operand (e.g. correlated filters).
type SubQuery struct {
	SubQuery *Query
}

// FlatArrayLen is the flat_array_len(field) expression; implements IFunction and IExpression.
type FlatArrayLen struct {
	Field string
}

// Now is the now(time_unit) expression; implements IFunction and IExpression.
type Now struct {
	TimeUnit TimeUnit
}

// ==================== IExpressions implemetations list ends here ====================

// Type returns expressionTypeField.
func (f Field) Type() int {
	return expressionTypeField
}

// Serialize writes the field expression tag and name.
func (f Field) Serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(int(f.Type()))
	ser.PutVString(f.Name)
}

// Type returns expressionTypeValues.
func (v Values) Type() int {
	return expressionTypeValues
}

// Serialize writes the values tag, count, and each element with PutValue.
func (v Values) Serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(int(v.Type()))
	ser.PutVarCUInt(len(v.Values))
	for _, v := range v.Values {
		ser.PutValue(reflect.ValueOf(v))
	}
}

// Type returns expressionTypeSubQuery.
func (q SubQuery) Type() int {
	return expressionTypeSubQuery
}

// Serialize writes the subquery tag and the nested query’s serialized buffer.
func (s SubQuery) Serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(int(s.Type()))
	ser.PutVBytes(s.SubQuery.ser.Bytes())
}

func (f FlatArrayLen) FunctionType() int {
	return functionFlatArrayLen
}

func (f FlatArrayLen) Fields() []string {
	return []string{f.Field}
}

func (f FlatArrayLen) Args() []any {
	return []any{}
}

func (f Now) FunctionType() int {
	return functionNow
}

func (f Now) Fields() []string {
	return []string{}
}

func (f Now) Args() []any {
	return []any{f.TimeUnit}
}

type IFunction interface {
	FunctionType() int
	Fields() []string
	Args() []any
}

func SerializeFunction(fn IFunction, ser *cjson.Serializer) {
	ser.PutVarCUInt(len(fn.Fields()))
	for _, field := range fn.Fields() {
		ser.PutVString(field)
	}
	ser.PutVarCUInt(len(fn.Args()))
	for _, arg := range fn.Args() {
		ser.PutValue(reflect.ValueOf(arg))
	}
	ser.PutVarCUInt(fn.FunctionType())
}

// Type reports expressionTypeExpression for the flat_array_len function node.
func (f FlatArrayLen) Type() int {
	return expressionTypeExpression
}

// Serialize writes the function expression tag and flat_array_len payload.
func (f FlatArrayLen) Serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(int(f.Type()))
	SerializeFunction(f, ser)
}

// Type reports expressionTypeExpression for the now function node.
func (n Now) Type() int {
	return expressionTypeExpression
}

// Serialize writes the function expression tag and now() payload.
func (n Now) Serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(int(n.Type()))
	SerializeFunction(n, ser)
}
