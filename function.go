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

type IFunction interface {
	FunctionType() int
	Fields() []string
	Args() []interface{}
}

type FlatArrayLen struct {
	Field string
}

type Now struct {
	TimeUnit TimeUnit
}

func (f FlatArrayLen) FunctionType() int {
	return functionFlatArrayLen
}

func (f FlatArrayLen) Fields() []string {
	return []string{f.Field}
}

func (f FlatArrayLen) Args() []interface{} {
	return []interface{}{}
}

func (f Now) FunctionType() int {
	return functionNow
}

func (f Now) Fields() []string {
	return []string{}
}

func (f Now) Args() []interface{} {
	return []interface{}{f.TimeUnit}
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

func (f FlatArrayLen) Type() int {
	return expressionTypeExpression
}

func (f FlatArrayLen) Serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(int(f.Type()))
	SerializeFunction(f, ser)
}

func (n Now) Type() int {
	return expressionTypeExpression
}

func (n Now) Serialize(ser *cjson.Serializer) {
	ser.PutVarCUInt(int(n.Type()))
	SerializeFunction(n, ser)
}
