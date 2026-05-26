package reindexer

import (
	"reflect"
	"testing"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
)

type benchPerfNested struct {
	Code   int      `json:"code" reindex:"code,hash"`
	Labels []string `json:"labels"`
}

type benchPerfJoined struct {
	ID   int    `json:"id" reindex:"id,,pk"`
	Name string `json:"name" reindex:"name,tree"`
}

type benchPerfItem struct {
	ID      int                `json:"id" reindex:"id,,pk"`
	Age     int                `json:"age" reindex:"age,tree"`
	Score   int64              `json:"score" reindex:"score,hash"`
	Price   float64            `json:"price" reindex:"price,tree"`
	Active  bool               `json:"active" reindex:"active,hash"`
	Name    string             `json:"name" reindex:"name,text"`
	Tags    []string           `json:"tags" reindex:"tags,hash"`
	Nums    []int              `json:"nums" reindex:"nums,hash"`
	Vector  []float32          `json:"vector" reindex:"vector,vec_bf,metric=l2,dimension=8"`
	Nested  benchPerfNested    `json:"nested"`
	Joined  []*benchPerfJoined `json:"joined" reindex:"joined,,joined"`
	Payload map[string]any     `json:"payload"`
}

type benchPerfJoinableItem struct {
	benchPerfItem
	joinedSink []any
}

func (i *benchPerfJoinableItem) Join(_ string, subitems []any, _ any) {
	i.joinedSink = subitems
}

var (
	benchPerfAnySink   any
	benchPerfBytesSink []byte
	benchPerfIntSink   int
)

func benchPerfKnnParam() KnnSearchParam {
	k := 10
	radius := float32(1.25)
	p, err := NewIndexHnswSearchParam(64, BaseKnnSearchParam{K: &k, Radius: &radius})
	if err != nil {
		panic(err)
	}
	return p
}

func newBenchPerfQuery() *Query {
	return newQuery(nil, "bench_items", nil)
}

func BenchmarkQueryBuildScalarWhere(b *testing.B) {
	for b.Loop() {
		q := newBenchPerfQuery().
			Where("id", EQ, 42).
			WhereString("name", EQ, "alice", "bob").
			WhereInt("age", GE, 18, 21, 30).
			WhereInt64("score", GT, 123456789).
			Sort("age", false, 18, 21, 30).
			Limit(20).
			Offset(5)
		benchPerfBytesSink = q.ser.Bytes()
		q.close()
	}
}

func BenchmarkQueryBuildSliceWhere(b *testing.B) {
	keys := []int{1, 2, 3, 4, 5, 6, 7, 8}
	strings := []string{"a", "b", "c", "d"}
	for b.Loop() {
		q := newBenchPerfQuery().
			Where("id", SET, keys).
			Where("name", SET, strings).
			WhereComposite("id+name", EQ, []any{42, "alice"}).
			Sort("id", true)
		benchPerfBytesSink = q.ser.Bytes()
		q.close()
	}
}

func BenchmarkQueryWhereQueryKeys(b *testing.B) {
	b.Run("nil", func(b *testing.B) {
		subQuery := newBenchPerfQuery().WhereInt("age", GE, 18)
		defer subQuery.close()
		for b.Loop() {
			q := newBenchPerfQuery().WhereQuery(subQuery, SET, nil)
			benchPerfBytesSink = q.ser.Bytes()
			q.close()
		}
	})

	b.Run("scalar", func(b *testing.B) {
		subQuery := newBenchPerfQuery().WhereInt("age", GE, 18)
		defer subQuery.close()
		for b.Loop() {
			q := newBenchPerfQuery().WhereQuery(subQuery, SET, 42)
			benchPerfBytesSink = q.ser.Bytes()
			q.close()
		}
	})

	b.Run("slice", func(b *testing.B) {
		keys := []int{1, 2, 3, 4, 5, 6, 7, 8}
		subQuery := newBenchPerfQuery().WhereInt("age", GE, 18)
		defer subQuery.close()
		for b.Loop() {
			q := newBenchPerfQuery().WhereQuery(subQuery, SET, keys)
			benchPerfBytesSink = q.ser.Bytes()
			q.close()
		}
	})
}

func BenchmarkQueryBuildKnn(b *testing.B) {
	vec := make([]float32, 256)
	for i := range vec {
		vec[i] = float32(i) * 0.125
	}
	param := benchPerfKnnParam()
	for b.Loop() {
		q := newBenchPerfQuery().
			WhereKnn("vector", vec, param).
			Sort("id", false).
			Limit(10)
		benchPerfBytesSink = q.ser.Bytes()
		q.close()
	}
}

func BenchmarkQueryBuildUpdateSet(b *testing.B) {
	obj := benchPerfNested{Code: 7, Labels: []string{"hot", "path"}}
	for b.Loop() {
		q := newBenchPerfQuery().
			WhereInt("id", EQ, 42).
			Set("age", 43).
			Set("tags", []string{"new", "tags"}).
			SetObject("nested", obj)
		benchPerfBytesSink = q.ser.Bytes()
		q.close()
	}
}

func BenchmarkQueryBuildJoinMergeCopy(b *testing.B) {
	for b.Loop() {
		q := newBenchPerfQuery().WhereInt("id", EQ, 42)
		jq := newQuery(nil, "bench_joined", nil).WhereInt("id", EQ, 42)
		mq := newQuery(nil, "bench_items_archive", nil).WhereString("name", EQ, "alice")
		q.LeftJoin(jq, "joined").On("id", EQ, "id").Merge(mq)
		qc := q.makeCopy(nil, nil)
		benchPerfBytesSink = qc.ser.Bytes()
		qc.close()
		q.close()
	}
}

func BenchmarkIteratorReflectJoin(b *testing.B) {
	subitems := []any{
		&benchPerfJoined{ID: 1, Name: "a"},
		&benchPerfJoined{ID: 2, Name: "b"},
		&benchPerfJoined{ID: 3, Name: "c"},
	}
	it := &Iterator{
		query: &Query{db: &reindexerImpl{}},
		nsArray: []nsArrayEntry{
			{reindexerNamespace: &reindexerNamespace{rtype: reflect.TypeFor[benchPerfItem](), joined: map[string][]int{"joined": []int{10}}}},
			{reindexerNamespace: &reindexerNamespace{name: "bench_joined", rtype: reflect.TypeFor[benchPerfJoined]()}},
		},
		joinToFields: []string{"joined"},
		joinHandlers: []JoinHandler{nil},
	}
	it.current.joinObj = make([][]any, 1)

	for b.Loop() {
		item := &benchPerfItem{}
		it.current.joinObj[0] = subitems
		if err := it.join(0, 1, 0, item); err != nil {
			b.Fatal(err)
		}
		benchPerfIntSink += len(item.Joined)
	}
}

func BenchmarkIteratorReflectJoinReuse(b *testing.B) {
	subitems := []any{
		&benchPerfJoined{ID: 1, Name: "a"},
		&benchPerfJoined{ID: 2, Name: "b"},
		&benchPerfJoined{ID: 3, Name: "c"},
	}
	it := &Iterator{
		query: &Query{db: &reindexerImpl{}},
		nsArray: []nsArrayEntry{
			{reindexerNamespace: &reindexerNamespace{rtype: reflect.TypeFor[benchPerfItem](), joined: map[string][]int{"joined": []int{10}}}},
			{reindexerNamespace: &reindexerNamespace{name: "bench_joined", rtype: reflect.TypeFor[benchPerfJoined]()}},
		},
		joinToFields: []string{"joined"},
		joinHandlers: []JoinHandler{nil},
	}
	it.current.joinObj = make([][]any, 1)
	item := &benchPerfItem{}

	for b.Loop() {
		item.Joined = item.Joined[:0]
		it.current.joinObj[0] = subitems
		if err := it.join(0, 1, 0, item); err != nil {
			b.Fatal(err)
		}
		benchPerfIntSink += len(item.Joined)
	}
}

func BenchmarkIteratorJoinable(b *testing.B) {
	subitems := []any{
		&benchPerfJoined{ID: 1, Name: "a"},
		&benchPerfJoined{ID: 2, Name: "b"},
		&benchPerfJoined{ID: 3, Name: "c"},
	}
	it := &Iterator{
		query:        &Query{db: &reindexerImpl{}},
		nsArray:      []nsArrayEntry{{reindexerNamespace: &reindexerNamespace{rtype: reflect.TypeFor[benchPerfJoinableItem]()}}, {reindexerNamespace: &reindexerNamespace{name: "bench_joined", rtype: reflect.TypeFor[benchPerfJoined]()}}},
		joinToFields: []string{"joined"},
		joinHandlers: []JoinHandler{nil},
	}
	it.current.joinObj = make([][]any, 1)

	for b.Loop() {
		item := &benchPerfJoinableItem{}
		it.current.joinObj[0] = subitems
		if err := it.join(0, 1, 0, item); err != nil {
			b.Fatal(err)
		}
		benchPerfIntSink += len(item.joinedSink)
	}
}

func BenchmarkReflectParseIndexes(b *testing.B) {
	t := reflect.TypeFor[benchPerfItem]()
	for b.Loop() {
		joined := make(map[string][]int)
		indexes, err := parseIndexes(t, &joined)
		if err != nil {
			b.Fatal(err)
		}
		benchPerfIntSink += len(indexes) + len(joined)
	}
}

func BenchmarkReflectParseSchema(b *testing.B) {
	t := reflect.TypeFor[benchPerfItem]()
	for b.Loop() {
		benchPerfAnySink = parseSchema(t)
	}
}

func BenchmarkReflectCjsonEncodeDecode(b *testing.B) {
	item := &benchPerfItem{
		ID:     42,
		Age:    31,
		Score:  123456789,
		Price:  99.75,
		Active: true,
		Name:   "alice",
		Tags:   []string{"a", "b", "c"},
		Nums:   []int{1, 2, 3, 4, 5},
		Vector: []float32{1, 2, 3, 4, 5, 6, 7, 8},
		Nested: benchPerfNested{Code: 7, Labels: []string{"x", "y"}},
		Payload: map[string]any{
			"k1": "v1",
			"k2": 42,
		},
	}
	state := cjson.NewState()
	enc := state.NewEncoder()
	wr := cjson.NewPoolSerializer()
	defer wr.Close()
	if _, err := enc.Encode(item, wr); err != nil {
		b.Fatal(err)
	}
	payload := append([]byte(nil), wr.Bytes()...)

	b.Run("encode", func(b *testing.B) {
		enc := state.NewEncoder()
		ser := cjson.NewPoolSerializer()
		defer ser.Close()
		b.ReportAllocs()
		for b.Loop() {
			ser.Reset()
			if _, err := enc.Encode(item, ser); err != nil {
				b.Fatal(err)
			}
			benchPerfBytesSink = ser.Bytes()
		}
	})

	b.Run("decode", func(b *testing.B) {
		var out benchPerfItem
		dec := state.NewDecoder(&out, nil)
		defer dec.Finalize()
		b.ReportAllocs()
		for b.Loop() {
			out = benchPerfItem{}
			if err := dec.Decode(payload, &out); err != nil {
				b.Fatal(err)
			}
			benchPerfAnySink = out
		}
	})
}

func BenchmarkReindexerPackItem(b *testing.B) {
	item := &benchPerfItem{
		ID:     42,
		Age:    31,
		Score:  123456789,
		Price:  99.75,
		Active: true,
		Name:   "alice",
		Tags:   []string{"a", "b", "c"},
		Nums:   []int{1, 2, 3, 4, 5},
		Vector: []float32{1, 2, 3, 4, 5, 6, 7, 8},
		Nested: benchPerfNested{Code: 7, Labels: []string{"x", "y"}},
	}
	ns := &reindexerNamespace{
		name:       "bench_items",
		rtype:      reflect.TypeFor[benchPerfItem](),
		cjsonState: cjson.NewState(),
	}
	ser := cjson.NewPoolSerializer()
	defer ser.Close()
	if _, _, _, err := packItem(ns, item, nil, ser); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		ser.Reset()
		format, token, itemIsPtr, err := packItem(ns, item, nil, ser)
		if err != nil {
			b.Fatal(err)
		}
		if itemIsPtr {
			benchPerfIntSink++
		}
		benchPerfIntSink += format + token + len(ser.Bytes())
	}
}

func BenchmarkReindexerPackJSON(b *testing.B) {
	ns := &reindexerNamespace{
		name:       "bench_items",
		rtype:      reflect.TypeFor[benchPerfItem](),
		cjsonState: cjson.NewState(),
	}
	json := []byte(`{"id":42,"age":31,"name":"alice"}`)
	ser := cjson.NewPoolSerializer()
	defer ser.Close()

	b.ReportAllocs()
	for b.Loop() {
		ser.Reset()
		format, token, itemIsPtr, err := packItem(ns, nil, json, ser)
		if err != nil {
			b.Fatal(err)
		}
		if itemIsPtr {
			b.Fatal("json packItem unexpectedly reported pointer item")
		}
		benchPerfIntSink += format + token + len(ser.Bytes())
	}
}

func BenchmarkQueryTypedVsReflectWhere(b *testing.B) {
	keys := []int{1, 2, 3, 4, 5, 6, 7, 8}

	b.Run("typed-WhereInt", func(b *testing.B) {
		for b.Loop() {
			q := newBenchPerfQuery().WhereInt("id", SET, keys...)
			benchPerfBytesSink = q.ser.Bytes()
			q.close()
		}
	})

	b.Run("reflect-Where", func(b *testing.B) {
		for b.Loop() {
			q := newBenchPerfQuery().Where("id", SET, keys)
			benchPerfBytesSink = q.ser.Bytes()
			q.close()
		}
	})
}

func BenchmarkQuerySetObjectBytesVsStruct(b *testing.B) {
	obj := benchPerfNested{Code: 7, Labels: []string{"hot", "path"}}
	objJSON := []byte(`{"code":7,"labels":["hot","path"]}`)
	objJSONArray := [][]byte{
		[]byte(`{"code":1,"labels":["a"]}`),
		[]byte(`{"code":2,"labels":["b"]}`),
	}

	b.Run("struct", func(b *testing.B) {
		for b.Loop() {
			q := newBenchPerfQuery().SetObject("nested", obj)
			benchPerfBytesSink = q.ser.Bytes()
			q.close()
		}
	})

	b.Run("bytes", func(b *testing.B) {
		for b.Loop() {
			q := newBenchPerfQuery().SetObject("nested", objJSON)
			benchPerfBytesSink = q.ser.Bytes()
			q.close()
		}
	})

	b.Run("nil", func(b *testing.B) {
		for b.Loop() {
			q := newBenchPerfQuery().SetObject("nested", nil)
			benchPerfBytesSink = q.ser.Bytes()
			q.close()
		}
	})

	b.Run("bytes-array", func(b *testing.B) {
		for b.Loop() {
			q := newBenchPerfQuery().SetObject("nested", objJSONArray)
			benchPerfBytesSink = q.ser.Bytes()
			q.close()
		}
	})
}

func BenchmarkQueryWhereValueKinds(b *testing.B) {
	values := []struct {
		name string
		val  any
	}{
		{"int", 42},
		{"int64", int64(123456789)},
		{"string", "alice"},
		{"bool", true},
		{"float64", 3.1415926535},
		{"int-slice", []int{1, 2, 3, 4}},
		{"string-slice", []string{"a", "b", "c"}},
		{"interface-slice", []any{1, "a", true}},
	}

	for _, tc := range values {
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				q := newBenchPerfQuery().Where("field", EQ, tc.val)
				benchPerfBytesSink = q.ser.Bytes()
				q.close()
			}
		})
	}
}

func BenchmarkReindexerModifyPreceptsSerialize(b *testing.B) {
	ns := &reindexerNamespace{
		name:       "bench_items",
		rtype:      reflect.TypeFor[benchPerfItem](),
		cjsonState: cjson.NewState(),
	}
	item := &benchPerfItem{ID: 42, Age: 31, Name: "alice"}
	precepts := []string{"updated_at=NOW()", "serial=SERIAL()"}
	ser := cjson.NewPoolSerializer()
	defer ser.Close()

	b.ReportAllocs()
	for b.Loop() {
		ser.Reset()
		format, token, itemIsPtr, err := packItem(ns, item, nil, ser)
		if err != nil {
			b.Fatal(err)
		}
		if itemIsPtr {
			benchPerfIntSink++
		}
		benchPerfIntSink += format + token + len(precepts) + len(ser.Bytes())
	}
}

func BenchmarkQueryBuildAggregationLike(b *testing.B) {
	for b.Loop() {
		q := newBenchPerfQuery().
			WhereInt("age", GE, 18).
			AggregateSum("score").
			AggregateAvg("price")
		q.AggregateFacet("active", "age").
			Limit(10).
			Sort("count", true)
		q.Select("id", "name", "score").
			Functions("rank()").
			EqualPosition("tags", "nums")
		benchPerfBytesSink = q.ser.Bytes()
		q.close()
	}
}

func BenchmarkReflectIndexDefAppend(b *testing.B) {
	defs := []bindings.IndexDef{
		{Name: "id", JSONPaths: []string{"id"}, IndexType: "hash", FieldType: "int"},
		{Name: "age", JSONPaths: []string{"age"}, IndexType: "tree", FieldType: "int"},
	}
	for b.Loop() {
		out := make([]bindings.IndexDef, 0, len(defs))
		for _, def := range defs {
			if err := indexDefAppend(&out, def, true); err != nil {
				b.Fatal(err)
			}
		}
		benchPerfIntSink += len(out)
	}
}

func benchRawQueryParamsBuf(withExtras bool) []byte {
	ser := cjson.NewSerializer(nil)
	ser.PutVarUInt(0)  // flags
	ser.PutVarUInt(10) // totalcount
	ser.PutVarUInt(10) // qcount
	ser.PutVarUInt(10) // count
	if withExtras {
		ser.PutVarUInt(bindings.QueryResultAggregation)
		agg := []byte(`[{"type":"sum","value":42}]`)
		ser.PutUInt32(uint32(len(agg))).Write(agg)
		ser.PutVarUInt(bindings.QueryResultExplain)
		explain := []byte(`{"cost":1}`)
		ser.PutUInt32(uint32(len(explain))).Write(explain)
		ser.PutVarUInt(bindings.QueryResultIncarnationTags)
		ser.PutVarUInt(4)
		for shardID := range 4 {
			ser.PutVarInt(int64(shardID))
			ser.PutVarUInt(4)
			for nsID := range 4 {
				ser.PutVarInt(int64(shardID*100 + nsID))
			}
		}
	}
	ser.PutVarUInt(bindings.QueryResultEnd)
	return append([]byte(nil), ser.Bytes()...)
}

func benchRawQueryParamsRankBuf() []byte {
	ser := cjson.NewSerializer(nil)
	ser.PutVarUInt(0)  // flags
	ser.PutVarUInt(10) // totalcount
	ser.PutVarUInt(10) // qcount
	ser.PutVarUInt(10) // count
	ser.PutVarUInt(bindings.QueryResultRankFormat)
	ser.PutVarUInt(bindings.RankFormatSingleFloat)
	ser.PutVarUInt(bindings.QueryResultEnd)
	return append([]byte(nil), ser.Bytes()...)
}

func BenchmarkResultSerializerReadExtraResults(b *testing.B) {
	b.Run("empty", func(b *testing.B) {
		buf := benchRawQueryParamsBuf(false)
		var params rawResultQueryParams
		b.ReportAllocs()
		for b.Loop() {
			ser := newSerializer(buf)
			ser.readRawQueryParamsKeepExtras(&params)
			benchPerfIntSink += params.count
		}
	})

	b.Run("extras", func(b *testing.B) {
		buf := benchRawQueryParamsBuf(true)
		var params rawResultQueryParams
		b.ReportAllocs()
		for b.Loop() {
			ser := newSerializer(buf)
			ser.readRawQueryParamsKeepExtras(&params)
			benchPerfIntSink += len(params.aggResults) + len(params.explainResults) + len(params.nsIncarnationTags)
		}
	})

	b.Run("rank-format", func(b *testing.B) {
		buf := benchRawQueryParamsRankBuf()
		var params rawResultQueryParams
		b.ReportAllocs()
		for b.Loop() {
			ser := newSerializer(buf)
			ser.readRawQueryParamsKeepExtras(&params)
			if params.rankFormat != nil {
				benchPerfIntSink += int(*params.rankFormat)
			}
		}
	})
}
