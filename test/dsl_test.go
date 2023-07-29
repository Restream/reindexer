package reindexer

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/restream/reindexer/v3/dsl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var allIDs = make([]int, 100)

type TestDSLItem struct {
	ID int `reindex:"id,,pk"`
}

type TestDSLFtItem struct {
	ID          int    `reindex:"id,,pk"`
	Description string `reindex:"description,text"`
}

type TestDSLJoinItem struct {
	JID int `reindex:"jid,,pk"`
}

func init() {
	tnamespaces["test_namespace_dsl"] = TestDSLItem{}
	tnamespaces["test_namespace_dsl_ft"] = TestDSLFtItem{}
	tnamespaces["test_namespace_dsl_joined_1"] = TestDSLJoinItem{}
	tnamespaces["test_namespace_dsl_joined_2"] = TestDSLJoinItem{}
}

func newTestDSLItem(id int) interface{} {
	return &TestDSLItem{
		ID: id,
	}
}

func newTestDSLFtItem(id int, descr string) interface{} {
	return &TestDSLFtItem{
		ID:          id,
		Description: descr,
	}
}

func newTestDSLJoinItem(id int) interface{} {
	return &TestDSLJoinItem{
		JID: id,
	}
}

func fillTestDSLItems(t *testing.T, ns string, start int, count int) {
	tx := newTestTx(DB, ns)
	for i := 0; i < count; i++ {
		testItem := newTestDSLItem(start + i)
		if err := tx.Upsert(testItem); err != nil {
			require.NoError(t, err)
		}
		allIDs[i] = i
	}
	tx.MustCommit()
}

func fillTestDSLFtItems(t *testing.T, ns string) {
	descriptions := []string{"test", "word", "worm", "sword", "www"}
	tx := newTestTx(DB, ns)
	for i := 0; i < 5; i++ {
		testItem := newTestDSLFtItem(i, descriptions[i])
		if err := tx.Upsert(testItem); err != nil {
			require.NoError(t, err)
		}
	}
	tx.MustCommit()
}

func fillTestDSLJoinItems(t *testing.T, ns string, start int, count int) {
	tx := newTestTx(DB, ns)
	for i := 0; i < count; i++ {
		testItem := newTestDSLJoinItem(start + i)
		if err := tx.Upsert(testItem); err != nil {
			require.NoError(t, err)
		}
	}
	tx.MustCommit()
}

func getTesDSLFtItemsDescr(items []interface{}) []string {
	resultDescr := make([]string, len(items))
	for i, v := range items {
		item := v.(*TestDSLFtItem)
		resultDescr[i] = item.Description
	}
	return resultDescr
}

func getTesDSLJoinItemsIDs(items []interface{}) []int {
	resultIDs := make([]int, len(items))
	for i, v := range items {
		item := v.(*TestDSLItem)
		resultIDs[i] = item.ID
	}
	return resultIDs
}

func TestDSLQueries(t *testing.T) {
	t.Parallel()

	fillTestDSLItems(t, "test_namespace_dsl", 0, 100)
	fillTestDSLFtItems(t, "test_namespace_dsl_ft")
	fillTestDSLJoinItems(t, "test_namespace_dsl_joined_1", 80, 40)
	fillTestDSLJoinItems(t, "test_namespace_dsl_joined_2", 10, 10)

	t.Run("basic dsl parsing", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_ns",
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "inner",
						"namespace": "test1",
						"filters": [
							{
								"op": "OR",
								"field": "id",
								"cond": "EMPTY"
							}
						],
						"sort": {
							"field": "test1",
							"desc": true
						},
						"limit": 3,
						"offset": 0,
						"on": [
							{
								"left_field": "joined",
								"right_field": "joined",
								"cond": "lt",
								"op": "OR"
							},
							{
								"left_field": "joined2",
								"right_field": "joined2",
								"cond": "gt",
								"op": "AND"
							}
						]
					}
				},
			   {
				"op": "OR",
				"join_query": {
					"type": "left",
					"namespace": "test2",
					"filters": [
						{
							"filters": [
								{
									"op": "And",
									"filters": [
										{
											"op": "Not",
											"field": "id2",
											"cond": "SET",
											"value": [
												81204872,
												101326571,
												101326882
											]
										},
										{
											"op": "Or",
											"field": "id2",
											"cond": "SET",
											"value": [
												81204872,
												101326571,
												101326882
											]
										},
										{
											"op": "And",
											"filters": [
												{
													"op": "Not",
													"field": "id2",
													"cond": "SET",
													"value": [
														81204872,
														101326571,
														101326882
													]
												},
												{
													"op": "Or",
													"field": "id2",
													"cond": "SET",
													"value": [
														81204872,
														101326571,
														101326882
													]
												}
											]
										}
									]
								},
								{
									"op": "Not",
									"field": "id2",
									"cond": "SET",
									"value": [
										81204872,
										101326571,
										101326882
									]
								}
							]
						}
					],
					"sort": {
						"field": "test2",
						"desc": true
					},
					"limit": 4,
					"offset": 5,
					"on": [
						{
							"left_field": "joined1",
							"right_field": "joined1",
							"cond": "le",
							"op": "AND"
						},
						{
							"left_field": "joined2",
							"right_field": "joined2",
							"cond": "ge",
							"op": "OR"
						}
					]
					}
				}
	
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		// No results validation here
	})

	t.Run("dsl equality condition", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"op": "And",
					"field": "id",
					"cond": "eq",
					"value": "91"
				},
				{
					"op": "Or",
					"field": "id",
					"cond": "eq",
					"value": [90]
				},
				{
					"op": "Or",
					"field": "id",
					"cond": "eq",
					"value": ["99","93","92"]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{90, 91, 92, 93, 99}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl multiple conditions", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"op": "And",
					"field": "id",
					"cond": "set",
					"value": [
						92,93.5,99,97,21
					]
				},
				{
					"op": "And",
					"field": "id",
					"cond": "le",
					"value": 93
				},
				{
					"op": "Or",
					"field": "id",
					"cond": "gt",
					"value": 95
				},
				{
					"op": "And",
					"field": "id",
					"cond": "eq",
					"value": [21.1,93,99,54,11]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{21, 93, 99}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl multiple conditions with brackets", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"filters": [
						{
							"field": "id",
							"cond": "set",
							"value": [
								92,93,99,97,21
							]
						},
						{
							"field": "id",
							"cond": "le",
							"value": 93
						}
					]
				},
				{
					"op": "Or",
					"filters": [
						{
							"field": "id",
							"cond": "gt",
							"value": 95
						},
						{
							"field": "id",
							"cond": "eq",
							"value": [21,93,99,54,11]
						}
					]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{21, 92, 93, 99}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl with req_total", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"field": "id",
					"cond": "lt",
					"value": 10
				}
			],
			"limit": 7,
			"offset": 2,
			"req_total": true
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)

		it := q.MustExec()
		require.NoError(t, it.Error())
		count := it.TotalCount()
		assert.Equal(t, 10, count)

		items, err := it.FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{2, 3, 4, 5, 6, 7, 8}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl with select_with_rank", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl_ft",
			"sort": {
				"field": "rank()",
				"desc": false
			},
			"filters": [
				{
					"field": "description",
					"cond": "eq",
					"value": "word~"
				}
			],
			"select_with_rank": true
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)

		it := q.MustExec()
		require.NoError(t, it.Error())
		items, ranks, err := it.FetchAllWithRank()
		require.NoError(t, err)

		expectedOrder := []string{"worm", "sword", "word"}
		expectedRanks := []int{75, 75, 107}
		require.Equal(t, expectedOrder, getTesDSLFtItemsDescr(items))
		require.Equal(t, expectedRanks, ranks)
	})

	t.Run("dsl filter nil value (expecting filter's skip)", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{"op": "AND", "field": "id", "cond": "set", "value": [92,93,99,97,21]},
				{"op": "AND", "field": "id", "cond": "set", "value": null},
				{"op": "AND", "field": "id", "cond": "eq", "value": null},
				{"op": "AND", "field": "id", "cond": "allset", "value": null},
				{"op": "AND", "field": "id", "cond": "range", "value": null},
				{"op": "AND", "field": "id", "cond": "ge", "value": null},
				{"op": "AND", "field": "id", "cond": "lt", "value": null}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{21, 92, 93, 97, 99}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl with empty array filter returns all items", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": []
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		require.Equal(t, allIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl with empty array filter and multiple conditions", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{"op": "And", "field": "id", "cond": "set", "value": [3]},
				{"op": "And", "field": "id", "cond": "eq", "value": []},
				{"op": "And", "field": "id", "cond": "set", "value": []},
				{"op": "And", "field": "id", "cond": "allset", "value": []},
				{"op": "And", "field": "id", "cond": "range", "value": []}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{3}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))

	})

	t.Run("dsl nil value with default index and any/empty cond", func(t *testing.T) {
		for _, cond := range [][]string{{"any", "NOT NULL"}, {"empty", "is NULL"}} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"op": "And",
						"field": "id",
						"cond": "%s",
						"value": null
					}
				]
			}
			`

			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond[0])), &dslQ)
			require.NoError(t, err)
			q, err := DBD.QueryFrom(dslQ)
			require.NoError(t, err)
			require.NotNil(t, q)
			_, err = q.Exec().FetchAll()
			require.ErrorContains(t, err, fmt.Sprintf("The '%s' condition is suported only by 'sparse' or 'array' indexes", cond[1]))
		}
	})

	t.Run("dsl invalid filter array values args", func(t *testing.T) {
		for _, cond := range []string{"gt", "lt", "ge", "le"} {
			for _, value := range []string{"[]", "[1,2]"} {
				const jsonDSL = `
				{
					"namespace": "test_namespace_dsl",
					"sort": {
						"field": "id"
					},
					"filters": [
						{
							"op": "AND", 
							"field": "id", 
							"cond": "%s", 
							"value": %s
						}
					]
				}
				`

				var dslQ dsl.DSL
				err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond, value)), &dslQ)
				require.EqualError(t, err, fmt.Sprintf("filter value can not be array with 0 or multiple values for '%s' condition", cond))
			}
		}
	})

	t.Run("dsl cant filter value has type object", func(t *testing.T) {
		for _, cond := range []string{"gt", "lt", "ge", "le", "eq"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"sort": {
					"field": "id"
				},
				"filters": [
					{
						"op": "AND", 
						"field": "id", 
						"cond": "%s", 
						"value": {}
					}
				]
			}
			`

			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond)), &dslQ)
			require.EqualError(t, err, "filter value can not be object")
		}
	})

	t.Run("dsl filter value must be array for 'set', 'allset', 'range'", func(t *testing.T) {
		for _, cond := range []string{"set", "allset", "range"} {
			for _, value := range []string{"123", "\"abc\"", "{}", "true"} {
				const jsonDSL = `
				{
					"namespace": "test_namespace_dsl",
					"sort": {
						"field": "id"
					},
					"filters": [
						{
							"op": "AND", 
							"field": "id", 
							"cond": "%s", 
							"value": %s
						}
					]
				}
				`

				var dslQ dsl.DSL
				err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond, value)), &dslQ)
				require.EqualError(t, err, fmt.Sprintf("filter expects array or null for '%s' condition", cond))
			}
		}
	})

	t.Run("dsl filter value must be array with 2 elements for 'range'", func(t *testing.T) {
		for _, value := range []string{"[1]", "[1,2,3]"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"sort": {
					"field": "id"
				},
				"filters": [
					{
						"op": "AND", 
						"field": "id", 
						"cond": "range", 
						"value": %s
					}
				]
			}
			`

			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, value)), &dslQ)
			require.EqualError(t, err, "range argument array must has 2 elements")
		}
	})

	t.Run("dsl invalid filter cond type", func(t *testing.T) {
		for _, cond := range []string{"equal", "not", "1"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"op": "AND", 
						"field": "id", 
						"cond": "%s", 
						"value": 1
					}
				]
			}
			`

			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond)), &dslQ)
			require.EqualError(t, err, fmt.Sprintf("cond type '%s' not found", cond))
		}
	})

	t.Run("dsl filter value array must be homogeneous", func(t *testing.T) {
		for _, cond := range []string{"eq", "set"} {
			for _, value := range [][]string{
				{"true, \"abc\"", "bool"}, {"false, 2", "bool"},
				{"1, \"2\"", "int/float"}, {"1.5, false", "int/float"},
				{"\"abc\", {}", "string"}, {"\"1\", 2", "string"}, {"\"a\", [\"b\"]", "string"},
			} {
				const jsonDSL = `
				{
					"namespace": "test_namespace_dsl",
					"filters": [
						{
							"op": "AND", 
							"field": "id", 
							"cond": "%s", 
							"value": [%s]
						}
					]
				}
				`

				var dslQ dsl.DSL
				err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond, value[0])), &dslQ)
				require.EqualError(t, err, fmt.Sprintf("array must be homogeneous (%s)", value[1]))
			}
		}
	})

	t.Run("dsl with repeated fields: the next field overrides the previous one", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl_joined_1",
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "none",
				"field": "id"
			},
			"filters": [
				{
					"field": "id",
					"cond": "eq",
					"value": 2,
					"value": 1
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{1}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl single join", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id",
				"desc": false
			},
			"explain": true,
			"filters": [
				{
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "range",
								"value": [10,15]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"field": "id",
					"cond": "set",
					"value": [
						1,10,12,14,17,19,99
					]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		it := q.MustExec()
		require.NoError(t, it.Error())
		explain, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explain)
		expectedIDs := []int{10, 12, 14}
		items, err := it.FetchAll()
		require.NoError(t, err)
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl single left join", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id",
				"desc": false
			},
			"explain": true,
			"filters": [
				{
					"join_query": {
						"type": "left",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "range",
								"value": [10,15]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"field": "id",
					"cond": "set",
					"value": [1,10,12,14,17,19,99]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		it := q.MustExec()
		require.NoError(t, it.Error())
		explain, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explain)
		expectedIDs := []int{1, 10, 12, 14, 17, 19, 99}
		items, err := it.FetchAll()
		require.NoError(t, err)
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl 2 joins", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id",
				"desc": true
			},
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_1",
						"limit": 1,
						"offset": 0,
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"op": "OR",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
						"limit": 10,
						"offset": 0,
						"filters": [
							{
								"field": "jid",
								"cond": "ge",
								"value": [15]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"op": "Not",
					"field": "id",
					"cond": "gt",
					"value": [
						90
					]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		it := q.MustExec()
		require.NoError(t, it.Error())
		explain, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.Nil(t, explain)
		expectedIDs := []int{90, 89, 88, 87, 86, 85, 84, 83, 82, 81, 80, 19, 18, 17, 16, 15}
		items, err := it.FetchAll()
		require.NoError(t, err)
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl join equality condition", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_1",
						"filters": [
							{
								"field": "jid",
								"cond": "eq",
								"value": 83
							},
							{   
								"op": "Or",
								"field": "jid",
								"cond": "eq",
								"value": [84, 85]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"op": "AND",
					"join_query": {
						"type": "orinner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "eq",
								"value": ["17"]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"op": "Or",
					"field": "id",
					"cond": "eq",
					"value": [91,93,99]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{17, 83, 84, 85, 91, 93, 99}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl join set condition", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_1",
						"filters": [
							{
								"field": "jid",
								"cond": "set",
								"value": [83]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"op": "AND",
					"join_query": {
						"type": "orinner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "set",
								"value": ["17", "18"]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"op": "Or",
					"field": "id",
					"cond": "set",
					"value": ["91","93","99"]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{17, 18, 83, 91, 93, 99}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl join multiple conditions", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_1",
						"filters": [
							{
								"op": "And",
								"field": "jid",
								"cond": "set",
								"value": [92,93,99,97,81,95,90.9]
							},
							{
								"op": "And",
								"field": "jid",
								"cond": "le",
								"value": 92
							},
							{
								"op": "Or",
								"field": "jid",
								"cond": "gt",
								"value": 95
							},
							{
								"op": "And",
								"field": "jid",
								"cond": "eq",
								"value": [92,81,90]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"op": "AND",
					"join_query": {
						"type": "orinner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "range",
								"value": ["11", "15"]
							},
							{
								"op": "Or",
								"field": "jid",
								"cond": "eq",
								"value": [16.5, 17]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"op": "Or",
					"field": "id",
					"cond": "set",
					"value": ["91","93","90", "80"]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{11, 12, 13, 14, 15, 16, 17, 80, 81, 90, 91, 92, 93}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl join multiple conditions and brackets", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_1",
						"filters": [
							{
								"filters": [
									{
										"field": "jid",
										"cond": "set",
										"value": [
											85,86,87,88,89,90
										]
									},
									{
										"field": "jid",
										"cond": "le",
										"value": "88"
									}
								]
							},
							{
								"op": "Or",
								"filters": [
									{
										"field": "jid",
										"cond": "gt",
										"value": 95
									},
									{
										"field": "jid",
										"cond": "eq",
										"value": [90, 95, 99, 98, 97]
									}
								]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				},
				{
					"op": "And",
					"field": "id",
					"cond": "range",
					"value": [85, 98]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{85, 86, 87, 88, 97, 98}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl join filter nil value", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{"op": "AND", "field": "jid", "cond": "set", "value": [10, 19, 11]},
							{"op": "AND", "field": "jid", "cond": "set", "value": null},
							{"op": "AND", "field": "jid", "cond": "eq", "value": null},
							{"op": "AND", "field": "jid", "cond": "allset", "value": null},
							{"op": "AND", "field": "jid", "cond": "range", "value": null},
							{"op": "AND", "field": "jid", "cond": "ge", "value": null},
							{"op": "AND", "field": "jid", "cond": "lt", "value": null}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{10, 11, 19}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl join with empty array filter returns all items", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl join with empty array filter and multiple conditions", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{"op": "And", "field": "jid", "cond": "set", "value": [15, 16]},
							{"op": "And", "field": "jid", "cond": "eq", "value": []},
							{"op": "And", "field": "jid", "cond": "set", "value": []},
							{"op": "And", "field": "jid", "cond": "allset", "value": []},
							{"op": "And", "field": "jid", "cond": "range", "value": []}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{15, 16}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))

	})

	t.Run("dsl join nil value with default index and any/empty cond", func(t *testing.T) {
		for _, cond := range [][]string{{"any", "NOT NULL"}, {"empty", "is NULL"}} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"op": "AND",
						"join_query": {
							"type": "inner",
							"namespace": "test_namespace_dsl_joined_2",
							"filters": [
								{
									"op": "And",
									"field": "jid",
									"cond": "%s",
									"value": null
								}
							],
							"on": [
								{
									"left_field": "id",
									"right_field": "jid",
									"cond": "EQ"
								}
							]
						}
					}
				]
			}
			`

			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond[0])), &dslQ)
			require.NoError(t, err)
			q, err := DBD.QueryFrom(dslQ)
			require.NoError(t, err)
			require.NotNil(t, q)
			_, err = q.Exec().FetchAll()
			require.ErrorContains(t, err, fmt.Sprintf("The '%s' condition is suported only by 'sparse' or 'array' indexes", cond[1]))
		}
	})

	t.Run("dsl join invalid filter array values args", func(t *testing.T) {
		for _, cond := range []string{"gt", "lt", "ge", "le"} {
			for _, value := range []string{"[]", "[1,2]"} {
				const jsonDSL = `
				{
					"namespace": "test_namespace_dsl",
					"sort": {
						"field": "id"
					},
					"filters": [
						{
							"op": "AND",
							"join_query": {
								"type": "inner",
								"namespace": "test_namespace_dsl_joined_2",
								"filters": [
									{
										"op": "AND", 
										"field": "jid", 
										"cond": "%s", 
										"value": %s
									}
								],
								"on": [
									{
										"left_field": "id",
										"right_field": "jid",
										"cond": "EQ"
									}
								]
							}
						}
					]
				}
				`

				var dslQ dsl.DSL
				err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond, value)), &dslQ)
				require.EqualError(t, err, fmt.Sprintf("filter value can not be array with 0 or multiple values for '%s' condition", cond))
			}
		}
	})

	t.Run("dsl join cant filter value has type object", func(t *testing.T) {
		for _, cond := range []string{"gt", "lt", "ge", "le", "eq"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"sort": {
					"field": "id"
				},
				"filters": [
					{
						"op": "AND",
						"join_query": {
							"type": "inner",
							"namespace": "test_namespace_dsl_joined_2",
							"filters": [
								{
									"op": "AND", 
									"field": "jid", 
									"cond": "%s", 
									"value": {}
								}
							],
							"on": [
								{
									"left_field": "id",
									"right_field": "jid",
									"cond": "EQ"
								}
							]
						}
					}
				]
			}
			`

			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond)), &dslQ)
			require.EqualError(t, err, "filter value can not be object")
		}
	})

	t.Run("dsl join filter value must be array for 'set', 'allset', 'range'", func(t *testing.T) {
		for _, cond := range []string{"set", "allset", "range"} {
			for _, value := range []string{"123", "\"abc\"", "{}", "true"} {
				const jsonDSL = `
				{
					"namespace": "test_namespace_dsl",
					"sort": {
						"field": "id"
					},
					"filters": [
						{
							"op": "AND",
							"join_query": {
								"type": "inner",
								"namespace": "test_namespace_dsl_joined_2",
								"filters": [
									{
										"op": "AND", 
										"field": "jid", 
										"cond": "%s", 
										"value": %s
									}
								],
								"on": [
									{
										"left_field": "id",
										"right_field": "jid",
										"cond": "EQ"
									}
								]
							}
						}
					]
				}
				`

				var dslQ dsl.DSL
				err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond, value)), &dslQ)
				require.EqualError(t, err, fmt.Sprintf("filter expects array or null for '%s' condition", cond))
			}
		}
	})

	t.Run("dsl join filter value must be array with 2 elements for 'range'", func(t *testing.T) {
		for _, value := range []string{"[1]", "[1,2,3]"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"sort": {
					"field": "id"
				},
				"filters": [
					{
						"op": "AND",
						"join_query": {
							"type": "inner",
							"namespace": "test_namespace_dsl_joined_2",
							"filters": [
								{
									"op": "AND", 
									"field": "jid", 
									"cond": "range", 
									"value": %s
								}
							],
							"on": [
								{
									"left_field": "id",
									"right_field": "jid",
									"cond": "EQ"
								}
							]
						}
					}
				]
			}
			`

			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, value)), &dslQ)
			require.EqualError(t, err, "range argument array must has 2 elements")
		}
	})

	t.Run("dsl join invalid filter cond type", func(t *testing.T) {
		for _, cond := range []string{"equal", "not", "1"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"op": "AND",
						"join_query": {
							"type": "inner",
							"namespace": "test_namespace_dsl_joined_2",
							"filters": [
								{
									"op": "AND", 
									"field": "jid", 
									"cond": "%s", 
									"value": 1
								}
							],
							"on": [
								{
									"left_field": "id",
									"right_field": "jid",
									"cond": "EQ"
								}
							]
						}
					}
				]
			}
			`

			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond)), &dslQ)
			require.EqualError(t, err, fmt.Sprintf("cond type '%s' not found", cond))
		}
	})

	t.Run("dsl join filter value array must be homogeneous", func(t *testing.T) {
		for _, cond := range []string{"eq", "set"} {
			for _, value := range [][]string{
				{"true, \"abc\"", "bool"}, {"false, 2", "bool"},
				{"1, \"2\"", "int/float"}, {"1.5, false", "int/float"},
				{"\"abc\", {}", "string"}, {"\"1\", 2", "string"}, {"\"a\", [\"b\"]", "string"},
			} {
				const jsonDSL = `
				{
					"namespace": "test_namespace_dsl",
					"filters": [
						{
							"op": "AND",
							"join_query": {
								"type": "inner",
								"namespace": "test_namespace_dsl_joined_2",
								"filters": [
									{
										"op": "AND", 
										"field": "jid", 
										"cond": "%s", 
										"value": [%s]
									}
								],
								"on": [
									{
										"left_field": "id",
										"right_field": "jid",
										"cond": "EQ"
									}
								]
							}
						}
					]
				}
				`

				var dslQ dsl.DSL
				err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond, value[0])), &dslQ)
				require.EqualError(t, err, fmt.Sprintf("array must be homogeneous (%s)", value[1]))
			}
		}
	})

	t.Run("dsl join with repeated fields: the next field overrides the previous one", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl_joined_1",
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "none",
				"field": "id"
			},
			"filters": [
				{
					"op": "AND",
					"join_query": {
						"type": "left",
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "range",
								"cond": "eq",
								"value": 14,
								"value": [15,16,17]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{15, 16, 17}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl unsupported fields don't affect the result", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id"
			},
			"filters": [
				{
					"field": "id",
					"cond": "set",
					"value": [1,9,15,11,14],
					"some_unsupported_0": true
				},
				{
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "range",
								"value": [11,15]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						],
						"some_unsupported_1": "abc"
					}
				}
			],
			"merge_queries": [
				{
					"namespace": "test_namespace_dsl_joined_2",
					"type": "select",
					"filters": [
						{
							"op": "And",
							"field": "id",
							"cond": "eq",
							"value": [18]
						}
					]
				}
			],
			"type": "select",
			"select_functions": [
				"id = highlight(<,>)"
			],
			"select_filter": ["age"],
			"strict_mode": "rrr",
			"some_unsupported_2": 123
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{11, 14, 15}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("dsl forsed sort", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id",
				"values": [98, 99]
			},
			"filters": [
				{
					"field": "id",
					"cond": "ge",
					"value": 95
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		items, err := q.MustExec().FetchAll()
		require.NoError(t, err)
		expectedIDs := []int{98, 99, 95, 96, 97}
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})

	t.Run("cant dsl sort with expression", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "id+2"
			},
			"filters": []
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		_, err = q.Exec().FetchAll()
		require.ErrorContains(t, err, "Current query strict mode allows sort by existing fields only. "+
			"There are no fields with name 'id+2' in namespace 'test_namespace_dsl'")
	})

	t.Run("dsl join with sort", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"sort": {
				"field": "test_namespace_dsl_joined_2.jid",
				"desc": false
			},
			"explain": true,
			"filters": [
				{
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "set",
								"value": [11, 19, 13, 14, 10]
							}
						],
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					}
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		it := q.MustExec()
		require.NoError(t, it.Error())
		explain, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.NotNil(t, explain)
		expectedIDs := []int{10, 11, 13, 14, 19}
		items, err := it.FetchAll()
		require.NoError(t, err)
		require.Equal(t, expectedIDs, getTesDSLJoinItemsIDs(items))
	})
}
