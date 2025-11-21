package reindexer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/dsl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestDSLItem struct {
	ID int `reindex:"id,,pk"`
}

type TestDSLFtItem struct {
	ID          int    `reindex:"id,,pk"`
	Description string `reindex:"description,text,dense"`
}

type TestDSLJoinItem struct {
	JID int `reindex:"jid,,pk"`
}

type TestDSLEqualPositionItem struct {
	ID          int   `reindex:"id,,pk"`
	ArrayField1 []int `reindex:"array_field_1"`
	ArrayField2 []int `reindex:"array_field_2"`
}

type TestDSLLikeConditionItem struct {
	ID        int    `reindex:"id,,pk"`
	TextField string `reindex:"text_idx"`
}

const (
	testDslNs              = "test_namespace_dsl"
	testDslNs2             = "test_namespace_dsl_2"
	testDslFtNs            = "test_namespace_dsl_ft"
	testDslJoinedNs1       = "test_namespace_dsl_joined_1"
	testDslJoinedNs2       = "test_namespace_dsl_joined_2"
	testDslJoinedNs3       = "test_namespace_dsl_joined_3"
	testDslEqualPositionNs = "test_namespace_dsl_equal_position"
	testDslLikeConditionNs = "test_namespace_dsl_like_conition"
)

func init() {
	tnamespaces[testDslNs] = TestDSLItem{}
	tnamespaces[testDslNs2] = TestDSLItem{}
	tnamespaces[testDslFtNs] = TestDSLFtItem{}
	tnamespaces[testDslJoinedNs1] = TestDSLJoinItem{}
	tnamespaces[testDslJoinedNs2] = TestDSLJoinItem{}
	tnamespaces[testDslJoinedNs3] = TestDSLJoinItem{}
	tnamespaces[testDslEqualPositionNs] = TestDSLEqualPositionItem{}
	tnamespaces[testDslLikeConditionNs] = TestDSLLikeConditionItem{}
}

var allIDs = make([]int, 100)

func fillTestDSLItem(t *testing.T, ns string, start int, count int) {
	tx := newTestTx(DB, ns)
	for i := 0; i < count; i++ {
		testItem := &TestDSLItem{ID: start + i}
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
	for i := 0; i < len(descriptions); i++ {
		testItem := &TestDSLFtItem{
			ID:          i,
			Description: descriptions[i],
		}
		if err := tx.Upsert(testItem); err != nil {
			require.NoError(t, err)
		}
	}
	tx.MustCommit()
}

func fillTestDSLJoinItems(t *testing.T, ns string, start int, count int) {
	tx := newTestTx(DB, ns)
	for i := 0; i < count; i++ {
		testItem := &TestDSLJoinItem{JID: start + i}
		if err := tx.Upsert(testItem); err != nil {
			require.NoError(t, err)
		}
	}
	tx.MustCommit()
}

func fillTestDSLEqualPositionItem(t *testing.T, ns string, start int, count int) {
	tx := newTestTx(DB, ns)
	for i := 0; i < count; i++ {
		if err := tx.Upsert(
			TestDSLEqualPositionItem{
				ID:          start + i,
				ArrayField1: []int{1 - i%2, 1, 2, 3, 100 * (i % 2)},
				ArrayField2: []int{0 + i%2, -1, -2, -3, 100},
			}); err != nil {
			require.NoError(t, err)
		}
	}
	tx.MustCommit()
}

func fillTestDSLLikeConditionItem(t *testing.T, ns string, start int, count int) {
	tx := newTestTx(DB, ns)
	for i := 0; i < count; i++ {
		if err := tx.Upsert(
			TestDSLLikeConditionItem{
				ID:        start + i,
				TextField: "test" + strconv.Itoa(i),
			}); err != nil {
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

func getTestDSLItemsIDs(items []interface{}) []int {
	resultIDs := make([]int, len(items))
	for i, v := range items {
		item := v.(*TestDSLItem)
		resultIDs[i] = item.ID
	}
	return resultIDs
}

func getTestDSLJoinItemsIDs(items []interface{}) []int {
	resultIDs := make([]int, len(items))
	for i, v := range items {
		item := v.(*TestDSLJoinItem)
		resultIDs[i] = item.JID
	}
	return resultIDs
}

func getTestDSLEqualPositionItemsIDs(items []interface{}) []int {
	resultIDs := make([]int, len(items))
	for i, v := range items {
		item := v.(*TestDSLEqualPositionItem)
		resultIDs[i] = item.ID
	}
	return resultIDs
}

func getTestDSLLikeCondItemsIDs(items []interface{}) []int {
	resultIDs := make([]int, len(items))
	for i, v := range items {
		item := v.(*TestDSLLikeConditionItem)
		resultIDs[i] = item.ID
	}
	return resultIDs
}

func execDSLTwice(t *testing.T, testF func(*testing.T, *reindexer.Query), jsonDSL string) {
	var marshaledJSON []byte
	{
		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		q.Debug(reindexer.TRACE)
		testF(t, q)
		marshaledJSON, err = json.Marshal(dslQ)
		require.NoError(t, err)
	}

	{
		var dslQ dsl.DSL
		err := json.Unmarshal(marshaledJSON, &dslQ)
		require.NoError(t, err)
		q, err := DBD.QueryFrom(dslQ)
		require.NoError(t, err)
		require.NotNil(t, q)
		q.Debug(reindexer.TRACE)
		testF(t, q)
	}
}

func checkErrorQueryFrom(t *testing.T, jsonDSL string, description string) {
	var dslQ dsl.DSL
	err := json.Unmarshal([]byte(jsonDSL), &dslQ)
	require.NoError(t, err)
	_, err = DBD.QueryFrom(dslQ)
	require.EqualError(t, err, description)
}

func TestDSLQueries(t *testing.T) {
	t.Parallel()

	fillTestDSLItem(t, testDslNs, 0, 100)
	fillTestDSLFtItems(t, testDslFtNs)
	fillTestDSLJoinItems(t, testDslJoinedNs1, 80, 40)
	fillTestDSLJoinItems(t, testDslJoinedNs2, 10, 10)
	fillTestDSLEqualPositionItem(t, testDslEqualPositionNs, 0, 10)
	fillTestDSLLikeConditionItem(t, testDslLikeConditionNs, 0, 15)

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

		_, err = json.Marshal(dslQ)
		require.NoError(t, err)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{90, 91, 92, 93, 99}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{21, 93, 99}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{21, 92, 93, 99}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			count := it.TotalCount()
			assert.Equal(t, 10, count)

			items, err := it.FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{2, 3, 4, 5, 6, 7, 8}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			items, ranks, err := it.FetchAllWithRank()
			require.NoError(t, err)

			expectedOrder := []string{"worm", "sword", "word"}
			expectedRanks := []float32{76, 76, 108}
			assert.Equal(t, expectedOrder, getTesDSLFtItemsDescr(items))
			assert.Equal(t, expectedRanks, ranks)
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{21, 92, 93, 97, 99}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			require.Equal(t, allIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{3}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)

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

			execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
				_, err := q.Exec().FetchAll()
				require.ErrorContains(t, err, fmt.Sprintf("The '%s' condition is supported only by 'sparse' or 'array' indexes", cond[1]))
			}, fmt.Sprintf(jsonDSL, cond[0]))
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{1}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.NotNil(t, explain)
			expectedIDs := []int{10, 12, 14}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.NotNil(t, explain)
			expectedIDs := []int{1, 10, 12, 14, 17, 19, 99}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.Nil(t, explain)
			expectedIDs := []int{90, 89, 88, 87, 86, 85, 84, 83, 82, 81, 80, 19, 18, 17, 16, 15}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{17, 83, 84, 85, 91, 93, 99}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{17, 18, 83, 91, 93, 99}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{11, 12, 13, 14, 15, 16, 17, 80, 81, 90, 91, 92, 93}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{85, 86, 87, 88, 97, 98}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{10, 11, 19}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{15, 16}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)

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

			execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
				_, err := q.Exec().FetchAll()
				require.ErrorContains(t, err, fmt.Sprintf("The '%s' condition is supported only by 'sparse' or 'array' indexes", cond[1]))
			}, fmt.Sprintf(jsonDSL, cond[0]))
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{15, 16, 17}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{98, 99, 95, 96, 97}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			_, err := q.Exec().FetchAll()
			require.ErrorContains(t, err, "Current query strict mode allows sort by existing fields only. "+
				"There are no fields with name 'id+2' in namespace 'test_namespace_dsl'")
		}, jsonDSL)
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.NotNil(t, explain)
			expectedIDs := []int{10, 11, 13, 14, 19}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
	})

	t.Run("dsl single subquery", func(t *testing.T) {
		// Using subquery the same way as join here
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
					"field": "id",
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "range",
								"value": [10,15]
							}
						],
						"select_filter": ["jid"]
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.NotNil(t, explain)
			expectedIDs := []int{10, 12, 14}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
	})

	t.Run("dsl single subquery with count", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl_joined_1",
			"sort": {
				"field": "jid",
				"desc": false
			},
			"explain": true,
			"filters": [
				{
					"field": "jid",
					"cond": "GT",
					"subquery": {
						"namespace": "test_namespace_dsl",
						"req_total": true
					}
				}
			]
		}
		`

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.NotNil(t, explain)
			expectedIDs := []int{101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLJoinItemsIDs(items))
		}, jsonDSL)
	})

	t.Run("dsl single subquery with max aggregation", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl_joined_1",
			"sort": {
				"field": "jid",
				"desc": false
			},
			"explain": true,
			"filters": [
				{
					"field": "jid",
					"cond": "GT",
					"subquery": {
						"namespace": "test_namespace_dsl",
						"aggregations": [
							{
								"type": "max",
								"fields": [ "id" ]
							}
						]
					}
				}
			]
		}
		`

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.NotNil(t, explain)
			expectedIDs := []int{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLJoinItemsIDs(items))
		}, jsonDSL)
	})

	t.Run("dsl single join with always true subqueries", func(t *testing.T) {
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
				},
				{
					"subquery": {
						"namespace": "test_namespace_dsl_joined_1"
					},
					"cond": "any"
				},
				{
					"subquery": {
						"namespace": "test_namespace_dsl_joined_1",
						"req_total": true
					},
					"cond": "gt",
					"value": [5]
				}
			]
		}
		`

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.NotNil(t, explain)
			expectedIDs := []int{10, 12, 14}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
	})

	t.Run("dsl single join with always false subqueries", func(t *testing.T) {
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
				},
				{
					"subquery": {
						"namespace": "test_namespace_dsl_joined_1"
					},
					"cond": "empty"
				},
				{
					"op": "OR",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_1",
						"req_total": true
					},
					"cond": "le",
					"value": [5]
				}
			]
		}
		`

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.NotNil(t, explain)
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, []int{}, getTestDSLItemsIDs(items))
		}, jsonDSL)
	})

	t.Run("dsl 2 subqueries", func(t *testing.T) {
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
					"field": "id",
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_1",
						"limit": 1,
						"offset": 0,
						"select_filter": [
							"jid"
						],
						"sort": {
							"field": "jid",
							"desc": false
						}
					}
				},
				{
					"op": "OR",
					"field": "id",
					"cond": "EQ",
					"subquery": {
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
						"sort": {
							"field": "jid",
							"desc": true
						},
						"select_filter": [ "jid" ]
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.Nil(t, explain)
			expectedIDs := []int{80, 19, 18, 17, 16, 15}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
	})

	t.Run("dsl with equal_positions", func(t *testing.T) {
		const eqPosFilter = `
		{
			"equal_positions":
			[
				{
					"positions":
					[
						"array_field_1",
						"array_field_2"
					]
				}
			]
		}`

		whereFilter := func(val1 int, val2 int) string {
			return fmt.Sprintf(
				`
				{
					"field": "array_field_1",
					"cond": "eq",
					"value": %d
				},
				{
					"field": "array_field_2",
					"cond": "eq",
					"value": %d
				}`,
				val1,
				val2,
			)
		}

		// the corresponding sql-like condition:
		// ( array_field_1 = 100 AND array_field_2 = 100 EQUAL_POSITION(array_field_1, array_field_2) )
		// AND
		// (
		//   ( array_field_1 = 1 AND array_field_2 = 0 EQUAL_POSITION(array_field_1, array_field_2) )
		//   OR
		//   ( array_field_1 = 0 AND array_field_2 = 1 EQUAL_POSITION(array_field_1, array_field_2) )
		// )
		jsonDSL := fmt.Sprintf(
			`
			{
				"namespace": "test_namespace_dsl_equal_position",
				"type": "select",
				"filters":
				[
					%[1]s,
					%[4]s,
					{
						"filters":
						[
							{
								"filters":
								[
									%[2]s,
									%[4]s
								]
							},
							{
								"op": "or",
								"filters":
								[
									%[3]s,
									%[4]s
								]
							}
						]
					}
				],
				"sort": {
					"field": "id"
				}
			}`,
			whereFilter(100, 100),
			whereFilter(1, 0),
			whereFilter(0, 1),
			eqPosFilter,
		)

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			expectedIDs := []int{1, 3, 5, 7, 9}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLEqualPositionItemsIDs(items))
		}, jsonDSL)
	})

	t.Run("dsl with like condition", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl_like_conition",
			"filters": [
				{
					"op": "AND",
					"field": "text_idx",
					"cond": "LIKE",
					"value": "%test1%"
				}
			]
		}
		`

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			it := q.MustExec()
			require.NoError(t, it.Error())
			explain, err := it.GetExplainResults()
			assert.NoError(t, err)
			assert.Nil(t, explain)
			expectedIDs := []int{1, 10, 11, 12, 13, 14}
			items, err := it.FetchAll()
			require.NoError(t, err)
			require.Equal(t, expectedIDs, getTestDSLLikeCondItemsIDs(items))
		}, jsonDSL)
	})
}

func TestDSLQueriesParsingErrors(t *testing.T) {
	t.Parallel()

	fillTestDSLItem(t, testDslNs2, 0, 100)
	fillTestDSLJoinItems(t, testDslJoinedNs3, 10, 10)

	t.Run("dsl unsupported fields will be ignored on unmarshalling with strict mode disabled", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl_2",
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
						"namespace": "test_namespace_dsl_joined_3",
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
					"namespace": "test_namespace_dsl_joined_3",
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

		execDSLTwice(t, func(t *testing.T, q *reindexer.Query) {
			items, err := q.MustExec().FetchAll()
			require.NoError(t, err)
			expectedIDs := []int{11, 14, 15}
			require.Equal(t, expectedIDs, getTestDSLItemsIDs(items))
		}, jsonDSL)
	})

	t.Run("dsl unsupported fields will cause an error on unmarshalling with dsl strict mode enabled", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl_2",
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
						"namespace": "test_namespace_dsl_joined_3",
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
					"namespace": "test_namespace_dsl_joined_3",
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

		dsl.EnableStrictMode()
		defer dsl.DisableStrictMode()
		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.EqualError(t, err, `json: unknown field "some_unsupported_0"`)
	})

	t.Run("dsl unsupported fields will cause an error on DecodeJSON with dsl strict mode enabled", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"some_unsupported_0": 123,
			"filters": [
				{
					"field": "id",
					"cond": "set",
					"value": [1,9,15,11,14]
				}
			]
		}
		`

		dsl.EnableStrictMode()
		defer dsl.DisableStrictMode()
		dslQ, err := dsl.DecodeJSON([]byte(jsonDSL))
		require.EqualError(t, err, `json: unknown field "some_unsupported_0"`)
		require.Nil(t, dslQ)
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

	t.Run("dsl filters can not be without 'cond', 'field' and 'value'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"subquery": {
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "SET",
								"value": [10,15]
							}
						],
						"select_filter": ["jid"]
					}
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.EqualError(t, err, "cond type '' not found")
	})

	t.Run("dsl filter can not contain 'cond' without 'field', 'value' or 'subquery'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"cond": "EQ",
					"filters": [
						{
							"field": "jid",
							"cond": "EQ",
							"value": [10]
						}
					]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.EqualError(t, err, "unable to use Cond without Field/Subquery/Value")
	})

	t.Run("dsl filter can not contain 'value' without 'field' or 'subquery'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"value": [1],
					"filters": [
						{
							"field": "jid",
							"cond": "EQ",
							"value": [10]
						}
					]
				}
			]
		}
		`

		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.EqualError(t, err, "unable to use Value without Field/Subquery")
	})

	t.Run("dsl subquery can not be with any/empty 'cond' and not empty 'value'", func(t *testing.T) {
		for _, cond := range []string{"any", "empty"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"value": [1],
						"cond": "%s",
						"subquery": {
							"namespace": "test_namespace_dsl_joined_2",
							"filters": [
								{
									"field": "jid",
									"cond": "EQ",
									"value": [10]
								}
							]
						}
					}
				]
			}
			`

			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, cond)), &dslQ)
			require.EqualError(t, err, fmt.Sprintf("filter expects no arguments or null for '%s' condition", cond))
		}
	})

	t.Run("dsl subquery can not contain prohibited fields", func(t *testing.T) {
		for _, field := range [][]string{
			{"select_functions", `["description = highlight(<,>)"]`, "select_functions"},
			{"update_fields", `[{"type": "value", "name": "id", "values": [1]}]`, "update_fields"},
			{"drop_fields", `["id"]`, "drop_fields"},
			{"select_with_rank", "true", "select_with_rank"},
			{"type", `"update"`, "type"},
		} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"value": ["<text>"],
						"cond": "EQ",
						"subquery": {
							"namespace": "test_namespace_dsl_ft",
							"select_filter": ["description"],
							"%s": %s
						}
					}
				]
			}
			`

			dsl.EnableStrictMode()
			defer dsl.DisableStrictMode()
			var dslQ dsl.DSL
			err := json.Unmarshal([]byte(fmt.Sprintf(jsonDSL, field[0], field[1])), &dslQ)
			require.EqualError(t, err, fmt.Sprintf(`json: unknown field "%s"`, field[2]))
		}
	})

	t.Run("dsl subquery can not contain merge", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_1",
						"select_filter": ["jid"],
                        "merge_queries": [
							{
								"namespace": "test_namespace_dsl_joined_2",
								"select_filter": ["jid"],
								"filters": [
									{
										"field": "id",
										"cond": "EQ",
										"value": [10]
									}
								]
                        	}
						]
					}
				}
			]
		}
		`

		dsl.EnableStrictMode()
		defer dsl.DisableStrictMode()
		var dslQ dsl.DSL
		err := json.Unmarshal([]byte(jsonDSL), &dslQ)
		require.EqualError(t, err, `json: unknown field "merge_queries"`)
	})
}

func TestCreateDSLQueriesErrors(t *testing.T) {
	t.Parallel()

	t.Run("dsl subquery can not be without 'namespace'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "EQ",
					"subquery": {
						"filters": [
							{
								"field": "jid",
								"cond": "SET",
								"value": [10,15]
							}
						],
						"select_filter": ["jid"]
					}
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL, "rq: empty namespace name in subquery")
	})

	t.Run("dsl filter for subquery can not be without 'field' and 'value'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "SET",
								"value": [10,15]
							}
						],
						"select_filter": ["jid"]
					}
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL,
			"rq: filter: subquery with condition EQ can not have nil target Value and empty field name")
	})

	t.Run("dsl filter for subquery can not be with both 'field' and 'value'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"value": [1,2],
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_2",
						"filters": [
							{
								"field": "jid",
								"cond": "SET",
								"value": [10,15]
							}
						],
						"select_filter": ["jid"]
					}
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL, "rq: filter: both field name and value in filter with subquery "+
			"are not empty (expecting exactly one of them)")
	})

	t.Run("dsl subquery can not be without 'select_filter'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_1",
						"filters": [
							{
								"field": "jid",
								"cond": "EQ",
								"value": [10]
							}
						]
					}
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL, "rq: subquery with Cond 'EQ' and without aggregations must have exactly 1 select_filter")
	})

	t.Run("dsl subquery can not be with empty 'select_filter'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "SET",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_1",
						"filters": [
							{
								"field": "jid",
								"cond": "EQ",
								"value": [10]
							}
						],
						"select_filter": []
					}
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL, "rq: subquery with Cond 'SET' and without aggregations must have exactly 1 select_filter")
	})

	t.Run("dsl subquery can not have multiple 'select_filters'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_ft",
						"filters": [
							{
								"field": "id",
								"cond": "EQ",
								"value": [1]
							}
						],
						"select_filter": ["id", "description"]
					}
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL, "rq: subquery can not have multiple select_filter's: [id description]")
	})

	t.Run("dsl subquery can not be with any/empty 'cond' and prohibited fields", func(t *testing.T) {
		for _, cond := range []string{"any", "empty"} {
			for _, field := range [][]string{{"limit", "1", "limit"}, {"offset", "1", "offset"},
				{"select_filter", `["jid"]`, "select filter"}, {"req_total", "true", "ReqTotal"},
				{"aggregations", `[{"fields": ["jid"], "type": "MAX"}]`, "aggregations"}} {
				const jsonDSL = `
				{
					"namespace": "test_namespace_dsl",
					"filters": [
						{
							"value": [],
							"cond": "%s",
							"subquery": {
								"namespace": "test_namespace_dsl_joined_2",
								"filters": [
									{
										"field": "jid",
										"cond": "EQ",
										"value": [10]
									}
								],
								"select_filter": ["jid"],
								"%s": %s
							}
						}
					]
				}
				`
				checkErrorQueryFrom(t, fmt.Sprintf(jsonDSL, cond, field[0], field[1]),
					fmt.Sprintf("rq: filter: subquery with condition Any or Empty can not contain %s", field[2]))
			}
		}
	})

	t.Run("dsl subquery can not be with any/empty 'cond' and 'select_filter'", func(t *testing.T) {
		for _, cond := range []string{"any", "empty"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"Value": [],
						"cond": "%s",
						"subquery": {
							"namespace": "test_namespace_dsl_joined_2",
							"filters": [
								{
									"field": "jid",
									"cond": "EQ",
									"value": [10]
								}
							],
							"select_filter": ["jid"]
						}
					}
				]
			}
			`
			checkErrorQueryFrom(t, fmt.Sprintf(jsonDSL, cond),
				"rq: filter: subquery with condition Any or Empty can not contain select filter")
		}
	})

	t.Run("dsl subquery can not be with any/empty 'cond' and 'field'", func(t *testing.T) {
		for _, cond := range []string{"any", "empty"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"field": "id",
						"cond": "%s",
						"subquery": {
							"namespace": "test_namespace_dsl_joined_2",
							"filters": [
								{
									"field": "jid",
									"cond": "EQ",
									"value": [10]
								}
							]
						}
					}
				]
			}
			`
			checkErrorQueryFrom(t, fmt.Sprintf(jsonDSL, cond),
				"rq: filter: subquery with condition Any or Empty can not have Field in the filter")
		}
	})

	t.Run("dsl subquery can not be with eq/set 'cond' and empty 'value'", func(t *testing.T) {
		for _, cond := range []string{"eq", "set"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"value": [],
						"cond": "%s",
						"subquery": {
							"namespace": "test_namespace_dsl_joined_2",
							"filters": [
								{
									"field": "jid",
									"cond": "EQ",
									"value": [10]
								}
							],
							"select_filter": ["jid"]
						}
					}
				]
			}
			`
			checkErrorQueryFrom(t, fmt.Sprintf(jsonDSL, cond),
				fmt.Sprintf("rq: filter: subquery with condition %s can not have nil target Value and empty field name", cond))
		}
	})

	t.Run("dsl subquery can not be with unsupported aggregations", func(t *testing.T) {
		for _, agg := range []string{"distinct", "facet"} {
			const jsonDSL = `
			{
				"namespace": "test_namespace_dsl",
				"filters": [
					{
						"field": "id",
						"cond": "EQ",
						"subquery": {
							"namespace": "test_namespace_dsl_joined_2",
							"aggregations": [
								{
									"type": "%s",
									"fields": ["jid"]
								}
							]
						}
					}
				]
			}
			`
			checkErrorQueryFrom(t, fmt.Sprintf(jsonDSL, agg),
				fmt.Sprintf("rq: filter: unsupported aggregation for subquery: '%s'", agg))
		}
	})

	t.Run("dsl subquery can not be with both 'aggregations' and 'select_filter'", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_2",
						"select_filter": ["jid"],
						"aggregations": [
							{
								"type": "max",
								"fields": ["jid"]
							}
						]
					}
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL,
			"rq: subquery can not have select_filter and aggregations at the same time")
	})

	t.Run("dsl subquery can not contain more than 1 aggregation", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_2",
						"select_filter": ["jid"],
						"req_total": true,
						"aggregations": [
							{
								"type": "max",
								"fields": ["jid"]
							}
						]
					}
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL,
			"rq: filter: subquery can not contain more than 1 aggregation / count request")
	})

	t.Run("dsl filters can not contain 'field' and 'join_query' at the same time", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "EQ",
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
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
		checkErrorQueryFrom(t, jsonDSL,
			"rq: dsl filter can not contain both 'field' and 'join_query' at the same time")
	})

	t.Run("dsl filters can not contain 'filters' and 'join_query' at the same time", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"join_query": {
						"type": "inner",
						"namespace": "test_namespace_dsl_joined_2",
						"on": [
							{
								"left_field": "id",
								"right_field": "jid",
								"cond": "EQ"
							}
						]
					},
					"filters": [
						{
							"field": "jid",
							"cond": "EQ",
							"value": [10]
						}
					]
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL,
			"rq: dsl filter can not contain both 'filters' and 'join_query' at the same time")
	})

	t.Run("dsl filters can not contain 'field' and 'filters' at the same time", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "EQ",
					"filters": [
						{
							"field": "jid",
							"cond": "EQ",
							"value": [10]
						}
					]
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL,
			"rq: dsl filter can not contain both 'field' and 'filters' at the same time")
	})

	t.Run("dsl subquery filters can not contain join_query", func(t *testing.T) {
		const jsonDSL = `
		{
			"namespace": "test_namespace_dsl",
			"filters": [
				{
					"field": "id",
					"cond": "EQ",
					"subquery": {
						"namespace": "test_namespace_dsl_joined_1",
						"filters": [
							{
								"join_query": {
									"type": "inner",
									"namespace": "test_namespace_dsl_joined_2",
									"on": [
										{
											"left_field": "jid",
											"right_field": "jid",
											"cond": "EQ"
										}
									]
								}
							}
						],
						"select_filter": ["jid"]
					}
				}
			]
		}
		`
		checkErrorQueryFrom(t, jsonDSL, "rq: nested join quieries are not supported")
	})

	const wrongJsonDSLTmplt = `
		{
			"namespace": "test_namespace_dsl_equal_position",
			"type": "select",
			"filters":
			[
				{
					"op": "and",
					"cond": "eq",
					"field": "f1",
					"value": 0
				},
				{
					"op": "and",
					"cond": "eq",
					"field": "f2",
					"value": 0
				},
				%s
			]
		}`

	testCase := func(errorMessage string, equalPositions string) {
		checkErrorQueryFrom(t, fmt.Sprintf(wrongJsonDSLTmplt, equalPositions), errorMessage)
	}

	t.Run("dsl equal_positions must contain at least 2 arguments", func(t *testing.T) {
		testCase(
			"rq: filter: 'equal_positions' is supposed to have at least 2 arguments. Arguments: [f1]",
			`{
				"equal_positions":
				[
					{
						"positions":
						[
							"f1"
						]
					}
				]

			}`,
		)
	})

	t.Run("dsl equal_positions fields must be specified in other filters", func(t *testing.T) {
		testCase(
			"rq: filter: fields from 'equal_positions'-filter must be specified in the 'where'-conditions of other filters in the current bracket",
			`{
				"equal_positions":
				[
					{
						"positions":
						[
							"f1",
							"f2",
							"f3"
						]
					}
				]

			}`,
		)
	})

	t.Run("dsl equal_positions should not contain extra fields", func(t *testing.T) {
		testCase(
			"rq: filter: filter with 'equal_positions'-field should not contain any other fields besides it",
			`{
				"equal_positions":
				[
					{
						"positions":
						[
							"f1",
							"f2"
						]
					}
				],
				"value": 100,
				"cond": "eq",
				"field": "f1"
			}`,
		)
	})

	t.Run("dsl equal_positions fields should be represented using AND operation only", func(t *testing.T) {
		testCase(
			"rq: filter: only AND operation allowed for equal position; equal position field with not AND operation: 'f3'; equal position fields: [f1 f2 f3]",
			`{
				"op": "or",
				"cond": "eq",
				"field": "f3",
				"value": 0
			},
			{
				"equal_positions":
				[
					{
						"positions":
						[
							"f1",
							"f2",
							"f3"
						]
					}
				]

			}`,
		)
	})
}
