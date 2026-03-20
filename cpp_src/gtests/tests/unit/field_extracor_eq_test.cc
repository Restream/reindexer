#include <gtest/gtest.h>
#include "field_extracor_eq_api.h"

TEST_F(FieldExtractorEqApi, ObjectInArrayTwoField) {
	const std::string_view json = R"#(
		{
			"id":0,
			"filters": [
				{"project": "wink", "countries": ["ru", "am"]},
				{"project":"dns", "countries":["ru"]}
			]
		}
	)#";

	{
		const std::string_view pathString = "filters[#].project";
		SCOPED_TRACE(pathString);
		const std::vector<reindexer::VariantArray> resultVals = {{reindexer::Variant("wink")}, {reindexer::Variant("dns")}};
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "filters[#].countries";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
				reindexer::Variant("ru"), reindexer::Variant("am")
			},
			{
				reindexer::Variant("ru")
			}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}

	{
		const std::string_view pathString = "filters[*].countries[#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
		 		reindexer::Variant("ru"),
		 		reindexer::Variant("ru"),
			},
			{
		 		reindexer::Variant("am")
		 	}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
}

TEST_F(FieldExtractorEqApi, ObjectInArrayOneField) {
	const std::string_view json = R"#(
		{
			"id":0,
			"arr": [
				{
					"field": [1,2]
				},
				{},
				{
					"field": 8
				}
			]
		}
	)#";
	{
		const std::string_view pathString = "id[#]";
		const std::vector<reindexer::VariantArray> resultVals = {};
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[#].field";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), i64v(2)},
			{},
			{i64v(8)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[*].field[#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1)},
			{i64v(2)}
			};
		// clang-format on

		Test(json, pathString, resultVals);
	}
}

TEST_F(FieldExtractorEqApi, ObjectInArrayWithSubArray) {
	const std::string_view json = R"#(
		{
			"id":1,
			"arr_root": [
				{
					"obj_nested": {
						"arr_nested": [
							{
								"field": 1
							},
							{
								"field": [5,3]
							}
						]
					}
				},
				{
				},
				{
					"obj_nested": {
						"arr_nested": [
							{
								"field": null
							},
							{
								"field": [4,7]
							}
						]
					}
				}
			]
		}
	)#";
	{
		const std::string_view pathString = "arr_root[*].obj_nested.arr_nested[*].field[#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(5), i64v(4)},
		 	{i64v(3), i64v(7)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr_root[*].obj_nested.arr_nested[#].field";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), reindexer::Variant()},
			{i64v(5), i64v(3), i64v(4), i64v(7)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr_root[#].obj_nested.arr_nested[*].field";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), i64v(5), i64v(3)},
			{},
			{reindexer::Variant(), i64v(4), i64v(7)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
}

TEST_F(FieldExtractorEqApi, ObjectInArrayManyFields) {
	const std::string_view json = R"#(
		{
			"id":1,
			"arr_root": [
			{
				"obj_nested": {
					"field1":1,
					"field2":10,
					"field3":{
						"field4":100,
						"field5":1000
					}
				}
			},
			{},
			{
				"obj_nested": {
					"field10":20,
					"field11":200
				}
			}
			]
		}
	)#";
	{
		const std::string_view pathString = "arr_root[#].obj_nested";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
		 		i64v(1), i64v(10),
				i64v(100), i64v(1000)
		 	},
			{},
		 	{
		 		i64v(20), i64v(200)
		 	}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
}

TEST_F(FieldExtractorEqApi, MixedArrayElementsCheckArray) {
	const std::string_view json = R"#(
		{
			"id":1,
			"arr": [
				{
					"field1":1,
					"field2":2
				},
				{},
				10,
				[
					[101,102],
					[103,104]
				],
				[201,202],
				{
					"field10":
						[
							[1001,1002],
							[1011,1012]
						]
				}
			]
		}
	)#";
	{
		const std::string_view pathString = "arr[#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
		 		i64v(1), i64v(2),
		 	},
			{},
		 	{
		 		i64v(10)
		 	},
		 	{
		 		i64v(101), i64v(102),
				i64v(103), i64v(104)
		 	},
		 	{
		 		i64v(201), i64v(202)
		 	},
		 	{
		 		i64v(1001), i64v(1002),
				i64v(1011), i64v(1012)
		 	}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[*][*][#]";
		const std::vector<reindexer::VariantArray> resultVals = {
			{
				i64v(101),
				i64v(103),
			},
			{
				i64v(102),
				i64v(104),
			},

		};

		Test(json, pathString, resultVals);
	}
}

TEST_F(FieldExtractorEqApi, MixedArrayElementsCheckObject) {
	// clang-format off
	const std::string_view json = R"#(
		{
			"id":1,
			"arr": [
				{
					"field1":1,
					"field2":2
				},
				10,
				[
					[
						{"val1":90, "val2":190},
						{"val1":91, "val2":191},
						{}
					],
					[103,104]
				],
				[201,202],
				{
					"field10": [
						[1001,1002],
						[1011,1012]
					]
				}
			]
		}
	)#";
	// clang-format on
	{
		const std::string_view pathString = "arr[*][*][#].val1";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(90)},
			{i64v(91)},
			{}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
}

TEST_F(FieldExtractorEqApi, NestedArrays) {
	const std::string_view json = R"#(
		{
			"id":1,
			"arr": [ 
				[
					1,
					[2,3]
				], 
				[
					4,
					[5]
				], 
				[] 
			],
			"field1": [
				[1,2],
				[5,6]
			],
			"field2": [
				[
					[1,2,3,4],
					[5,6,7,8,9]
				],
				[
					[10,11,12]
				]
			]
		}
	)#";
	{
		const std::string_view pathString = "arr[#]";
		SCOPED_TRACE(pathString);
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), i64v(2), i64v(3)},
			{i64v(4), i64v(5)},
			{}
		};
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[*][#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), i64v(4)},
			{i64v(2), i64v(3), i64v(5)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[#][*]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), i64v(2), i64v(3)},
			{i64v(4), i64v(5)},
			{}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[#][*][*]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
	 		{i64v(2), i64v(3)},
	 		{i64v(5)},
	 		{}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[*][#][*]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
	 		{},
	 		{i64v(2), i64v(3), i64v(5)}

		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[*][*][#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(2), i64v(5)},
			{i64v(3)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "field1[#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), i64v(2)},
			{i64v(5), i64v(6)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "field1[#][*]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), i64v(2)},
			{i64v(5), i64v(6)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "field1[*][#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), i64v(5)},
			{i64v(2), i64v(6)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "field2[*][#][*]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(1), i64v(2), i64v(3), i64v(4), i64v(10), i64v(11), i64v(12)},
			{i64v(5), i64v(6), i64v(7), i64v(8), i64v(9)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
}

TEST_F(FieldExtractorEqApi, DeepNestedArrays) {
	// clang format off
	const std::string_view json = R"#(
		{
			"id":1,
			"arr":	
			[
				[1,12, 78, 33, 91, 27, 5, 64], 
				[
					[
						[3, 45, 89, 73],
						[3, 19, 56, 82],
						[
							[4, 94, 8, 61],
							[4, 37, 42, 15],
							[
								[5, 76, 23, 68],
								[5, 31, 54, 97]
							]
						]
					],
					[
						[3, 22, 84, 14],
						[3, 96, 38, 71],
						[3, 29, 53, 88]
					]
				],
				[1, 67, 38, 51, 76, 23],
				[
					[
						[3, 34, 92, 17],
						[3, 63, 41, 75]
					],
					[
						[
							[4, 81, 26, 59],
							[4, 43, 77, 95]
						],
						[
							[4, 11, 68, 34],
							[4, 72, 49, 86],
							[
								[5, 18, 93, 57, 89],
								[5, 39, 84]
							]
						]
					]
				],
				[1, 98, 15, 47, 82, 30, 56, 91, 24]
			]	
		}
	)#";
	// clang format on
	{
		const std::string_view pathString = "arr[*][*][*][*][*][#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{ i64v(5),  i64v(5),  i64v(5),  i64v(5)    },
			{ i64v(76), i64v(31), i64v(18), i64v(39)   },
			{ i64v(23), i64v(54), i64v(93), i64v(84)   },
			{ i64v(68), i64v(97), i64v(57),            },
			{                     i64v(89)             }
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	// clang format on
	{
		const std::string_view pathString = "arr[#][*][*][*][*][*]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{},
			{
					i64v(5), i64v(76), i64v(23), i64v(68),
					i64v(5), i64v(31), i64v(54), i64v(97)
			},
			{},
			{
					i64v(5), i64v(18), i64v(93), i64v(57), i64v(89),
					i64v(5), i64v(39), i64v(84)
			},
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[*][*][*][*][#][*]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
				i64v(5), i64v(76), i64v(23), i64v(68),
				i64v(5), i64v(18), i64v(93), i64v(57), i64v(89)
			},
			{
				i64v(5), i64v(31), i64v(54), i64v(97),
				i64v(5), i64v(39), i64v(84)
			}

		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr[*][*][#][*]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
	  		{
				i64v(3), i64v(45), i64v(89), i64v(73),
				i64v(3), i64v(22), i64v(84), i64v(14),
				i64v(3), i64v(34), i64v(92), i64v(17),
				i64v(4), i64v(81), i64v(26), i64v(59), i64v(4), i64v(43), i64v(77), i64v(95)
			},
			{
				i64v(3), i64v(19), i64v(56), i64v(82),
				i64v(3), i64v(96), i64v(38), i64v(71),
				i64v(3), i64v(63), i64v(41), i64v(75),
				i64v(4), i64v(11), i64v(68), i64v(34),
				i64v(4), i64v(72), i64v(49), i64v(86),
				i64v(5), i64v(18), i64v(93), i64v(57), i64v(89),
				i64v(5), i64v(39), i64v(84)
			},
			{
				i64v(4), i64v(94), i64v(8), i64v(61), i64v(4), i64v(37), i64v(42), i64v(15),
				i64v(5), i64v(76), i64v(23), i64v(68), i64v(5), i64v(31), i64v(54), i64v(97),
				i64v(3), i64v(29), i64v(53), i64v(88),
			}

		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
}

TEST_F(FieldExtractorEqApi, ComplexNestedArrays) {
	// clang-format off
	const std::string_view json = R"#(
		{
			"id": 1,
			"arr_root": [
				{
					"obj": {
						"arr": [
							{
								"field1": 1,
								"field2": 1
							},
							{
								"field1": [2, 5],
								"field2": [5, 2]
							},
							{
								"field1": [100]
							},
							{
								"field2": [100]
							}
						]
					}
				},
				{
					"obj": {"arr": [{"field1": 33}]}
				},
				{
					"obj": {"arr": [{"field2": 33}]}
				},
				{},
				{
					"obj": {
						"arr": [
							{
								"field1": null,
								"field2": null
							},
							{
								"field1": [4, 10],
								"field2": [4, 10]
							}
						]
					}
				}
			]
		}
	)#";
	// clang-format on	
	{
		const std::string_view pathString = "arr_root[*].obj.arr[*].field1[#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(2),i64v(100),i64v(4)},
			{i64v(5),i64v(10)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr_root[*].obj.arr[#].field1";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
					{i64v(1), i64v(33),reindexer::Variant()}, 
					{i64v(2), i64v(5), i64v(4),i64v(10)}, 
					{i64v(100)},
					{}
				};
		// clang-format on
		Test(json, pathString, resultVals);
	}
}

TEST_F(FieldExtractorEqApi, ComplexNestedArraysWithAdditionalDepth) {
	// clang format off
	const std::string_view json = R"#(
		{
			"id": 1,
			"arr_root": [
				{
					"obj":
						{
							"arr": [
								{
									"field1": 1,
									"field2": 1
								},
								{
									"field1": [
										2,
										[5, 6]
									],
									"field2": [
										5,
										[2, 6]
									]
								},
								{
									"field1": [
										[100]
									]
								},
								{
									"field2": [
										[100]
									]
								}
							]
						}
				},
				{
					"obj": {"arr": [{"field1": 33}]
							}
				},
				{
					"obj": {"arr": [{"field2": 33}]}
				},
				{},
				{
					"obj": {
						"arr": [
							{
								"field1": null,
								"field2": null
							},
							{
								"field1":
									[
										4,
										[10, [11]]
									],
								"field2":
									[
										4,
										[10, 11]
									]
							}
						]
					}
				}
			]
		}
	)#";
	// clang format on
	{
		const std::string_view pathString = "arr_root[*].obj.arr[*].field1[#][*]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{i64v(100)},
			{i64v(5), i64v(6), i64v(10), i64v(11)}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
}
