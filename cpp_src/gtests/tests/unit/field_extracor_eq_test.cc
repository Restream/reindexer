#include "field_extracor_eq_api.h"

TEST_F(FieldExtractorEqApi, ObjectInArrayTwoField) {
	const std::string_view json = R"#({
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
		const std::string_view pathString = "filters.countries[#]";
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
		          {
		          },
		          {
		              "field": 8
		          }
		      ]
		  }
		)#";
	{
		const std::string_view pathString = "arr[#].field";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
				reindexer::Variant(int64_t(1)), reindexer::Variant(int64_t(2))
			},
			{},
			{
				reindexer::Variant(int64_t(8))
			}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr.field[#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
				{reindexer::Variant(int64_t(1)), reindexer::Variant(int64_t(8))},
				{reindexer::Variant(int64_t(2))}
			}
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
		const std::string_view pathString = "arr_root.obj_nested.arr_nested.field[#]";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
				reindexer::Variant(int64_t(1)), reindexer::Variant(int64_t(5)), reindexer::Variant(),reindexer::Variant(int64_t(4))
		 	},
		 	{
		 		reindexer::Variant(int64_t(3)),	reindexer::Variant(int64_t(7))
		 	}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr_root.obj_nested.arr_nested[#].field";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
				reindexer::Variant(int64_t(1)), reindexer::Variant()
			},
			{
				reindexer::Variant(int64_t(5)), reindexer::Variant(int64_t(3)),reindexer::Variant(int64_t(4)),reindexer::Variant(int64_t(7))
			}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr_root.obj_nested[#].arr_nested.field";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
				reindexer::Variant(int64_t(1)), reindexer::Variant(int64_t(5)), reindexer::Variant(int64_t(3)),
				reindexer::Variant(),	reindexer::Variant(int64_t(4)), reindexer::Variant(int64_t(7)),
			}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
	{
		const std::string_view pathString = "arr_root[#].obj_nested.arr_nested.field";
		SCOPED_TRACE(pathString);
		// clang-format off
		const std::vector<reindexer::VariantArray> resultVals = {
			{
				reindexer::Variant(int64_t(1)), reindexer::Variant(int64_t(5)), reindexer::Variant(int64_t(3))
			},
			{},
			{
				reindexer::Variant(), reindexer::Variant(int64_t(4)), reindexer::Variant(int64_t(7))
			}
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
				{
				},
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
		 		reindexer::Variant(int64_t(1)), reindexer::Variant(int64_t(10)),
				reindexer::Variant(int64_t(100)),reindexer::Variant(int64_t(1000))
		 	},
			{},
		 	{
		 		reindexer::Variant(int64_t(20)),	reindexer::Variant(int64_t(200))
		 	}
		};
		// clang-format on
		Test(json, pathString, resultVals);
	}
}
