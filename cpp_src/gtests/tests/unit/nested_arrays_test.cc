#include "nested_arrays.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"

void NestedArraysApi::SetUp() {
	auto dir = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "/nested_arrays_test");
	std::ignore = reindexer::fs::RmDirAll(dir);
	auto err = rt.reindexer->Connect("builtin://" + dir);
	ASSERT_TRUE(err.ok()) << err.what();

	rt.OpenNamespace(nsName_);
	rt.DefineNamespaceDataset(nsName_, {IndexDeclaration{idIdxName_, "hash", "int", IndexOpts{}.PK(), 0}});
}

std::string NestedArraysApi::newItemJson(int id) {
	constexpr auto Empty = reindexer::TagName::Empty();
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder builder{ser};
		builder.Put(idIdxName_, id);
		{
			const auto fillHetero = [&](reindexer::JsonBuilder& array) {
				array.Put(Empty, rand());
				// nested int array
				const auto intArray = RandIntVector(rand() % 5, 0, 1000);
				array.Array(Empty, std::span(intArray));
				array.Put(Empty, RandString());
				// nested str array
				const auto strArray = RandStrVector(rand() % 5);
				array.Array(Empty, std::span(strArray));
				const std::function<void(reindexer::JsonBuilder&, size_t)> addHeteroNestedArray = [&](reindexer::JsonBuilder& array,
																									  size_t level) {
					auto heteroNestedArray = array.Array(Empty);
					for (size_t i = 0, s = rand() % 5; i < s; ++i) {
						if (level < 3 && rand() % 2 == 0) {
							addHeteroNestedArray(heteroNestedArray, level + 1);
						} else if (rand() % 2 == 0) {
							heteroNestedArray.Put(Empty, rand());
						} else {
							heteroNestedArray.Put(Empty, RandString());
						}
					}
				};
				addHeteroNestedArray(array, 0);
			};
			auto heteroArray = builder.Array(heteroArrayFldName_);
			fillHetero(heteroArray);
			auto nestedHeteroArray = heteroArray.Array(Empty);
			fillHetero(nestedHeteroArray);
			auto nestedNestedHeteroArray = nestedHeteroArray.Array(Empty);
			fillHetero(nestedNestedHeteroArray);
		}
		{  // in obj
			auto objArray = builder.Array(objectArrayFldName_);
			for (size_t i = 0, s = 3; i < s; ++i) {
				auto obj = objArray.Object();
				if (i % 2 == 0) {
					obj.Put("arr", rand() % 1000);
					obj.Put("arr_sparse", rand() % 1000);
				} else {
					const auto intArray = RandIntVector(3, 0, 1000);
					obj.Array("arr", std::span(intArray));
					obj.Array("arr_sparse", std::span(intArray));
				}
			}
		}
		const auto fillStrIdx = [&](std::string_view idxName) {
			const auto fillStr = [&](reindexer::JsonBuilder& array) {
				for (int i = 0, s = rand() % 3; i < s; ++i) {
					array.Put(Empty, RandString());
					const auto arrValues = RandStrVector(rand() % 5);
					array.Array(Empty, std::span(arrValues));
				}
			};
			auto strArray = builder.Array(idxName);
			fillStr(strArray);
			auto nestedStrArray = strArray.Array(Empty);
			fillStr(nestedStrArray);
			auto nestedNestedStrArray = nestedStrArray.Array(Empty);
			fillStr(nestedNestedStrArray);
		};
		fillStrIdx(strArrayIdxName_);
		fillStrIdx(strSparseArrayIdxName_);
		const auto fillIntIdx = [&](std::string_view idxName) {
			const auto fillInt = [&](reindexer::JsonBuilder& array) {
				for (int i = 0, s = rand() % 3; i < s; ++i) {
					array.Put(Empty, rand());
					const auto arrValues = RandIntVector(rand() % 5, 0, 1000);
					array.Array(Empty, std::span(arrValues));
				}
			};
			auto intArray = builder.Array(idxName);
			fillInt(intArray);
			auto nestedIntArray = intArray.Array(Empty);
			fillInt(nestedIntArray);
			auto nestedNestedIntArray = nestedIntArray.Array(Empty);
			fillInt(nestedNestedIntArray);
		};
		fillIntIdx(intArrayIdxName_);
		fillIntIdx(intSparseArrayIdxName_);
	}
	return std::string{ser.Slice()};
}

void NestedArraysApi::checkData() const {
	auto qr = rt.Select(reindexer::Query(nsName_));
	ASSERT_EQ(qr.Count(), jsons_.size());
	int id = 0;
	for (auto& item : qr) {
		reindexer::WrSerializer ser;
		const auto err = item.GetJSON(ser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(ser.Slice(), jsons_[id]);
		++id;
	}
}

void NestedArraysApi::insertItems(int count, auto newItemJsonFn) {
	for (int id = jsons_.size(), end = id + count; id < end; ++id) {
		std::string json = newItemJsonFn(id);
		rt.UpsertJSON(nsName_, json);
		jsons_.emplace_back(std::move(json));
	}
	checkData();
}

TEST_F(NestedArraysApi, Create) {
	const auto insert = [this]() { insertItems(20, [this](int id) { return newItemJson(id); }); };
	// insert not indexed arrays
	insert();
	// add indexes with data
	rt.AddIndex(nsName_, reindexer::IndexDef{nestedArrayIdxName_, {objectArrayFldName_ + ".arr"}, "hash", "int", IndexOpts().Array()});
	checkData();
	insert();
	rt.AddIndex(
		nsName_,
		reindexer::IndexDef{nestedArraySparseIdxName_, {objectArrayFldName_ + ".arr_sparse"}, "hash", "int", IndexOpts().Array().Sparse()});
	checkData();
	insert();
	rt.AddIndex(nsName_, reindexer::IndexDef{strArrayIdxName_, {strArrayIdxName_}, "hash", "string", IndexOpts().Array()});
	checkData();
	insert();
	rt.AddIndex(nsName_,
				reindexer::IndexDef{strSparseArrayIdxName_, {strSparseArrayIdxName_}, "hash", "string", IndexOpts().Array().Sparse()});
	checkData();
	insert();
	rt.AddIndex(nsName_, reindexer::IndexDef{intArrayIdxName_, {intArrayIdxName_}, "hash", "int", IndexOpts().Array()});
	checkData();
	insert();
	rt.AddIndex(nsName_,
				reindexer::IndexDef{intSparseArrayIdxName_, {intSparseArrayIdxName_}, "hash", "int", IndexOpts().Array().Sparse()});
	checkData();
	// insert indexed arrays
	insert();
	// drop indexes
	rt.DropIndex(nsName_, objectArrayFldName_);
	rt.DropIndex(nsName_, strArrayIdxName_);
	rt.DropIndex(nsName_, intArrayIdxName_);
	checkData();
	// insert data after indexes drop
	insert();
}

std::string NestedArraysApi::makeUpdateItemJson(int id, const std::optional<std::string>& intValue,
												const std::optional<std::string>& strValue) {
	std::stringstream ss;
	ss << "{\"" << idIdxName_ << "\":" << id;
	if (intValue) {
		for (const auto& fldName : {intArrayFld_, intArrayIdxName_, intSparseArrayIdxName_}) {
			ss << ",\"" << fldName << "\":" << *intValue;
		}
	}
	if (strValue) {
		for (const auto& fldName : {strArrayFld_, strArrayIdxName_, strSparseArrayIdxName_}) {
			ss << ",\"" << fldName << "\":" << *strValue;
		}
	}
	const auto& heteroValue = (id % 2 ? intValue : strValue);
	if (heteroValue) {
		ss << ",\"" << heteroArrayFldName_ << "\":" << *heteroValue;
	}
	ss << '}';
	return ss.str();
}

void NestedArraysApi::addItemForUpdate(std::optional<std::string> intValue, std::optional<std::string> strValue) {
	const int id = jsons_.size();
	jsons_.emplace_back(makeUpdateItemJson(id, intValue, strValue));
	rt.UpsertJSON(nsName_, jsons_.back());
	checkData();
	lastIntValue_ = std::move(intValue);
	lastStrValue_ = std::move(strValue);
}

void NestedArraysApi::set(int id, const std::string& fieldName, const std::string& updateExpr) {
	rt.Update(reindexer::Query::FromSQL("UPDATE " + nsName_ + " SET " + fieldName + updateExpr + " where " + idIdxName_ + " = " +
										std::to_string(id)));
}

void NestedArraysApi::drop(int id, const std::string& fieldName, const std::string& arrayIndex) {
	rt.Update(reindexer::Query::FromSQL("UPDATE " + nsName_ + " DROP " + fieldName + arrayIndex + " where " + idIdxName_ + " = " +
										std::to_string(id)));
}

void NestedArraysApi::testSetInt(const std::string& updateExpr, std::string expectedValue) {
	SCOPED_TRACE(updateExpr);
	const int id = jsons_.size() - 1;
	set(id, intArrayFld_, updateExpr);
	set(id, intArrayIdxName_, updateExpr);
	set(id, intSparseArrayIdxName_, updateExpr);
	if (id % 2 != 0) {
		set(id, heteroArrayFldName_, updateExpr);
	}
	jsons_.back() = makeUpdateItemJson(id, expectedValue, lastStrValue_);
	checkData();
	lastIntValue_ = std::move(expectedValue);
}

void NestedArraysApi::testSetStr(const std::string& updateExpr, std::string expectedValue) {
	SCOPED_TRACE(updateExpr);
	const int id = jsons_.size() - 1;
	set(id, strArrayFld_, updateExpr);
	set(id, strArrayIdxName_, updateExpr);
	set(id, strSparseArrayIdxName_, updateExpr);
	if (id % 2 == 0) {
		set(id, heteroArrayFldName_, updateExpr);
	}
	jsons_.back() = makeUpdateItemJson(id, lastIntValue_, expectedValue);
	checkData();
	lastStrValue_ = std::move(expectedValue);
}

void NestedArraysApi::testDropInt(const std::string& arrayIndex, std::optional<std::string> expectedValue) {
	SCOPED_TRACE("DROP " + arrayIndex);
	const int id = jsons_.size() - 1;
	jsons_.back() = makeUpdateItemJson(id, expectedValue, lastStrValue_);
	drop(id, intArrayFld_, arrayIndex);
	drop(id, intArrayIdxName_, arrayIndex);
	drop(id, intSparseArrayIdxName_, arrayIndex);
	if (id % 2 != 0) {
		drop(id, heteroArrayFldName_, arrayIndex);
	}
	checkData();
	lastIntValue_ = std::move(expectedValue);
}

void NestedArraysApi::testDropStr(const std::string& arrayIndex, std::optional<std::string> expectedValue) {
	SCOPED_TRACE("DROP " + arrayIndex);
	const int id = jsons_.size() - 1;
	jsons_.back() = makeUpdateItemJson(id, lastIntValue_, expectedValue);
	drop(id, strArrayFld_, arrayIndex);
	drop(id, strArrayIdxName_, arrayIndex);
	drop(id, strSparseArrayIdxName_, arrayIndex);
	if (id % 2 == 0) {
		drop(id, heteroArrayFldName_, arrayIndex);
	}
	checkData();
	lastStrValue_ = std::move(expectedValue);
}

TEST_F(NestedArraysApi, UpdateSetByArrayIndex) {
	rt.AddIndex(nsName_, reindexer::IndexDef{strArrayIdxName_, {strArrayIdxName_}, "hash", "string", IndexOpts().Array()});
	rt.AddIndex(nsName_,
				reindexer::IndexDef{strSparseArrayIdxName_, {strSparseArrayIdxName_}, "hash", "string", IndexOpts().Array().Sparse()});
	rt.AddIndex(nsName_, reindexer::IndexDef{intArrayIdxName_, {intArrayIdxName_}, "hash", "int", IndexOpts().Array()});
	rt.AddIndex(nsName_,
				reindexer::IndexDef{intSparseArrayIdxName_, {intSparseArrayIdxName_}, "hash", "int", IndexOpts().Array().Sparse()});

	addItemForUpdate("[1,[2,5],3]", R"(["aaa",["bbb"],"ccc"])");
	testSetInt("[2]=9", "[1,[2,5],9]");
	testSetInt("[1]=7", "[1,7,9]");
	testSetStr("[2]='CCC'", R"(["aaa",["bbb"],"CCC"])");

	addItemForUpdate("[1,[2,[4],5],3]", R"(["aaa",["bbb"],"ccc"])");
	testSetInt("[1]=9", "[1,9,3]");

	addItemForUpdate("[1,[2,[3],[4,[5,6]]],7]", R"(["aaa",["bbb"]])");
	testSetInt("[0]=2", "[2,[2,[3],[4,[5,6]]],7]");
	testSetInt("[2]=9", "[2,[2,[3],[4,[5,6]]],9]");
	testSetInt("[1][2][0]=9", "[2,[2,[3],[9,[5,6]]],9]");
}

TEST_F(NestedArraysApi, UpdateDropField) {
	rt.AddIndex(nsName_,
				reindexer::IndexDef{strSparseArrayIdxName_, {strSparseArrayIdxName_}, "hash", "string", IndexOpts().Array().Sparse()});
	rt.AddIndex(nsName_,
				reindexer::IndexDef{intSparseArrayIdxName_, {intSparseArrayIdxName_}, "hash", "int", IndexOpts().Array().Sparse()});

	addItemForUpdate("[1,[2,5],3]", R"(["aaa",["bbb"],"ccc"])");
	testDropInt();
	testDropStr();

	addItemForUpdate("[1,[2,[4],5],3]", R"(["aaa",["bbb",["ddd"],"kkk"],"ccc"])");
	testDropInt();
	testDropStr();

	addItemForUpdate("[1,[2,[3],[4,[5,6]]],7]", R"(["aaa",["bbb"],"kkk",[["ccc",["ddd",["lll"],"rrr"]],"sss"]])");
	testDropInt();
	testDropStr();
}

TEST_F(NestedArraysApi, UpdateDropArrayItem) {
	rt.AddIndex(nsName_,
				reindexer::IndexDef{strSparseArrayIdxName_, {strSparseArrayIdxName_}, "hash", "string", IndexOpts().Array().Sparse()});
	rt.AddIndex(nsName_,
				reindexer::IndexDef{intSparseArrayIdxName_, {intSparseArrayIdxName_}, "hash", "int", IndexOpts().Array().Sparse()});

	addItemForUpdate("[1,[2,5],3]", R"(["aaa",["bbb"],"ccc"])");
	testDropInt("[0]", "[[2,5],3]");
	testDropStr("[0]", R"([["bbb"],"ccc"])");

	addItemForUpdate("[1,[2,5],3]", R"(["aaa",["bbb"],"ccc"])");
	testDropInt("[1]", "[1,3]");
	testDropStr("[1]", R"(["aaa","ccc"])");

	addItemForUpdate("[1,[2,5],3]", R"(["aaa",["bbb"],"ccc"])");
	testDropInt("[1][0]", "[1,[5],3]");
	testDropStr("[1][0]", R"(["aaa",[],"ccc"])");

	addItemForUpdate("[1,[2,[3],[4,[5,6]]],7]", R"(["aaa",["bbb"],"kkk",[["ccc",["ddd",["lll"],"rrr"]],"sss"]])");
	testDropInt("[1]", "[1,7]");
	testDropStr("[3]", R"(["aaa",["bbb"],"kkk"])");

	addItemForUpdate("[1,[2,[3],[4,[5,6]]],7]", R"(["aaa",["bbb"],"kkk",[["ccc",["ddd",["lll"],"rrr"]],"sss"]])");
	testDropInt("[1][1][0]", "[1,[2,[],[4,[5,6]]],7]");
	testDropInt("[1][1]", "[1,[2,[4,[5,6]]],7]");
	testDropStr("[3][0][1][1]", R"(["aaa",["bbb"],"kkk",[["ccc",["ddd","rrr"]],"sss"]])");

	addItemForUpdate("[1,[2,5],[[2],[3,4]]]", R"(["aaa",["bbb"],"ccc"])");
	testDropInt("[*][0]", "[1,[5],[[3,4]]]");
	testDropInt("[*][1]", "[1,[5],[[3,4]]]");

	addItemForUpdate(
		"[[[111,112,113],[121,122,123],[131,132,133]],"
		"[[211,212,213],[221,222,223],[231,232,233]],"
		"[[311,312,313],[321,322,323],[331,332,333]]]",
		"[]");
	testDropInt("[*][0][1]",
				"[[[111,113],[121,122,123],[131,132,133]],"
				"[[211,213],[221,222,223],[231,232,233]],"
				"[[311,313],[321,322,323],[331,332,333]]]");
	testDropInt("[1][*][1]",
				"[[[111,113],[121,122,123],[131,132,133]],"
				"[[211],[221,223],[231,233]],"
				"[[311,313],[321,322,323],[331,332,333]]]");
	testDropInt("[2][1][*]",
				"[[[111,113],[121,122,123],[131,132,133]],"
				"[[211],[221,223],[231,233]],"
				"[[311,313],[],[331,332,333]]]");
}
