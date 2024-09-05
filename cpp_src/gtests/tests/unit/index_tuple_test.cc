#include <gtest/gtest.h>
#include "reindexer_api.h"
#include "tools/fsops.h"

class IndexTupleTest : public ReindexerApi {
public:
	void SetUp() override {
		const std::string kStoragePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "IndexTupleTest");
		reindexer::fs::RmDirAll(kStoragePath);
		auto err = rt.reindexer->Connect(std::string("builtin://") + kStoragePath);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void CreateEmptyNamespace(std::string_view ns) { return createNS(ns, R"json({"id":%d})json"); }

	void CreateNamespace(std::string_view ns) {
		static const char pattern[] =
			R"json({"id":%d,"objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}]})json";
		createNS(ns, pattern);
	}

	void CreateSparseNamespace(std::string_view ns) { return createNS(ns, R"json({"id":%d,"fld1":1,"fld2":{"nested":"test"}})json"); }

	void CreateArrayNamespace(std::string_view ns) {
		static const char pattern[] = R"json({"id":%d,"obj":{"val":10},"arr":[1,2,3]})json";
		return createNS(ns, pattern);
	}

	void DoTestDefault(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns, const reindexer::IndexDef& indexDef,
					   std::string_view pattern, std::string_view field, const VariantArray& expectedValues,
					   std::string_view description) const {
		auto err = rt.reindexer->AddIndex(ns, indexDef);
		ASSERT_TRUE(err.ok()) << err.what() << "\n" << description;

		validateResults(reindexer, ns, pattern, field, expectedValues, description);
	}

	void DoTestEmpty(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns, const reindexer::IndexDef& indexDef,
					 std::string_view pattern, std::string_view description) const {
		auto err = reindexer->AddIndex(ns, indexDef);
		ASSERT_TRUE(err.ok()) << err.what();

		checkExpectations(reindexer, ns, pattern, description);
	}

	void DoCallAndCheckError(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns,
							 const reindexer::IndexDef& indexDef, std::string_view errMsg) const {
		std::vector<std::string> items;
		getItems(reindexer, ns, items);

		auto err = reindexer->AddIndex(ns, indexDef);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), errMsg);

		checkItems(reindexer, ns, items);
	}

	void ValidateReloadState(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns, std::string_view pattern,
							 std::string_view description) const {
		auto err = rt.reindexer->CloseNamespace(ns);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->OpenNamespace(ns, StorageOpts().Enabled().CreateIfMissing().VerifyChecksums());
		ASSERT_TRUE(err.ok()) << err.what();

		checkExpectations(reindexer, ns, pattern, description);

		// remove storage
		err = rt.reindexer->DropNamespace(ns);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void SpecialCheckForNull(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns, std::string_view firstItemPattern,
							 std::string_view itemPattern, std::string_view description) const {
		specialCheckForNull(reindexer, ns, firstItemPattern, itemPattern, description);
		validateReloadStateForNull(reindexer, ns, firstItemPattern, itemPattern, description);
	}

	static constexpr uint32_t IdStart = 2000;

private:
	static constexpr uint32_t itemNumber_ = 5;	// NOTE: minimum 2

	void createNS(std::string_view ns, std::string_view itemPattern) {
		createNamespace(ns);
		generateItems(ns, itemPattern);
	}

	void createNamespace(std::string_view ns) {
		auto err = rt.reindexer->OpenNamespace(ns, StorageOpts().Enabled().CreateIfMissing());
		EXPECT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(ns, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});
	}

	void generateItems(std::string_view ns, std::string_view pattern) {
		for (uint32_t idx = IdStart, sz = IdStart + itemNumber_; idx < sz; ++idx) {
			Item item = NewItem(ns);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			const auto json = fmt::sprintf(pattern.data(), idx);
			auto err = item.FromJSON(json);
			ASSERT_TRUE(err.ok()) << err.what();

			Upsert(ns, item);
		}
	}

	void checkIfItemJSONValid(QueryResults::Iterator& it, bool print = false) const {
		reindexer::WrSerializer wrser;
		Error err = it.GetJSON(wrser, false);
		EXPECT_TRUE(err.ok()) << err.what();
		if (err.ok() && print) {
			std::cout << wrser.Slice() << std::endl;
		}
	}

	void validateResults(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns, std::string_view pattern,
						 std::string_view field, const VariantArray& expectedValues, std::string_view description) const {
		SCOPED_TRACE(description);

		QueryResults qr;
		auto err = reindexer->Select("SELECT * FROM " + std::string(ns), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), itemNumber_);

		for (auto it : qr) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			const auto json = item.GetJSON();
			ASSERT_NE(json.find(pattern), std::string::npos) << "JSON: " << json << ";\npattern: " << pattern;
			const VariantArray values = item[field];
			ASSERT_EQ(values.size(), expectedValues.size());
			ASSERT_EQ(values.IsArrayValue(), expectedValues.IsArrayValue());
			for (size_t i = 0; i < values.size(); ++i) {
				ASSERT_TRUE(values[i].Type().IsSame(expectedValues[i].Type()))
					<< values[i].Type().Name() << "!=" << expectedValues[i].Type().Name();
				if (values[i].Type().IsSame(reindexer::KeyValueType::Null())) {
					continue;
				}

				ASSERT_EQ(values[i], expectedValues[i]);
			}
		}
	}

	void checkExpectations(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns, std::string_view pattern,
						   std::string_view description) const {
		SCOPED_TRACE(description);

		QueryResults qr;
		auto err = reindexer->Select("SELECT * FROM " + std::string(ns), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), itemNumber_);

		uint32_t idx = IdStart;
		for (auto it : qr) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);

			const auto json = item.GetJSON();
			const auto extJson = fmt::sprintf(pattern.data(), idx);
			ASSERT_EQ(json, extJson);

			++idx;
		}
	}

	void getItems(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns, std::vector<std::string>& items) const {
		QueryResults qr;
		auto err = reindexer->Select("SELECT * FROM " + std::string(ns), qr);
		ASSERT_TRUE(err.ok()) << err.what();

		items.clear();
		items.reserve(qr.Count());
		for (auto& it : qr) {
			auto item = it.GetItem(false);
			items.emplace_back(item.GetJSON());
		}
	}

	void checkItems(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns,
					const std::vector<std::string>& items) const {
		QueryResults qr;
		auto err = reindexer->Select("SELECT * FROM " + std::string(ns), qr);
		ASSERT_TRUE(err.ok()) << err.what();

		ASSERT_EQ(items.size(), qr.Count());
		auto itItems = items.cbegin();
		auto itQR = qr.begin();
		auto endItems = items.cend();
		auto endQR = qr.end();
		for (; (itItems != endItems) && (itQR != endQR); ++itItems, ++itQR) {
			auto item = itQR.GetItem(false);
			ASSERT_EQ(*itItems, item.GetJSON());
		}
	}

	void specialCheckForNull(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns, std::string_view firstItemPattern,
							 std::string_view itemPattern, std::string_view description) const {
		SCOPED_TRACE(description);

		// first element should not update values, all others should be initialized to default values
		// Note: but index array updates element type
		QueryResults qr;
		auto err = reindexer->Select("SELECT * FROM " + std::string(ns), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), itemNumber_);

		uint32_t idx = IdStart;
		for (auto it : qr) {
			Item item = it.GetItem(false);
			checkIfItemJSONValid(it);
			const auto json = item.GetJSON();
			const auto& pattern = (idx == IdStart) ? firstItemPattern : itemPattern;
			const auto expJson = fmt::sprintf(pattern.data(), idx);
			ASSERT_EQ(json, expJson);

			++idx;
		}
	}

	void validateReloadStateForNull(const std::shared_ptr<reindexer::Reindexer>& reindexer, std::string_view ns,
									std::string_view firstItemPattern, std::string_view itemPattern, std::string_view description) const {
		auto err = rt.reindexer->CloseNamespace(ns);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->OpenNamespace(ns, StorageOpts().Enabled().VerifyChecksums());
		ASSERT_TRUE(err.ok()) << err.what();

		specialCheckForNull(reindexer, ns, firstItemPattern, itemPattern, description);

		// remove storage
		err = rt.reindexer->DropNamespace(ns);
		ASSERT_TRUE(err.ok()) << err.what();
	}
};

TEST_F(IndexTupleTest, DISABLED_ScalarTest) {
	static const std::string ns = "testNSScalar";
	CreateEmptyNamespace(ns);

	DoTestEmpty(rt.reindexer, ns, {"sparse", "text", "string", IndexOpts().Sparse()}, R"({"id":%d})", "add some sparse index. Do nothing");
	DoTestDefault(rt.reindexer, ns, {"text", "text", "string", IndexOpts()}, R"("text":"")", "text", {Variant("")},
				  "add text scalar index. Add default value");
	DoTestEmpty(rt.reindexer, ns, {"text", "text", "string", IndexOpts()}, R"({"id":%d,"text":""})", "update text index. Do nothing");
	DoCallAndCheckError(rt.reindexer, ns, {"text", "hash", "int", IndexOpts()},
						"Index 'testNSScalar.text' already exists with different settings");
	DoTestDefault(rt.reindexer, ns, {"int", "hash", "int", IndexOpts()}, R"("int":0)", "int", {Variant(0)},
				  "add int scalar index. Add default value");
	ValidateReloadState(rt.reindexer, ns, R"({"id":%d,"text":"","int":0})", "reload ns (ScalarTest)");
}

TEST_F(IndexTupleTest, DISABLED_ScalarNestedTest) {
	static const std::string ns = "testNSNested";
	CreateEmptyNamespace(ns);

	DoTestDefault(rt.reindexer, ns, {"obj.more.nested", {"obj.more.nested"}, "hash", "int64", IndexOpts()},
				  R"("obj":{"more":{"nested":0}})", "obj.more.nested", {Variant(int64_t(0))}, "add new nested scalar index");
	DoTestEmpty(rt.reindexer, ns, {"obj.more.nested", {"obj.more.nested"}, "hash", "int64", IndexOpts()},
				R"({"id":%d,"obj":{"more":{"nested":0}}})", "update nested index. Do nothing");
	DoTestEmpty(rt.reindexer, ns, {"id+obj.more.nested", {"id", "obj.more.nested"}, "tree", "composite", IndexOpts{}},
				R"({"id":%d,"obj":{"more":{"nested":0}}})", "add new composite index. Do nothing");
	DoCallAndCheckError(rt.reindexer, ns, {"obj.more", {"obj.more"}, "hash", "string", IndexOpts()},
						"Invalid tag type value for KeyValueType: '<object>'");
	DoCallAndCheckError(rt.reindexer, ns, {"obj", "hash", "int64", IndexOpts()}, "Invalid tag type value for KeyValueType: '<object>'");
	DoTestDefault(rt.reindexer, ns, {"obj.near", {"obj.near"}, "tree", "string", IndexOpts()}, R"("obj":{"more":{"nested":0},"near":""})",
				  "obj.near", {Variant("")}, "add nested scalar index to root");
	DoTestDefault(rt.reindexer, ns, {"obj.nested.text", {"obj.nested.text"}, "hash", "string", IndexOpts()},
				  R"("obj":{"more":{"nested":0},"near":"","nested":{"text":""}}})", "obj.nested.text", {Variant("")},
				  "add nested another path scalar index");
	DoTestDefault(rt.reindexer, ns, {"obj.idx", {"obj.idx"}, "hash", "int64", IndexOpts()},
				  R"("obj":{"more":{"nested":0},"near":"","nested":{"text":""},"idx":0})", "obj.idx", {Variant(int64_t(0))},
				  "add nested 2nd level path scalar index");
	DoTestDefault(rt.reindexer, ns, {"obj.new.another.one", {"obj.new.another.one"}, "tree", "double", IndexOpts()},
				  R"("obj":{"more":{"nested":0},"near":"","nested":{"text":""},"idx":0,"new":{"another":{"one":0.0}}}})",
				  "obj.new.another.one", {Variant(0.0)}, "add nested scalar index with multiple new path levels");
	DoCallAndCheckError(rt.reindexer, ns, {"boom", {"obj.new.another.one"}, "tree", "string", IndexOpts()},
						"Cannot add field with name 'boom' to namespace 'testNSNested'. Json path 'obj.new.another.one' already used"
						" in field 'obj.new.another.one'");
	DoCallAndCheckError(rt.reindexer, ns, {"boom", {"obj.new.another.one.two"}, "hash", "int64", IndexOpts()},
						"Cannot add field with name 'boom' (jsonpath 'obj.new.another.one.two') and type 'int64' to namespace"
						" 'testNSNested'. Already exists json path 'obj.new.another.one' with type 'double' in field"
						" 'obj.new.another.one'. Rewriting is impossible");
	DoTestDefault(
		rt.reindexer, ns, {"root2.more.nested", {"root2.more.nested"}, "hash", "int64", IndexOpts()},
		R"("obj":{"more":{"nested":0},"near":"","nested":{"text":""},"idx":0,"new":{"another":{"one":0.0}}},"root2":{"more":{"nested":0}})",
		"root2.more.nested", {Variant(int64_t(0))}, "add new root with nested");
	DoTestDefault(
		rt.reindexer, ns, {"boom", {"obj.new.another.one_ext"}, "hash", "int64", IndexOpts()},
		R"("obj":{"more":{"nested":0},"near":"","nested":{"text":""},"idx":0,"new":{"another":{"one":0.0,"one_ext":0}}},"root2":{"more":{"nested":0}})",
		"obj.new.another.one_ext", {Variant(int64_t(0))}, "add new nested scalar index with name extension in last part");
	DoTestDefault(
		rt.reindexer, ns, {"a-ha", {"a.ha"}, "hash", "int64", IndexOpts()},
		R"("obj":{"more":{"nested":0},"near":"","nested":{"text":""},"idx":0,"new":{"another":{"one":0.0,"one_ext":0}}},"root2":{"more":{"nested":0}},"a":{"ha":0})",
		"a.ha", {Variant(int64_t(0))}, "add another nested scalar index on top level");
	ValidateReloadState(
		rt.reindexer, ns,
		R"({"id":%d,"obj":{"more":{"nested":0},"near":"","nested":{"text":""},"idx":0,"new":{"another":{"one":0.0,"one_ext":0}}},"root2":{"more":{"nested":0}},"a":{"ha":0}})",
		"reload ns (ScalarNestedTest)");
}

TEST_F(IndexTupleTest, SparseItemTest) {
	static const std::string ns = "testNSSparse";
	CreateSparseNamespace(ns);

	DoTestEmpty(rt.reindexer, ns, {"sparse1", {"fld1"}, "hash", "int", IndexOpts().Sparse()},
				R"({"id":%d,"fld1":1,"fld2":{"nested":"test"}})", "add some sparse index to present nested field. Do nothing");
	DoCallAndCheckError(rt.reindexer, ns, {"sparse2", {"fld2"}, "hash", "int", IndexOpts().Sparse()}, "Can't convert 'test' to number");
	DoCallAndCheckError(rt.reindexer, ns, {"sparse3", {"fld2.nested"}, "hash", "int", IndexOpts().Sparse()},
						"Can't convert 'test' to number");
	DoTestEmpty(rt.reindexer, ns, {"sparse2", {"fld2"}, "hash", "string", IndexOpts().Sparse()},
				R"({"id":%d,"fld1":1,"fld2":{"nested":"test"}})", "add some sparse index to present part path field. Do nothing");
	ValidateReloadState(rt.reindexer, ns, R"({"id":%d,"fld1":1,"fld2":{"nested":"test"}})", "reload ns (SparseItemTest)");
}

TEST_F(IndexTupleTest, NestedUpdateTest) {
	static const std::string ns = "testNSUpdate";
	CreateNamespace(ns);

	DoTestDefault(
		rt.reindexer, ns, {"obj.nested", {"obj.nested"}, "hash", "string", IndexOpts()},
		R"("objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":"0"},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}])",
		"obj.nested", VariantArray{Variant{"0"}}, "add obj.nested index - update field type");
	DoCallAndCheckError(rt.reindexer, ns, {"try_change_type", {"last.text"}, "hash", "int", IndexOpts()}, "Can't convert 'OK' to number");
	ValidateReloadState(
		rt.reindexer, ns,
		R"({"id":%d,"objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":"0"},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}]})",
		"reload ns (NestedUpdateTest)");
}

// TODO: This test must be reenabled after #1353
TEST_F(IndexTupleTest, DISABLED_ArrayTest) {
	static const std::string ns = "testNSArray";
	CreateEmptyNamespace(ns);

	DoTestDefault(rt.reindexer, ns, {"array", "hash", "int", IndexOpts().Array()}, R"("array":[])", "array", {},
				  "add int array index. Add empty array");
	DoTestDefault(rt.reindexer, ns,
				  {"obj.some.arr_1st", {"obj.some.array", "arr_fld", "obj.array"}, "hash", "int64", IndexOpts().Array(), 0},
				  R"("array":[],"arr_fld":[])", "arr_fld", VariantArray{}.MarkArray(), "add array index. Add empty array");
	DoCallAndCheckError(rt.reindexer, ns, {"obj.some.array", {"obj.array"}, "hash", "int64", IndexOpts().Array(), 0},
						"Cannot add field with name 'obj.some.array' to namespace 'testNSArray'. Json path 'obj.array' already used in "
						"field 'obj.some.arr_1st'");
	DoTestDefault(rt.reindexer, ns,
				  {"obj.some.new_array", {"obj.some.new_array", "arr_fld1", "arr_fld2"}, "hash", "int64", IndexOpts().Array(), 0},
				  R"("array":[],"arr_fld":[],"arr_fld2":[])", "arr_fld2", VariantArray{}.MarkArray(),
				  "add another array index (chooses last of two). Add empty array");
	// TODO: This logic is disabled due to #1819
	DoTestDefault(rt.reindexer, ns, {"obj.new.array", {"obj.new.array"}, "hash", "int64", IndexOpts().Array(), 0},
				  R"("array":[],"arr_fld":[],"arr_fld2":[]})" /*,"obj":{"new":{"array":[]}})"*/, "obj.new.array", VariantArray{},
				  "add new nested (only) index. Add empty array");
	// TODO: This logic is disabled due to #1819
	DoTestDefault(rt.reindexer, ns, {"arr", "hash", "int64", IndexOpts().Array()},
				  R"("array":[],"arr_fld":[],"arr_fld2":[],"arr":[]})" /*,"obj":{"new":{"array":[]}},"arr":[])"*/, "arr", VariantArray{},
				  "add new field with nested (only) indexes. Add empty array");
	DoCallAndCheckError(rt.reindexer, ns,
						{"arr_restriction", {"arr_fld3", "arr_fld4", "arr.some.arr_1st"}, "hash", "int64", IndexOpts().Array(), 0},
						"Cannot add field with name 'arr_restriction' (jsonpath 'arr.some.arr_1st') and type 'int64' to namespace"
						" 'testNSArray'. Already exists json path 'arr' with type 'int64' in field 'arr'. Rewriting is impossible");
	DoTestEmpty(rt.reindexer, ns, {"new_sparse_array", {"new_sparse_array"}, "hash", "int64", IndexOpts().Array().Sparse(), 0},
				R"({"id":%d,"array":[],"arr_fld":[],"arr_fld2":[],"arr":[]})" /*,"obj":{"new":{"array":[]}},"arr":[]})"*/,
				"add new sparse array index. Do nothing");
	ValidateReloadState(rt.reindexer, ns,
						R"({"id":%d,"array":[],"arr_fld":[],"arr_fld2":[],"arr":[]})" /*,"obj":{"new":{"array":[]}},"arr":[]})"*/,
						"reload ns (ArrayTest)");
}

// TODO: This test must be reenabled after #1353
TEST_F(IndexTupleTest, DISABLED_ArrayNestedTest) {
	static const std::string ns = "testNSArrayObj";
	CreateNamespace(ns);

	DoCallAndCheckError(rt.reindexer, ns, {"try_change_type", {"last.text"}, "hash", "int", IndexOpts().Array().Sparse()},
						"Can't convert 'OK' to number");
	DoCallAndCheckError(rt.reindexer, ns, {"try_change_type", {"last.text"}, "hash", "int", IndexOpts().Array()},
						"Can't convert 'OK' to number");
	// TODO: This logic is disabled due to #1819
	DoTestDefault(
		rt.reindexer, ns, {"next.another.last", {"next.another.last"}, "hash", "string", IndexOpts().Array()},
		R"("objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}]})" /*,"next":{"another":{"last":[]}})"*/
		,
		"next.another.last", VariantArray{}, "add nested index to field by new path. Add empty array");
	DoTestDefault(
		rt.reindexer, ns, {"obj.alternative", {"obj.alternative"}, "hash", "string", IndexOpts().Array()},
		R"("objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0,"alternative":[]},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}]})" /*,"next":{"another":{"last":[]}})"*/
		,
		"obj.alternative", VariantArray{}, "add nested index to field. Add empty array");

	DoTestDefault(
		rt.reindexer, ns, {"last.1st.2nd.ext", {"last.1st.2nd.ext"}, "hash", "string", IndexOpts().Array()},
		R"("objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0,"alternative":[]},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14,"ext":[]}}},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}]})" /*,"next":{"another":{"last":[]}})"*/
		,
		"last.1st.2nd.ext.more", VariantArray{}, "add nested-nested index to field. Add empty array");
	DoCallAndCheckError(rt.reindexer, ns, {"last.1st.2nd.ext", {"last.alt", "last.1st.2nd.ext"}, "hash", "string", IndexOpts().Array()},
						"Index 'testNSArrayObj.last.1st.2nd.ext' already exists with different settings");
	// TODO: This logic is disabled due to #1819
	// DoTestDefault(
	// 	rt.reindexer, ns, {"last.1st.2nd.ext.more", {"last.1st.2nd.ext.more"}, "hash", "string", IndexOpts().Array()},
	// 	R"("objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0,"alternative":[]},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14,"ext":{"more":[]}}}},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}],"next":{"another":{"last":[]}})",
	// 	"last.1st.2nd.ext.more", VariantArray{}, "add nested-nested index to field. Add empty array");
	// DoCallAndCheckError(rt.reindexer, ns,
	// 					{"last.1st.2nd.ext.more", {"last.alt", "last.1st.2nd.ext.more"}, "hash", "string", IndexOpts().Array()},
	// 					"Index 'testNSArrayObj.last.1st.2nd.ext.more' already exists with different settings");
	DoTestDefault(
		rt.reindexer, ns, {"last.1st.ext", {"last.1st.ext"}, "hash", "string", IndexOpts().Array()},
		R"("objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0,"alternative":[]},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14,"ext":[]}},"ext":[]},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}]})" /*,"next":{"another":{"last":[]}})"*/
		,
		"last.1st.ext", VariantArray{}, "add array index into the presented nested field. Add empty array");
	ValidateReloadState(
		rt.reindexer, ns,
		R"({"id":%d,"objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0,"alternative":[]},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14,"ext":[]}},"ext":[]},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}]})" /*,"next":{"another":{"last":[]}}})"*/
		,
		"reload ns (ArrayNestedTest)");
}

TEST_F(IndexTupleTest, ArrayInToArrayTest) {
	static const std::string ns = "testNSArrayArr";
	CreateNamespace(ns);

	// TODO: This logic is disabled due to #1819
	DoTestDefault(
		rt.reindexer, ns, {"objs.more", {"objs.more"}, "hash", "string", IndexOpts().Array()},
		R"("objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":[3,2,1]},{"field":11},{"field":[9]}]}])",
		"obj.more", VariantArray{}, "do not add anything into objects array");
	DoTestEmpty(
		rt.reindexer, ns, {"arr.nested_arr.field", {"arr.nested_arr.field"}, "hash", "string", IndexOpts().Array()},
		R"({"id":%d,"objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":["3","2","1"]},{"field":"11"},{"field":["9"]}]}]})",
		"add nested index to array array (update). Do nothing");
	DoTestEmpty(
		rt.reindexer, ns, {"arr.new_fld", {"arr.new_fld"}, "hash", "string", IndexOpts().Array()},
		R"({"id":%d,"objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":["3","2","1"]},{"field":"11"},{"field":["9"]}]}]})",
		"add nested index to array array. Do nothing");
	DoTestEmpty(
		rt.reindexer, ns, {"arr.nested_arr.ext_fld", {"arr.nested_arr.ext_fld"}, "hash", "string", IndexOpts().Array()},
		R"({"id":%d,"objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":["3","2","1"]},{"field":"11"},{"field":["9"]}]}]})",
		"add nested nested index to array array. Do nothing");
	ValidateReloadState(
		rt.reindexer, ns,
		R"({"id":%d,"objs":[{"fld":1},{"fld":2},{"fld":5}],"obj":{"nested":0},"last":{"text":"OK","1st":{"2nd":{"3rd":3.14}}},"arr":[{"nested_arr":[{"field":["3","2","1"]},{"field":"11"},{"field":["9"]}]}]})",
		"reload ns (ArrayInToArrayTest)");
}

// TODO: This test must be reenabled after #1353
TEST_F(IndexTupleTest, DISABLED_NestedOrderingTest) {
	static const std::string ns = "testNSNestedOrdering";
	CreateEmptyNamespace(ns);

	DoTestDefault(rt.reindexer, ns, {"nest1", {"obj.more.nested"}, "hash", "int", IndexOpts()}, R"("obj":{"more":{"nested":0}})", "nest1",
				  VariantArray{Variant{0}}, "add nest1. Add default value");
	DoTestDefault(rt.reindexer, ns, {"nest2", {"obj.near"}, "hash", "int", IndexOpts()}, R"("obj":{"more":{"nested":0},"near":0})", "nest2",
				  VariantArray{Variant{0}}, "add nest2. Add default value");
	DoTestDefault(rt.reindexer, ns, {"nest3", {"obj.nestd.text"}, "text", "string", IndexOpts()},
				  R"("obj":{"more":{"nested":0},"near":0,"nestd":{"text":""}})", "nest3", VariantArray{Variant{""}},
				  "add nest3. Add default value");
	DoTestDefault(rt.reindexer, ns, {"nest11", {"obj.more.nested2"}, "hash", "int", IndexOpts()},
				  R"("obj":{"more":{"nested":0,"nested2":0},"near":0,"nestd":{"text":""}})", "nest11", VariantArray{Variant{0}},
				  "add nest11. Add default value");
	ValidateReloadState(rt.reindexer, ns, R"({"id":%d,"obj":{"more":{"nested":0,"nested2":0},"near":0,"nestd":{"text":""}}})",
						"reload ns (NestedDiffOrderingTest)");
}

// TODO: This test must be reenabled after #1353
TEST_F(IndexTupleTest, DISABLED_NullTest) {
	static const std::string ns = "testNSNull";
	CreateEmptyNamespace(ns);

	// update only one first item
	{
		const std::string sql = "UPDATE testNSNull SET fld1 = null, fld2 = [null, null] WHERE id = " + std::to_string(IdStart);
		Query query = Query::FromSQL(sql);
		QueryResults qr;
		auto err = rt.reindexer->Update(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 1);
	}

	// add indexes (simple and array)
	{
		auto err = rt.reindexer->AddIndex(ns, {"fld1", "hash", "int", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->AddIndex(ns, {"fld2", "hash", "string", IndexOpts().Array()});
		ASSERT_TRUE(err.ok()) << err.what();
	}

	SpecialCheckForNull(rt.reindexer, ns, R"({"id":%d,"fld1":null,"fld2":["null","null"]})", R"({"id":%d,"fld1":0,"fld2":[]})",
						"null values test");
}

// TODO: This test must be reenabled after #1353
TEST_F(IndexTupleTest, DISABLED_FailTest) {
	static const std::string ns = "testNSFail";
	CreateEmptyNamespace(ns);

	DoTestDefault(rt.reindexer, ns, {"nest", {"obj.nest"}, "hash", "int", IndexOpts()}, R"("obj":{"nest":0})", "nest",
				  VariantArray{Variant{0}}, "add nest. Add default value");
	DoTestDefault(rt.reindexer, ns, {"idx", {"idx"}, "-", "bool", IndexOpts()}, R"("obj":{"nest":0},"idx":false)", "idx",
				  VariantArray{Variant{false}}, "add idx. Add default value");
	ValidateReloadState(rt.reindexer, ns, R"({"id":%d,"obj":{"nest":0},"idx":false})", "reload ns (FailTest)");
}

// TODO: This test must be reenabled after #1353
TEST_F(IndexTupleTest, DISABLED_NestedArrayTest) {
	static const std::string ns = "testNSNestedArray";
	CreateArrayNamespace(ns);

	// TODO: This logic is disabled due to #1819
	DoTestDefault(rt.reindexer, ns, {"obj.obj1.arr", {"obj.obj1.arr"}, "hash", "int", IndexOpts().Array()},
				  R"("obj":{"val":10},"arr":[1,2,3])", "obj.obj1.arr", VariantArray{},
				  // R"("obj":{"val":10,"obj1":{"arr":[]}},"arr":[1,2,3])", "obj.obj1.arr", VariantArray{},
				  "add obj.obj1.arr. Add default value");
	DoTestDefault(rt.reindexer, ns, {"obj.arr", {"obj.arr"}, "hash", "int", IndexOpts().Array()},
				  R"("obj":{"val":10,"arr":[]},"arr":[1,2,3])", "obj.arr", VariantArray{}, "add obj.arr. Add default value");
	ValidateReloadState(rt.reindexer, ns, R"({"id":%d,"obj":{"val":10,"arr":[]},"arr":[1,2,3]})", "reload ns (NestedArrayTest)");
}
