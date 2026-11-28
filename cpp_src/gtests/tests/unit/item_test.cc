#include "core/cjson/jsonbuilder.h"
#include "item_move_semantics_api.h"

namespace reindexer_tests {

using reindexer::IndexOpts;

TEST_F(ReindexerApi, ItemRejectsNullIndexedArrayElement) {
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"arr", "hash", "int", IndexOpts().Array()});
	constexpr std::string_view kOriginal = R"({"id":0,"arr":[1,2]})";
	auto original = rt.NewItem(default_namespace);
	ASSERT_TRUE(original.FromJSON(kOriginal).ok());
	rt.Upsert(default_namespace, original);
	for (const auto path : {"arr[0]", "arr[1]", "arr[*]"}) {
		SCOPED_TRACE(path);
		auto item = rt.NewItem(default_namespace);
		ASSERT_TRUE(item.FromJSON(kOriginal).ok());
		EXPECT_THROW((item[path] = reindexer::Variant{}), reindexer::Error);

		QueryResults updated;
		const auto err = rt.reindexer->ExecSQL("UPDATE " + default_namespace + " SET " + path + " = null", updated);
		ASSERT_EQ(err.code(), errParams) << err.what();
		auto stored = rt.Select(Query(default_namespace));
		ASSERT_EQ(stored.Count(), 1u);
		EXPECT_EQ(stored.begin().GetItem().GetJSON(), kOriginal);
	}
}

TEST_F(ItemMoveSemanticsApi, MoveSemanticsOperator) {
	prepareItems();
	verifyAndUpsertItems();
	verifyJsonsOfUpsertedItems();
}

TEST_F(ItemMoveSemanticsApi, StoreFPNANINF) {
	const std::string nsName = "naninf";
	rt.OpenNamespace(nsName);
	rt.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	reindexer::WrSerializer ser;
	reindexer::JsonBuilder builder{ser};
	builder.Put("id", 0);
	builder.Put("pocomaxa", NAN);
	builder.Put("wolverine", INFINITY);
	builder.Put("vielfrass", -INFINITY);
	builder.End();
	Item item(rt.NewItem(nsName));
	auto err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(R"json({"id":0,"pocomaxa":null,"wolverine":null,"vielfrass":null})json", item.GetJSON());
}

TEST_F(ItemMoveSemanticsApi, ItemWithSelectFilterSurvivesQueryResults) {
	Item item(rt.NewItem(default_namespace));
	auto err = item.FromJSON(R"json({"bookid":0,"title":"title","pages":200,"price":299,"genreid_fk":3,"authorid_fk":10})json");
	ASSERT_TRUE(err.ok()) << err.what();
	rt.Upsert(default_namespace, item);

	Item selected;
	{
		auto qr = rt.Select(Query(default_namespace).Select(pkField).Where(pkField, CondEq, 0));
		ASSERT_EQ(qr.Count(), 1);
		selected = qr.begin().GetItem(false);
	}

	EXPECT_EQ(selected.GetJSON(), R"json({"bookid":0})json");
}

TEST_F(ItemMoveSemanticsApi, ItemWithoutSelectFilterSurvivesQueryResults) {
	constexpr std::string_view kJSON = R"json({"bookid":0,"title":"title","pages":200,"price":299,"genreid_fk":3,"authorid_fk":10})json";
	constexpr std::string_view kSchema = R"json({
		"type":"object",
		"properties":{
			"bookid":{"type":"integer"},
			"title":{"type":"string"},
			"pages":{"type":"integer"},
			"price":{"type":"integer"},
			"genreid_fk":{"type":"integer"},
			"authorid_fk":{"type":"integer"}
		}
	})json";
	rt.SetSchema(default_namespace, kSchema);

	Item item(rt.NewItem(default_namespace));
	auto err = item.FromJSON(kJSON);
	ASSERT_TRUE(err.ok()) << err.what();
	rt.Upsert(default_namespace, item);

	auto selected{rt.Select(Query(default_namespace).Where(pkField, CondEq, 0)).begin().GetItem(false)};
	EXPECT_EQ(selected.GetJSON(), kJSON);

	Item fromCJSON{rt.NewItem(default_namespace)};
	err = fromCJSON.FromCJSON(selected.GetCJSON());
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(fromCJSON.GetJSON(), kJSON);

	Item fromMsgPack{rt.NewItem(default_namespace)};
	size_t offset = 0;
	err = fromMsgPack.FromMsgPack(selected.GetMsgPack(), offset);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(fromMsgPack.GetJSON(), kJSON);

	reindexer::WrSerializer protobuf;
	err = selected.GetProtobuf(protobuf);
	ASSERT_TRUE(err.ok()) << err.what();

	Item fromProtobuf{rt.NewItem(default_namespace)};
	err = fromProtobuf.FromProtobuf(protobuf.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(fromProtobuf.GetJSON(), kJSON);
}

TEST_F(ItemMoveSemanticsApi, FieldsFilterIsAllocatedOnlyForExplicitSelectFilter) {
	auto unfiltered{rt.Select(Query(default_namespace))};
	const auto& defaultFilter{unfiltered.GetFieldsFilter(0)};
	ASSERT_TRUE(defaultFilter);
	EXPECT_EQ(defaultFilter.get(), &reindexer::FieldsFilter::Empty());
	EXPECT_EQ(defaultFilter.use_count(), 0);

	auto filtered{rt.Select(Query(default_namespace).Select(pkField))};
	const auto& explicitFilter{filtered.GetFieldsFilter(0)};
	ASSERT_TRUE(explicitFilter);
	EXPECT_NE(explicitFilter.get(), &reindexer::FieldsFilter::Empty());
	EXPECT_GT(explicitFilter.use_count(), 0);
}

}  // namespace reindexer_tests
