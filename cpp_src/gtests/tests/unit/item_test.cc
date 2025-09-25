#include "core/cjson/jsonbuilder.h"
#include "item_move_semantics_api.h"

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
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(R"json({"id":0,"pocomaxa":null,"wolverine":null,"vielfrass":null})json", item.GetJSON());
}