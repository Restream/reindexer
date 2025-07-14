#include <gtest/gtest.h>
#include "core/cjson/jsonbuilder.h"
#include "core/reindexer.h"
#include "estl/gift_str.h"
#include "vendor/gason/gason.h"

TEST(JSONParsingTest, EmptyDocument) {
	reindexer::Reindexer rx;
	constexpr std::string_view kNsName("json_empty_doc_test");
	auto err = rx.Connect("builtin://");
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx.OpenNamespace(kNsName);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::Item item(rx.NewItem(kNsName));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON("\n");
	EXPECT_EQ(err.code(), errParseJson);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON("\t");
	EXPECT_EQ(err.code(), errParseJson);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(" ");
	EXPECT_EQ(err.code(), errParseJson);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
}

TEST(JSONParsingTest, NestedNodesRead) {
	constexpr std::string_view jsonTest{R"json({
								"type":"replication",
								"replication":{
									"server_id":  10,
									"cluster_id": 11
								}
							})json"};

	// Parse json and check keys
	gason::JsonParser parser;
	auto root = parser.Parse(jsonTest);

	EXPECT_EQ(root["replication"]["server_id"].As<int>(), 10);
	EXPECT_EQ(root["replication"]["cluster_id"].As<int>(), 11);
	EXPECT_ANY_THROW(root["no-node"]["server_id"].As<int>());
}

TEST(JSONParsingTest, Strings) {
	const std::vector<unsigned> lens = {0, 100, 8 < 10, 2 << 20, 8 << 20, 16 << 20, 32 << 20, 60 << 20};
	for (auto len : lens) {
		std::string strs[2];
		strs[0].resize(len / 2);
		std::fill(strs[0].begin(), strs[0].end(), 'a');
		strs[1].resize(len);
		std::fill(strs[1].begin(), strs[1].end(), 'b');

		std::string d("{\"id\":1,\"str0\":\"" + strs[0] + "\",\"str1\":\"" + strs[1] + "\",\"val\":999}");
		std::span<char> data(d);
		try {
			gason::JsonParser parser;
			auto root = parser.Parse(data, nullptr);
			EXPECT_EQ(root["id"].As<int>(), 1) << len;
			auto rstr = root["str0"].As<std::string_view>();
			EXPECT_EQ(rstr, strs[0]) << len;
			rstr = root["str1"].As<std::string_view>();
			EXPECT_EQ(rstr, strs[1]) << len;
			EXPECT_EQ(root["val"].As<int>(), 999) << len;
		} catch (gason::Exception& e) {
			EXPECT_TRUE(false) << e.what();
		}
	}
}

TEST(JSONParsingTest, LargeAllocations) {
	constexpr int64_t kArrElemsCnt = 50000;
	// Create json
	reindexer::WrSerializer ser;
	reindexer::JsonBuilder jb(ser);
	jb.Put("mode", "mode");
	auto arr = jb.Array("array");
	for (int64_t i = 0; i < kArrElemsCnt; ++i) {
		arr.Put(reindexer::TagName::Empty(), reindexer::Variant{i});
	}
	arr.End();
	jb.End();

	// Parse json and check keys
	gason::JsonParser parser;
	auto root = parser.Parse(reindexer::giftStr(ser.Slice()));
	ASSERT_EQ(std::string_view(root["mode"].key), "mode");
	for (auto el : root["array"]) {
		ASSERT_EQ(std::string_view(el.key), std::string_view());
		ASSERT_EQ(el.value.getTag(), gason::JsonTag::NUMBER);
	}
}
