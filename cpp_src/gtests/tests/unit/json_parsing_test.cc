#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "core/cjson/cjsonbuilder.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/jsondecoder.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "core/reindexer.h"
#include "estl/gift_str.h"
#include "tools/errors.h"
#include "vendor/gason/gason.h"

namespace reindexer_tests {

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

TEST(JSONParsingTest, TrailingData) {
	using namespace std::string_view_literals;
	for (auto json : {R"({"a":1})"sv, "{ \"a\" : 1 }\n\t "sv, "{\"a\":1}\0 \0"sv, "[1, 2] "sv, "true\n"sv, "42 "sv}) {
		gason::JsonParser parser;
		EXPECT_NO_THROW(parser.Parse(json)) << json;
	}

	struct {
		std::string_view json;
		size_t valueLength;
		size_t tailPos;
	} withTail[] = {
		{R"({"a":1} asd)"sv, 7, 8},
		{R"({"a":1} {"b":2})"sv, 7, 8},
		{R"({"a":1}{"b":2}{"c":3})"sv, 7, 7},
		{R"({"a":1},)"sv, 7, 7},
		{"{\"a\":1}\0x"sv, 7, 8},
		{"[1] [2]"sv, 3, 4},
		{"true more but under baby"sv, 4, 5},
		{"null more"sv, 4, 5},
		{"42 more"sv, 2, 3},
		{"1.5 more"sv, 3, 4},
	};
	for (const auto& [json, valueLength, tailPos] : withTail) {
		gason::JsonParser parser;
		try {
			parser.Parse(json);
			ADD_FAILURE() << json;
		} catch (const gason::Exception& e) {
			EXPECT_EQ(std::string_view(e.what()), "Error parsing json: unexpected character, pos " + std::to_string(tailPos)) << json;
		}

		size_t length = 0;
		EXPECT_NO_THROW(parser.Parse(json, &length)) << json;
		EXPECT_EQ(length, valueLength) << json;
	}
}

namespace {

void decodeTopLevelJson(std::string_view json) {
	reindexer::TagsMatcher tagsMatcher;
	reindexer::FloatVectorsHolderVector floatVectors;
	reindexer::ScalarIndexesSetT objectScalarIndexes;
	reindexer::JsonDecoder decoder(tagsMatcher, floatVectors, objectScalarIndexes);
	reindexer::WrSerializer ser;
	reindexer::CJsonBuilder builder(ser, reindexer::ObjType::TypeObject, &tagsMatcher);
	decoder.Decode(json, builder, reindexer::TagsPath{});
}

void expectDecodeError(std::string_view json, std::string_view message) {
	try {
		decodeTopLevelJson(json);
		ADD_FAILURE() << json;
	} catch (const reindexer::Error& err) {
		EXPECT_EQ(err.code(), errParseJson) << json << ": " << err.what();
		EXPECT_THAT(err.whatStr(), ::testing::HasSubstr(message)) << json;
	}
}

}  // namespace

TEST(JSONParsingTest, DecoderRejectsTrailingData) {
	EXPECT_NO_THROW(decodeTopLevelJson(R"({"a":1})"));
	EXPECT_NO_THROW(decodeTopLevelJson("{ \"a\" : 1 }\n\t "));
	EXPECT_NO_THROW(decodeTopLevelJson(R"({"nested":{"x":true}})"));

	constexpr std::string_view kTailError = "JSONDecoder: Error parsing json: unexpected character";
	expectDecodeError(R"({"a":1} asd)", kTailError);
	expectDecodeError(R"({"a":1} {"b":2})", kTailError);
	expectDecodeError(R"({"a":1}{"b":2}{"c":3})", kTailError);
	expectDecodeError("true more but under baby", kTailError);
	expectDecodeError("42 more", kTailError);

	constexpr std::string_view kNotAnObjectError = "Json node must be an object or array";
	expectDecodeError("true\n", kNotAnObjectError);
	expectDecodeError("null\t", kNotAnObjectError);
	expectDecodeError("1.5\n", kNotAnObjectError);
}

TEST(JSONParsingTest, ItemTrailingData) {
	reindexer::Reindexer rx;
	constexpr std::string_view kNsName("json_item_tail_test");
	auto err = rx.Connect("builtin://");
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx.OpenNamespace(kNsName);
	ASSERT_TRUE(err.ok()) << err.what();

	for (bool unsafe : {false, true}) {
		for (std::string_view json : {R"({"id":1} asd)", R"({"id":1} {"id":2})", R"({"id":1}{"id":2})"}) {
			std::string data(json);
			reindexer::Item item(rx.NewItem(kNsName));
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			err = item.Unsafe(unsafe).FromJSON(data);
			EXPECT_EQ(err.code(), errParseJson) << json << "; unsafe: " << unsafe;

			std::string batchData(json);
			reindexer::Item batchItem(rx.NewItem(kNsName));
			ASSERT_TRUE(batchItem.Status().ok()) << batchItem.Status().what();
			char* endp = nullptr;
			err = batchItem.Unsafe(unsafe).FromJSON(batchData, &endp);
			ASSERT_TRUE(err.ok()) << json << "; unsafe: " << unsafe << "; " << err.what();
			EXPECT_EQ(endp, batchData.data() + std::string_view(R"({"id":1})").size()) << json << "; unsafe: " << unsafe;
			EXPECT_EQ(batchItem["id"].As<int>(), 1) << json << "; unsafe: " << unsafe;
		}
	}
}

}  // namespace reindexer_tests
