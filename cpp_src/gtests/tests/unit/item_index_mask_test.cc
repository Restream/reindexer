#include "item_index_mask_api.h"

TEST_F(ItemIndexMaskApi, MaskTest) {
	auto itemBase = NewItem(default_namespace);
	Error err = itemBase.FromJSON(R"({"id":1, "f1":10, "f2":20})");

	{
		ASSERT_TRUE(err.ok()) << err.what();
		auto mask = itemBase.GetScalarIndexMask();
		ASSERT_EQ(mask.count(), 3);
		ASSERT_TRUE(mask.test(1) && mask.test(2) && mask.test(3));
	}
	{
		auto itemCjson = itemBase.GetCJSON();
		auto item = NewItem(default_namespace);
		err = item.FromCJSON(itemCjson);
		ASSERT_TRUE(err.ok()) << err.what();
		auto mask = item.GetScalarIndexMask();
		ASSERT_EQ(mask.count(), 3);
		ASSERT_TRUE(mask.test(1) && mask.test(2) && mask.test(3));
	}
	{
		auto itemMsgPack = itemBase.GetMsgPack();
		auto item = NewItem(default_namespace);
		size_t offset = 0;
		err = item.FromMsgPack(itemMsgPack, offset);
		ASSERT_TRUE(err.ok()) << err.what();
		auto mask = item.GetScalarIndexMask();
		ASSERT_EQ(mask.count(), 3);
		ASSERT_TRUE(mask.test(1) && mask.test(2) && mask.test(3));
	}
	{
		reindexer::WrSerializer ser;
		err = itemBase.GetProtobuf(ser);
		ASSERT_TRUE(err.ok()) << err.what();
		auto item = NewItem(default_namespace);
		err = item.FromProtobuf(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		auto mask = item.GetScalarIndexMask();
		ASSERT_EQ(mask.count(), 3);
		ASSERT_TRUE(mask.test(1) && mask.test(2) && mask.test(3));
	}
	{
		auto item = NewItem(default_namespace);
		err = item.FromJSON(R"({"id":1})");
		ASSERT_TRUE(err.ok()) << err.what();
		{
			auto mask = item.GetScalarIndexMask();
			ASSERT_EQ(mask.count(), 1);
			ASSERT_TRUE(mask.test(1));
		}

		item[kFieldOne] = 90;
		{
			auto mask = item.GetScalarIndexMask();
			ASSERT_EQ(mask.count(), 2);
			ASSERT_TRUE(mask.test(1) && mask.test(2));
		}
		item[kFieldTwo] = std::vector{90, 100};
		{
			auto mask = item.GetScalarIndexMask();
			ASSERT_EQ(mask.count(), 3);
			ASSERT_TRUE(mask.test(1) && mask.test(2));
		}
	}
}
