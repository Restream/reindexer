#include <gtest/gtest.h>
#include <string>

#include "tools/serilize/wrserializer.h"

TEST(WrSerializer, GrowthPolicyNoExceedsMaxCapacity) {
	reindexer::WrSerializer ser{reindexer::WrSerializer::GrowthPolicy{1024}};
	const std::string payload(1024, 'x');
	EXPECT_NO_THROW(ser.Write(payload));
	EXPECT_EQ(ser.Len(), payload.size());
	EXPECT_EQ(ser.Cap(), 1024);
}

TEST(WrSerializer, GrowthPolicyExceedsMaxCapacity) {
	reindexer::WrSerializer ser{reindexer::WrSerializer::GrowthPolicy{1024}};
	const std::string payload(1025, 'x');
	EXPECT_THROW(ser.Write(payload), reindexer::Error);
}

TEST(WrSerializer, GrowthPolicyChunkGrowing) {
	constexpr size_t kMaxCap = 1024;
	reindexer::WrSerializer ser{reindexer::WrSerializer::GrowthPolicy{kMaxCap}};
	std::vector<size_t> chunkSizes = {100, 200, 300, 400, 500};
	size_t total = 0;
	for (auto& ch : chunkSizes) {
		const std::string payload(ch, 'x');
		total += ch;
		if (total < kMaxCap) {
			EXPECT_NO_THROW(ser.Write(payload));
		} else {
			EXPECT_THROW(ser.Write(payload), reindexer::Error);
		}
		EXPECT_LE(ser.Cap(), kMaxCap);
	}
}

TEST(WrSerializer, GrowthPolicyMinStaticCapacity) {
	reindexer::WrSerializer ser{reindexer::WrSerializer::GrowthPolicy{10}};
	const size_t initialCap = ser.Cap();
	EXPECT_EQ(ser.Cap(), 0x100);
	const std::string payload(initialCap, 'x');
	EXPECT_NO_THROW(ser.Write(payload));
	EXPECT_THROW(ser << 1, reindexer::Error);
}

TEST(WrSerializer, GrowthPolicyDefault) {
	auto isPageAligned = [](size_t cap) { return (cap & 0xFFF) == 0; };

	reindexer::WrSerializer ser{reindexer::WrSerializer::GrowthPolicy{}};
	EXPECT_EQ(ser.Cap(), 0x100);
	const std::string payload(1023, 'x');
	for (size_t i = 0; i < 10; ++i) {
		EXPECT_NO_THROW(ser.Write(payload));
		EXPECT_TRUE(isPageAligned(ser.Cap()));
	}
}
