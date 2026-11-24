#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <vector>

#include "core/keyvalue/p_string.h"
#include "core/keyvalue/uuid.h"
#include "core/payload/payload_access.h"
#include "tools/unaligned.h"

namespace reindexer_tests {

using reindexer::Uuid;
using reindexer::p_string;

TEST(UnalignedPayloadAccess, ReadWriteScalars) {
	alignas(1) std::array<uint8_t, 64> buf{};
	for (size_t off : {1U, 3U, 5U, 7U}) {
		reindexer::unaligned::write(buf.data() + off, int{42});
		EXPECT_EQ(reindexer::unaligned::read<int>(buf.data() + off), 42);

		reindexer::unaligned::write(buf.data() + off, int64_t{-123456789});
		EXPECT_EQ(reindexer::unaligned::read<int64_t>(buf.data() + off), -123456789);

		reindexer::unaligned::write(buf.data() + off, 3.14);
		EXPECT_EQ(reindexer::unaligned::read<double>(buf.data() + off), 3.14);

		const Uuid uuid;
		reindexer::unaligned::write(buf.data() + off, uuid);
		EXPECT_EQ(reindexer::unaligned::read<Uuid>(buf.data() + off), uuid);

		p_string str{};
		reindexer::unaligned::write(buf.data() + off, str);
		const auto readStr = reindexer::unaligned::read<p_string>(buf.data() + off);
		EXPECT_EQ(std::memcmp(&readStr, &str, sizeof(str)), 0);
	}
}

TEST(UnalignedPayloadAccess, ArrayMetaAndView) {
	alignas(1) std::array<uint8_t, 64> buf{};
	constexpr size_t metaOffset = 1;
	constexpr size_t dataOffset = 9;

	reindexer::payload_access::ArrayMeta arr{.offset = static_cast<unsigned>(dataOffset), .len = 3};
	reindexer::payload_access::writeArrayMeta(buf.data(), metaOffset, arr);

	const auto readMeta = reindexer::payload_access::readArrayMeta(buf.data(), metaOffset);
	EXPECT_EQ(readMeta.offset, dataOffset);
	EXPECT_EQ(readMeta.len, 3);

	reindexer::unaligned::write(buf.data() + dataOffset + 0 * sizeof(int), 10);
	reindexer::unaligned::write(buf.data() + dataOffset + 1 * sizeof(int), 20);
	reindexer::unaligned::write(buf.data() + dataOffset + 2 * sizeof(int), 30);

	const auto elems = reindexer::payload_access::arrayElems<int>(buf.data(), metaOffset);
	EXPECT_EQ(elems.size(), 3U);
	EXPECT_EQ(elems[0], 10);
	EXPECT_EQ(elems[2], 30);
	EXPECT_EQ(elems.front(), 10);
	EXPECT_EQ(elems.back(), 30);

	std::vector<int> collected;
	collected.reserve(elems.size());
	for (int v : elems) {
		collected.push_back(v);
	}
	EXPECT_EQ(collected, (std::vector<int>{10, 20, 30}));

	const auto sub = elems.subspan(1, 2);
	EXPECT_EQ(sub.size(), 2U);
	EXPECT_EQ(sub[0], 20);
	EXPECT_EQ(sub[1], 30);
}

TEST(UnalignedPayloadAccess, ViewFromAlignedSpan) {
	std::array<int, 3> data{10, 20, 30};
	const reindexer::unaligned::view<int> view{std::span<const int>{data.data(), data.size()}};
	EXPECT_EQ(view.size(), 3U);
	EXPECT_EQ(view[1], 20);
	EXPECT_TRUE(view.is_aligned());

	std::vector<int> collected(view.begin(), view.end());
	EXPECT_EQ(collected, (std::vector<int>{10, 20, 30}));
}

TEST(UnalignedPayloadAccess, SubspanBounds) {
	alignas(1) std::array<uint8_t, 64> buf{};
	constexpr size_t metaOffset = 1;
	constexpr size_t dataOffset = 9;

	reindexer::payload_access::ArrayMeta arr{.offset = static_cast<unsigned>(dataOffset), .len = 3};
	reindexer::payload_access::writeArrayMeta(buf.data(), metaOffset, arr);

	reindexer::unaligned::write(buf.data() + dataOffset + 0 * sizeof(int), 10);
	reindexer::unaligned::write(buf.data() + dataOffset + 1 * sizeof(int), 20);
	reindexer::unaligned::write(buf.data() + dataOffset + 2 * sizeof(int), 30);

	const auto elems = reindexer::payload_access::arrayElems<int>(buf.data(), metaOffset);
	EXPECT_TRUE(elems.subspan(10).empty());
	EXPECT_TRUE(elems.subspan(3).empty());
	EXPECT_EQ(elems.subspan(1, 1)[0], 20);
}

}  // namespace reindexer_tests
