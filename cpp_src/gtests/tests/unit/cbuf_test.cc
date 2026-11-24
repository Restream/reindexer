#include "estl/cbuf.h"
#include "gtest/gtest.h"

#include <algorithm>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace reindexer_tests {

class [[nodiscard]] CBufApi : public virtual ::testing::Test {
public:
	using Buf = reindexer::cbuf<char>;

	static_assert(!std::is_copy_constructible_v<Buf>);
	static_assert(!std::is_copy_assignable_v<Buf>);
	static_assert(std::is_move_constructible_v<Buf>);
	static_assert(std::is_move_assignable_v<Buf>);

	static void WriteAll(Buf& b, std::string_view s) { ASSERT_EQ(b.write(s.data(), s.size()), s.size()); }

	static std::string PeekAll(Buf& b) {
		std::string out(b.size(), '\0');
		if (!out.empty()) {
			EXPECT_EQ(b.peek(out.data(), out.size()), out.size());
		}
		return out;
	}

	static std::string ReadAll(Buf& b) {
		std::string out(b.size(), '\0');
		if (!out.empty()) {
			EXPECT_EQ(b.read(out.data(), out.size()), out.size());
		}
		return out;
	}

	static void ExpectContents(Buf& b, std::string_view expected) {
		EXPECT_EQ(b.size(), expected.size());
		EXPECT_EQ(b.available(), b.capacity() - b.size());
		EXPECT_EQ(PeekAll(b), expected);
	}

	static std::string SpanStr(std::span<char> s) { return std::string(s.data(), s.size()); }

	// Places `keep` at the end of the ring, then writes `extra` from the start so the payload wraps.
	static void WriteWrapped(Buf& b, std::string_view keep, std::string_view extra) {
		ASSERT_GE(b.capacity(), keep.size() + extra.size());
		b.clear();
		const size_t pad = b.capacity() - keep.size();
		WriteAll(b, std::string(pad, 'X') + std::string(keep));
		ASSERT_EQ(b.erase(pad), pad);
		ExpectContents(b, keep);
		WriteAll(b, extra);
		ExpectContents(b, std::string(keep) + std::string(extra));
	}

	static void FillFromHead(Buf& b, std::string_view data) {
		ASSERT_GE(b.available(), data.size());
		size_t off = 0;
		while (off < data.size()) {
			auto h = b.head(data.size() - off);
			ASSERT_FALSE(h.empty());
			const size_t n = std::min(h.size(), data.size() - off);
			std::copy_n(data.data() + off, n, h.data());
			b.advance_head(n);
			off += n;
		}
	}
};

TEST_F(CBufApi, DefaultAndSizedCtor) {
	Buf empty;
	EXPECT_EQ(empty.size(), 0);
	EXPECT_EQ(empty.capacity(), 0);
	EXPECT_EQ(empty.available(), 0);

	Buf b(16);
	EXPECT_EQ(b.size(), 0);
	EXPECT_EQ(b.capacity(), 16);
	EXPECT_EQ(b.available(), 16);
	EXPECT_TRUE(b.tail().empty());
	EXPECT_EQ(b.head().size(), 16);
}

TEST_F(CBufApi, WriteToZeroCapacityGrows) {
	Buf b;
	WriteAll(b, "hello");
	EXPECT_GE(b.capacity(), 5);
	ExpectContents(b, "hello");
}

TEST_F(CBufApi, WriteReadPeekBasic) {
	Buf b(16);
	EXPECT_EQ(b.write("", 0), 0);
	EXPECT_EQ(b.read(nullptr, 0), 0);
	EXPECT_EQ(b.peek(nullptr, 0), 0);

	WriteAll(b, "abcdef");
	ExpectContents(b, "abcdef");

	char peekBuf[4] = {};
	EXPECT_EQ(b.peek(peekBuf, 3), 3);
	EXPECT_EQ(std::string_view(peekBuf, 3), "abc");
	ExpectContents(b, "abcdef");

	char readBuf[16] = {};
	EXPECT_EQ(b.read(readBuf, 2), 2);
	EXPECT_EQ(std::string_view(readBuf, 2), "ab");
	ExpectContents(b, "cdef");

	EXPECT_EQ(b.read(readBuf, 16), 4);
	EXPECT_EQ(std::string_view(readBuf, 4), "cdef");
	ExpectContents(b, "");
	EXPECT_EQ(b.read(readBuf, 4), 0);
	EXPECT_EQ(b.peek(readBuf, 4), 0);
}

TEST_F(CBufApi, PeekDoesNotConsume) {
	Buf b(8);
	WriteAll(b, "xyz");
	EXPECT_EQ(PeekAll(b), "xyz");
	EXPECT_EQ(PeekAll(b), "xyz");
	EXPECT_EQ(b.size(), 3);
}

TEST_F(CBufApi, EraseAndClearKeepCapacity) {
	Buf b(16);
	WriteAll(b, "abcdefgh");
	EXPECT_EQ(b.erase(0), 0);
	ExpectContents(b, "abcdefgh");

	EXPECT_EQ(b.erase(3), 3);
	ExpectContents(b, "defgh");

	b.clear();
	EXPECT_EQ(b.size(), 0);
	EXPECT_EQ(b.capacity(), 16);
	EXPECT_EQ(b.available(), 16);
	ExpectContents(b, "");

	WriteAll(b, "ok");
	ExpectContents(b, "ok");
}

TEST_F(CBufApi, WrapAroundWriteReadPeek) {
	Buf b(8);
	WriteWrapped(b, "ef", "ghijk");
	EXPECT_EQ(b.size(), 7);
	EXPECT_EQ(b.tail().size(), 2);
	EXPECT_EQ(SpanStr(b.tail()), "ef");

	char peekBuf[8] = {};
	EXPECT_EQ(b.peek(peekBuf, 8), 7);
	EXPECT_EQ(std::string_view(peekBuf, 7), "efghijk");

	char readBuf[8] = {};
	EXPECT_EQ(b.read(readBuf, 4), 4);
	EXPECT_EQ(std::string_view(readBuf, 4), "efgh");
	ExpectContents(b, "ijk");

	EXPECT_EQ(ReadAll(b), "ijk");
	ExpectContents(b, "");

	WriteWrapped(b, "ef", "ghijk");
	EXPECT_EQ(b.erase(4), 4);
	ExpectContents(b, "ijk");
}

TEST_F(CBufApi, HeadTailSpansAndLimits) {
	Buf b(8);
	EXPECT_TRUE(b.head(0).empty());
	EXPECT_TRUE(b.tail(0).empty());
	EXPECT_EQ(b.head(3).size(), 3);
	EXPECT_TRUE(b.tail().empty());

	WriteAll(b, "abcd");
	EXPECT_EQ(SpanStr(b.tail()), "abcd");
	EXPECT_EQ(SpanStr(b.tail(2)), "ab");
	EXPECT_EQ(b.head().size(), 4);
	EXPECT_EQ(b.head(1).size(), 1);

	WriteWrapped(b, "yz", "12");
	EXPECT_EQ(SpanStr(b.tail()), "yz");
	EXPECT_EQ(SpanStr(b.tail(1)), "y");
	EXPECT_EQ(PeekAll(b), "yz12");
}

TEST_F(CBufApi, AdvanceHeadRecvStyle) {
	Buf b(8);
	FillFromHead(b, "hello");
	ExpectContents(b, "hello");

	Buf wrapFill(8);
	WriteAll(wrapFill, "abcdef");
	ASSERT_EQ(wrapFill.erase(4), 4);
	ExpectContents(wrapFill, "ef");
	ASSERT_EQ(wrapFill.head().size(), 2);
	FillFromHead(wrapFill, "ghijk");
	ExpectContents(wrapFill, "efghijk");
}

TEST_F(CBufApi, FillToFullLeavesEmptyHead) {
	Buf b(8);
	WriteAll(b, "12345678");
	EXPECT_EQ(b.size(), 8);
	EXPECT_EQ(b.available(), 0);
	EXPECT_TRUE(b.head().empty());
	EXPECT_EQ(SpanStr(b.tail()), "12345678");
	ExpectContents(b, "12345678");

	EXPECT_EQ(b.erase(0), 0);
	EXPECT_TRUE(b.head().empty());
	EXPECT_EQ(b.size(), 8);

	b.advance_head(0);
	EXPECT_TRUE(b.head().empty());
	EXPECT_EQ(b.size(), 8);
}

TEST_F(CBufApi, UnrollLinearizesWrappedData) {
	Buf b(8);
	WriteWrapped(b, "ef", "ghijk");
	ASSERT_EQ(b.tail().size(), 2);
	ASSERT_EQ(SpanStr(b.tail()), "ef");

	b.unroll();
	EXPECT_EQ(b.capacity(), 8);
	ExpectContents(b, "efghijk");
	EXPECT_EQ(b.tail().size(), 7);
	EXPECT_EQ(SpanStr(b.tail()), "efghijk");

	b.unroll();
	ExpectContents(b, "efghijk");
	EXPECT_EQ(SpanStr(b.tail()), "efghijk");
}

TEST_F(CBufApi, ReserveAndWriteGrow) {
	Buf b(4);
	b.reserve(4);
	EXPECT_EQ(b.capacity(), 4);

	b.reserve(2);
	EXPECT_EQ(b.capacity(), 4);

	b.reserve(10);
	EXPECT_EQ(b.capacity(), 10);
	EXPECT_EQ(b.size(), 0);

	WriteAll(b, "abcd");
	WriteAll(b, "efghij");
	EXPECT_GE(b.capacity(), 10);
	ExpectContents(b, "abcdefghij");

	Buf small(4);
	WriteAll(small, "abcd");
	WriteAll(small, "xy");
	EXPECT_GE(small.capacity(), 6);
	ExpectContents(small, "abcdxy");
}

TEST_F(CBufApi, GrowPreservesWrappedData) {
	Buf b(8);
	WriteWrapped(b, "ab", "cd");
	WriteAll(b, "efghij");
	EXPECT_GE(b.capacity(), 10);
	ExpectContents(b, "abcdefghij");
}

TEST_F(CBufApi, ShrinkDoesNotGrowAndKeepsData) {
	Buf b(16);
	WriteAll(b, "abcd");
	EXPECT_TRUE(b.shrink(100));
	EXPECT_EQ(b.capacity(), 16);
	ExpectContents(b, "abcd");

	EXPECT_TRUE(b.shrink(16));
	EXPECT_EQ(b.capacity(), 16);

	EXPECT_TRUE(b.shrink(8));
	EXPECT_EQ(b.capacity(), 8);
	ExpectContents(b, "abcd");
	EXPECT_GT(b.available(), 0);

	EXPECT_TRUE(b.shrink(2));
	EXPECT_EQ(b.capacity(), 4);
	ExpectContents(b, "abcd");
	EXPECT_EQ(b.available(), 0);
	EXPECT_TRUE(b.head().empty());
}

TEST_F(CBufApi, ShrinkWrappedData) {
	Buf b(32);
	WriteWrapped(b, "keep", "wrap");
	EXPECT_TRUE(b.shrink(16));
	EXPECT_EQ(b.capacity(), 16);
	ExpectContents(b, "keepwrap");
	EXPECT_EQ(SpanStr(b.tail()), "keepwrap");
}

TEST_F(CBufApi, ShrinkEmptyToZero) {
	Buf b(16);
	EXPECT_TRUE(b.shrink(0));
	EXPECT_EQ(b.size(), 0);
	EXPECT_EQ(b.capacity(), 0);
	EXPECT_EQ(b.available(), 0);
}

TEST_F(CBufApi, HeadTailOnZeroCapacity) {
	auto expectEmptySpans = [](Buf& b) {
		// NOLINTBEGIN(clang-analyzer-cplusplus.Move)
		EXPECT_TRUE(b.head().empty());
		EXPECT_TRUE(b.tail().empty());
		EXPECT_TRUE(b.head(1).empty());
		EXPECT_TRUE(b.tail(1).empty());
		EXPECT_EQ(b.head().data(), nullptr);
		EXPECT_EQ(b.tail().data(), nullptr);
		// NOLINTEND(clang-analyzer-cplusplus.Move)
	};

	{
		Buf empty;
		expectEmptySpans(empty);
	}
	{
		Buf b(16);
		EXPECT_TRUE(b.shrink(0));
		EXPECT_EQ(b.capacity(), 0);
		expectEmptySpans(b);
		WriteAll(b, "ok");
		ExpectContents(b, "ok");
	}
	{
		Buf src(16);
		WriteAll(src, "payload");
		Buf moved(std::move(src));
		// NOLINTBEGIN(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
		expectEmptySpans(src);
		src = std::move(moved);
		expectEmptySpans(moved);
		// NOLINTEND(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
	}
}

TEST_F(CBufApi, ShrinkIfNeededHysteresis) {
	{
		Buf b(32);
		WriteAll(b, "abc");
		EXPECT_TRUE(b.shrink_if_needed(8, 16));
		EXPECT_EQ(b.capacity(), 8);
		ExpectContents(b, "abc");
		EXPECT_GT(b.available(), 0);
	}
	{
		Buf b(16);
		WriteAll(b, "abc");
		EXPECT_TRUE(b.shrink_if_needed(8, 16));
		EXPECT_EQ(b.capacity(), 16);
		ExpectContents(b, "abc");
	}
	{
		Buf b(32);
		WriteAll(b, std::string(8, 'x'));
		EXPECT_TRUE(b.shrink_if_needed(8, 16));
		EXPECT_EQ(b.capacity(), 32);
		ExpectContents(b, std::string(8, 'x'));
	}
	{
		Buf b(32);
		WriteAll(b, std::string(7, 'x'));
		EXPECT_TRUE(b.shrink_if_needed(8, 16));
		EXPECT_EQ(b.capacity(), 8);
		ExpectContents(b, std::string(7, 'x'));
		EXPECT_EQ(b.available(), 1);
	}
	{
		Buf b(1024);
		EXPECT_TRUE(b.shrink_if_needed(64, 128));
		EXPECT_EQ(b.capacity(), 64);
		EXPECT_EQ(b.size(), 0);
	}
	{
		Buf b(1024);
		EXPECT_TRUE(b.shrink_if_needed(0, 128));
		EXPECT_EQ(b.capacity(), 1024);
	}
}

TEST_F(CBufApi, MoveCtorAndAssign) {
	Buf src(16);
	WriteAll(src, "payload");

	Buf moved(std::move(src));
	ExpectContents(moved, "payload");
	// NOLINTBEGIN(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
	EXPECT_EQ(src.size(), 0);
	EXPECT_EQ(src.capacity(), 0);
	EXPECT_EQ(src.available(), 0);
	// NOLINTEND(bugprone-use-after-move,clang-analyzer-cplusplus.Move)

	Buf dst(4);
	WriteAll(dst, "old");
	dst = std::move(moved);
	ExpectContents(dst, "payload");
	// NOLINTBEGIN(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
	EXPECT_EQ(moved.size(), 0);
	EXPECT_EQ(moved.capacity(), 0);
	// NOLINTEND(bugprone-use-after-move,clang-analyzer-cplusplus.Move)

	Buf wrapped(8);
	WriteWrapped(wrapped, "ef", "gh");
	Buf wrappedMoved = std::move(wrapped);
	ExpectContents(wrappedMoved, "efgh");
}

}  // namespace reindexer_tests
