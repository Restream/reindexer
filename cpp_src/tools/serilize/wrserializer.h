#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>

#include "core/cjson/ctag.h"
#include "core/enums.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/uuid.h"
#include "core/keyvalue/variant.h"
#include "core/rank_t.h"
#include "estl/chunk.h"
#include "estl/membuf.h"
#include "tools/stringstools.h"
#include "tools/varint.h"

// Defined in vendored libs
char* i32toa(int32_t value, char* buffer);
char* i64toa(int64_t value, char* buffer);

namespace reindexer {

class [[nodiscard]] WrSerializer {
public:
	class [[nodiscard]] GrowthPolicy {
	public:
		GrowthPolicy() noexcept : GrowthPolicy(0) {}
		explicit GrowthPolicy(size_t maxCap) noexcept : maxCap_((maxCap > 0 && maxCap < kMaxCapacity) ? maxCap : kMaxCapacity) {}

		size_t GetNewCapacity(size_t curCap, size_t requestedSize) const {
			constexpr static size_t kPageMask = ~size_t(0xFFF);
			const auto newCap = ((curCap * 2) + requestedSize);
			const auto newCapAligned = newCap & kPageMask;
			const auto result = (newCap == newCapAligned) ? newCap : (newCapAligned + 0x1000);
			if (result <= maxCap_) [[likely]] {
				return result;
			}
			if (requestedSize > maxCap_) [[unlikely]] {
				throwOverflowError(requestedSize);
			}
			return maxCap_;
		}

	private:
		constexpr static size_t kMaxCapacity = std::numeric_limits<std::ptrdiff_t>::max();

		[[noreturn]] void throwOverflowError(size_t requestedSize) const;
		size_t maxCap_;
	};

	WrSerializer(GrowthPolicy&& growthPolicy = GrowthPolicy()) noexcept
		: buf_(inBuf_), len_(0), cap_(sizeof(inBuf_)), growthPolicy_(std::move(growthPolicy)) {}
	template <unsigned N>
	explicit WrSerializer(uint8_t (&buf)[N], GrowthPolicy&& growthPolicy = GrowthPolicy()) noexcept
		: buf_(buf), len_(0), cap_(N), hasExternalBuf_(true), growthPolicy_(std::move(growthPolicy)) {}
	explicit WrSerializer(chunk&& ch, GrowthPolicy&& growthPolicy = GrowthPolicy()) noexcept
		: buf_(ch.release()), len_(ch.len()), cap_(ch.capacity()), growthPolicy_(std::move(growthPolicy)) {
		if (!buf_) {
			buf_ = inBuf_;
			cap_ = sizeof(inBuf_);
			len_ = 0;
		}
	}
	WrSerializer(const WrSerializer&) = delete;
	WrSerializer(WrSerializer&& other) noexcept
		: len_(other.len_), cap_(other.cap_), hasExternalBuf_(other.hasExternalBuf_), growthPolicy_(std::move(other.growthPolicy_)) {
		if (other.buf_ == other.inBuf_) {
			buf_ = inBuf_;
			memcpy(buf_, other.buf_, other.len_ * sizeof(other.inBuf_[0]));
		} else {
			buf_ = other.buf_;
			other.buf_ = other.inBuf_;
		}
		other.len_ = 0;
		other.cap_ = 0;
		other.hasExternalBuf_ = false;
	}
	~WrSerializer() {
		if (HasAllocatedBuffer()) {
			delete[] buf_;	// NOLINT(*.NewDelete) False positive
		}
	}
	WrSerializer& operator=(const WrSerializer&) = delete;
	WrSerializer& operator=(WrSerializer&& other) noexcept {
		if (this != &other) {
			if (HasAllocatedBuffer()) {
				delete[] buf_;	// NOLINT(*.NewDelete) False positive
			}

			len_ = other.len_;
			cap_ = other.cap_;

			if (other.buf_ == other.inBuf_) {
				buf_ = inBuf_;
				memcpy(buf_, other.buf_, other.len_ * sizeof(other.inBuf_[0]));
			} else {
				buf_ = other.buf_;
				other.buf_ = other.inBuf_;
			}

			hasExternalBuf_ = other.hasExternalBuf_;
			other.len_ = 0;
			other.cap_ = 0;
			other.hasExternalBuf_ = false;
		}

		return *this;
	}
	bool HasAllocatedBuffer() const noexcept { return buf_ != inBuf_ && !hasExternalBuf_; }

	RX_ALWAYS_INLINE void PutKeyValueType(KeyValueType t) { PutVarUint(t.ToNumber()); }
	void PutVariant(const Variant& kv) {
		PutKeyValueType(kv.Type());
		kv.Type().EvaluateOneOf(
			[&](KeyValueType::Tuple) {
				auto compositeValues = kv.getCompositeValues();
				PutVarUint(compositeValues.size());
				for (auto& v : compositeValues) {
					PutVariant(v);
				}
			},
			[&](KeyValueType::Bool) { PutBool(bool(kv)); }, [&](KeyValueType::Int64) { PutVarint(int64_t(kv)); },
			[&](KeyValueType::Int) { PutVarint(int(kv)); }, [&](KeyValueType::Double) { PutDouble(double(kv)); },
			[&](KeyValueType::Float) { PutFloat(float(kv)); }, [&](KeyValueType::String) { PutVString(std::string_view(kv)); },
			[&](KeyValueType::Null) noexcept {}, [&](KeyValueType::Uuid) { PutUuid(Uuid{kv}); },
			[&](KeyValueType::FloatVector) { PutFloatVectorView(ConstFloatVectorView{kv}); },
			[&](concepts::OneOf<KeyValueType::Composite, KeyValueType::Undefined> auto) {
				std::string name(kv.Type().Name());
				fprintf(stderr, "reindexer error: unknown keyType %s\n", name.data());
				abort();
			});
	}

	// Put slice with 4 bytes len header
	RX_ALWAYS_INLINE void PutSlice(std::string_view slice) {
		PutUInt32(slice.size());
		grow(slice.size());
		// TODO: Check with newer version. Clang-tidy v22 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		memcpy(&buf_[len_], slice.data(), slice.size());
		len_ += slice.size();
	}

private:
	class [[nodiscard]] SliceHelper {
	public:
		SliceHelper(WrSerializer* ser, size_t pos) noexcept : ser_(ser), pos_(pos) {}
		SliceHelper(const SliceHelper&) = delete;
		SliceHelper operator=(const SliceHelper&) = delete;
		SliceHelper(SliceHelper&& other) noexcept : ser_(other.ser_), pos_(other.pos_) { other.ser_ = nullptr; }
		SliceHelper& operator=(SliceHelper&& other) noexcept {
			if (this != &other) {
				ser_ = other.ser_;
				pos_ = other.pos_;
				other.ser_ = nullptr;
			}
			return *this;
		}
		~SliceHelper() {
			if (ser_) {
				uint32_t sliceSize = ser_->len_ - pos_ - sizeof(uint32_t);
				memcpy(&ser_->buf_[pos_], &sliceSize, sizeof(sliceSize));
			}
			ser_ = nullptr;
		}

	private:
		const WrSerializer* ser_{nullptr};
		size_t pos_{0};
	};

public:
	class [[nodiscard]] VStringHelper {
	public:
		VStringHelper() noexcept : ser_(nullptr), pos_(0) {}
		VStringHelper(WrSerializer* ser, size_t pos) noexcept : ser_(ser), pos_(pos) {}
		VStringHelper(const VStringHelper&) = delete;
		VStringHelper operator=(const VStringHelper&) = delete;
		VStringHelper(VStringHelper&& other) noexcept : ser_(other.ser_), pos_(other.pos_) { other.ser_ = nullptr; }
		VStringHelper& operator=(VStringHelper&& other) noexcept {
			if (this != &other) {
				ser_ = other.ser_;
				pos_ = other.pos_;
				other.ser_ = nullptr;
			}
			return *this;
		}
		~VStringHelper() noexcept(false) {
			if (std::uncaught_exceptions() == 0) {
				// The End() call may throw if the internal WrSerializer is unable to allocate memory due to logical
				// (GrowthPolicy) or system (std::bad_alloc) limitations. Checking std::uncaught_exceptions() allows us
				// to avoid throwing an exception in scenarios where we're already handling another exception.
				End();
			}
		}
		void End();

	private:
		WrSerializer* ser_{nullptr};
		size_t pos_{0};
	};

	SliceHelper StartSlice() {
		const size_t savePos = len_;
		PutUInt32(0);
		return {this, savePos};
	}
	VStringHelper StartVString() noexcept { return {this, len_}; }

	// Put raw data
	RX_ALWAYS_INLINE void PutUInt8(uint8_t v) {
		grow(sizeof(v));
		memcpy(&buf_[len_], &v, sizeof(v));
		len_ += sizeof(v);
	}
	RX_ALWAYS_INLINE void PutUInt32(uint32_t v) {
		grow(sizeof(v));
		// TODO: Check with newer version. Clang-tidy v22 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		memcpy(&buf_[len_], &v, sizeof(v));
		len_ += sizeof(v);
	}
	RX_ALWAYS_INLINE void PutCArrayTag(carraytag atag) { carraytag::Serialize(atag, *this); }
	RX_ALWAYS_INLINE void PutUInt64(uint64_t v) {
		grow(sizeof(v));
		memcpy(&buf_[len_], &v, sizeof(v));
		len_ += sizeof(v);
	}
	RX_ALWAYS_INLINE void PutDouble(double v) {
		grow(sizeof(v));
		memcpy(&buf_[len_], &v, sizeof(v));
		len_ += sizeof(v);
	}
	RX_ALWAYS_INLINE void PutFloat(float v) {
		grow(sizeof(v));
		memcpy(&buf_[len_], &v, sizeof(v));
		len_ += sizeof(v);
	}
	RX_ALWAYS_INLINE void PutRank(RankT v) { PutFloat(v.Value()); }
	void PutFloatVectorView(ConstFloatVectorView v) {
		using Float = ConstFloatVectorView::DataType;
		static_assert(alignof(Float) <= sizeof(Float));
		PutVarUint((uint64_t(v.Dimension()) << 1) | (v.IsStripped() ? 1 : 0));
		if (!v.IsStrippedOrEmpty()) {
			const uint64_t memSize = sizeof(Float) * v.Dimension().Value();
			grow(memSize);
			memcpy(&buf_[len_], v.Data(), memSize);
			len_ += memSize;
		}
	}
	void PutFPStrNoTrailing(double v) {
		grow(32);
		len_ += double_to_str_no_trailing(v, reinterpret_cast<char*>(buf_ + len_), 32);
	}
	void PutFPStrNoTrailing(float v) {
		grow(32);
		len_ += float_to_str_no_trailing(v, reinterpret_cast<char*>(buf_ + len_), 32);
	}

	template <typename T, typename std::enable_if<sizeof(T) == 8 && std::is_integral<T>::value>::type* = nullptr>
	WrSerializer& operator<<(T k) {
		grow(32);
		char* b = i64toa(k, reinterpret_cast<char*>(buf_ + len_));
		len_ = b - reinterpret_cast<char*>(buf_);
		return *this;
	}
	template <typename T, typename std::enable_if<sizeof(T) <= 4 && std::is_integral<T>::value>::type* = nullptr>
	WrSerializer& operator<<(T k) {
		grow(32);
		char* b = i32toa(k, reinterpret_cast<char*>(buf_ + len_));
		len_ = b - reinterpret_cast<char*>(buf_);
		return *this;
	}

	WrSerializer& operator<<(char c) {
		grow(1);
		// TODO: Check with newer version. Clang-tidy v22 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		buf_[len_++] = c;
		return *this;
	}
	WrSerializer& operator<<(std::string_view sv) {
		Write(sv);
		return *this;
	}
	WrSerializer& operator<<(const char* sv) {
		Write(std::string_view(sv));
		return *this;
	}
	WrSerializer& operator<<(bool v) {
		using namespace std::string_view_literals;
		Write(v ? "true"sv : "false"sv);
		return *this;
	}
	WrSerializer& operator<<(double v) {
		grow(32);
		len_ += double_to_str(v, reinterpret_cast<char*>(buf_ + len_), 32);
		return *this;
	}
	WrSerializer& operator<<(float v) {
		grow(32);
		len_ += float_to_str(v, reinterpret_cast<char*>(buf_ + len_), 32);
		return *this;
	}
	WrSerializer& operator<<(Uuid uuid) {
		grow(Uuid::kStrFormLen + 2);
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		buf_[len_] = '\'';
		++len_;
		uuid.PutToStr(std::span<char>{reinterpret_cast<char*>(&buf_[len_]), Uuid::kStrFormLen});
		len_ += Uuid::kStrFormLen;
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		buf_[len_] = '\'';
		++len_;
		return *this;
	}

	enum class [[nodiscard]] PrintJsonStringMode { Default = 0, QuotedQuote = 1 };
	void PrintJsonString(std::string_view str, PrintJsonStringMode mode = PrintJsonStringMode::Default);
	void PrintJsonUuid(Uuid);

	void PrintHexDump(std::string_view str);
	void Fill(char c, size_t count) {
		grow(count);
		// TODO: Check with newer version. Clang-tidy v22 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		memset(&buf_[len_], c, count);
		len_ += count;
	}
	template <typename T, typename std::enable_if_t<sizeof(T) == 8 && std::is_integral_v<T>>* = nullptr>
	RX_ALWAYS_INLINE void PutVarint(T v) {
		grow(10);
		len_ += sint64_pack(v, buf_ + len_);
	}
	template <typename T, typename std::enable_if_t<sizeof(T) == 8 && std::is_integral_v<T>>* = nullptr>
	RX_ALWAYS_INLINE void PutVarUint(T v) {
		grow(10);
		len_ += uint64_pack(v, buf_ + len_);
	}
	template <typename T, typename std::enable_if_t<sizeof(T) <= 4 && std::is_integral_v<T>>* = nullptr>
	RX_ALWAYS_INLINE void PutVarint(T v) {
		grow(10);
		len_ += sint32_pack(v, buf_ + len_);
	}
	template <typename T, typename std::enable_if_t<sizeof(T) <= 4 && std::is_integral_v<T>>* = nullptr>
	RX_ALWAYS_INLINE void PutVarUint(T v) {
		grow(10);
		len_ += uint32_pack(v, buf_ + len_);
	}
	template <typename T, typename std::enable_if_t<std::is_enum_v<T>>* = nullptr>
	RX_ALWAYS_INLINE void PutVarUint(T v) {
		assertrx(int64_t(v) >= 0 && int64_t(v) < 128);
		grow(1);
		// TODO: Check with newer version. Clang-tidy v22 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		buf_[len_++] = uint8_t(v);
	}
	RX_ALWAYS_INLINE void PutCTag(ctag tag) { ctag::Serialize(tag, *this); }
	void PutCTag(KeyValueType fieldType, TagName tagName, int indexNumber) {
		fieldType.EvaluateOneOf(
			[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64> auto) { PutCTag(ctag{TAG_VARINT, tagName, indexNumber}); },
			[&](concepts::OneOf<KeyValueType::Double, KeyValueType::Float, KeyValueType::String, KeyValueType::Bool, KeyValueType::Null,
								KeyValueType::Uuid> auto) { PutCTag(ctag{fieldType.ToTagType(), tagName, indexNumber}); },
			[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::FloatVector> auto) {
				assertrx(false);
			});
	}
	RX_ALWAYS_INLINE void PutBool(bool v) {
		grow(1);
		len_ += boolean_pack(v, buf_ + len_);
	}
	RX_ALWAYS_INLINE void PutVString(std::string_view str) {
		grow(str.size() + 10);
		len_ += string_pack(str.data(), str.size(), buf_ + len_);
	}
	void PutStrUuid(Uuid);
	void PutUuid(Uuid uuid) { Uuid::Serialize(uuid, *this); }

	// Buffer manipulation functions
	RX_ALWAYS_INLINE void Write(std::string_view slice) {
		grow(slice.size());
		// TODO: Check with newer version. Clang-tidy v22 is unable to validate len correctly
		// NOLINTNEXTLINE (clang-analyzer-security.ArrayBound)
		memcpy(&buf_[len_], slice.data(), slice.size());
		len_ += slice.size();
	}
	RX_ALWAYS_INLINE uint8_t* Buf() const noexcept { return buf_; }
	std::unique_ptr<uint8_t[]> DetachBuf(size_t minCap = 0) {
		std::unique_ptr<uint8_t[]> ret;

		if (!HasAllocatedBuffer()) {
			ret.reset(new uint8_t[std::max(len_, minCap)]);
			memcpy(ret.get(), buf_, len_);
		} else {
			ret.reset(buf_);
		}
		buf_ = inBuf_;
		cap_ = sizeof(inBuf_);
		len_ = 0;
		hasExternalBuf_ = false;
		return ret;
	}
	MemBuf DetachLStr(Shrink shrink = Shrink_False);
	chunk DetachChunk() {
		chunk ch;
		if (!HasAllocatedBuffer()) {
			ch.append_strict(Slice());
		} else {
			ch = chunk(buf_, len_, cap_);
		}
		buf_ = inBuf_;
		cap_ = sizeof(inBuf_);
		len_ = 0;
		hasExternalBuf_ = false;
		return ch;
	}
	void Reset(size_t len = 0) noexcept { len_ = len; }
	size_t Len() const noexcept { return len_; }
	size_t Cap() const noexcept { return cap_; }
	void Reserve(size_t cap) {
		if (cap > cap_) {
			auto b = std::make_unique<uint8_t[]>(cap);
			memcpy(b.get(), buf_, len_);
			if (HasAllocatedBuffer()) {
				delete[] buf_;	// NOLINT(*.NewDelete) False positive
			}
			buf_ = b.release();
			cap_ = cap;
			hasExternalBuf_ = false;
		}
	}
	RX_ALWAYS_INLINE std::string_view Slice() const noexcept { return {reinterpret_cast<const char*>(buf_), len_}; }
	bool HasHeap() const noexcept { return buf_ != inBuf_ && cap_; }

protected:
	RX_ALWAYS_INLINE void grow(size_t sz) {
		if (len_ + sz > cap_) {
			Reserve(growthPolicy_.GetNewCapacity(cap_, len_ + sz));
			assertrx_dbg(len_ + sz <= cap_);
		}
	}

private:
	uint8_t* buf_{nullptr};
	size_t len_{0};
	size_t cap_{0};
	uint8_t inBuf_[0x100];
	bool hasExternalBuf_{false};
	GrowthPolicy growthPolicy_;
};

inline int msgpack_wrserializer_write(void* data, const char* buf, size_t len) {
	reinterpret_cast<WrSerializer*>(data)->Write(std::string_view(buf, len));
	return 0;
}

}  // namespace reindexer
