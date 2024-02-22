#pragma once

#include <memory>
#include <string_view>
#include "core/cjson/ctag.h"
#include "core/keyvalue/uuid.h"
#include "core/keyvalue/variant.h"
#include "estl/chunk.h"
#include "estl/one_of.h"
#include "estl/span.h"
#include "tools/stringstools.h"
#include "tools/varint.h"

char *i32toa(int32_t value, char *buffer);
char *i64toa(int64_t value, char *buffer);

namespace reindexer {

struct p_string;
struct v_string_hdr;
class chunk;

class Serializer {
public:
	Serializer(const void *buf, size_t len) noexcept : buf_(static_cast<const uint8_t *>(buf)), len_(len), pos_(0) {}
	Serializer(std::string_view buf) noexcept : buf_(reinterpret_cast<const uint8_t *>(buf.data())), len_(buf.length()), pos_(0) {}
	bool Eof() const noexcept { return pos_ >= len_; }
	[[nodiscard]] KeyValueType GetKeyValueType() { return KeyValueType::fromNumber(GetVarUint()); }
	[[nodiscard]] Variant GetVariant() {
		const KeyValueType type = GetKeyValueType();
		if (type.Is<KeyValueType::Tuple>()) {
			VariantArray compositeValues;
			uint64_t count = GetVarUint();
			compositeValues.reserve(count);
			for (size_t i = 0; i < count; ++i) {
				compositeValues.emplace_back(GetVariant());
			}
			return Variant(compositeValues);
		} else {
			return GetRawVariant(type);
		}
	}
	[[nodiscard]] Variant GetRawVariant(KeyValueType type) {
		return type.EvaluateOneOf(
			[this](KeyValueType::Int) { return Variant(int(GetVarint())); },
			[this](KeyValueType::Bool) { return Variant(bool(GetVarUint())); },
			[this](KeyValueType::Int64) { return Variant(int64_t(GetVarint())); },
			[this](KeyValueType::Double) { return Variant(GetDouble()); }, [this](KeyValueType::String) { return getPVStringVariant(); },
			[](KeyValueType::Null) noexcept { return Variant(); }, [this](KeyValueType::Uuid) { return Variant{GetUuid()}; },
			[this, &type](OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined>) -> Variant {
				throwUnknowTypeError(type.Name());
			});
	}
	void SkipRawVariant(KeyValueType type) {
		type.EvaluateOneOf([this](KeyValueType::Int) { GetVarint(); }, [this](KeyValueType::Bool) { GetVarUint(); },
						   [this](KeyValueType::Int64) { GetVarint(); }, [this](KeyValueType::Double) { GetDouble(); },
						   [this](KeyValueType::String) { getPVStringPtr(); }, [](KeyValueType::Null) noexcept {},
						   [this](KeyValueType::Uuid) { GetUuid(); },
						   [this, &type](OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined>) {
							   throwUnknowTypeError(type.Name());
						   });
	}
	std::string_view GetSlice() {
		auto l = GetUInt32();
		std::string_view b(reinterpret_cast<const char *>(buf_ + pos_), l);
		checkbound(pos_, b.size(), len_);
		pos_ += b.size();
		return b;
	}
	uint32_t GetUInt32() {
		uint32_t ret;
		checkbound(pos_, sizeof(ret), len_);
		memcpy(&ret, buf_ + pos_, sizeof(ret));
		pos_ += sizeof(ret);
		return ret;
	}
	uint64_t GetUInt64() {
		uint64_t ret;
		checkbound(pos_, sizeof(ret), len_);
		memcpy(&ret, buf_ + pos_, sizeof(ret));
		pos_ += sizeof(ret);
		return ret;
	}
	double GetDouble() {
		double ret;
		checkbound(pos_, sizeof(ret), len_);
		memcpy(&ret, buf_ + pos_, sizeof(ret));
		pos_ += sizeof(ret);
		return ret;
	}
	Uuid GetUuid() {
		const uint64_t v1 = GetUInt64();
		const uint64_t v2 = GetUInt64();
		return Uuid{v1, v2};
	}
	int64_t GetVarint() {
		auto l = scan_varint(len_ - pos_, buf_ + pos_);
		if (l == 0) {
			using namespace std::string_view_literals;
			throwScanIntError("scan_varint"sv);
		}

		checkbound(pos_, l, len_);
		pos_ += l;
		return unzigzag64(parse_uint64(l, buf_ + pos_ - l));
	}
	uint64_t GetVarUint() {	 // -V1071
		auto l = scan_varint(len_ - pos_, buf_ + pos_);
		if (l == 0) {
			using namespace std::string_view_literals;
			throwScanIntError("scan_varuint"sv);
		}
		checkbound(pos_, l, len_);
		pos_ += l;
		return parse_uint64(l, buf_ + pos_ - l);
	}
	[[nodiscard]] ctag GetCTag() { return ctag{GetVarUint()}; }
	[[nodiscard]] carraytag GetCArrayTag() { return carraytag{GetUInt32()}; }
	std::string_view GetVString() {
		auto l = GetVarUint();
		checkbound(pos_, l, len_);
		pos_ += l;
		return std::string_view(reinterpret_cast<const char *>(buf_ + pos_ - l), l);
	}
	p_string GetPVString();
	p_string GetPSlice();
	[[nodiscard]] Uuid GetStrUuid() { return Uuid{GetVString()}; }
	bool GetBool() { return bool(GetVarUint()); }
	size_t Pos() const noexcept { return pos_; }
	void SetPos(size_t p) noexcept { pos_ = p; }
	const uint8_t *Buf() const noexcept { return buf_; }
	size_t Len() const noexcept { return len_; }
	void Reset() noexcept { pos_ = 0; }

private:
	void checkbound(uint64_t pos, uint64_t need, uint64_t len) {
		if (pos + need > len) {
			throwUnderflowError(pos, need, len);
		}
	}
	[[noreturn]] void throwUnderflowError(uint64_t pos, uint64_t need, uint64_t len);
	[[noreturn]] void throwScanIntError(std::string_view type);
	[[noreturn]] void throwUnknowTypeError(std::string_view type);
	Variant getPVStringVariant();
	const v_string_hdr *getPVStringPtr();

	const uint8_t *buf_;
	size_t len_;
	size_t pos_;
};

class WrSerializer {
public:
	WrSerializer() noexcept : buf_(inBuf_), len_(0), cap_(sizeof(inBuf_)) {}
	template <unsigned N>
	WrSerializer(uint8_t (&buf)[N]) noexcept : buf_(buf), len_(0), cap_(N), hasExternalBuf_(true) {}
	WrSerializer(chunk &&ch) noexcept : buf_(ch.release()), len_(ch.len()), cap_(ch.capacity()) {
		if (!buf_) {
			buf_ = inBuf_;
			cap_ = sizeof(inBuf_);
			len_ = 0;
		}
	}
	WrSerializer(const WrSerializer &) = delete;
	WrSerializer(WrSerializer &&other) noexcept : len_(other.len_), cap_(other.cap_), hasExternalBuf_(other.hasExternalBuf_) {
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
	~WrSerializer() {
		if (HasAllocatedBuffer()) delete[] buf_;  // NOLINT(*.NewDelete) False positive
	}
	WrSerializer &operator=(const WrSerializer &) = delete;
	WrSerializer &operator=(WrSerializer &&other) noexcept {
		if (this != &other) {
			if (HasAllocatedBuffer()) delete[] buf_;

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

	void PutKeyValueType(KeyValueType t) { PutVarUint(t.toNumber()); }
	void PutVariant(const Variant &kv) {
		PutKeyValueType(kv.Type());
		kv.Type().EvaluateOneOf(
			[&](KeyValueType::Tuple) {
				auto compositeValues = kv.getCompositeValues();
				PutVarUint(compositeValues.size());
				for (auto &v : compositeValues) {
					PutVariant(v);
				}
			},
			[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Double, KeyValueType::String,
					  KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Uuid>) {
				kv.Type().EvaluateOneOf(
					[&](KeyValueType::Bool) { PutBool(bool(kv)); }, [&](KeyValueType::Int64) { PutVarint(int64_t(kv)); },
					[&](KeyValueType::Int) { PutVarint(int(kv)); }, [&](KeyValueType::Double) { PutDouble(double(kv)); },
					[&](KeyValueType::String) { PutVString(std::string_view(kv)); }, [&](KeyValueType::Null) noexcept {},
					[&](KeyValueType::Uuid) { PutUuid(Uuid{kv}); },
					[&](OneOf<KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined>) {
						fprintf(stderr, "Unknown keyType %s\n", kv.Type().Name().data());
						abort();
					});
			});
	}

	// Put slice with 4 bytes len header
	void PutSlice(std::string_view slice) {
		PutUInt32(slice.size());
		grow(slice.size());
		memcpy(&buf_[len_], slice.data(), slice.size());
		len_ += slice.size();
	}

private:
	class [[nodiscard]] SliceHelper {
	public:
		SliceHelper(WrSerializer *ser, size_t pos) noexcept : ser_(ser), pos_(pos) {}
		SliceHelper(const SliceHelper &) = delete;
		SliceHelper operator=(const SliceHelper &) = delete;
		SliceHelper(SliceHelper &&other) noexcept : ser_(other.ser_), pos_(other.pos_) { other.ser_ = nullptr; }
		SliceHelper &operator=(SliceHelper &&other) noexcept {
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
		WrSerializer *ser_;
		size_t pos_;
	};

public:
	class [[nodiscard]] VStringHelper {
	public:
		VStringHelper() noexcept : ser_(nullptr), pos_(0) {}
		VStringHelper(WrSerializer *ser, size_t pos) noexcept : ser_(ser), pos_(pos) {}
		VStringHelper(const VStringHelper &) = delete;
		VStringHelper operator=(const VStringHelper &) = delete;
		VStringHelper(VStringHelper &&other) noexcept : ser_(other.ser_), pos_(other.pos_) { other.ser_ = nullptr; }
		VStringHelper &operator=(VStringHelper &&other) noexcept {
			if (this != &other) {
				ser_ = other.ser_;
				pos_ = other.pos_;
				other.ser_ = nullptr;
			}
			return *this;
		}
		~VStringHelper() { End(); }
		void End();

	private:
		WrSerializer *ser_;
		size_t pos_;
	};

	SliceHelper StartSlice() {
		size_t savePos = len_;
		PutUInt32(0);
		return SliceHelper(this, savePos);
	}
	VStringHelper StartVString() {
		size_t savePos = len_;
		return VStringHelper(this, savePos);
	}

	// Put raw data
	void PutUInt32(uint32_t v) {
		grow(sizeof(v));
		memcpy(&buf_[len_], &v, sizeof(v));
		len_ += sizeof(v);
	}
	void PutCArrayTag(carraytag atag) { PutUInt32(atag.asNumber()); }
	void PutUInt64(uint64_t v) {
		grow(sizeof(v));
		memcpy(&buf_[len_], &v, sizeof(v));
		len_ += sizeof(v);
	}
	void PutDouble(double v) {
		grow(sizeof(v));
		memcpy(&buf_[len_], &v, sizeof(v));
		len_ += sizeof(v);
	}
	void PutDoubleStrNoTrailing(double v) {
		grow(32);
		len_ += double_to_str_no_trailing(v, reinterpret_cast<char *>(buf_ + len_), 32);
	}

	template <typename T, typename std::enable_if<sizeof(T) == 8 && std::is_integral<T>::value>::type * = nullptr>
	WrSerializer &operator<<(T k) {
		grow(32);
		char *b = i64toa(k, reinterpret_cast<char *>(buf_ + len_));
		len_ = b - reinterpret_cast<char *>(buf_);
		return *this;
	}
	template <typename T, typename std::enable_if<sizeof(T) <= 4 && std::is_integral<T>::value>::type * = nullptr>
	WrSerializer &operator<<(T k) {
		grow(32);
		char *b = i32toa(k, reinterpret_cast<char *>(buf_ + len_));
		len_ = b - reinterpret_cast<char *>(buf_);
		return *this;
	}

	WrSerializer &operator<<(char c) {
		if (len_ + 1 >= cap_) grow(1);
		buf_[len_++] = c;
		return *this;
	}
	WrSerializer &operator<<(std::string_view sv) {
		Write(sv);
		return *this;
	}
	WrSerializer &operator<<(const char *sv) {
		Write(std::string_view(sv));
		return *this;
	}
	WrSerializer &operator<<(bool v) {
		using namespace std::string_view_literals;
		Write(v ? "true"sv : "false"sv);
		return *this;
	}
	WrSerializer &operator<<(double v) {
		grow(32);
		len_ += double_to_str(v, reinterpret_cast<char *>(buf_ + len_), 32);
		return *this;
	}
	WrSerializer &operator<<(Uuid uuid) {
		grow(Uuid::kStrFormLen + 2);
		buf_[len_] = '\'';
		++len_;
		uuid.PutToStr(span<char>{reinterpret_cast<char *>(&buf_[len_]), Uuid::kStrFormLen});
		len_ += Uuid::kStrFormLen;
		buf_[len_] = '\'';
		++len_;
		return *this;
	}

	enum class PrintJsonStringMode { Default = 0, QuotedQuote = 1 };
	void PrintJsonString(std::string_view str, PrintJsonStringMode mode = PrintJsonStringMode::Default);
	void PrintJsonUuid(Uuid);

	void PrintHexDump(std::string_view str);
	void Fill(char c, size_t count) {
		grow(count);
		memset(&buf_[len_], c, count);
		len_ += count;
	}
	template <typename T, typename std::enable_if_t<sizeof(T) == 8 && std::is_integral_v<T>> * = nullptr>
	void PutVarint(T v) {
		grow(10);
		len_ += sint64_pack(v, buf_ + len_);
	}
	template <typename T, typename std::enable_if_t<sizeof(T) == 8 && std::is_integral_v<T>> * = nullptr>
	void PutVarUint(T v) {
		grow(10);
		len_ += uint64_pack(v, buf_ + len_);
	}
	template <typename T, typename std::enable_if_t<sizeof(T) <= 4 && std::is_integral_v<T>> * = nullptr>
	void PutVarint(T v) {
		grow(10);
		len_ += sint32_pack(v, buf_ + len_);
	}
	template <typename T, typename std::enable_if_t<sizeof(T) <= 4 && std::is_integral_v<T>> * = nullptr>
	void PutVarUint(T v) {
		grow(10);
		len_ += uint32_pack(v, buf_ + len_);
	}
	template <typename T, typename std::enable_if_t<std::is_enum_v<T>> * = nullptr>
	void PutVarUint(T v) {
		assertrx(v >= 0 && v < 128);
		grow(1);
		buf_[len_++] = v;
	}
	void PutCTag(ctag tag) { PutVarUint(tag.asNumber()); }
	void PutBool(bool v) {
		grow(1);
		len_ += boolean_pack(v, buf_ + len_);
	}
	void PutVString(std::string_view str) {
		grow(str.size() + 10);
		len_ += string_pack(str.data(), str.size(), buf_ + len_);
	}
	void PutStrUuid(Uuid);
	void PutUuid(Uuid uuid) {
		PutUInt64(uuid[0]);
		PutUInt64(uuid[1]);
	}

	// Buffer manipulation functions
	void Write(std::string_view slice) {
		grow(slice.size());
		memcpy(&buf_[len_], slice.data(), slice.size());
		len_ += slice.size();
	}
	uint8_t *Buf() const noexcept { return buf_; }
	std::unique_ptr<uint8_t[]> DetachBuf() {
		std::unique_ptr<uint8_t[]> ret;

		if (!HasAllocatedBuffer()) {
			ret.reset(new uint8_t[len_]);
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
	std::unique_ptr<uint8_t[]> DetachLStr();
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
			cap_ = cap;
			uint8_t *b = new uint8_t[cap_];
			memcpy(b, buf_, len_);
			if (HasAllocatedBuffer()) delete[] buf_;  // NOLINT(*.NewDelete) False positive
			buf_ = b;
			hasExternalBuf_ = false;
		}
	}
	std::string_view Slice() const noexcept { return std::string_view(reinterpret_cast<const char *>(buf_), len_); }
	const char *c_str() noexcept {
		if (!len_ || buf_[len_] != 0) {
			grow(1);
			buf_[len_] = 0;
		}
		return reinterpret_cast<const char *>(buf_);
	}
	bool HasHeap() const noexcept { return buf_ != inBuf_ && cap_; }

protected:
	void grow(size_t sz) {
		if (len_ + sz > cap_) {
			constexpr size_t kPageMask = ~size_t(0xFFF);
			const auto newCap = ((cap_ * 2) + sz);
			const auto newCapAligned = newCap & kPageMask;
			Reserve((newCap == newCapAligned) ? newCap : (newCapAligned + 0x1000));
		}
	}
	uint8_t *buf_;
	size_t len_;
	size_t cap_;
	uint8_t inBuf_[0x100];
	bool hasExternalBuf_ = false;
};

inline int msgpack_wrserializer_write(void *data, const char *buf, size_t len) {
	reinterpret_cast<WrSerializer *>(data)->Write(std::string_view(buf, len));
	return 0;
}

}  // namespace reindexer
