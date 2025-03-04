#pragma once

#include "recoder.h"

namespace reindexer {

template <bool Array>
class RecoderUuidToString final : public Recoder {
public:
	explicit RecoderUuidToString(TagsPath tp) noexcept : tagsPath_{std::move(tp)} {}
	[[nodiscard]] TagType Type([[maybe_unused]] TagType oldTagType) noexcept override {
		if constexpr (Array) {
			assertrx(oldTagType == TAG_ARRAY);
			return TAG_ARRAY;
		} else {
			assertrx(oldTagType == TAG_UUID);
			return TAG_STRING;
		}
	}
	void Recode(Serializer&, WrSerializer&) const override;
	void Recode(Serializer&, Payload&, int, WrSerializer&) override { assertrx(false); }
	[[nodiscard]] bool Match(int) const noexcept override { return false; }
	[[nodiscard]] bool Match(const TagsPath& tp) const noexcept override { return tagsPath_ == tp; }
	void Prepare(IdType) noexcept override {}

private:
	TagsPath tagsPath_;
};

template <>
inline void RecoderUuidToString<false>::Recode(Serializer& rdser, WrSerializer& wrser) const {
	wrser.PutStrUuid(rdser.GetUuid());
}

template <>
inline void RecoderUuidToString<true>::Recode(Serializer& rdser, WrSerializer& wrser) const {
	const carraytag atag = rdser.GetCArrayTag();
	const auto count = atag.Count();
	assertrx(atag.Type() == TAG_UUID);
	wrser.PutCArrayTag(carraytag{count, TAG_STRING});
	for (size_t i = 0; i < count; ++i) {
		wrser.PutStrUuid(rdser.GetUuid());
	}
}

class RecoderStringToUuidArray final : public Recoder {
public:
	explicit RecoderStringToUuidArray(int f) noexcept : field_{f} {}
	[[nodiscard]] TagType Type(TagType oldTagType) override {
		fromNotArrayField_ = oldTagType != TAG_ARRAY;
		if (fromNotArrayField_ && oldTagType != TAG_STRING) {
			throw Error(errLogic, "Cannot convert not string field to UUID");
		}
		return TAG_ARRAY;
	}
	[[nodiscard]] bool Match(int f) const noexcept override { return f == field_; }
	[[nodiscard]] bool Match(const TagsPath&) const noexcept override { return false; }
	void Recode(Serializer&, WrSerializer&) const override { assertrx(false); }
	void Recode(Serializer& rdser, Payload& pl, int tagName, WrSerializer& wrser) override {
		if (fromNotArrayField_) {
			pl.Set(field_, Variant{rdser.GetStrUuid()}, true);
			wrser.PutCTag(ctag{TAG_ARRAY, tagName, field_});
			wrser.PutVarUint(1);
		} else {
			const carraytag atag = rdser.GetCArrayTag();
			const auto count = atag.Count();
			if (count > 0 && atag.Type() != TAG_STRING) {
				throw Error(errLogic, "Cannot convert not string field to UUID");
			}
			varBuf_.clear<false>();
			varBuf_.reserve(count);
			for (size_t i = 0; i < count; ++i) {
				varBuf_.emplace_back(rdser.GetStrUuid());
			}
			pl.Set(field_, varBuf_, true);
			wrser.PutCTag(ctag{TAG_ARRAY, tagName, field_});
			wrser.PutVarUint(count);
		}
	}
	void Prepare(IdType) noexcept override {}

private:
	const int field_{std::numeric_limits<int>::max()};
	VariantArray varBuf_;
	bool fromNotArrayField_{false};
};

class RecoderStringToUuid final : public Recoder {
public:
	explicit RecoderStringToUuid(int f) noexcept : field_{f} {}
	[[nodiscard]] TagType Type(TagType oldTagType) override {
		if (oldTagType == TAG_ARRAY) {
			throw Error(errLogic, "Cannot convert array field to not array UUID");
		} else if (oldTagType != TAG_STRING) {
			throw Error(errLogic, "Cannot convert not string field to UUID");
		}
		return TAG_UUID;
	}
	[[nodiscard]] bool Match(int f) const noexcept override { return f == field_; }
	[[nodiscard]] bool Match(const TagsPath&) const noexcept override { return false; }
	void Recode(Serializer&, WrSerializer&) const override { assertrx(false); }
	void Recode(Serializer& rdser, Payload& pl, int tagName, WrSerializer& wrser) override {
		pl.Set(field_, Variant{rdser.GetStrUuid()}, true);
		wrser.PutCTag(ctag{TAG_UUID, tagName, field_});
	}
	void Prepare(IdType) noexcept override {}

private:
	const int field_{std::numeric_limits<int>::max()};
};

}  // namespace reindexer
