#pragma once

#include "cjsondecoder.h"

namespace reindexer {

template <bool Array>
class RecoderUuidToString : public Recoder {
public:
	RecoderUuidToString(TagsPath tp) noexcept : tagsPath_{std::move(tp)} {}
	[[nodiscard]] TagType Type([[maybe_unused]] TagType oldTagType) noexcept override final {
		if constexpr (Array) {
			assertrx(oldTagType == TAG_ARRAY);
			return TAG_ARRAY;
		} else {
			assertrx(oldTagType == TAG_UUID);
			return TAG_STRING;
		}
	}
	void Recode(Serializer &, WrSerializer &) const override final;
	void Recode(Serializer &, Payload &, int /*tagName*/, WrSerializer &) override final { assertrx(0); }
	[[nodiscard]] bool Match(int) const noexcept override final { return false; }
	[[nodiscard]] bool Match(const TagsPath &tp) const noexcept override final { return tagsPath_ == tp; }

private:
	TagsPath tagsPath_;
};

template <>
inline void RecoderUuidToString<false>::Recode(Serializer &rdser, WrSerializer &wrser) const {
	wrser.PutStrUuid(rdser.GetUuid());
}

template <>
inline void RecoderUuidToString<true>::Recode(Serializer &rdser, WrSerializer &wrser) const {
	const carraytag atag = rdser.GetCArrayTag();
	const auto count = atag.Count();
	assertrx(atag.Type() == TAG_UUID);
	wrser.PutCArrayTag(carraytag{count, TAG_STRING});
	for (size_t i = 0; i < count; ++i) {
		wrser.PutStrUuid(rdser.GetUuid());
	}
}

class RecoderStringToUuidArray : public Recoder {
public:
	RecoderStringToUuidArray(int f) noexcept : field_{f} {}
	[[nodiscard]] TagType Type(TagType oldTagType) override final {
		fromNotArrayField_ = oldTagType != TAG_ARRAY;
		if (fromNotArrayField_ && oldTagType != TAG_STRING) {
			throw Error(errLogic, "Cannot convert not string field to UUID");
		}
		return TAG_ARRAY;
	}
	[[nodiscard]] bool Match(int f) const noexcept override final { return f == field_; }
	[[nodiscard]] bool Match(const TagsPath &) const noexcept override final { return false; }
	void Recode(Serializer &, WrSerializer &) const override final { assertrx(0); }
	void Recode(Serializer &rdser, Payload &pl, int tagName, WrSerializer &wrser) override final {
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

private:
	VariantArray varBuf_;
	int field_;
	bool fromNotArrayField_{false};
};

class RecoderStringToUuid : public Recoder {
public:
	RecoderStringToUuid(int f) noexcept : field_{f} {}
	[[nodiscard]] TagType Type(TagType oldTagType) override final {
		if (oldTagType == TAG_ARRAY) {
			throw Error(errLogic, "Cannot convert array field to not array UUID");
		} else if (oldTagType != TAG_STRING) {
			throw Error(errLogic, "Cannot convert not string field to UUID");
		}
		return TAG_UUID;
	}
	[[nodiscard]] bool Match(int f) const noexcept override final { return f == field_; }
	[[nodiscard]] bool Match(const TagsPath &) const noexcept override final { return false; }
	void Recode(Serializer &, WrSerializer &) const override final { assertrx(0); }
	void Recode(Serializer &rdser, Payload &pl, int tagName, WrSerializer &wrser) override final {
		pl.Set(field_, Variant{rdser.GetStrUuid()}, true);
		wrser.PutCTag(ctag{TAG_UUID, tagName, field_});
	}

private:
	int field_;
};

}  // namespace reindexer
