#pragma once

#include "recoder.h"

namespace reindexer {

template <bool Array>
class [[nodiscard]] RecoderUuidToString final : public Recoder {
public:
	explicit RecoderUuidToString(TagsPath tp) noexcept : tagsPath_{std::move(tp)} {}
	TagType Type([[maybe_unused]] TagType oldTagType) noexcept override {
		if constexpr (Array) {
			assertrx(oldTagType == TAG_ARRAY);
			return TAG_ARRAY;
		} else {
			assertrx(oldTagType == TAG_UUID);
			return TAG_STRING;
		}
	}
	void Recode(Serializer&, WrSerializer&) const override;
	void Recode(Serializer&, Payload&, TagName, WrSerializer&) override { assertrx(false); }
	bool Match(int) const noexcept override { return false; }
	bool Match(const TagsPath& tp) const noexcept override { return tagsPath_ == tp; }
	void Prepare(IdType) noexcept override {}

private:
	void recodeNestedArray(Serializer&, WrSerializer&, uint32_t size) const;
	TagsPath tagsPath_;
};

template <>
inline void RecoderUuidToString<false>::Recode(Serializer& rdser, WrSerializer& wrser) const {
	wrser.PutStrUuid(rdser.GetUuid());
}

template <>
void RecoderUuidToString<true>::recodeNestedArray(Serializer&, WrSerializer&, uint32_t size) const;

template <>
inline void RecoderUuidToString<true>::Recode(Serializer& rdser, WrSerializer& wrser) const {
	const carraytag atag = rdser.GetCArrayTag();
	const auto count = atag.Count();
	const auto arrayType = atag.Type();
	if (arrayType == TAG_OBJECT) {
		recodeNestedArray(rdser, wrser, count);
	} else {
		assertrx(arrayType == TAG_UUID);
		wrser.PutCArrayTag(carraytag{count, TAG_STRING});
		for (size_t i = 0; i < count; ++i) {
			wrser.PutStrUuid(rdser.GetUuid());
		}
	}
}

template <>
inline void RecoderUuidToString<true>::recodeNestedArray(Serializer& rdser, WrSerializer& wrser, uint32_t size) const {
	wrser.PutCArrayTag(carraytag{size, TAG_OBJECT});
	for (size_t i = 0; i < size; ++i) {
		const ctag tag = rdser.GetCTag();
		assertrx_dbg(tag.Name() == TagName::Empty());
		wrser.PutCTag(tag);
		if (tag.Type() == TAG_ARRAY) {
			Recode(rdser, wrser);
		} else {
			assertrx_dbg(tag.Type() == TAG_UUID);
			wrser.PutStrUuid(rdser.GetUuid());
		}
	}
}

class [[nodiscard]] RecoderStringToUuidArray final : public Recoder {
public:
	explicit RecoderStringToUuidArray(int f) noexcept : field_{f} {}
	TagType Type(TagType oldTagType) override {
		fromNotArrayField_ = oldTagType != TAG_ARRAY;
		if (fromNotArrayField_ && oldTagType != TAG_STRING) {
			throw Error(errLogic, "Cannot convert not string field to UUID");
		}
		return TAG_ARRAY;
	}
	bool Match(int f) const noexcept override { return f == field_; }
	bool Match(const TagsPath&) const noexcept override { return false; }
	void Recode(Serializer&, WrSerializer&) const override { assertrx(false); }
	void Recode(Serializer& rdser, Payload& pl, TagName tagName, WrSerializer& wrser) override {
		if (fromNotArrayField_) {
			pl.Set(field_, Variant{rdser.GetStrUuid()}, Append_True);
			wrser.PutCTag(ctag{TAG_ARRAY, tagName, field_});
			wrser.PutVarUint(1);
		} else {
			recodeArray(rdser, pl, tagName, wrser);
		}
	}
	void Prepare(IdType) noexcept override {}

private:
	void recodeArray(Serializer& rdser, Payload& pl, TagName tagName, WrSerializer& wrser) {
		const carraytag atag = rdser.GetCArrayTag();
		const auto count = atag.Count();
		const auto arrayType = atag.Type();
		if (arrayType == TAG_OBJECT) {
			recodeNestedArray(rdser, pl, tagName, wrser, count);
		} else {
			if (count > 0 && atag.Type() != TAG_STRING) {
				throw Error(errLogic, "Cannot convert non-string field to UUID");
			}
			varBuf_.clear<false>();
			varBuf_.reserve(count);
			for (size_t i = 0; i < count; ++i) {
				varBuf_.emplace_back(rdser.GetStrUuid());
			}
			pl.Set(field_, varBuf_, Append_True);
			wrser.PutCTag(ctag{TAG_ARRAY, tagName, field_});
			wrser.PutVarUint(count);
		}
	}

	void recodeNestedArray(Serializer& rdser, Payload& pl, TagName tagName, WrSerializer& wrser, uint32_t size) {
		wrser.PutCTag(ctag{TAG_ARRAY, tagName});
		wrser.PutCArrayTag(carraytag{size, TAG_OBJECT});
		for (size_t i = 0; i < size; ++i) {
			const auto tag = rdser.GetCTag();
			const auto tagType = tag.Type();
			if (tagType == TAG_ARRAY) {
				recodeArray(rdser, pl, TagName::Empty(), wrser);
			} else {
				if (tagType != TAG_STRING) {
					throw Error(errLogic, "Cannot convert non-string field to UUID");
				}
				pl.Set(field_, Variant{rdser.GetStrUuid()}, Append_True);
				wrser.PutCTag(ctag{TAG_UUID, TagName::Empty(), field_});
			}
		}
	}

	const int field_{std::numeric_limits<int>::max()};
	VariantArray varBuf_;
	bool fromNotArrayField_{false};
};

class [[nodiscard]] RecoderStringToUuid final : public Recoder {
public:
	explicit RecoderStringToUuid(int f) noexcept : field_{f} {}
	TagType Type(TagType oldTagType) override {
		if (oldTagType == TAG_ARRAY) {
			throw Error(errLogic, "Cannot convert array field to not array UUID");
		} else if (oldTagType != TAG_STRING) {
			throw Error(errLogic, "Cannot convert not string field to UUID");
		}
		return TAG_UUID;
	}
	bool Match(int f) const noexcept override { return f == field_; }
	bool Match(const TagsPath&) const noexcept override { return false; }
	void Recode(Serializer&, WrSerializer&) const override { assertrx(false); }
	void Recode(Serializer& rdser, Payload& pl, TagName tagName, WrSerializer& wrser) override {
		pl.Set(field_, Variant{rdser.GetStrUuid()}, Append_True);
		wrser.PutCTag(ctag{TAG_UUID, tagName, field_});
	}
	void Prepare(IdType) noexcept override {}

private:
	const int field_{std::numeric_limits<int>::max()};
};

}  // namespace reindexer
