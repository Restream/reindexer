#pragma once

#include "recoder.h"

namespace reindexer {

class [[nodiscard]] RecoderUuidToString final : public Recoder {
public:
	explicit RecoderUuidToString(std::vector<TagsPath>&& tps) noexcept : tagsPaths_{std::move(tps)} {}
	TagType Type(TagType fromType) noexcept override {
		fromType_ = fromType;
		if (fromType_ == TAG_ARRAY) {
			return TAG_ARRAY;
		}
		if (fromType_ == TAG_NULL) {
			return TAG_NULL;
		}
		assertrx(fromType_ == TAG_UUID);
		return TAG_STRING;
	}
	void Recode(Serializer&, WrSerializer&) const override;
	void Recode(Serializer&, Payload&, TagName, WrSerializer&) override { assertrx(false); }
	bool Match(int) const noexcept override { return false; }
	bool Match(const TagsPath& tp) const noexcept override {
		return std::ranges::any_of(tagsPaths_, [&](const auto& ltp) { return ltp == tp; });
	}
	void Prepare(IdType) noexcept override {}

private:
	void recodeNestedArray(Serializer&, WrSerializer&, uint32_t size) const;
	std::vector<TagsPath> tagsPaths_;
	TagType fromType_ = TAG_UUID;
};

void RecoderUuidToString::Recode(Serializer& rdser, WrSerializer& wrser) const {
	if (fromType_ == TAG_UUID) {
		wrser.PutStrUuid(rdser.GetUuid());
	} else if (fromType_ == TAG_ARRAY) {
		const carraytag atag = rdser.GetCArrayTag();
		const auto count = atag.Count();
		const auto arrayType = atag.Type();
		switch (arrayType) {
			case TAG_OBJECT:
				recodeNestedArray(rdser, wrser, count);
				break;
			case TAG_UUID:
				wrser.PutCArrayTag(carraytag{count, TAG_STRING});
				for (size_t i = 0; i < count; ++i) {
					wrser.PutStrUuid(rdser.GetUuid());
				}
				break;
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_STRING:
			case TAG_BOOL:
			case TAG_NULL:
			case TAG_ARRAY:
			case TAG_END:
			case TAG_FLOAT:
			default:
				assertrx(false);
		}
	} else {
		assertrx_dbg(fromType_ == TAG_NULL);
	}
}

void RecoderUuidToString::recodeNestedArray(Serializer& rdser, WrSerializer& wrser, uint32_t size) const {
	wrser.PutCArrayTag(carraytag{size, TAG_OBJECT});
	for (size_t i = 0; i < size; ++i) {
		const ctag tag = rdser.GetCTag();
		assertrx_dbg(tag.Name() == TagName::Empty());
		const auto tagType = tag.Type();
		if (tagType == TAG_ARRAY) {
			wrser.PutCTag(tag);
			Recode(rdser, wrser);
		} else {
			assertrx_dbg(tag.Type() == TAG_UUID);
			wrser.PutCTag(ctag{TAG_STRING, tag.Name(), tag.Field()});
			wrser.PutStrUuid(rdser.GetUuid());
		}
	}
}

class [[nodiscard]] RecoderStringToUuidArray final : public Recoder {
public:
	explicit RecoderStringToUuidArray(int f) noexcept : field_{f} {}
	TagType Type(TagType oldTagType) override {
		if (oldTagType == TAG_NULL) {
			return TAG_NULL;
		}
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
