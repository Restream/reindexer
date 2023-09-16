#pragma once

#include "core/payload/payloadiface.h"

namespace reindexer {

class TagsMatcher;
class Serializer;
class WrSerializer;

class Recoder {
public:
	[[nodiscard]] virtual TagType Type(TagType oldTagType) = 0;
	virtual void Recode(Serializer &, WrSerializer &) const = 0;
	virtual void Recode(Serializer &, Payload &, int tagName, WrSerializer &) = 0;
	[[nodiscard]] virtual bool Match(int field) const noexcept = 0;
	[[nodiscard]] virtual bool Match(const TagsPath &) const noexcept = 0;
	virtual ~Recoder() = default;
};

class CJsonDecoder {
public:
	CJsonDecoder(TagsMatcher &tagsMatcher) noexcept : tagsMatcher_(tagsMatcher), filter_(nullptr) {}
	CJsonDecoder(TagsMatcher &tagsMatcher, const FieldsSet *filter, Recoder *recoder) noexcept
		: tagsMatcher_(tagsMatcher), filter_(filter), recoder_(recoder) {}

	void Decode(Payload &pl, Serializer &rdSer, WrSerializer &wrSer) {
		objectScalarIndexes_.reset();
		decodeCJson(pl, rdSer, wrSer, true);
	}

private:
	bool decodeCJson(Payload &pl, Serializer &rdser, WrSerializer &wrser, bool match);
	bool isInArray() const noexcept { return arrayLevel_ > 0; }

	TagsMatcher &tagsMatcher_;
	const FieldsSet *filter_;
	TagsPath tagsPath_;
	Recoder *recoder_{nullptr};
	int32_t arrayLevel_ = 0;
	ScalarIndexesSetT objectScalarIndexes_;
};

}  // namespace reindexer
