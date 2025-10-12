#pragma once

#include "core/payload/payloadiface.h"
#include "tagspath.h"

namespace reindexer {

class Serializer;
class WrSerializer;

class [[nodiscard]] Recoder {
public:
	virtual TagType Type(TagType oldTagType) = 0;
	virtual void Recode(Serializer&, WrSerializer&) const = 0;
	virtual void Recode(Serializer&, Payload&, TagName, WrSerializer&) = 0;
	virtual bool Match(int field) const noexcept = 0;
	virtual bool Match(const TagsPath&) const = 0;
	virtual void Prepare(IdType rowId) noexcept = 0;
	virtual ~Recoder() = default;
};

}  // namespace reindexer
