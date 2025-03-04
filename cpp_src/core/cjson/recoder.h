#pragma once

#include "core/payload/payloadiface.h"
#include "tagspath.h"

namespace reindexer {

class Serializer;
class WrSerializer;

class Recoder {
public:
	[[nodiscard]] virtual TagType Type(TagType oldTagType) = 0;
	virtual void Recode(Serializer&, WrSerializer&) const = 0;
	virtual void Recode(Serializer&, Payload&, int tagName, WrSerializer&) = 0;
	[[nodiscard]] virtual bool Match(int field) const noexcept = 0;
	[[nodiscard]] virtual bool Match(const TagsPath&) const = 0;
	virtual void Prepare(IdType rowId) noexcept = 0;
	virtual ~Recoder() = default;
};

}  // namespace reindexer
