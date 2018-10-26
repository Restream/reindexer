#pragma once

#include "core/keyvalue/p_string.h"
#include "core/keyvalue/variant.h"
#include "tools/serializer.h"
namespace reindexer {
namespace net {
namespace cproto {

using Arg = Variant;

class Args : public VariantArray {
public:
	using VariantArray::VariantArray;
	void Unpack(Serializer &ser);
	void Pack(WrSerializer &ser) const;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
