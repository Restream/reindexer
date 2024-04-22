#pragma once

#include "core/keyvalue/variant.h"
#include "tools/serializer.h"
namespace reindexer {
namespace net {
namespace cproto {

using Arg = Variant;

class Args : public h_vector<Variant, 9> {
public:
	using h_vector<Variant, 9>::h_vector;
	void Unpack(Serializer &ser);
	void Pack(WrSerializer &ser) const;
	void Dump(WrSerializer &wrser) const;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
