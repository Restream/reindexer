#pragma once

#include "core/keyvalue/keyref.h"
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

using Arg = KeyRef;

class Args : public KeyRefs {
public:
	using KeyRefs::KeyRefs;
	void Unpack(Serializer &ser);
	void Pack(WrSerializer &ser) const;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
