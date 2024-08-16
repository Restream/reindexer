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
	void Unpack(Serializer& ser) {
		resize(0);
		unsigned count = ser.GetVarUint();

		while (count--) {
			push_back(ser.GetVariant());
		}
	}
	void Pack(WrSerializer& ser) const {
		ser.PutVarUint(size());
		for (auto& arg : *this) {
			ser.PutVariant(arg);
		}
	}
	void Dump(WrSerializer& wrser) const;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
