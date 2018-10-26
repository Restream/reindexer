
#include "args.h"

namespace reindexer {
namespace net {
namespace cproto {

void Args::Unpack(Serializer &ser) {
	resize(0);
	unsigned count = ser.GetVarUint();

	while (count--) {
		push_back(ser.GetVariant());
	}
}

void Args::Pack(WrSerializer &ser) const {
	ser.PutVarUint(size());
	for (auto &arg : *this) {
		ser.PutVariant(arg);
	}
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
