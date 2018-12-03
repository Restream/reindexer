
#include "args.h"
#include "tools/stringstools.h"

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
void Args::Dump(WrSerializer &wrser) const {
	wrser << '{';

	for (auto &arg : *this) {
		if (&arg != &at(0)) {
			wrser << ", ";
		}
		switch (arg.Type()) {
			case KeyValueString: {
				p_string str(arg);
				if (isPrintable(str)) {
					wrser << '\'' << string_view(str) << '\'';
				} else {
					wrser << "slice{len:" << str.length() << "}";
				}
				break;
			}
			case KeyValueInt:
				wrser << int(arg);
				break;
			case KeyValueBool:
				wrser << bool(arg);
				break;
			case KeyValueInt64:
				wrser << int64_t(arg);
				break;
			default:
				wrser << "??";
				break;
		}
	}
	wrser << '}';
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
