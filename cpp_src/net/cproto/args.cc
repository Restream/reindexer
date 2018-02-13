
#include "args.h"

namespace reindexer {
namespace net {
namespace cproto {

void Args::Unpack(Serializer &ser) {
	resize(0);
	unsigned count = ser.GetVarUint();

	while (count--) {
		KeyValueType t = KeyValueType(ser.GetVarUint());
		switch (t) {
			case KeyValueInt:
				push_back(KeyRef(int(ser.GetVarint())));
				break;
			case KeyValueInt64:
				push_back(KeyRef(int64_t(ser.GetVarint())));
				break;
			case KeyValueDouble:
				push_back(KeyRef(double(ser.GetDouble())));
				break;
			case KeyValueString:
				push_back(KeyRef(ser.GetPVString()));
				break;
			default:
				throw Error(errParams, "Unexpected type %d", t);
		}
	}
}

void Args::Pack(WrSerializer &ser) const {
	ser.PutVarUint(size());
	for (auto &arg : *this) {
		KeyValueType t = arg.Type();
		ser.PutVarUint(t);
		switch (t) {
			case KeyValueInt:
				ser.PutVarint(int(arg));
				break;
			case KeyValueInt64:
				ser.PutVarint(int64_t(arg));
				break;
			case KeyValueDouble:
				ser.PutDouble(double(arg));
				break;
			case KeyValueString:
				ser.PutVString(p_string(arg));
				break;
			default:
				throw Error(errParams, "Unexpected type %d", t);
		}
	}
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
