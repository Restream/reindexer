
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

	for (const auto &arg : *this) {
		if (&arg != &at(0)) {
			wrser << ", ";
		}
		arg.Type().EvaluateOneOf(
			[&](KeyValueType::String) {
				std::string_view str(arg);
				if (isPrintable(str)) {
					wrser << '\'' << str << '\'';
				} else {
					wrser << "slice{len:" << str.length() << '}';
				}
			},
			[&](KeyValueType::Int) { wrser << int(arg); }, [&](KeyValueType::Bool) { wrser << bool(arg); },
			[&](KeyValueType::Int64) { wrser << int64_t(arg); },
			[&](OneOf<KeyValueType::Double, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined>) {
				wrser << "??";
			});
	}
	wrser << '}';
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
