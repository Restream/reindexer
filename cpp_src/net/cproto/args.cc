
#include "args.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace net {
namespace cproto {

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
			[&](KeyValueType::Int64) { wrser << int64_t(arg); }, [&](KeyValueType::Uuid) { wrser << Uuid{arg}; },
			[&](OneOf<KeyValueType::Double, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined>) {
				wrser << "??";
			});
	}
	wrser << '}';
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
