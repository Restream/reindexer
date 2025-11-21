#include "args.h"
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

void Args::Dump(WrSerializer& wrser, const h_vector<MaskingFunc, 2>& maskArgsFuncs) const {
	wrser << '{';

	for (size_t i = 0; i < size(); ++i) {
		const auto& arg = (*this)[i];
		if (&arg != &at(0)) {
			wrser << ", ";
		}
		arg.Type().EvaluateOneOf(
			[&](KeyValueType::String) {
				std::string_view str(arg);
				if (isPrintable(str)) {
					wrser << '\'' << (i < maskArgsFuncs.size() && maskArgsFuncs.at(i) ? maskArgsFuncs.at(i)(str).c_str() : str) << '\'';
				} else {
					wrser << "slice{len:" << str.length() << '}';
				}
			},
			[&](KeyValueType::Int) { wrser << int(arg); }, [&](KeyValueType::Bool) { wrser << bool(arg); },
			[&](KeyValueType::Int64) { wrser << int64_t(arg); }, [&](KeyValueType::Uuid) { wrser << Uuid{arg}; },
			[&](KeyValueType::FloatVector) { wrser << "[??]"; },
			[&](concepts::OneOf<KeyValueType::Double, KeyValueType::Float, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
								KeyValueType::Undefined> auto) { wrser << "??"; });
	}
	wrser << '}';
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
