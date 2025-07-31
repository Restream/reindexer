#include "distincthelpers.h"

namespace reindexer {
namespace DistinctHelpers {

[[nodiscard]] bool GetMultiFieldValue(const std::vector<DataType>& data, unsigned long dataIndex, unsigned int rowLen,
									  FieldsValue& values) {
	values.resize(0);
	values.reserve(rowLen);
	bool isNullValue = true;

	const auto process = [&](const auto& vv, reindexer::IsArray isArray) {
		if (isArray) {
			if (dataIndex < vv.size()) {
				values.emplace_back(vv[dataIndex]);
				isNullValue = false;
			} else {
				values.emplace_back();
			}
		} else {
			if (vv.size()) {
				values.emplace_back(vv[0]);
				isNullValue = false;
			} else {
				values.emplace_back();
			}
		}
	};
	for (unsigned int k = 0; k < rowLen; k++) {
		std::visit(overloaded{[&](const concepts::SpanFromOneOf<const bool, const int32_t, const int64_t, const double, const float,
																const Uuid, const p_string> auto& vv) { process(vv, data[k].isArray); },
							  [&](const VariantArray& vv) { process(vv, data[k].isArray); },
							  [&](const std::span<const std::string_view>& vv) {
								  if (dataIndex < vv.size()) {
									  values.emplace_back(p_string(&vv[dataIndex]));
									  isNullValue = false;
								  } else {
									  values.emplace_back();
								  }
							  }},
				   data[k].data);
	}
	return isNullValue;
}
}  // namespace DistinctHelpers
}  // namespace reindexer
