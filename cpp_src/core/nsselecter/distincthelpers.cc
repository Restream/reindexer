#include "distincthelpers.h"

namespace reindexer {
namespace DistinctHelpers {

bool GetMultiFieldValue(const std::vector<DataType>& data, unsigned long dataIndex, unsigned int rowLen, FieldsValue& values) {
	values.resize(0);
	values.reserve(rowLen);
	bool isNullValue = true;

	const auto process = [&]<typename T>(const T& vv, reindexer::IsArray isArray) {
		const auto realDataIndex = isArray ? dataIndex : 0;
		if (realDataIndex < vv.size()) {
			const auto& v = vv[realDataIndex];
			if constexpr (std::is_same_v<T, std::span<const std::string_view>>) {
				values.emplace_back(p_string(&v));
			} else {
				values.emplace_back(v);
			}
			isNullValue = false;
		} else {
			values.emplace_back();
		}
	};
	for (unsigned int k = 0; k < rowLen; k++) {
		std::visit(overloaded{[&](const auto& vv) { process(vv, data[k].isArray); }}, data[k].data);
	}
	return isNullValue;
}
}  // namespace DistinctHelpers
}  // namespace reindexer
