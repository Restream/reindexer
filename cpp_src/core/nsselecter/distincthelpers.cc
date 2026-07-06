#include "distincthelpers.h"

namespace reindexer {
namespace DistinctHelpers {

bool GetMultiFieldValue(const std::vector<DataType>& data, unsigned long dataIndex, unsigned int rowLen, FieldsValue& values) {
	values.resize(0);
	values.reserve(rowLen);
	bool isNullValue = true;

	for (unsigned int k = 0; k < rowLen; k++) {
		std::visit(
			[&]<typename T>(const T& vv) {
				const auto realDataIndex = data[k].isArray ? dataIndex : 0;
				if (realDataIndex < vv.size()) {
					if constexpr (std::is_same_v<typename T::value_type, std::string_view>) {
						assertrx_dbg(vv.is_aligned());
						values.emplace_back(p_string(static_cast<const std::string_view*>(vv.bytes()) + realDataIndex), Variant::noHold);
					} else {
						values.emplace_back(vv[realDataIndex]);
					}
					isNullValue = false;
				} else {
					values.emplace_back();
				}
			},
			data[k].data);
	}
	return isNullValue;
}
}  // namespace DistinctHelpers
}  // namespace reindexer
