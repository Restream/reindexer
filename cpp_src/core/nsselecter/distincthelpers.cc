#include "distincthelpers.h"

namespace reindexer {
namespace DistinctHelpers {

[[nodiscard]] bool GetMultiFieldValue(const std::vector<DataType>& data, unsigned long dataIndex, unsigned int rowLen,
									  FieldsValue& values) {
	values.resize(0);
	values.reserve(rowLen);
	bool isNullValue = true;

	const auto process = [&](const auto& vv) {
		if (dataIndex < vv.size()) {
			values.push_back(Variant(vv[dataIndex]));
			isNullValue = false;
		} else {
			values.push_back(Variant{});
		}
	};
	for (unsigned int k = 0; k < rowLen; k++) {
		std::visit(
			overloaded{[&](const std::span<const bool>& vv) { process(vv); }, [&](const std::span<const int64_t>& vv) { process(vv); },
					   [&](const std::span<const double>& vv) { process(vv); }, [&](const std::span<const float>& vv) { process(vv); },
					   [&](const std::span<const int32_t>& vv) { process(vv); }, [&](const std::span<const Uuid>& vv) { process(vv); },
					   [&](const std::span<const p_string>& vv) { process(vv); }, [&](const VariantArray& vv) { process(vv); },
					   [&](const std::span<const std::string_view>& vv) {
						   if (dataIndex < vv.size()) {
							   values.push_back(Variant(p_string(&vv[dataIndex])));
							   isNullValue = false;
						   } else {
							   values.push_back(Variant{});
						   }
					   }},
			data[k]);
	}
	return isNullValue;
}
}  // namespace DistinctHelpers
}  // namespace reindexer
