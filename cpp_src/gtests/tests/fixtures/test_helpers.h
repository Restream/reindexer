#pragma once

#include <gtest/gtest.h>
#include "core/query/impl.h"
#include "core/query/query_impl.h"
#include "gtests/tests/gtest_cout.h"

namespace reindexer_tests {

class [[nodiscard]] QueryWatcher {
public:
	QueryWatcher(reindexer::impl::Query& q) noexcept : q_{q} {}
	QueryWatcher(reindexer::Query& q) : q_{*reindexer::impl::Impl{q}} {}

	~QueryWatcher() {
		if (::testing::Test::HasFailure()) {
			reindexer::WrSerializer ser;
			q_.GetSQL(ser);
			TEST_COUT << "Failed query dest: " << ser.Slice() << std::endl;
		}
	}

private:
	const reindexer::impl::Query& q_;
};

template <typename ItemType>
std::string PrintItem(const ItemType& item) {
	std::stringstream out;
	for (auto idx = 1; idx < item.NumFields(); idx++) {
		out << item[idx].Name() << '=';
		const auto values = item[idx].operator reindexer::VariantArray();
		if (values.size() == 1) {
			out << values[0].template As<std::string>() << ' ';
		} else {
			out << '[';
			for (size_t i = 0, s = values.size(); i < s; ++i) {
				if (i != 0) {
					out << ", ";
				}
				out << values[i].template As<std::string>();
			}
			out << "] ";
		}
	}
	return out.str();
}

}  // namespace reindexer_tests
