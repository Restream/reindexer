#pragma once

#include <string>
#include <string_view>
#include <vector>
#include "tools/stringstools.h"

namespace reindexer {

class [[nodiscard]] FieldsNamesFilter {
public:
	static constexpr std::string_view kAllVectorFieldsName = "vectors()";
	static constexpr std::string_view kRankFieldName = "rank()";
	static constexpr std::string_view kAllRegularFieldsName = "*";

	FieldsNamesFilter() noexcept = default;

	bool operator==(const FieldsNamesFilter& other) const noexcept {
		return (!allVectorFields_ && !other.allVectorFields_ && fields_.empty() && other.fields_.empty()) ||
			   (allRegularFields_ == other.allRegularFields_ && allVectorFields_ == other.allVectorFields_ && fields_ == other.fields_);
	}
	bool operator!=(const FieldsNamesFilter& other) const noexcept { return !operator==(other); }

	template <typename It, typename QueryType>
	void Add(It begin, It end, QueryType& q) {
		fields_.reserve(fields_.size() + std::distance(begin, end));
		for (; begin != end; ++begin) {
			Add(*begin, q);
		}
	}

	template <typename Str, typename QueryType, std::enable_if_t<std::is_constructible_v<std::string, Str>>* = nullptr>
	void Add(Str&& field, QueryType& q) {
		using namespace std::string_view_literals;
		if (field == kAllRegularFieldsName) {
			allRegularFields_ = true;
			if (allVectorFields_) {
				fields_.clear();
			}
		} else if (iequals(field, kAllVectorFieldsName)) {
			allVectorFields_ = true;
			if (allRegularFields_) {
				fields_.clear();
			}
		} else if (iequals(field, kRankFieldName)) {
			q.WithRank();
		} else if (field != ""sv && !(allRegularFields_ && allVectorFields_)) {
			fields_.emplace_back(std::forward<Str>(field));
		}
	}

	void SetAllRegularFields() noexcept { allRegularFields_ = true; }
	void SetAllVectorFields() noexcept { allVectorFields_ = true; }

	bool Empty() const noexcept { return fields_.empty() && !allRegularFields_ && !allVectorFields_; }

	bool AllRegularFields() const noexcept { return allRegularFields_ || Empty(); }
	bool AllVectorFields() const noexcept { return allVectorFields_; }
	bool ExplicitAllRegularFields() const noexcept { return allRegularFields_; }
	bool OnlyAllRegularFields() const noexcept { return fields_.empty() && !allVectorFields_; }

	const std::vector<std::string>& Fields() const& noexcept { return fields_; }
	std::vector<std::string>& Fields() & noexcept { return fields_; }
	std::vector<std::string>&& Fields() && noexcept { return std::move(fields_); }

	void Clear() {
		fields_.clear();
		allRegularFields_ = allVectorFields_ = false;
	}

private:
	std::vector<std::string> fields_;
	bool allRegularFields_{false};
	bool allVectorFields_{false};
};

}  // namespace reindexer
