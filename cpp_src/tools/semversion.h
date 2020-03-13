#pragma once

#include "atoi/atoi.h"
#include "estl/h_vector.h"
// #include "estl/string_view.h"
#include "stringstools.h"

namespace reindexer {

constexpr size_t kVersionDigitsCount = 3;
class SemVersion {
public:
	SemVersion() = default;
	SemVersion(string_view version) { parse(version); }

	bool operator<(const SemVersion &rVersion) const {
		return std::lexicographical_compare(versionDigits_.begin(), versionDigits_.end(), rVersion.versionDigits_.begin(),
											rVersion.versionDigits_.end());
	}
	bool operator==(const SemVersion &rVersion) const { return (versionDigits_ == rVersion.versionDigits_); }
	bool operator!=(const SemVersion &rVersion) const { return (versionDigits_ != rVersion.versionDigits_); }

	const std::string &StrippedString() const { return versionStr_; }

private:
	void parse(string_view input) {
		h_vector<string_view, kVersionDigitsCount> splitted;
		h_vector<int16_t, kVersionDigitsCount> result;
		versionDigits_ = {0, 0, 0};
		versionStr_.assign("0.0.0");
		string_view version = input;
		if (input.size() && input.data()[0] == 'v') {
			version = input.substr(1);
		}
		split(version, "-", false, splitted);
		if (!splitted.size()) {
			return;
		}
		version = splitted[0];
		split(version, ".", false, splitted);
		for (auto &it : splitted) {
			bool valid = true;
			int res = jsteemann::atoi<int>(it.begin(), it.end(), valid);
			if (!valid) {
				return;
			}
			result.push_back(res);
		}
		versionDigits_ = std::move(result);
		versionStr_ = string(version);
	}

	h_vector<int16_t, kVersionDigitsCount> versionDigits_;
	std::string versionStr_;
};

}  // namespace reindexer
