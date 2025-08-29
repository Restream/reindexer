#pragma once

#include <memory>
#include "selectiteratorcontainer.h"

namespace reindexer {

class [[nodiscard]] QresExplainHolder {
public:
	enum class [[nodiscard]] ExplainEnabled : bool { Yes, No };

	QresExplainHolder(SelectIteratorContainer& current, ExplainEnabled explainEnabled) noexcept
		: current_(current), explainEnabled_(explainEnabled) {}

	void BackupContainer() {
		if (explainEnabled_ == ExplainEnabled::Yes) {
			if (data_) {
				throw Error(errLogic, "Attempt to create second backup of the query results. This should not happen");
			}
			data_ = std::make_unique<Data>(current_);
		}
	}
	SelectIteratorContainer& GetResultsRef() noexcept {
		if (!data_) {
			return current_;
		}
		if (data_->result.Empty()) {
			data_->result.OpenBracket(OpAnd);
			data_->result.Append(data_->backup.begin(), data_->backup.end());
			data_->result.CloseBracket();
			data_->result.Append(current_.begin(), current_.end());
		}
		return data_->result;
	}

private:
	SelectIteratorContainer& current_;
	class [[nodiscard]] Data {
	public:
		Data(const SelectIteratorContainer& c) : backup(c) {}

		SelectIteratorContainer backup;
		SelectIteratorContainer result;
	};
	std::unique_ptr<Data> data_;
	const ExplainEnabled explainEnabled_;
};

}  // namespace reindexer
