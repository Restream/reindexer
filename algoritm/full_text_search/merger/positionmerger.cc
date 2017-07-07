#include "positionmerger.h"
#include "tools/strings.h"
namespace search_engine {
using std::make_shared;
using std::make_pair;

void PositionMerger::AddPosition(Info* info, uint16_t curent_pos) {
	for (auto& id : info->ids_) {
		auto it = context_.find(id.first);
		if (it == context_.end()) {
			PositionHolder hld(find_word_size_);
			hld.AddPosition(info, id.second, curent_pos);
			context_.insert(make_pair(id.first, hld));
		} else {
			it->second.AddPosition(info, id.second, curent_pos);
		}
	}
}

BaseHolder::SearchTypePtr PositionMerger::CalcResult() {
	auto result = make_shared<BaseHolder::SearchType>();
	for (auto& context : context_) {
		ProcType procent = context.second.CalcResult();
		if ((procent >= (word_size / static_cast<float>(context.second.total_size_)) * context.second.CMainProc) ||
			(context.second.total_size_ < 5 && procent > 20)) {
			result->insert(std::make_pair(context.first, procent));
		}
	}
	return result;
}
}  // namespace search_engine
