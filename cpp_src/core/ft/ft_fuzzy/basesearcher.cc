#include "basesearcher.h"
#include "core/ft/ft_fuzzy/advacedpackedvec.h"
#include "core/ft/ftdsl.h"
#include "core/rdxcontext.h"
#include "tools/stringstools.h"

#ifdef FULL_LOG_FT
#include <iostream>
#endif

namespace search_engine {

using namespace reindexer;

void BaseSearcher::AddSearcher(ITokenFilter::Ptr&& searcher) { searchers_.push_back(std::move(searcher)); }

std::pair<bool, size_t> BaseSearcher::GetData(const BaseHolder::Ptr& holder, unsigned int i, wchar_t* buf, const wchar_t* src_data,
											  size_t data_size) {
	size_t counter = 0;
	size_t final_counter = 0;

	int offset = i - holder->cfg_.spaceSize;
	if (offset < 0) {
		counter = abs(offset);
		offset = 0;
	} else if (size_t(offset) >= data_size) {
		return std::make_pair(false, 0);
	}
	size_t data_counter = holder->cfg_.bufferSize - counter;

	if (data_counter > data_size - offset) {
		final_counter = holder->cfg_.bufferSize - (data_size - offset + counter);

		data_counter = (data_size - offset);
	}
	wmemset(buf, L'_', counter);
	wmemcpy(buf + counter, src_data + offset, data_counter);
	wmemset(buf + counter + data_counter, L'_', final_counter);
	bool cont = false;
	if (data_size < holder->cfg_.bufferSize) {
		cont = data_size * holder->cfg_.spaceSize > i + 1;
	} else {
		cont = offset + holder->cfg_.bufferSize < data_size + holder->cfg_.spaceSize;
	}
	return std::make_pair(cont, counter + final_counter);
}

size_t BaseSearcher::ParseData(const BaseHolder::Ptr& holder, const std::wstring& src_data, int& max_id, int& min_id,
							   std::vector<FirstResult>& results, const FtDslOpts& opts, double proc) {
	wchar_t res_buf[maxFuzzyFTBufferSize];
	size_t total_size = 0;
	size_t size = src_data.size();
	unsigned int i = 0;
	std::pair<bool, size_t> cont;
	do {
		cont = GetData(holder, i, res_buf, src_data.c_str(), size);
		total_size++;
		auto it = holder->GetData(res_buf);

		if (it != holder->end()) {
			if (it->second.max_id_ > max_id) {
				max_id = it->second.max_id_;
			}
			if (it->second.min_id_ < min_id) {
				min_id = it->second.min_id_;
			}
			double final_proc = double(holder->cfg_.bufferSize * holder->cfg_.startDecreeseBoost - cont.second) /
								double(holder->cfg_.bufferSize * holder->cfg_.startDecreeseBoost);
			results.push_back(FirstResult{&it->second, &opts, static_cast<int>(i), proc * final_proc});
		}
		i++;
	} while (cont.first);
	return total_size;
}

SearchResult BaseSearcher::Compare(const BaseHolder::Ptr& holder, const FtDSLQuery& dsl, bool inTransaction,
								   const reindexer::RdxContext& rdxCtx) {
	size_t data_size = 0;

	ITokenFilter::ResultsStorage data;
	std::pair<PosType, ProcType> pos;
	pos.first = 0;

	std::vector<FirstResult> results;
	int max_id = 0;
	int min_id = INT32_MAX;

	if (!inTransaction) {
		ThrowOnCancel(rdxCtx);
	}
	for (const auto& term : dsl) {
		data_size += ParseData(holder, term.pattern, max_id, min_id, results, term.opts, 1);
		fast_hash_map<std::wstring, size_t> patternsUsed;

		if (holder->cfg_.enableTranslit) {
			searchers_[0]->GetVariants(term.pattern, data, holder->cfg_.rankingConfig.translit, patternsUsed);

			ParseData(holder, data[0].pattern, max_id, min_id, results, term.opts, holder->cfg_.startDefaultDecreese);
		}
		if (holder->cfg_.enableKbLayout) {
			data.clear();
			searchers_[1]->GetVariants(term.pattern, data, holder->cfg_.rankingConfig.kblayout, patternsUsed);
			ParseData(holder, data[0].pattern, max_id, min_id, results, term.opts, holder->cfg_.startDefaultDecreese);
		}
	}

	BaseMerger mrg(max_id, min_id);

	MergeCtx ctx{&results, &holder->cfg_, data_size, &holder->words_};

	auto res = mrg.Merge(ctx, inTransaction, rdxCtx);
#ifdef FULL_LOG_FT
	for (size_t i = 0; i < res.data_->size(); ++i) {
		std::cout << res.data_->at(i).id_ << "   ";
		for (size_t j = 0; j < words.size(); ++j) {
			if (words[j].first == res.data_->at(i).id_) {
				std::cout << words[j].second << "   ";
			}
		}
		std::cout << res.data_->at(i).proc_ << "  ";

		std::cout << std::endl;
	}
#endif

	return res;
}

void BaseSearcher::AddIndex(BaseHolder::Ptr& holder, std::string_view src_data, const IdType id, unsigned field, unsigned arrayIdx,
							const reindexer::SplitOptions& splitOptions) {
#ifdef FULL_LOG_FT
	words.push_back(std::make_pair(id, *src_data));
#endif
	if (!src_data.length()) {
		return;
	}
	std::pair<PosType, ProcType> pos;
	pos.first = 0;
	std::wstring utf16str;
	std::vector<std::wstring> wrds;
	split(src_data, utf16str, wrds, splitOptions);
	wchar_t res_buf[maxFuzzyFTBufferSize];
	size_t total_size = 0;
	for (auto& term : wrds) {
		unsigned int i = 0;
		std::pair<bool, size_t> cont;
		do {
			cont = GetData(holder, i, res_buf, term.c_str(), term.size());
			holder->AddData(res_buf, id, i, field, arrayIdx);
			i++;
			total_size++;

		} while (cont.first);
	}
	holder->SetSize(total_size, id, field);
}

void BaseSearcher::Commit(BaseHolder::Ptr& holder) { holder->Commit(); }
}  // namespace search_engine
