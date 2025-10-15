#include "ft_function.h"
#include <algorithm>
#include <cctype>
#include "core/nsselecter/ranks_holder.h"
#include "core/payload/fieldsset.h"
#include "core/query/expression/function_parser.h"
#include "core/queryresults/localqueryresults.h"
#include "core/type_consts_helpers.h"
#include "highlight.h"
#include "snippet.h"

namespace reindexer {

FtFuncStruct::FtFuncStruct(ParsedQueryFunction&& parsed) : ParsedQueryFunction{std::move(parsed)} {
	using namespace std::string_view_literals;
	if (iequals(funcName, "snippet"sv)) {
		func = Snippet();
	} else if (iequals(funcName, "highlight"sv)) {
		func = Highlight();
	} else if (iequals(funcName, "snippet_n"sv)) {
		func = SnippetN();
	} else if (iequals(funcName, "debug_rank"sv)) {
		func = DebugRank();
	}
}

// trim from both ends (in place)
inline void trim(std::string& s) {
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
	s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

bool FtFunction::Empty() const noexcept {
	return std::all_of(functions_.begin(), functions_.end(),
					   [](const auto& fn) noexcept { return std::holds_alternative<FuncNone>(fn.second.func); });
}

FtFunction::Ptr FtFunctionsHolder::AddNamespace(const Query& q, const NamespaceImpl& nm, uint32_t nsid, bool force) {
	if (q.selectFunctions_.empty() && !force) {
		return nullptr;
	} else if (!q.selectFunctions_.empty()) {
		force_only_ = false;
	}

	if (queries_.size() <= nsid) {
		queries_.resize(nsid + 1);
	}
	queries_[nsid] = make_intrusive<FtFunction>(q, NsFtFuncInterface(nm));
	return queries_[nsid];
}

FtFunction::FtFunction(const Query& q, NsFtFuncInterface&& nm) : nm_(std::move(nm)), currCjsonFieldIdx_(nm_.getIndexesCount()) {
	for (auto& func : q.selectFunctions_) {
		auto result = QueryFunctionParser::Parse(func);
		if (!result.isFunction) {
			continue;
		}
		createFunc(FtFuncStruct{std::move(result)});
	}
}

void FtFunction::createFunc(FtFuncStruct&& data) {
	int indexNo = IndexValueType::NotSet;
	if (data.indexNo == IndexValueType::NotSet) {
		if (!nm_.getIndexByName(data.field, indexNo)) {
			trim(data.field);
			if (!nm_.getIndexByName(data.field, indexNo)) {
				return;
			}
		}
	} else {
		indexNo = data.indexNo;
	}

	// if index is composite then create function for inner use only
	if (IsComposite(nm_.getIndexType(indexNo))) {
		int fieldNo = 0;
		const FieldsSet& fields = nm_.getIndexFields(indexNo);

		int jsPathIdx = 0;
		for (auto field : fields) {
			data.fieldNo = fieldNo;
			if (field == IndexValueType::SetByJsonPath) {
				data.field = fields.getJsonPath(jsPathIdx);
				data.tagsPath = nm_.getTagsPathForField(data.field);
				data.indexNo = currCjsonFieldIdx_++;
				functions_.emplace(data.indexNo, data);
			} else {
				data.field = nm_.getIndexName(field);
				data.indexNo = field;
				functions_.emplace(field, data);
			}
		}
	} else {
		data.indexNo = indexNo;
		data.field = nm_.getIndexName(indexNo);
		data.fieldNo = 0;
		functions_.emplace(std::make_pair(indexNo, data));
	}
}

FtCtx::Ptr FtFunction::createFuncForRank(int indexNo, const RanksHolder::Ptr& ranks) {
	const int lastCjsonIdx = currCjsonFieldIdx_;
	{
		FtFuncStruct data{ParsedQueryFunction{}};
		data.isFunction = true;
		data.indexNo = indexNo;
		createFunc(std::move(data));
	}
	if (IsComposite(nm_.getIndexType(indexNo))) {
		auto field = nm_.getIndexFields(indexNo)[0];
		if (field == IndexValueType::SetByJsonPath) {
			field = lastCjsonIdx;
		}
		auto it = functions_.find(field);
		assertrx(it != functions_.end());
		return createCtx(it->second, nullptr, nm_.getIndexType(indexNo), ranks);
	} else {
		auto it = functions_.find(indexNo);
		assertrx(it != functions_.end());
		return createCtx(it->second, nullptr, nm_.getIndexType(indexNo), ranks);
	}
}

FtCtx::Ptr FtFunction::CreateCtx(int indexNo, const RanksHolder::Ptr& ranks) {
	const auto indexType = nm_.getIndexType(indexNo);
	assertrx_throw(IsFullText(indexType));
	if (functions_.empty()) {
		// we use this hack because ft always needs ctx to generate rank in response
		return createFuncForRank(indexNo, ranks);
	}
	FtCtx::Ptr ctx;
	if (IsComposite(indexType)) {
		int fieldNo = 0;
		int cjsonFieldIdx = nm_.getIndexesCount();
		for (auto field : nm_.getIndexFields(indexNo)) {
			if (field == IndexValueType::SetByJsonPath) {
				field = cjsonFieldIdx++;
			}
			auto it = functions_.find(field);
			if (it != functions_.end()) {
				it->second.fieldNo = fieldNo;
				ctx = createCtx(it->second, ctx, indexType, ranks);
			}
			fieldNo++;
		}
	} else {
		auto it = functions_.find(indexNo);
		if (it != functions_.end()) {
			it->second.fieldNo = 0;
			ctx = createCtx(it->second, ctx, indexType, ranks);
		}
	}
	return ctx ? ctx : createFuncForRank(indexNo, ranks);
}

void FtFunctionsHolder::Process(LocalQueryResults& res) const {
	if (queries_.empty() || force_only_) {
		return;
	}
	bool hasFuncs = false;
	for (auto& q : queries_) {
		if (q) {
			hasFuncs = true;
			break;
		}
	}
	if (!hasFuncs) {
		return;
	}

	bool changed = false;
	for (auto& it : res) {
		ItemRef& item = it.GetItemRef();
		const auto nsid = item.Nsid();
		if (queries_.size() <= nsid) {
			continue;
		}
		if (auto& funcPtr = queries_[nsid]; funcPtr && funcPtr->ProcessItem(item, res.getPayloadType(nsid), res.stringsHolder_)) {
			changed = true;
		}
	}
	res.nonCacheableData = changed;
}

bool FtFunction::ProcessItem(ItemRef& res, PayloadType& pl_type, std::vector<key_string>& stringsHolder) {
	bool changed = false;
	for (auto& func : functions_) {
		if (func.second.ctx &&
			std::visit([&](auto& f) -> bool { return f.Process(res, pl_type, func.second, stringsHolder); }, func.second.func)) {
			changed = true;
		}
	}
	return changed;
}

FtCtx::Ptr FtFunction::createCtx(FtFuncStruct& data, FtCtx::Ptr ctx, IndexType index_type, const RanksHolder::Ptr& ranks) {
	if (IsFullText(index_type)) {
		if (!ctx) {
			switch (FtFuncType(data.func.index())) {
				case FtFuncType::None:
					data.ctx = make_intrusive<FtCtx>(FtCtxType::kFtCtx, ranks);
					break;
				case FtFuncType::Snippet:
				case FtFuncType::Highlight:
				case FtFuncType::SnippetN:
					data.ctx = make_intrusive<FtCtx>(FtCtxType::kFtArea, ranks);
					break;
				case FtFuncType::DebugRank:
					data.ctx = make_intrusive<FtCtx>(FtCtxType::kFtAreaDebug, ranks);
					break;
				default:
					throw reindexer::Error(errLogic, "incorrect function type 'Max'");
			}
		} else {
			switch (FtFuncType(data.func.index())) {
				case FtFuncType::None:
					if (ctx->Type() != FtCtxType::kFtCtx) {
						throw reindexer::Error(errLogic, "The existing calling context type '{}' does not allow this function",
											   int(ctx->Type()));
					}
					break;
				case FtFuncType::Snippet:
				case FtFuncType::Highlight:
				case FtFuncType::SnippetN:
					if (ctx->Type() != FtCtxType::kFtArea) {
						throw reindexer::Error(errLogic, "The existing calling context type '{}' does not allow this function",
											   int(ctx->Type()));
					}
					break;
				case FtFuncType::DebugRank:
					if (ctx->Type() != FtCtxType::kFtAreaDebug) {
						throw reindexer::Error(errLogic, "The existing calling context type '{}' does not allow this function",
											   int(ctx->Type()));
					}
					break;
				default:
					throw reindexer::Error(errLogic, "incorrect function type 'Max'");
			}
			data.ctx = std::move(ctx);
		}
		const std::string& indexName = (data.indexNo >= nm_.getIndexesCount()) ? data.field : nm_.getIndexName(data.indexNo);
		data.ctx->AddFunction(indexName, FtFuncType(data.func.index()));
	}
	return data.ctx;
}

}  // namespace reindexer
