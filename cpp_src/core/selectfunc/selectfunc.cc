#include "selectfunc.h"
#include <cctype>
#include <memory>
#include "core/namespacedef.h"
#include "ctx/ftctx.h"
#include "functions/highlight.h"
#include "functions/snippet.h"

namespace reindexer {
using std::make_pair;
using std::make_shared;
static inline void ltrim(std::string &s) {
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
}

// trim from end (in place)
static inline void rtrim(std::string &s) {
	s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

// trim from both ends (in place)
static inline void trim(std::string &s) {
	ltrim(s);
	rtrim(s);
}

SelectFunction::Ptr SelectFunctionsHolder::AddNamespace(const Query &q, const Namespace &nm, bool force) {
	if (q.selectFunctions_.empty() && !force) {
		return nullptr;
	} else if (!q.selectFunctions_.empty()) {
		force_only_ = false;
	}

	if (!querys_) {
		querys_.reset(new fast_hash_map<string, SelectFunction::Ptr>);
	}

	NsSelectFuncInterface nm_interface(nm);
	SelectFunction::Ptr func = make_shared<SelectFunction>(q, nm_interface);
	return querys_->emplace(nm_interface.GetName(), func).first->second;
}

SelectFunction::SelectFunction(const Query &q, NsSelectFuncInterface &nm) : nm_(nm) {
	functions_.reserve(q.selectFunctions_.size());
	for (auto &func : q.selectFunctions_) {
		SelectFuncParser parser;
		SelectFuncStruct &result = parser.Parse(func);
		if (!result.isFunction) continue;
		createFunc(result);
	}
};

void SelectFunction::createFunc(SelectFuncStruct &data) {
	int indexNo = -1;
	if (data.indexNo == -1) {
		try {
			indexNo = nm_.getIndexByName(data.field);
		} catch (const Error &) {
			std::size_t found = data.field.find("=");
			if (found != std::string::npos) data.field.erase(0, found + 1);
			trim(data.field);
			try {
				indexNo = nm_.getIndexByName(data.field);
			} catch (...) {
				return;
			}
		}
	} else {
		indexNo = data.indexNo;
	}

	// if composite crete function only for inside
	if (isComposite(nm_.getIndexType(indexNo))) {
		int fieldNo = 0;
		for (auto filed : nm_.getIndexFields(indexNo)) {
			data.indexNo = filed;
			data.fieldNo = fieldNo;
			data.field = nm_.getIndexName(filed);
			functions_.emplace(std::make_pair(filed, data));
			fieldNo++;
		}
	} else {
		data.indexNo = indexNo;
		data.field = nm_.getIndexName(indexNo);
		data.fieldNo = 0;
		functions_.emplace(std::make_pair(indexNo, data));
	}
}  // namespace reindexer

BaseFunctionCtx::Ptr SelectFunction::createFuncForProc(int indexNo) {
	SelectFuncStruct data;
	data.isFunction = true;
	data.type = SelectFuncStruct::kSelectFuncProc;
	data.indexNo = indexNo;
	createFunc(data);
	if (isComposite(nm_.getIndexType(indexNo))) {
		auto field = nm_.getIndexFields(indexNo)[0];
		auto it = functions_.find(field);
		assert(it != functions_.end());
		return createCtx(it->second, nullptr, nm_.getIndexType(indexNo));
	} else {
		auto it = functions_.find(indexNo);
		assert(it != functions_.end());
		return createCtx(it->second, nullptr, nm_.getIndexType(indexNo));
	}
}

BaseFunctionCtx::Ptr SelectFunction::CreateCtx(int indexNo) {
	// we use this hack becouse ft always need ctx to generate proc in answer
	if (functions_.empty() && isFullText(nm_.getIndexType(indexNo))) {
		return createFuncForProc(indexNo);
	} else if (functions_.empty())
		return nullptr;
	BaseFunctionCtx::Ptr ctx;

	if (isComposite(nm_.getIndexType(indexNo))) {
		int fieldNo = 0;
		for (auto filed : nm_.getIndexFields(indexNo)) {
			auto it = functions_.find(filed);
			if (it != functions_.end()) {
				it->second.fieldNo = fieldNo;
				if (isFullText(nm_.getIndexType(indexNo)) && it->second.type == SelectFuncStruct::kSelectFuncNone) {
					it->second.type = SelectFuncStruct::kSelectFuncProc;
				}
				ctx = createCtx(it->second, ctx, nm_.getIndexType(indexNo));
			}
			fieldNo++;
		}
	} else {
		auto it = functions_.find(indexNo);
		if (it != functions_.end()) {
			it->second.fieldNo = 0;
			ctx = createCtx(it->second, ctx, nm_.getIndexType(indexNo));
		}
	}
	if (!ctx && isFullText(nm_.getIndexType(indexNo))) {
		return createFuncForProc(indexNo);
	}
	return ctx;
}
void SelectFunctionsHolder::Process(QueryResults &res) {
	if (!querys_ || querys_->empty() || force_only_) return;
	bool changed = false;

	for (size_t i = 0; i < res.size(); ++i) {
		auto &pl_type = res.getPayloadType(res[i].nsid);
		auto it = querys_->find(pl_type.Name());
		if (it != querys_->end()) {
			if (it->second->ProcessItem(res[i], pl_type)) changed = true;
		}
	}
	res.nonCacheableData = changed;
}
bool SelectFunction::ProcessItem(ItemRef &res, PayloadType &pl_type) {
	bool changed = false;
	for (auto &func : functions_) {
		if (!func.second.ctx) continue;
		switch (func.second.type) {
			case SelectFuncStruct::kSelectFuncSnippet:
				if (Snippet::process(res, pl_type, func.second)) changed = true;
				break;
			case SelectFuncStruct::kSelectFuncHighlight:
				if (Highlight::process(res, pl_type, func.second)) changed = true;
				break;
			case SelectFuncStruct::kSelectFuncNone:
			case SelectFuncStruct::kSelectFuncProc:
				break;
		}
	}
	return changed;
}

BaseFunctionCtx::Ptr SelectFunction::createCtx(SelectFuncStruct &data, BaseFunctionCtx::Ptr ctx, IndexType index_type) {
	switch (data.type) {
		case SelectFuncStruct::kSelectFuncNone:
		case SelectFuncStruct::kSelectFuncSnippet:
		case SelectFuncStruct::kSelectFuncHighlight:
		case SelectFuncStruct::kSelectFuncProc:
			if (isFullText(index_type)) {
				if (!ctx) {
					data.ctx = make_shared<FtCtx>();
				} else {
					data.ctx = ctx;
				}
				data.ctx->AddFunction(nm_.getIndexName(data.indexNo), data.type);
			}
	}
	return data.ctx;
}

}  // namespace reindexer
