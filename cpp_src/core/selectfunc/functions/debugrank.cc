#include "debugrank.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "core/queryresults/itemref.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "core/selectfunc/selectfuncparser.h"
#include "core/queryresults/itemref.h"

namespace reindexer {

bool DebugRank::Process(ItemRef& res, PayloadType& plType, const SelectFuncStruct& func, std::vector<key_string>& stringsHolder) {
	if (!func.funcArgs.empty()) {
		throw Error(errParams, "'debug_rank()' does not expect any arguments, but got {}", func.funcArgs.size());
	}
	if (!func.ctx || func.ctx->Type() != BaseFunctionCtx::CtxType::kFtAreaDebug) {
		return false;
	}
	if (!func.tagsPath.empty()) {
		throw Error(errConflict, "SetByJsonPath is not implemented yet!");
	}

	FtCtx::Ptr ftctx = reindexer::static_ctx_pointer_cast<FtCtx>(func.ctx);
	if (!ftctx->GetData()->isWordPositions) {
		throw Error(errParams, "debug_rank() is supported for 'text' index only");
	}

	FtCtxAreaData<AreaDebug>& dataFtCtx = *(reindexer::static_ctx_pointer_cast<FtCtxAreaData<AreaDebug>>(ftctx->GetData()));
	if (!dataFtCtx.holders.has_value()) {
		return false;
	}
	const auto it = dataFtCtx.holders->find(res.Id());
	if (it == dataFtCtx.holders->end()) {
		return false;
	}

	Payload pl(plType, res.Value());

	VariantArray kr;
	pl.Get(func.field, kr);

	if (kr.empty() || !kr[0].Type().IsSame(KeyValueType::String{})) {
		throw Error(errLogic, "Unable to apply debug_rank function to the non-string field '{}'", func.field);
	}

	const std::string_view data = std::string_view(p_string(kr[0]));

	const auto pva = dataFtCtx.area[it->second].GetAreas(func.fieldNo);
	if (!pva || pva->Empty()) {
		return false;
	}
	const auto& va = *pva;

	std::string resultString;

	auto splitterTask = ftctx->GetData()->splitter->CreateTask();
	splitterTask->SetText(data);

	static const std::string_view startString = "<!>";
	static const std::string_view endString = "<!!>";

	const auto& areaVector = va.GetData();
	size_t id = 0;
	size_t beforeStr = 0;
	while (id < areaVector.size()) {
		bool next = false;
		int endStringCount = 0;
		std::pair<int, int> pos = splitterTask->Convert(areaVector[id].start, areaVector[id].end);
		resultString += std::string_view(data.data() + beforeStr, pos.first - beforeStr);
		do {
			next = false;
			switch (areaVector[id].phraseMode) {
				case AreaDebug::PhraseMode::Start:
					resultString += startString;
					break;
				case AreaDebug::PhraseMode::End:
					endStringCount++;
					break;
				case AreaDebug::PhraseMode::None:
					break;
			}
			resultString += areaVector[id].props;
			id++;
			if (id < areaVector.size() && areaVector[id].start == areaVector[id - 1].start) {
				if (areaVector[id].end != areaVector[id - 1].end) {
					throw Error(errLogic, "areas not equals start={} ends({} {})", areaVector[id].start, areaVector[id].end,
								areaVector[id - 1].end);
				}
				next = true;
			}
		} while (next);
		resultString += std::string_view(data.data() + pos.first, pos.second - pos.first);
		beforeStr = pos.second;
		for (int i = 0; i < endStringCount; i++) {
			resultString += endString;
		}
	}
	resultString += std::string_view(data.data() + beforeStr, data.size() - beforeStr);

	stringsHolder.emplace_back(make_key_string(std::move(resultString)));
	res.Value().Clone();

	pl.Set(func.field, VariantArray{Variant{stringsHolder.back()}});

	return true;
}
}  // namespace reindexer
