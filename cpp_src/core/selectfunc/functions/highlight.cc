#include "highlight.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "core/selectfunc/ctx/ftctx.h"
namespace reindexer {

bool Highlight::process(ItemRef &res, PayloadType &pl_type, const SelectFuncStruct &func) {
	if (func.funcArgs.size() < 2) throw Error(errParams, "Invalid highlight params need minimum 2 - have %d", func.funcArgs.size());

	if (!func.ctx || func.ctx->type != BaseFunctionCtx::kFtCtx) return false;

	FtCtx::Ptr ftctx = reindexer::reinterpret_pointer_cast<FtCtx>(func.ctx);
	AreaHolder::Ptr area = ftctx->Area(res.Id());
	if (!area) {
		return false;
	}
	Payload pl(pl_type, res.Value());

	VariantArray kr;
	if (func.tagsPath.empty()) {
		pl.Get(func.field, kr);
	} else {
		pl.GetByJsonPath(func.tagsPath, kr, KeyValueUndefined);
	}

	const string *data = p_string(kr[0]).getCxxstr();
	auto pva = area->GetAreas(func.fieldNo);
	if (!pva || pva->empty()) return false;
	auto &va = *pva;

	string result_string;
	result_string.reserve(result_string.size() + va.size() * (func.funcArgs[0].size() + func.funcArgs[1].size()));
	result_string = *data;

	int offset = 0;

	Word2PosHelper word2pos(*data, ftctx->GetData()->extraWordSymbols_);
	for (auto area : va) {
		std::pair<int, int> pos =
			ftctx->GetData()->isWordPositions_ ? word2pos.convert(area.start_, area.end_) : std::make_pair(area.start_, area.end_);

		// printf("%d(%d),%d(%d) %s\n", area.start_, pos.first, area.end_, pos.second, data->c_str());

		result_string.insert(pos.first + offset, func.funcArgs[0]);
		offset += func.funcArgs[0].size();

		result_string.insert(pos.second + offset, func.funcArgs[1]);

		offset += func.funcArgs[1].size();
	}

	key_string_release(const_cast<string *>(data));
	auto str = make_key_string(result_string);
	key_string_add_ref(str.get());
	res.Value().Clone();

	if (func.tagsPath.empty()) {
		pl.Set(func.field, VariantArray{Variant{str}});
	} else {
		throw Error(errConflict, "SetByJsonPath is not implemented yet!");
	}

	return true;
}

}  // namespace reindexer
