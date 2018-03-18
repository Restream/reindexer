#include "highlight.h"
#include "core/selectfunc/ctx/ftctx.h"

namespace reindexer {

bool Highlight::process(ItemRef &res, PayloadType &pl_type, const SelectFuncStruct &func) {
	if (func.funcArgs.size() < 2) throw Error(errParams, "Invalid highlight params need minimum 2 - have %d", int(func.funcArgs.size()));

	if (!func.ctx || func.ctx->type != BaseFunctionCtx::kFtCtx) return false;

	FtCtx::Ptr ftctx = reinterpret_pointer_cast<FtCtx>(func.ctx);
	AreaHolder::Ptr area = ftctx->Area(res.id);
	if (!area) {
		return false;
	}
	Payload pl(pl_type, res.value);

	KeyRefs kr;
	pl.Get(func.field, kr);
	const string *data = p_string(kr[0]).getCxxstr();
	auto pva = area->GetAreas(func.fieldNo);
	if (!pva || pva->empty()) return false;
	auto &va = *pva;

	string result_string;
	result_string.reserve(result_string.size() + va.size() * (func.funcArgs[0].size() + func.funcArgs[1].size()));
	result_string = *data;

	int offset = 0;

	for (auto area : va) {
		result_string.insert(area.start_ + offset, func.funcArgs[0]);
		offset += func.funcArgs[0].size();

		result_string.insert(area.end_ + offset, func.funcArgs[1]);

		offset += func.funcArgs[1].size();
	}

	key_string_release(const_cast<string *>(data));
	auto str = make_key_string(result_string);
	key_string_add_ref(str.get());
	res.value.AllocOrClone(0);

	pl.Set(func.field, KeyRefs{KeyRef{str}});
	return true;
}

}  // namespace reindexer
