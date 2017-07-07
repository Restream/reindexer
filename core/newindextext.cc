#include <stdio.h>

#include <algorithm>
#include <cwctype>

#include "algoritm/full_text_search/fulltextdumper.h"
#include "core/newindextext.h"
#include "index.h"
#include "tools/errors.h"
#include "tools/strings.h"

namespace reindexer {
using search_engine::FullTextDumper;

const char* newWordDelimeters = " ,\t\n.-:':!?\"";

template <typename T>
NewIndexText<T>::NewIndexText(IndexType _type, const string& _name) : IndexUnordered<T>(_type, _name) {}

template <typename T>
KeyRef NewIndexText<T>::Upsert(const KeyRef& key, IdType id) {
	auto storedKey = IndexUnordered<T>::Upsert(key, id);
	p_string str(storedKey);
	wstring utf16str = utf8_to_utf16(string(str.data(), str.length()));
	std::transform(utf16str.begin(), utf16str.end(), utf16str.begin(), std::towlower);
	id_holder_.AddData(utf16_to_utf8(utf16str), id);

	return storedKey;
}

template <typename T>
void NewIndexText<T>::Delete(const KeyRef& key, IdType id) {
	p_string str(key);
	wstring utf16str = utf8_to_utf16(string(str.data(), str.length()));
	std::transform(utf16str.begin(), utf16str.end(), utf16str.begin(), std::towlower);

	id_holder_.RemoveData(utf16_to_utf8(utf16str), id);
}

template <typename T>
void NewIndexText<T>::Commit(const CommitContext& ctx) {
	if (ctx.phases() & CommitContext::MakeIdsets) {
		updated_ = ctx.getUpdatedCount() != 0;
		id_holder_.Commit(ctx);
	}

	IndexUnordered<T>::Commit(ctx);

	if (!(ctx.phases() & CommitContext::PrepareForSelect)) return;

	engine_.Rebuild(id_holder_.GetDataForBuild(), !updated_);
}

template <typename T>
SelectKeyResults NewIndexText<T>::SelectKey(const KeyValues& keys, CondType condition, SortType, Index::ResultType /*res_type*/) {
	if (keys.size() != 1 || condition != CondEq) {
		throw Error(errParams, "Full text index support only EQ condition with 1 parameter");
	}

	wstring utf16str = utf8_to_utf16(keys[0].toString());
	std::transform(utf16str.begin(), utf16str.end(), utf16str.begin(), std::towlower);
	SelectKeyResult res;

	bool need_put = false;
	auto cached = IndexUnordered<T>::cache_->Get(IdSetCacheKey{keys, condition, 0});
	if (cached.key) {
		if (!cached.val.ids->size()) {
			need_put = true;
			//;
		} else {
			res.push_back(cached.val.ids);
			return SelectKeyResults(res);
		}
	}

	auto result = engine_.Search(utf16_to_utf8(utf16str));

	FullTextDumper::Init().AddResultData(keys[0].toString(), result, id_holder_);
	auto res_id_set = id_holder_.GetByVirtualId(result);
	res.push_back(SingleSelectKeyResult(res_id_set));
	if (need_put) IndexUnordered<T>::cache_->Put(*cached.key, res_id_set);

	return SelectKeyResults(res);
}

template <typename T>
void NewIndexText<T>::UpdateSortedIds(const UpdateSortedContext& /*ctx*/) {
	//  We need not sort by other indexes
	//	IndexUnordered<T>::UpdateSortedIds (ctx);
}

template <typename T>
Index* NewIndexText<T>::Clone() {
	return new NewIndexText<T>(*this);
}

template class NewIndexText<unordered_str_map<Index::KeyEntry>>;
}  // namespace reindexer
