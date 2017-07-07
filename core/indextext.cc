#include <stdio.h>

#include <locale>
#include "core/indextext.h"
#include "tools/errors.h"
#include "tools/logger.h"
#include "tools/strings.h"

namespace reindexer {

const char *wordDelimeters = " ,\t\n.-:':!?\"";

template <typename T>
IndexText<T>::IndexText(IndexType _type, const string &_name) : IndexUnordered<T>(_type, _name) {}

template <typename T>
void IndexText<T>::Commit(const CommitContext &ctx) {
	IndexUnordered<T>::Commit(ctx);

	if (!(ctx.phases() & CommitContext::PrepareForSelect)) return;

	typos.clear();
	words_map_.clear();
	wstring utf16str;
	string str;

	key_string word = make_shared<string>();
	for (auto &doc : this->idx_map) {
		utf8_to_utf16(*doc.first, utf16str);
		std::transform(utf16str.begin(), utf16str.end(), utf16str.begin(), std::towlower);
		utf16_to_utf8(utf16str, str);

		for (size_t pos, lastPos = 0;; lastPos = pos + 1) {
			pos = str.find_first_of(wordDelimeters, lastPos);
			if (pos == string::npos) {
				pos = str.length();
				if (pos != lastPos) {
					word->assign(str.data() + lastPos, (pos - lastPos));
					auto idxIt = words_map_.find(word);
					if (idxIt == words_map_.end()) {
						auto ke = Index::KeyEntry();
						ke.Unsorted().reserve((doc.second.Unsorted().end() - doc.second.Unsorted().begin()) * 50);
						idxIt = words_map_.emplace(word, ke).first;
						word = make_shared<string>();
					}
					idxIt->second.Unsorted().insert(idxIt->second.Unsorted().end(), doc.second.Unsorted().begin(),
													doc.second.Unsorted().end());
				}
				break;
			} else if (pos != lastPos) {
				word->assign(str.data() + lastPos, (pos - lastPos));
				auto idxIt = words_map_.find(word);
				if (idxIt == words_map_.end()) {
					auto ke = Index::KeyEntry();
					ke.Unsorted().reserve((doc.second.Unsorted().end() - doc.second.Unsorted().begin()) * 50);
					idxIt = words_map_.emplace(word, ke).first;
					word = make_shared<string>();
				}
				idxIt->second.Unsorted().insert(idxIt->second.Unsorted().end(), doc.second.Unsorted().begin(), doc.second.Unsorted().end());
			}
		}
	}
	for (auto keyIt = words_map_.begin(); keyIt != words_map_.end(); keyIt++) keyIt->second.Unsorted().commit(ctx);

	wstring utf16Word, utf16Typo;
	string typo;

	for (auto &keyIt : words_map_) {
		const key_string &key = keyIt.first;

		utf8_to_utf16(*key, utf16Word);
		if (!utf16Word.length()) continue;
		utf16Typo.assign(utf16Word.data() + 1, utf16Word.size() - 1);
		utf16_to_utf8(utf16Typo, typo);
		typos.emplace(typo, &key);

		for (size_t i = 0; i < utf16Typo.length(); ++i) {
			utf16Typo[i] = utf16Word[i];
			utf16_to_utf8(utf16Typo, typo);
			typos.emplace(typo, &key);
		}
	}
	logPrintf(LogInfo, "idx=%d,typos=%d", (int)words_map_.size(), (int)typos.size());
}

template <typename T>
void IndexText<T>::UpdateSortedIds(const UpdateSortedContext & /*ctx*/) {
	//  We need not sort by other indexes
	//	IndexUnordered<T>::UpdateSortedIds (ctx);
}

template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const KeyValues &keys, CondType condition, SortType /*stype*/, Index::ResultType /*res_type*/) {
	if (keys.size() != 1 || condition != CondEq) {
		throw Error(errParams, "Full text index support only EQ condition with 1 parameter");
	}

	SelectKeyResults reslts;
	vector<string> tokens;

	// To lower key
	wstring utf16str = utf8_to_utf16(keys[0].toString());
	std::transform(utf16str.begin(), utf16str.end(), utf16str.begin(), std::towlower);

	// Split query to words
	for (auto &tk : split(utf16_to_utf8(utf16str), wordDelimeters, true, tokens)) {
		KeyValues tkKeys;
		// push back word to condition keys
		tkKeys.push_back(KeyValue(tk));
		// make possible typos
		vector<string> _typos;
		mktypos(tk, _typos);
		_typos.push_back(tk);

		SelectKeyResult res;
		for (auto &typo : _typos) {
			// search typos in typos dictionary
			auto typoIt = typos.find(typo);

			if (typoIt != typos.end()) {
				auto keyIt = words_map_.find((typename T::key_type) * typoIt->second);
				if (keyIt != words_map_.end()) res.push_back(keyIt->second.Sorted(0));
			}
		}
		reslts.push_back(std::move(res));
	}
	return reslts;
}

template <typename T>
Index *IndexText<T>::Clone() {
	return new IndexText<T>(*this);
}

template class IndexText<unordered_str_map<Index::KeyEntry>>;

}  // namespace reindexer
