#include "baseseacher.h"
#include "algoritm/full_text_search/merger/positionmerger.h"
#include "tools/customhash.h"

namespace search_engine {

using std::make_shared;
using std::pair;

void BaseSearcher::AddSeacher(ISeacher::Ptr seacher) { searchers_.push_back(seacher); }

const wchar_t *BaseSearcher::GetData(size_t i, wchar_t *buf, const wstring &src_data, size_t data_size) {
	if (data_size == 3 && i == 1) {
		buf[0] = L'_';
		buf[1] = src_data[0];
		buf[2] = L'_';
		return buf;
	} else if (i == (data_size - 2)) {
		buf[0] = src_data[i - 2];
		buf[1] = src_data[i - 1];
		buf[2] = L'_';
		return buf;
	} else if (i == (data_size - 1)) {
		buf[0] = src_data[i - 2];
		buf[1] = L'_';
		return buf;
	}

	else if (i == 0) {
		buf[2] = src_data[0];
		return buf;
	} else if (i == 1) {
		buf[1] = src_data[0];
		buf[2] = src_data[1];
		return buf;
		//		TODO andrey: fix ambiguous conditions
	} else if (i == 1) {
		buf[2] = src_data[0];
		return buf;
	}

	else {
		return &src_data[i - 2];
	}
	return 0;
}

BaseHolder::SearchTypePtr BaseSearcher::Compare(BaseHolder::Ptr holder, const wstring &src_data) {
	if (src_data.empty()) return make_shared<BaseHolder::SearchType>();

	wchar_t buf[] = L"___";
	size_t data_size = src_data.size() + 2;
	PositionMerger merger(data_size);

	uint16_t pos = 0;
	pair<HashType, ProcType> res;
	// Todo remove substr
	for (size_t i = 0; i < data_size; ++i, ++pos) {
		const DataStruct *id_data = holder->GetData(HashTreGram(GetData(i, buf, src_data, data_size)));

		if (!id_data || id_data->info == nullptr) {
			continue;
		}

		merger.AddPosition(id_data->info, pos);
	}

	return merger.CalcResult();
}

void BuildThread(size_t, const wstring &) {}

void BaseSearcher::AddIndex(BaseHolder::Ptr holder, const wstring &src_data, const IdType ids) {
	if (src_data.empty()) return;

	wchar_t buf[] = L"___";
	pair<PosType, ProcType> pos;
	pos.first = 0;
	vector<pair<HashType, ProcType>> res;

	size_t data_size = src_data.size() + 2;
	ProcType proc = 100 / float(data_size);
	for (size_t i = 0; i < data_size; ++i, ++pos.first) {
		const wchar_t *res_buf = GetData(i, buf, src_data, data_size);
		uint32_t current_hash = HashTreGram(res_buf);

		Info *info = holder->GetInfo(current_hash);
		pos.second = proc;

		if (info == nullptr) {
			info = new Info;
			AddIdToInfo(info, ids, pos, data_size);
			DataStruct curent_data;
			curent_data.info = info;
			curent_data.hash = current_hash;
			holder->AddData(curent_data);

			for (auto seacher : searchers_) {
				seacher->Build(res_buf, 3, res);
				for (auto val : res) {
					curent_data.hash = val.first;
					pos.second = val.second;
					AddIdToInfo(info, ids, pos, data_size);
					holder->AddData(curent_data);
				}
			}

		} else {
			AddIdToInfo(info, ids, pos, data_size);
		}
	}
}

inline void BaseSearcher::AddIdToInfo(Info *info, const IdType id, pair<PosType, ProcType> pos, uint32_t total_size) {
	auto it = info->ids_.find(id);
	if (it == info->ids_.end()) {
		IdContext ctx;
		ctx.proc_.insert(pos);
		ctx.tota_size_ = total_size;
		info->ids_[id] = ctx;
		return;
	}

	it->second.proc_.insert(pos);
	it->second.tota_size_ = total_size;
}
}  // namespace search_engine
