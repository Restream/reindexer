#pragma once
#include <core/type_consts.h>
#include <memory>
#include "core/idset.h"
#include "estl/h_vector.h"
namespace reindexer {
using std::shared_ptr;
struct FullTextCtx {
	typedef shared_ptr<FullTextCtx> Ptr;
	int16_t Proc(IdType id) {
		if (static_cast<size_t>(id) > result.size()) return 0;
		return result[id];
	}

	size_t GetSize() { return result.size(); }
	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc) {
		auto cnt = end - begin;
		for (int i = 0; i < cnt; ++i) {
			result.push_back(proc);
		}
	}
	void Reserve(size_t size) { result.reserve(size); }
	void Add(IdType, int16_t proc) { result.push_back(proc); }
	size_t Size() { return result.size(); }

private:
	h_vector<IdType> result;
};
}  // namespace reindexer
