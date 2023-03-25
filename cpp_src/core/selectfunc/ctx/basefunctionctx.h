#pragma once

#include <memory>
#include "core/selectfunc/selectfuncparser.h"
#include "core/type_consts.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

template <class T, class U>
std::shared_ptr<T> reinterpret_pointer_cast(const std::shared_ptr<U>& r) noexcept {
	auto p = reinterpret_cast<typename std::shared_ptr<T>::element_type*>(r.get());
	return std::shared_ptr<T>(r, p);
}

class BaseFunctionCtx {
public:
	typedef std::shared_ptr<BaseFunctionCtx> Ptr;
	enum CtxType { kFtCtx = 0 };
	virtual ~BaseFunctionCtx() {}

	void AddFunction(const std::string& name, SelectFuncStruct::Type function) { functions_[name].insert(function); }
	bool CheckFunction(const std::string& name, std::initializer_list<SelectFuncStruct::Type> types) {
		auto it = functions_.find(name);

		if (it == functions_.end()) return false;

		for (auto type : types) {
			auto fit = it->second.find(type);
			if (fit != it->second.end()) return true;
		}
		return false;
	}
	CtxType type;

protected:
	fast_hash_map<std::string, fast_hash_set<SelectFuncStruct::Type, std::hash<int>>> functions_;
};

}  // namespace reindexer
