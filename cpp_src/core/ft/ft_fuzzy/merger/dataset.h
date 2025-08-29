#pragma once
#include <algorithm>
#include <memory>
#include <vector>
namespace search_engine {

template <class T, class Base = uint16_t>
class [[nodiscard]] DataSet {
public:
	DataSet(int minval, int maxval) : minval_(minval), exists_(maxval - minval + 1, false), offsets_(maxval - minval + 1) {
		data_ = std::make_shared<std::vector<T>>();
		data_->reserve(1000);
	}
	template <class D>
	void AddData(size_t id, const D& ctx) {
		if (exists_[id - minval_]) {
			data_->at(offsets_[id - minval_]).Add(ctx);
			return;
		}
		exists_[id - minval_] = true;
		offsets_[id - minval_] = data_->size();
		data_->push_back({id, ctx});
	}

	T* Get(size_t id) {
		if (exists_[id - minval_]) {
			return data_[offsets_[id - minval_]];
		}
		return nullptr;
	}
	std::vector<T>& GetData() { return data_; }
	std::shared_ptr<std::vector<T>> data_;

private:
	int minval_;

	std::vector<bool> exists_;

	std::vector<Base> offsets_;
};
}  // namespace search_engine
