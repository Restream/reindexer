#include "smardeque.h"
#include <assert.h>
#include <cstring>
#include "datastruct.h"
namespace search_engine {

template <class T, size_t block_size>
SmartDeque<T, block_size>::SmartDeque() : size_(0), count_(0), data_(nullptr) {}

template <class T, size_t block_size>
void SmartDeque<T, block_size>::Add(size_t id, const T& context) {
	size_t size = (id / block_size);
	size_t offset = (id % block_size);
	if ((size + 1) > size_) {
		allocDataPtr(size + 10);
	}
	if (!data_[size]) {
		allocSection(size);
	}
	count_++;
	data_[size][offset] = context;
}

template <class T, size_t block_size>
void SmartDeque<T, block_size>::Delete(size_t id) {
	size_t size = (id / block_size);
	size_t offset = (id % block_size);
	if ((size + 1) > size_) {
		return;
	}
	if (!data_[size]) {
		return;
	}
	count_--;
	data_[size][offset].proc_ = 0;
}

template <class T, size_t block_size>
typename SmartDeque<T, block_size>::pointer SmartDeque<T, block_size>::operator[](size_t num) const {
	size_t size = (num / block_size);
	size_t offset = (num % block_size);
	if (!data_ || size >= size_ || !data_[size]) {
		return nullptr;
	}
	if (data_[size][offset].proc_ == 0) {
		return nullptr;
	}
	return &data_[size][offset];
}
template <class T, size_t block_size>
void SmartDeque<T, block_size>::allocDataPtr(size_t num) {
	auto tmp_data = new pointer[num];
	if (data_) {
		memcpy(reinterpret_cast<void*>(tmp_data), reinterpret_cast<const void*>(data_), sizeof(pointer) * size_);
	}
	memset(reinterpret_cast<void*>(tmp_data + size_), 0, (num - size_) * sizeof(pointer));
	delete[] data_;
	data_ = tmp_data;
	size_ = num;
}
template <class T, size_t block_size>
SearchTypePtr SmartDeque<T, block_size>::Merge(std::vector<SmartDeque*>& deques, size_t max_size) {
	SearchTypePtr res = std::make_shared<SearchType>();
	for (size_t i = 0; i < max_size; ++i) {
		for (size_t h = 0; h < deques.size(); ++h) {
			pointer ptr;
			if (deques[h]->size_ <= i || !deques[h]->data_) {
				continue;
			}
			ptr = deques[h]->data_[i];
			if (ptr) {
				for (size_t g = 0; g < block_size; ++g) {
					if (ptr[g].proc_ != 0) {
						if (size_ > i && data_[i] && data_[i][g].proc_ != 0) {
							data_[i][g].proc_ += ptr[g].proc_;
						} else {
							Add(i * block_size + g, ptr[g]);
						}
					}
				}
			}
		}
	}
	res->reserve(count_);

	for (auto it = Begin(); it != End(); ++it) {
		if (it->proc_ > 0) {
			res->push_back(std::make_pair(it.GetId(), it->proc_));
		}
	}
	return res;
}

template <class T, size_t block_size>
SmartDeque<T, block_size>::SmartDeque(const SmartDeque& rhs) {
	if (!rhs.size_) {
		size_ = 0;
		count_ = 0;
		data_ = nullptr;
		return;
	}
	size_ = rhs.size_;
	data_ = new pointer[size_];
	memcpy(reinterpret_cast<void*>(data_), reinterpret_cast<const void*>(rhs.data_), sizeof(pointer) * size_);
	count_ = rhs.count_;
	for (size_t i = 0; i < size_; ++i) {
		if (data_[i] != nullptr) {
			data_[i] = new T[block_size];
			memcpy(data_[i], rhs.data_[i], block_size * sizeof(T));
		}
	}
}

template <class T, size_t block_size>
SmartDeque<T, block_size>& SmartDeque<T, block_size>::operator=(const SmartDeque& rhs) {
	if (this != &rhs) {
		SmartDeque tmp(rhs);
		Swap(tmp);
	}
	return *this;
}
template <class T, size_t block_size>
void SmartDeque<T, block_size>::Swap(SmartDeque& rhs) noexcept {
	size_t size = size_;
	size_t count = count_;
	pointer* data = data_;
	size_ = rhs.size_;
	count_ = rhs.count_;
	data_ = rhs.data_;
	rhs.size_ = size;
	rhs.count_ = count;
	rhs.data_ = data;
}

template <class T, size_t block_size>
typename SmartDeque<T, block_size>::iterator SmartDeque<T, block_size>::Begin() {
	return SmartDeque<T, block_size>::iterator(this);
}

template <class T, size_t block_size>
typename SmartDeque<T, block_size>::iterator SmartDeque<T, block_size>::End() {
	return SmartDeque<T, block_size>::iterator();
}

template <class T, size_t block_size>
SmartDeque<T, block_size>::SmartDeque(SmartDeque&& rhs) noexcept {
	size_ = rhs.size_;
	count_ = rhs.count_;
	data_ = rhs.data_;
	rhs.size_ = 0;
	rhs.count_ = 0;
	rhs.data_ = nullptr;
}

template <class T, size_t block_size>
SmartDeque<T, block_size>& SmartDeque<T, block_size>::operator=(SmartDeque&& rhs) noexcept {
	if (this != &rhs) {
		size_ = rhs.size_;
		count_ = rhs.count_;
		data_ = rhs.data_;
		rhs.size_ = 0;
		rhs.count_ = 0;
		rhs.data_ = nullptr;
	}
	return *this;
}

template <class T, size_t block_size>
SmartDeque<T, block_size>::~SmartDeque() {
	if (!data_) {
		return;
	}
	for (size_t i = 0; i < size_; ++i) {
		delete[] data_[i];
	}
	delete[] data_;
}

template <class T, size_t block_size>
void SmartDeque<T, block_size>::allocSection(size_t num) {
	if (!data_) {
		return;
	}
	data_[num] = new T[block_size];
	memset(data_[num], 0, block_size * sizeof(T));
}

template <class T, size_t block_size>
typename SmartDeque<T, block_size>::iterator& SmartDeque<T, block_size>::iterator::operator++() {
	if (parent_ == nullptr) {
		return *this;
	}

	for (; size_ < parent_->size_; ++size_, offset_ = 0) {
		if (parent_->data_[size_]) {
			for (; offset_ < block_size; ++offset_) {
				if (parent_->data_[size_][offset_].proc_ != 0) {
					current_ = parent_->data_[size_] + offset_;
					++offset_;
					return *this;
				}
			}
		}
	}
	parent_ = nullptr;
	current_ = nullptr;
	return *this;
}

template <class T, size_t block_size>
SmartDeque<T, block_size>::iterator::iterator() : size_(0), offset_(0), parent_(nullptr), current_(nullptr) {
	if constexpr (std::is_trivial<T>::value) {
		memset(&default_data, 0, sizeof(default_data));
	} else {
		static_assert(std::is_default_constructible<T>::value, "Expecting default contractible type");
	}
}
template <class T, size_t block_size>
SmartDeque<T, block_size>::iterator::iterator(SmartDeque* parent) : size_(0), offset_(0), parent_(parent), current_(nullptr) {
	if constexpr (std::is_trivial<T>::value) {
		memset(&default_data, 0, sizeof(default_data));
	} else {
		static_assert(std::is_default_constructible<T>::value, "Expecting default contractible type");
	}
	++(*this);
}

template <class T, size_t block_size>
bool SmartDeque<T, block_size>::iterator::operator==(const iterator& rhs) const {
	if (!parent_ && !rhs.parent_) {
		return true;
	}
	return current_ == rhs.current_;
}
template class SmartDeque<IdContext, 100>;
}  // namespace search_engine
