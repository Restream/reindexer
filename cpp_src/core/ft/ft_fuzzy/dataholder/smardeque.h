#pragma once
#include <stdint.h>
#include <cstring>
#include <memory>
#include <vector>

namespace search_engine {

using std::pair;
using std::vector;
using std::shared_ptr;
typedef uint8_t ProcType;
typedef uint32_t HashType;
typedef uint16_t PosType;

typedef pair<int, ProcType> ResData;

typedef vector<ResData> SearchType;

typedef shared_ptr<SearchType> SearchTypePtr;

template <class T, size_t block_size>
class SmartDeque {
public:
	typedef T* pointer;

	class iterator {
	public:
		iterator& operator++();
		size_t GetId() const { return size_ * block_size + offset_ - 1; }

		pointer operator->() {
			if (current_) {
				return current_;
			}
			return &default_data;
		}

		T& operator*() {
			if (current_) {
				return *current_;
			}
			return default_data;
		}
		iterator();
		iterator(SmartDeque* parent);
		bool operator==(const iterator& rhs) const;
		inline bool operator!=(const iterator& rhs) const { return !(*this == rhs); }

	private:
		size_t size_;
		size_t offset_;

		SmartDeque* parent_;
		pointer current_;
		T default_data;
	};
	explicit SmartDeque();
	SmartDeque(const SmartDeque& rhs);
	SmartDeque& operator=(const SmartDeque& rhs);
	SmartDeque(SmartDeque&& rhs) noexcept;

	void Add(size_t id, const T& context);
	void Delete(size_t id);

	SearchTypePtr Merge(std::vector<SmartDeque*>& deques, size_t max_size);
	size_t GetSize() const { return size_; }
	size_t GetCount() const { return count_; }
	iterator Begin();
	iterator End();
	static const ProcType MinMergeProc = 20;

	pointer operator[](size_t num) const;
	void Swap(SmartDeque& rhs) noexcept;
	~SmartDeque();

private:
	friend class iterator;
	void allocSection(size_t num);
	void allocDataPtr(size_t num);
	size_t size_;
	size_t count_;

	// it is max possible data for text index now it is 100gb
	pointer* data_;
};
}  // namespace search_engine
