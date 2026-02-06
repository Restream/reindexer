#pragma once

#include <array>
#include "estl/concepts.h"

namespace reindexer {

template <typename T, unsigned kPoolSize, size_t kMaxObjSize = 128>
class [[nodiscard]] ObjectsPool {
	class [[nodiscard]] ObjectGuard {
	public:
		ObjectGuard(ObjectsPool& owner, T&& data) noexcept : data_{std::move(data)}, owner_{&owner} {}
		~ObjectGuard() {
			if (owner_) {
				owner_->put(std::move(data_));
			}
		}

		T& Data() noexcept { return data_; }

	private:
		T data_;
		ObjectsPool* owner_{nullptr};
	};

public:
	ObjectGuard Get() {
		if (objectsCount_) {
			return ObjectGuard{*this, std::move(objects_[--objectsCount_])};
		}
		return ObjectGuard{*this, T()};
	}

private:
	void put(T&& obj) noexcept {
		const size_t capacity = getCapacity(obj);
		if (capacity && capacity <= kMaxObjSize && objectsCount_ < kPoolSize) {
			try {
				if constexpr (concepts::HasResize<T>) {
					obj.resize(0);
				} else {
					obj.clear();
				}
			} catch (...) {
				return;
			}

			objects_[objectsCount_++] = std::move(obj);
		}
	}
	static size_t getCapacity(const T& obj) noexcept {
		if constexpr (concepts::HasCapacity<T>) {
			return obj.capacity();
		} else {
			return obj.bucket_count();
		}
	}

	std::array<T, kPoolSize> objects_;
	unsigned objectsCount_ = 0;
};

}  // namespace reindexer
