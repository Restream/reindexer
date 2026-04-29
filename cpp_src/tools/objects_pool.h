#pragma once

#include <array>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include "estl/concepts.h"

namespace reindexer {

template <typename T, unsigned kPoolSize, size_t kMaxObjSize = 128>
class [[nodiscard]] ObjectsPool {
	class [[nodiscard]] ObjectGuard {
	public:
		ObjectGuard(ObjectsPool& owner, T&& data) noexcept : data_{std::move(data)}, owner_{&owner} {}
		ObjectGuard(const ObjectGuard& o) = delete;
		ObjectGuard(ObjectGuard&& o) noexcept : data_{std::move(o.data_)}, owner_{o.owner_} { o.owner_ = nullptr; }
		ObjectGuard& operator=(const ObjectGuard& o) = delete;
		ObjectGuard& operator=(ObjectGuard&& o) noexcept {
			if (&o != this) [[likely]] {
				data_ = std::move(o.data_);
				owner_ = o.owner_;
				o.owner_ = nullptr;
			}
			return *this;
		}
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

#define __RX_VAR_FROM_POOL__(Type, VarName)            \
	thread_local ObjectsPool<Type, 1> _pool_##VarName; \
	auto _buf_##VarName = _pool_##VarName.Get();       \
	Type& VarName = _buf_##VarName.Data();
