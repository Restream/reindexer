#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace reindexer {

class [[nodiscard]] NodeArena {
public:
	static constexpr size_t kInitialBlockSize = 64 * 1024;

	NodeArena() = default;
	NodeArena(const NodeArena&) = delete;
	NodeArena& operator=(const NodeArena&) = delete;
	NodeArena(NodeArena&&) = delete;
	NodeArena& operator=(NodeArena&&) = delete;
	~NodeArena() { release(); }

	[[nodiscard]] void* Allocate(size_t size, size_t alignment) {
		if (size == 0) {
			return nullptr;
		}
		if (alignment == 0 || alignment > kBlockAlignment) {
			throw std::bad_alloc{};
		}
		if (current_) {
			if (void* ptr = tryAllocate(*current_, size, alignment)) {
				return ptr;
			}
		}

		if (size > std::numeric_limits<size_t>::max() - (alignment - 1)) {
			throw std::bad_alloc{};
		}
		const size_t requiredCapacity = size + alignment - 1;
		size_t nextCapacity = kInitialBlockSize;
		if (current_) {
			if (current_->capacity > std::numeric_limits<size_t>::max() / 2) {
				nextCapacity = std::numeric_limits<size_t>::max();
			} else {
				nextCapacity = current_->capacity * 2;
			}
		}
		addBlock(std::max(requiredCapacity, nextCapacity));

		void* ptr = tryAllocate(*current_, size, alignment);
		if (!ptr) {
			throw std::bad_alloc{};
		}
		return ptr;
	}

private:
	static constexpr size_t kBlockAlignment = alignof(std::max_align_t);

	struct [[nodiscard]] alignas(std::max_align_t) Block {
		Block* next = nullptr;
		size_t capacity = 0;
		size_t used = 0;
	};

	[[nodiscard]] static std::byte* data(Block& block) noexcept {
		return reinterpret_cast<std::byte*>(std::addressof(block)) + sizeof(Block);
	}

	[[nodiscard]] static void* tryAllocate(Block& block, size_t size, size_t alignment) noexcept {
		void* ptr = data(block) + block.used;
		size_t space = block.capacity - block.used;
		if (!std::align(alignment, size, ptr, space)) {
			return nullptr;
		}
		block.used = static_cast<std::byte*>(ptr) - data(block) + size;
		return ptr;
	}

	void addBlock(size_t capacity) {
		if (capacity > std::numeric_limits<size_t>::max() - sizeof(Block)) {
			throw std::bad_alloc{};
		}
		void* memory = ::operator new(sizeof(Block) + capacity);
		auto* block = ::new (memory) Block{nullptr, capacity, 0};
		if (current_) {
			current_->next = block;
		} else {
			first_ = block;
		}
		current_ = block;
	}

	void release() noexcept {
		for (Block* block = first_; block != nullptr;) {
			Block* next = block->next;
			std::destroy_at(block);
			::operator delete(block);
			block = next;
		}
		first_ = nullptr;
		current_ = nullptr;
	}

	Block* first_ = nullptr;
	Block* current_ = nullptr;
};

template <typename T>
class [[nodiscard]] NodeAllocator {
public:
	using value_type = T;
	using size_type = size_t;
	using difference_type = std::ptrdiff_t;
	using is_always_equal = std::false_type;
	using propagate_on_container_copy_assignment = std::false_type;
	using propagate_on_container_move_assignment = std::false_type;
	using propagate_on_container_swap = std::false_type;

	explicit NodeAllocator(NodeArena& arena) noexcept : arena_{std::addressof(arena)} {}

	template <typename U>
	NodeAllocator(const NodeAllocator<U>& other) noexcept : arena_{other.arena_} {}

	[[nodiscard]] T* allocate(size_t count) {
		if (count > std::numeric_limits<size_t>::max() / sizeof(T)) {
			throw std::bad_array_new_length{};
		}
		return static_cast<T*>(arena_->Allocate(count * sizeof(T), alignof(T)));
	}

	void deallocate(T*, size_t) noexcept {}

	template <typename U>
	[[nodiscard]] bool operator==(const NodeAllocator<U>& other) const noexcept {
		return arena_ == other.arena_;
	}

	template <typename U>
	[[nodiscard]] bool operator!=(const NodeAllocator<U>& other) const noexcept {
		return !(*this == other);
	}

private:
	template <typename>
	friend class NodeAllocator;

	NodeArena* arena_;
};

template <typename T>
class [[nodiscard]] ReusablePool {
public:
	explicit ReusablePool(NodeArena& arena) noexcept : arena_{arena} {}
	ReusablePool(const ReusablePool&) = delete;
	ReusablePool& operator=(const ReusablePool&) = delete;
	ReusablePool(ReusablePool&&) = delete;
	ReusablePool& operator=(ReusablePool&&) = delete;
	~ReusablePool() {
		for (Slab* slab = first_; slab != nullptr; slab = slab->next) {
			for (size_t i = 0; i < slab->initializedSlotCount; ++i) {
				std::destroy_at(slab->at(i));
			}
		}
	}

	template <typename... Args>
	T* Get(bool clear, Args&&... args) {
		ensureSlot();
		T* value = current_->at(usedSlotCount_);
		if (usedSlotCount_ == current_->initializedSlotCount) {
			std::construct_at(value, std::forward<Args>(args)...);
			++current_->initializedSlotCount;
		} else if (clear) {
			value->Clear();
		}
		++usedSlotCount_;
		return value;
	}

	void Reset() noexcept {
		current_ = first_;
		usedSlotCount_ = 0;
	}

private:
	static constexpr size_t kSlabTargetSize = 16 * 1024;
	static constexpr size_t kSlabCapacity = std::max<size_t>(1, kSlabTargetSize / sizeof(T));

	struct [[nodiscard]] Slab {
		Slab* next = nullptr;
		size_t initializedSlotCount = 0;
		alignas(T) std::byte storage[kSlabCapacity * sizeof(T)];

		[[nodiscard]] T* at(size_t index) noexcept { return std::launder(reinterpret_cast<T*>(storage + index * sizeof(T))); }
	};

	[[nodiscard]] Slab* addSlab() {
		void* memory = arena_.Allocate(sizeof(Slab), alignof(Slab));
		auto* slab = std::construct_at(static_cast<Slab*>(memory));
		if (current_) {
			current_->next = slab;
		} else {
			first_ = slab;
		}
		return slab;
	}

	void ensureSlot() {
		if (!current_) {
			current_ = addSlab();
			usedSlotCount_ = 0;
		} else if (usedSlotCount_ == kSlabCapacity) {
			current_ = current_->next ? current_->next : addSlab();
			usedSlotCount_ = 0;
		}
	}

	NodeArena& arena_;
	Slab* first_ = nullptr;
	Slab* current_ = nullptr;
	size_t usedSlotCount_ = 0;
};

}  // namespace reindexer
