#pragma once

#include <algorithm>
#include <bitset>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include "tools/assertrx.h"

namespace reindexer {

template <size_t BlockSize = 64>
class [[nodiscard]] DynamicBitset {
public:
	DynamicBitset() noexcept : size_(0) {}

	explicit DynamicBitset(size_t size, bool value = false) : size_(size) {
		const size_t blocks = blocksCount(size);
		data_.resize(blocks);
		if (value) {
			fillAllTrueRespectSize();
		}
	}

	void set(size_t pos, bool value = true) noexcept {
		size_t block_index = pos / BlockSize;
		size_t bit_index = pos % BlockSize;
		data_[block_index].set(bit_index, value);
	}

	// setting all bits to true
	void set() noexcept { fillAllTrueRespectSize(); }

	// setting bit to false
	void reset(size_t pos) noexcept {
		size_t block_index = pos / BlockSize;
		size_t bit_index = pos % BlockSize;
		data_[block_index].reset(bit_index);
	}

	// setting all bits to false
	void reset() noexcept {
		for (auto& block : data_) {
			block.reset();
		}
	}

	bool get(size_t pos) const noexcept {
		size_t block_index = pos / BlockSize;
		size_t bit_index = pos % BlockSize;
		return data_[block_index][bit_index];
	}

	bool operator[](size_t pos) const noexcept { return get(pos); }

	void resize(size_t newSize, bool value = false) {
		const size_t oldSize = size_;
		const size_t oldBlocks = blocksCount(oldSize);
		const size_t newBlocks = blocksCount(newSize);
		if (newBlocks < data_.size()) {
			data_.resize(newBlocks);
		} else if (newBlocks > data_.size()) {
			data_.resize(newBlocks, value ? fullSetBlock() : BlockType{});
		}

		size_ = newSize;

		if (newSize > oldSize) {
			// Newly added blocks are already initialized by resize(newBlocks, fullSetBlock()).
			// We only need to fill the tail in the old last block.
			if (oldBlocks == newBlocks) {
				const size_t newEndBit = (newSize % BlockSize == 0) ? BlockSize : (newSize % BlockSize);
				setBitsInBlock(oldBlocks - 1, oldSize % BlockSize, newEndBit, value);
			} else {
				const size_t oldTailBit = oldSize % BlockSize;
				if (oldTailBit != 0) {
					setBitsInBlock(oldBlocks - 1, oldTailBit, BlockSize, value);
				}
			}
		}

		trimUnusedBits();
	}

	DynamicBitset& operator&=(const DynamicBitset& other) {
		if (size_ != other.size_) {
			throw std::invalid_argument("Bitsets must be of the same size");
		}

		for (size_t i = 0; i < data_.size(); ++i) {
			data_[i] &= other.data_[i];
		}
		trimUnusedBits();

		return *this;
	}

	DynamicBitset& operator|=(const DynamicBitset& other) {
		if (size_ != other.size_) {
			throw std::invalid_argument("Bitsets must be of the same size");
		}

		for (size_t i = 0; i < data_.size(); ++i) {
			data_[i] |= other.data_[i];
		}
		trimUnusedBits();

		return *this;
	}

	DynamicBitset& Exclude(const DynamicBitset& other) {
		if (size_ != other.size_) {
			throw std::invalid_argument("Bitsets must be of the same size");
		}

		for (size_t i = 0; i < data_.size(); ++i) {
			data_[i] &= (~other.data_[i]);
		}
		trimUnusedBits();

		return *this;
	}

	DynamicBitset& Invert() {
		for (size_t i = 0; i < data_.size(); ++i) {
			data_[i] = ~data_[i];
		}
		trimUnusedBits();
		return *this;
	}

	size_t size() const noexcept { return size_; }

	void clear() noexcept {
		data_.clear();
		size_ = 0;
	}

	bool operator==(const DynamicBitset& other) const noexcept = default;

	std::string to_string() const {
		std::string result;
		result.reserve(size_);
		for (size_t i = 0; i < size_; ++i) {
			result += get(i) ? '1' : '0';
		}
		return result;
	}

	void swap(DynamicBitset& bs) noexcept {
		if (this != &bs) {
			data_.swap(bs.data_);
			std::swap(size_, bs.size_);
		}
	}

	// Fast path for repeated patterns like: resize(0); resize(N, false)
	void ResizeAndReset(size_t newSize) {
		const size_t newBlocks = blocksCount(newSize);
		data_.resize(newBlocks);
		for (auto& block : data_) {
			block.reset();
		}
		size_ = newSize;
	}
	// Fast path for repeated patterns like: resize(0); resize(N, true)
	void ResizeAndSet(size_t newSize) {
		const size_t newBlocks = blocksCount(newSize);
		data_.resize(newBlocks);
		size_ = newSize;
		fillAllTrueRespectSize();
	}

	void AccumulateAnd(DynamicBitset& bs) {
		if (this != &bs) {
			if (this->size()) {
				*this &= bs;
			} else {
				swap(bs);
			}
		}
	}

	void AccumulateOr(DynamicBitset& bs) {
		if (this != &bs) {
			if (this->size()) {
				*this |= bs;
			} else {
				swap(bs);
			}
		}
	}

	size_t PopCount() const noexcept {
		size_t res = 0;
		for (const auto& b : data_) {
			res += b.count();
		}

		return res;
	}

private:
	using BlockType = std::bitset<BlockSize>;

	static constexpr size_t blocksCount(size_t size) noexcept { return (size + BlockSize - 1) / BlockSize; }
	static const BlockType& fullSetBlock() noexcept {
		thread_local const BlockType block = [] {
			BlockType b;
			b.set();
			return b;
		}();
		return block;
	}
	static BlockType makeRangeMask(size_t beginBit, size_t endBitExclusive) noexcept {
		BlockType mask;
		assertrx_dbg(beginBit < endBitExclusive);
		const size_t width = endBitExclusive - beginBit;
		mask = fullSetBlock();
		mask >>= (BlockSize - width);
		mask <<= beginBit;
		return mask;
	}

	void fillRange(size_t begin, size_t end, bool value) noexcept {
		if (begin >= end) {
			return;
		}
		const size_t firstBlock = begin / BlockSize;
		const size_t lastBlock = (end - 1) / BlockSize;
		const size_t firstBit = begin % BlockSize;
		const size_t lastBitExclusive = ((end - 1) % BlockSize) + 1;

		if (firstBlock == lastBlock) {
			setBitsInBlock(firstBlock, firstBit, lastBitExclusive, value);
			return;
		}

		setBitsInBlock(firstBlock, firstBit, BlockSize, value);
		const BlockType fillValue = value ? fullSetBlock() : BlockType{};
		std::fill(data_.begin() + firstBlock + 1, data_.begin() + lastBlock, fillValue);
		setBitsInBlock(lastBlock, 0, lastBitExclusive, value);
	}

	void setBitsInBlock(size_t blockIdx, size_t beginBit, size_t endBitExclusive, bool value) noexcept {
		if (beginBit >= endBitExclusive) [[unlikely]] {
			return;
		}
		if (beginBit == 0 && endBitExclusive == BlockSize) {
			if (value) {
				data_[blockIdx].set();
			} else {
				data_[blockIdx].reset();
			}
			return;
		}
		const BlockType mask = makeRangeMask(beginBit, endBitExclusive);
		if (value) {
			data_[blockIdx] |= mask;
		} else {
			data_[blockIdx] &= ~mask;
		}
	}

	void trimUnusedBits() noexcept {
		const size_t usedBitsInLastBlock = size_ % BlockSize;
		if (usedBitsInLastBlock == 0) {
			return;
		}
		BlockType mask = fullSetBlock();
		mask >>= (BlockSize - usedBitsInLastBlock);
		data_.back() &= mask;
	}

	void fillAllTrueRespectSize() noexcept {
		const size_t usedBitsInLastBlock = size_ % BlockSize;
		if (usedBitsInLastBlock == 0) {
			for (auto& block : data_) {
				block.set();
			}
			return;
		}
		for (size_t i = 0; i + 1 < data_.size(); ++i) {
			data_[i].set();
		}
		data_.back() = makeRangeMask(0, usedBitsInLastBlock);
	}

	std::vector<BlockType> data_;
	size_t size_;
};

template <size_t BlockSize>
std::ostream& operator<<(std::ostream& os, const DynamicBitset<BlockSize>& bitset) {
	os << bitset.to_string();
	return os;
}

}  // namespace reindexer