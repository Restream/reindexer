#include <bitset>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace reindexer {

template <size_t BlockSize = 64>
class [[nodiscard]] DynamicBitset {
public:
	DynamicBitset() noexcept : size_(0) {}

	explicit DynamicBitset(size_t size, bool value = false) : size_(size) {
		size_t blocks = (size + BlockSize - 1) / BlockSize;
		data_.resize(blocks, value ? BlockType().set() : BlockType());
	}

	void set(size_t pos, bool value = true) noexcept {
		size_t block_index = pos / BlockSize;
		size_t bit_index = pos % BlockSize;
		data_[block_index].set(bit_index, value);
	}

	// setting all bits to true
	void set() noexcept {
		for (auto& block : data_) {
			block.set();
		}
	}

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
		size_t newBlocks = (newSize + BlockSize - 1) / BlockSize;
		if (newBlocks < data_.size()) {
			data_.resize(newBlocks);
		} else if (newBlocks > data_.size()) {
			data_.resize(newBlocks, value ? BlockType().set() : BlockType());
		}

		size_t oldSize = size_;
		size_t oldNumBlocks = data_.size();
		size_ = newSize;
		// filling only last block
		for (size_t i = oldSize; i < newSize && i < oldNumBlocks * BlockSize; ++i) {
			set(i, value);
		}
	}

	DynamicBitset& operator&=(const DynamicBitset& other) {
		if (size_ != other.size_) {
			throw std::invalid_argument("Bitsets must be of the same size");
		}

		for (size_t i = 0; i < data_.size(); ++i) {
			data_[i] &= other.data_[i];
		}

		return *this;
	}

	DynamicBitset& operator|=(const DynamicBitset& other) {
		if (size_ != other.size_) {
			throw std::invalid_argument("Bitsets must be of the same size");
		}

		for (size_t i = 0; i < data_.size(); ++i) {
			data_[i] |= other.data_[i];
		}

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

private:
	using BlockType = std::bitset<BlockSize>;
	std::vector<BlockType> data_;
	size_t size_;
};

template <size_t BlockSize>
std::ostream& operator<<(std::ostream& os, const DynamicBitset<BlockSize>& bitset) {
	os << bitset.to_string();
	return os;
}

}  // namespace reindexer