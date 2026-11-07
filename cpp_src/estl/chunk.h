#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace reindexer {

class [[nodiscard]] chunk {
public:
	chunk() noexcept : data_(nullptr), len_(0), offset_(0), cap_(0) {}
	chunk(uint8_t* data, size_t len, size_t cap, size_t offset = 0) noexcept : data_(data), len_(len), offset_(offset), cap_(cap) {}
	explicit chunk(size_t cap) : chunk(new uint8_t[cap], 0, cap, 0) {}
	~chunk() { delete[] data_; }
	chunk(const chunk&) = delete;
	chunk& operator=(const chunk&) = delete;
	chunk(chunk&& other) noexcept {
		data_ = other.data_;
		len_ = other.len_;
		offset_ = other.offset_;
		cap_ = other.cap_;
		other.data_ = nullptr;
		other.len_ = 0;
		other.cap_ = 0;
		other.offset_ = 0;
	}
	chunk& operator=(chunk&& other) noexcept {
		if (this != &other) {
			delete[] data_;
			data_ = other.data_;
			len_ = other.len_;
			offset_ = other.offset_;
			cap_ = other.cap_;
			other.data_ = nullptr;
			other.len_ = 0;
			other.cap_ = 0;
			other.offset_ = 0;
		}
		return *this;
	}
	void append(std::string_view data) { append_impl(data, std::max(size_t(0x1000), size_t(len_ + data.size()))); }
	void append_strict(std::string_view data) { append_impl(data, len_ + data.size()); }

	size_t size() const noexcept { return len_ - offset_; }
	uint8_t* data() const noexcept { return data_ + offset_; }
	size_t capacity() const noexcept { return cap_; }
	size_t len() const noexcept { return len_; }
	size_t offset() const noexcept { return offset_; }

	void clear() noexcept {
		len_ = 0;
		offset_ = 0;
	}
	void shift(size_t offset) noexcept { offset_ += offset; }
	uint8_t* release() noexcept {
		auto res = data_;
		data_ = nullptr;
		return res;
	}

	void shrink(size_t k) {
		if (k * size() >= cap_) {
			return;
		}

		cap_ = k * size();
		uint8_t* newdata = new uint8_t[cap_];
		if (data_) {
			memcpy(newdata, data(), size());
			len_ = size();
			offset_ = 0;
		}
		delete[] data_;
		data_ = newdata;
	}
	explicit operator std::string_view() const noexcept { return std::string_view(reinterpret_cast<char*>(data()), size()); }

private:
	void append_impl(std::string_view data, size_t newCapacity) {
		if (!data_ || len_ + data.size() > cap_) {
			cap_ = newCapacity;
			uint8_t* newdata = new uint8_t[cap_];
			if (data_) {
				memcpy(newdata, data_, len_);
			}
			delete[] data_;
			data_ = newdata;
		}
		memcpy(data_ + len_, data.data(), data.size());
		len_ += data.size();
	}

	uint8_t* data_;
	size_t len_;
	size_t offset_;
	size_t cap_;
};

}  // namespace reindexer
