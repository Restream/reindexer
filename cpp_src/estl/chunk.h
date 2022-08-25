#pragma once

#include <cstring>
#include <string_view>

namespace reindexer {

class chunk {
public:
	chunk() : data_(nullptr), len_(0), offset_(0), cap_(0) {}
	~chunk() { delete[] data_; }
	chunk(const chunk &) = delete;
	chunk &operator=(const chunk &) = delete;
	chunk(chunk &&other) noexcept {
		data_ = other.data_;
		len_ = other.len_;
		offset_ = other.offset_;
		cap_ = other.cap_;
		other.data_ = nullptr;
		other.len_ = 0;
		other.cap_ = 0;
		other.offset_ = 0;
	}
	chunk &operator=(chunk &&other) noexcept {
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
	void append(std::string_view data) {
		if (!data_ || len_ + data.size() > cap_) {
			cap_ = std::max(size_t(0x1000), size_t(len_ + data.size()));
			uint8_t *newdata = new uint8_t[cap_];
			if (data_) {
				memcpy(newdata, data_, len_);
			}
			delete data_;
			data_ = newdata;
		}
		memcpy(data_ + len_, data.data(), data.size());
		len_ += data.size();
	}
	size_t size() { return len_ - offset_; }
	uint8_t *data() { return data_ + offset_; }

	uint8_t *data_;
	size_t len_;
	size_t offset_;
	size_t cap_;
};

}  // namespace reindexer
