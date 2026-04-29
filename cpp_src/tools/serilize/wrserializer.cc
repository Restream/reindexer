#include "tools/serilize/wrserializer.h"
#include "tools/errors.h"
#include "vendor/itoa/itoa.h"

namespace reindexer {

static unsigned uint32ByteSize(uint32_t value) noexcept {
	unsigned bytes = 1;
	if (value >= 0x80) {
		++bytes;
		value >>= 7;
		if (value >= 0x80) {
			++bytes;
			value >>= 7;
			if (value >= 0x80) {
				++bytes;
				value >>= 7;
				if (value >= 0x80) {
					++bytes;
				}
			}
		}
	}
	return bytes;
}

void WrSerializer::VStringHelper::End() {
	if (ser_) {
		int size = ser_->len_ - pos_;
		if (size < 0) {
			throw Error(errParseBin, "Size of object is unexpectedly negative: {}", size);
		}
		if (size == 0) {
			ser_->grow(1);
			uint32_pack(0, ser_->buf_ + pos_);
			++ser_->len_;
		} else {
			unsigned bytesToGrow = uint32ByteSize(size);
			ser_->grow(bytesToGrow);
			ser_->len_ += bytesToGrow;
			memmove(&ser_->buf_[0] + pos_ + bytesToGrow, &ser_->buf_[0] + pos_, size);
			uint32_pack(size, ser_->buf_ + pos_);
		}
		ser_ = nullptr;
	}
}

void WrSerializer::PrintJsonString(std::string_view str, PrintJsonStringMode mode) {
	grow(str.length() * 6 + 3);
	char* d = reinterpret_cast<char*>(buf_ + len_);
	*d++ = '"';
	d = escapeString(str.begin(), str.end(), d, mode == PrintJsonStringMode::QuotedQuote ? AddQuotes_True : AddQuotes_False);
	*d++ = '"';
	len_ = d - reinterpret_cast<char*>(buf_);
	return;
}

void WrSerializer::PutStrUuid(Uuid uuid) {
	grow(Uuid::kStrFormLen + 10);
	len_ += uint32_pack(Uuid::kStrFormLen, buf_ + len_);
	uuid.PutToStr({reinterpret_cast<char*>(buf_ + len_), cap_ - len_});
	len_ += Uuid::kStrFormLen;
}

void WrSerializer::PrintJsonUuid(Uuid uuid) {
	grow(Uuid::kStrFormLen + 2);
	char* d = reinterpret_cast<char*>(buf_ + len_);
	*d++ = '"';
	uuid.PutToStr({d, cap_ - len_});
	d += Uuid::kStrFormLen;
	*d++ = '"';
	len_ = d - reinterpret_cast<char*>(buf_);
}

constexpr static int kHexDumpBytesInRow = 16;

void WrSerializer::PrintHexDump(std::string_view str) {
	grow((kHexDumpBytesInRow * 4 + 12) * (1 + (str.size() / kHexDumpBytesInRow)));

	char* d = reinterpret_cast<char*>(buf_ + len_);

	for (int row = 0; row < int(str.size()); row += kHexDumpBytesInRow) {
		d = u32toax(row, d, 8);
		*d++ = ' ';
		*d++ = ' ';
		for (int i = row; i < row + kHexDumpBytesInRow; i++) {
			if (i < int(str.size())) {
				d = u32toax(unsigned(str[i]) & 0xFF, d, 2);
			} else {
				*d++ = ' ';
				*d++ = ' ';
			}
			*d++ = ' ';
		}
		*d++ = ' ';
		for (int i = row; i < row + kHexDumpBytesInRow; i++) {
			char c = (i < int(str.size()) && unsigned(str[i]) > 0x20) ? str[i] : '.';
			*d++ = c;
		}
		*d++ = '\n';
	}
	len_ = d - reinterpret_cast<char*>(buf_);
}

MemBuf WrSerializer::DetachLStr(Shrink shrink) {
	reinterpret_cast<l_string_hdr*>(buf_)->length = len_ - sizeof(uint32_t);
	auto cap = Cap();
	if (!HasAllocatedBuffer() && shrink == Shrink_True) {
		cap = Len();
	}
	return MemBuf(DetachBuf(cap), cap);
}

[[noreturn]] void WrSerializer::GrowthPolicy::throwOverflowError(size_t requestedSize) const {
	throw Error(errLogic, "Requested WrSerializer size({}) is greater than max capacity({})", requestedSize, maxCap_);
}

}  // namespace reindexer
