#pragma once

#include "core/keyvalue/key_string.h"
#include "payload/payloadvalue.h"

#include <deque>
#include <memory>

namespace reindexer {

struct ItemImplRawData {
	ItemImplRawData() = default;
	ItemImplRawData(PayloadValue v) : payloadValue_(std::move(v)) {}
	ItemImplRawData(const ItemImplRawData &) = delete;
	ItemImplRawData(ItemImplRawData &&) = default;
	ItemImplRawData &operator=(const ItemImplRawData &) = delete;
	ItemImplRawData &operator=(ItemImplRawData &&) = default;

	PayloadValue payloadValue_;
	std::unique_ptr<uint8_t[]> tupleData_;
	std::unique_ptr<char[]> sourceData_;
	std::vector<std::unique_ptr<char[]>> largeJSONStrings_;
	std::vector<std::string> precepts_;
	std::unique_ptr<std::deque<std::string>> holder_;
	std::unique_ptr<std::vector<key_string>> keyStringsHolder_;
};

}  // namespace reindexer
