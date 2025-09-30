#pragma once

#include "core/keyvalue/float_vectors_holder.h"
#include "core/keyvalue/key_string.h"
#include "estl/h_vector.h"
#include "payload/payloadvalue.h"

#include <memory>

namespace reindexer {

class MsgPackDecoder;

struct [[nodiscard]] ItemImplRawData {
	using HolderT = h_vector<key_string, 16>;

	ItemImplRawData() = default;
	explicit ItemImplRawData(PayloadValue v) : payloadValue_(std::move(v)) {}
	ItemImplRawData(const ItemImplRawData&) = delete;
	ItemImplRawData(ItemImplRawData&&) = default;
	ItemImplRawData& operator=(const ItemImplRawData&) = delete;
	ItemImplRawData& operator=(ItemImplRawData&&) = default;

	PayloadValue payloadValue_;
	std::unique_ptr<uint8_t[]> tupleData_;
	std::unique_ptr<char[]> sourceData_;
	std::vector<std::unique_ptr<char[]>> largeJSONStrings_;
	std::vector<std::string> precepts_;
	std::unique_ptr<HolderT> holder_;
	FloatVectorsHolderVector floatVectorsHolder_;
	std::shared_ptr<MsgPackDecoder> msgPackDecoder_;
};

}  // namespace reindexer
