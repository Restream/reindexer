#pragma once

#include "fields_explorer.h"
#include "tools/serilize/serializer.h"

namespace reindexer::cjson {

class [[nodiscard]] FieldSizeEvaluatorStrategy {
protected:
	FieldSizeEvaluatorStrategy(size_t& fieldSize) noexcept : fieldSize_(fieldSize) {}
	FieldSizeEvaluatorStrategy(FieldSizeEvaluatorStrategy& other) noexcept : fieldSize_(other.fieldSize_) {}
	void ExactMatchObject() noexcept { ++fieldSize_; }
	void StartArray() const noexcept {}

	template <typename T>
	void Array(const PathFilter& filter, TagIndex, std::span<const T> data, unsigned /*offset*/,
			   TreatAsSingleElement = TreatAsSingleElement_False) noexcept {
		if (filter.ExactMatch()) {
			fieldSize_ += data.size();
		}
	}
	void Array(const PathFilter& filter, TagIndex, Serializer& ser, TagType tagType, int count) {
		const KeyValueType kvt{tagType};
		for (int i = 0; i < count; ++i) {
			std::ignore = ser.GetRawVariant(kvt);
		}
		if (filter.ExactMatch()) {
			fieldSize_ += count;
		}
	}

	void Put(const PathFilter& filter, const Variant& v, int /*offset*/) noexcept {
		if (filter.ExactMatch()) {
			if (!v.Type().Is<KeyValueType::Null>()) {
				fieldSize_ += 1;
			}
		}
	}

private:
	size_t& fieldSize_;
};

using FieldSizeEvaluator = FieldsExplorer<FieldSizeEvaluatorStrategy>;

}  // namespace reindexer::cjson
