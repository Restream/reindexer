#pragma once

#include <span>
#include "tagsmatcher.h"

#include "core/nsselecter/comparator/equalposition_comparator.h"

namespace reindexer {

struct [[nodiscard]] FieldsExtractorGroupingState {
	h_vector<VariantArray, 2>* values = nullptr;
	std::span<FieldPathPart> path;
	bool isPath = false;
	bool isArrayLevel = false;
	unsigned int* arrayIndex = nullptr;
	bool isTargetValue = false;
	bool isRoot = true;
};

class [[nodiscard]] FieldsExtractorGrouping {
public:
	FieldsExtractorGrouping() noexcept = default;
	FieldsExtractorGrouping(FieldsExtractorGroupingState state) noexcept : state_(state) {}

	FieldsExtractorGrouping Object(TagName tag = TagName::Empty()) {
		FieldsExtractorGroupingState stateNew = state_;
		if (tag == TagName::Empty()) {	// start object or arrayelement
			if (stateNew.isArrayLevel) {
				stateNew.isArrayLevel = false;
				(*stateNew.arrayIndex)++;
			}
		} else if (!state_.path.empty() && tag == state_.path[0].tag) {
			if (state_.path[0].flags == FieldPathPartFlags::Array) {
				stateNew.isArrayLevel = false;
				*stateNew.arrayIndex = 1;
			}
			stateNew.isPath = true;
			stateNew.path = state_.path.subspan(1);
			stateNew.isTargetValue = (state_.path.size() == 1);
		}
		return FieldsExtractorGrouping(stateNew);
	}
	FieldsExtractorGrouping Array(TagName tag) {  // array of object
		FieldsExtractorGroupingState stateNew = state_;
		if (!state_.path.empty() && tag == state_.path[0].tag) {
			stateNew.isPath = true;
			if (state_.path[0].flags == FieldPathPartFlags::Array) {
				stateNew.isArrayLevel = true;
				*stateNew.arrayIndex = 0;
			}
			stateNew.isTargetValue = (state_.path.size() == 1);
			stateNew.path = state_.path.subspan(1);
			return FieldsExtractorGrouping(stateNew);
		}
		stateNew.path = std::span<FieldPathPart>{};
		return FieldsExtractorGrouping(stateNew);
	}

	FieldsExtractorGrouping Array(std::string_view) {
		assertrx_throw(false && "not implemented");
		return *this;
	}

	template <typename T>
	void Array(TagName tag, std::span<T> data, unsigned) {
		auto getValue = [&data](size_t i) -> Variant { return Variant(data[i]); };
		rx_unused = processArray(data.size(), tag, getValue);
	}

	void Array(TagName tag, Serializer& ser, TagType tagType, int count) {
		const KeyValueType kvt{tagType};
		auto getValue = [&kvt, &ser](size_t) -> Variant { return ser.GetRawVariant(kvt); };
		if (!processArray(count, tag, getValue)) {
			for (int i = 0; i < count; ++i) {
				rx_unused = ser.GetRawVariant(kvt);
			}
		}
	}

	FieldsExtractorGrouping& Put(TagName tag, Variant arg, int) {
		if (state_.isTargetValue) {
			resizeValue(*state_.arrayIndex);
			(*state_.values)[*state_.arrayIndex - 1].emplace_back(std::move(arg));
		} else if (!state_.path.empty() && tag == state_.path[0].tag) {
			if (state_.isArrayLevel) {
				(*state_.arrayIndex)++;
			} else if (state_.path[0].flags == FieldPathPartFlags::Array) {
				(*state_.arrayIndex) = 1;
			}
			resizeValue(*state_.arrayIndex);
			(*state_.values)[*state_.arrayIndex - 1].emplace_back(std::move(arg));
		}
		return *this;
	}

	template <typename T>
	FieldsExtractorGrouping& Put(TagName tag, const T& arg, int offset) {
		return Put(tag, Variant{arg}, offset);
	}
	FieldsExtractorGrouping& Null(TagName = TagName::Empty()) noexcept { return *this; }
	void SetTagsMatcher(const TagsMatcher*) noexcept {}

private:
	FieldsExtractorGroupingState state_;
	void resizeValue(size_t value) {
		if (state_.values->size() < value) {
			state_.values->resize(value);
		}
	}

	template <typename T>
	RX_ALWAYS_INLINE void addToOneLevel(size_t count, const T& getValue) {
		for (size_t i = 0; i < count; ++i) {
			(*state_.values)[*state_.arrayIndex - 1].emplace_back(getValue(i));
		}
	}
	template <typename T>
	RX_ALWAYS_INLINE void addToAllLevel(size_t count, const T& getValue) {
		for (size_t i = 0; i < count; ++i) {
			(*state_.values)[i].emplace_back(getValue(i));
		}
	}

	template <typename T>
	bool processArray(size_t count, TagName tag, T& getValue) {
		if (state_.isTargetValue) {
			resizeValue(*state_.arrayIndex);
			addToOneLevel(count, getValue);
			return true;
		} else if (!state_.path.empty() && tag == state_.path[0].tag) {
			if (state_.path[0].flags == FieldPathPartFlags::Array) {
				resizeValue(count);
				addToAllLevel(count, getValue);
				return true;
			} else {
				resizeValue(*state_.arrayIndex);
				addToOneLevel(count, getValue);
				return true;
			}
		}
		return false;
	}
};

}  // namespace reindexer
