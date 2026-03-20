#pragma once

#include <span>
#include "core/keyvalue/variant.h"
#include "core/tag_name_index.h"
#include "tagsmatcher.h"

#include "core/nsselecter/comparator/equalposition_comparator.h"
#include "tools/assertrx.h"

namespace reindexer {

struct [[nodiscard]] FieldsExtractorGroupingState {
	FieldsExtractorGroupingState(h_vector<VariantArray, 2>& v, unsigned int& index, std::span<FieldPathPart> p)
		: values(v), path(p), valuesArrayIndex(index) {}

	friend class FieldsExtractorGrouping;

private:
	h_vector<VariantArray, 2>& values;
	std::span<FieldPathPart> path;
	bool isPath = true;				 // true if the current path in the JSON tree matches the beginning of the target path
	unsigned int& valuesArrayIndex;	 // index of the target array where the values should be added
	bool isTargetValue = false;		 // true if the path in the JSON tree matches the target path
};

class [[nodiscard]] FieldsExtractorGrouping {
public:
	FieldsExtractorGrouping(FieldsExtractorGroupingState state) noexcept : state_(std::move(state)) {}

	FieldsExtractorGrouping Object(TagIndex ii) {
		auto checkPath = [](const FieldPathPart& pathPart) {
			return pathPart.type == PathPartType::AnyValue || pathPart.type == PathPartType::ArrayTarget;
		};
		return processPath(checkPath, ii.AsNumber());
	}
	FieldsExtractorGrouping Object(TagName tag = TagName::Empty()) {
		auto checkPath = [tag](const FieldPathPart& pathPart) { return pathPart.type == PathPartType::Name && pathPart.tag == tag; };
		if (tag == TagName::Empty()) {	// root object
			FieldsExtractorGroupingState stateNew = state_;
			return FieldsExtractorGrouping(stateNew);
		}

		return processPath(checkPath, 0);
	}
	FieldsExtractorGrouping Array(TagName tag) {
		auto checkPath = [tag](const FieldPathPart& pathPart) { return (pathPart.type == PathPartType::Name && pathPart.tag == tag); };
		return processPath(checkPath, 0);
	}

	FieldsExtractorGrouping Array(TagIndex ii) {
		auto checkPath = [](const FieldPathPart& pathPart) {
			return pathPart.type == PathPartType::AnyValue || pathPart.type == PathPartType::ArrayTarget;
		};
		return processPath(checkPath, ii.AsNumber());
	}
	FieldsExtractorGrouping Array(std::string_view) {
		assertrx_throw(false && "not implemented");
		return *this;
	}

	template <typename T>
	void Array(concepts::TagNameOrIndex auto tag, std::span<T> data, unsigned) {
		auto getValue = [&data](size_t i) -> Variant { return Variant(data[i]); };
		std::ignore = processArray(data.size(), tag, getValue);
	}

	void Array(concepts::TagNameOrIndex auto tag, Serializer& ser, TagType tagType, int count) {
		const KeyValueType kvt{tagType};
		auto getValue = [&kvt, &ser](size_t) -> Variant { return ser.GetRawVariant(kvt); };
		if (!processArray(count, tag, getValue)) {
			for (int i = 0; i < count; ++i) {
				std::ignore = ser.GetRawVariant(kvt);
			}
		}
	}

	void Put(TagName tag, Variant&& arg, [[maybe_unused]] int i) {
		if (state_.isTargetValue) {
			resizeValue(state_.valuesArrayIndex);
			(state_.values)[state_.valuesArrayIndex - 1].emplace_back(std::move(arg));
			return;
		}
		if (!state_.isPath) {
			return;
		}
		if (!state_.path.empty() && state_.path[0].type == PathPartType::Name && state_.path[0].tag == tag) {
			auto pathLocal = state_.path.subspan(1);
			if (pathLocal.size() > 1) {
				return;
			}
			if (!pathLocal.empty()) {
				return;
			}
			resizeValue(state_.valuesArrayIndex);
			(state_.values)[state_.valuesArrayIndex - 1].emplace_back(std::move(arg));
			return;
		}
	}

	template <typename T>
	void Put(TagName tag, const T& arg, int offset) {
		return Put(tag, Variant{arg}, offset);
	}

	void Put(TagIndex ii, Variant&& val, int) {
		if (state_.isTargetValue) {
			resizeValue(state_.valuesArrayIndex);
			(state_.values)[state_.valuesArrayIndex - 1].emplace_back(std::move(val));
			return;
		}
		if (!state_.isPath) {
			return;
		}
		if (state_.path.size() == 1) {
			switch (state_.path[0].type) {
				case PathPartType::ArrayTarget: {
					auto index = ii.AsNumber() + 1;
					resizeValue(index);
					(state_.values)[index - 1].emplace_back(std::move(val));
					break;
				}
				case PathPartType::AnyValue: {
					resizeValue(state_.valuesArrayIndex);
					(state_.values)[state_.valuesArrayIndex - 1].emplace_back(std::move(val));
					break;
				}
				case PathPartType::Name:
				default:
					break;
			}
		}
	}
	void Null(TagName = TagName::Empty()) noexcept {}
	void Null(TagIndex) noexcept {}
	void SetTagsMatcher(const TagsMatcher*) noexcept {}

private:
	FieldsExtractorGroupingState state_;
	void resizeValue(size_t value) {
		if (state_.values.size() < value) {
			state_.values.resize(value);
		}
	}

	template <typename T>
	RX_ALWAYS_INLINE void addToOneLevel(size_t count, const T& getValue) {
		for (size_t i = 0; i < count; ++i) {
			(state_.values)[state_.valuesArrayIndex - 1].emplace_back(getValue(i));
		}
	}
	template <typename T>
	RX_ALWAYS_INLINE void addToAllLevel(size_t count, const T& getValue) {
		for (size_t i = 0; i < count; ++i) {
			(state_.values)[i].emplace_back(getValue(i));
		}
	}

	template <typename T>
	bool processSubArray(size_t count, T& getValue) {
		auto pathLocal = state_.path.subspan(1);
		if (pathLocal.size() != 1) {
			return false;
		}
		switch (pathLocal[0].type) {
			case PathPartType::ArrayTarget: {
				resizeValue(count);
				addToAllLevel(count, getValue);
				return true;
			}
			case PathPartType::AnyValue: {
				resizeValue(state_.valuesArrayIndex);
				addToOneLevel(count, getValue);
				return true;
			}
			case PathPartType::Name:
				return false;
		}
		return false;
	}

	template <typename T>
	bool processArray(size_t count, TagName tag, T& getValue) {
		if (state_.isTargetValue) {
			resizeValue(state_.valuesArrayIndex);
			addToOneLevel(count, getValue);
			return true;
		}
		if (!state_.isPath) {
			return false;
		}
		if (!state_.path.empty() && state_.path[0].type == PathPartType::Name && tag == state_.path[0].tag) {
			if (state_.path.size() == 1) {
				resizeValue(state_.valuesArrayIndex);
				addToOneLevel(count, getValue);
				return true;
			}

			return processSubArray(count, getValue);
		}
		return false;
	}

	template <typename T>
	bool processArray(size_t count, TagIndex tagIndx, T& getValue) {  // subarray
		if (state_.isTargetValue) {
			resizeValue(state_.valuesArrayIndex);
			addToOneLevel(count, getValue);
			return true;
		}
		if (!state_.isPath) {
			return false;
		}
		if (!state_.path.empty()) {
			switch (state_.path[0].type) {
				case PathPartType::ArrayTarget: {
					state_.valuesArrayIndex = tagIndx.AsNumber() + 1;
					if (state_.path.size() == 1) {
						resizeValue(state_.valuesArrayIndex);
						addToOneLevel(count, getValue);
						return true;
					}
					break;
				}
				case PathPartType::AnyValue: {
					if (state_.path.size() == 1) {
						resizeValue(state_.valuesArrayIndex);
						addToOneLevel(count, getValue);
						return true;
					}
					break;
				}
				case reindexer::PathPartType::Name:
					return false;
			}
			return processSubArray(count, getValue);
		}
		return false;
	}
	FieldsExtractorGrouping processPath(const auto& checkPath, unsigned arrayIndex) {
		FieldsExtractorGroupingState stateNew = state_;
		stateNew.isPath = false;
		if (stateNew.isTargetValue) {
			return FieldsExtractorGrouping(stateNew);
		}
		if (!state_.isPath) {
			stateNew.path = std::span<FieldPathPart>{};
			return FieldsExtractorGrouping(stateNew);
		}

		if (!stateNew.path.empty() && checkPath(stateNew.path[0])) {
			if (stateNew.path[0].type == PathPartType::ArrayTarget) {
				stateNew.valuesArrayIndex = arrayIndex + 1;
				if (stateNew.values.size() < stateNew.valuesArrayIndex) {
					stateNew.values.resize(stateNew.valuesArrayIndex);
				}
			}
			stateNew.isPath = true;
			stateNew.path = stateNew.path.subspan(1);
			stateNew.isTargetValue = stateNew.path.empty();

		} else {
			stateNew.path = std::span<FieldPathPart>{};
			return FieldsExtractorGrouping(stateNew);
		}
		return FieldsExtractorGrouping(stateNew);
	}
};
}  // namespace reindexer
