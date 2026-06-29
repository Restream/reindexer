#pragma once

#include "core/cjson/fields_explorer/pathfilter.h"
#include "fields_explorer.h"
#include "tools/serilize/serializer.h"

namespace reindexer::cjson {

class [[nodiscard]] FieldArrayAnalizerStrategy {
public:
	class [[nodiscard]] FieldParams {
	public:
		int& index;
		int& length;
		int field;
	};

protected:
	FieldArrayAnalizerStrategy(FieldParams& params) noexcept : params_(params) {}
	FieldArrayAnalizerStrategy(FieldArrayAnalizerStrategy& other) noexcept : params_(other.params_) {}

	void ExactMatchObject() const noexcept {}
	void StartArray() const noexcept {}

	void Array(const PathFilter&, TagIndex tagIndex, size_t count, unsigned offset,
			   TreatAsSingleElement treatAsSingleElement = TreatAsSingleElement_False) noexcept {
		updateParams(tagIndex, treatAsSingleElement ? 1 : count, offset);
	}

	template <typename T>
	void Array(const PathFilter& filter, TagIndex tagIndex, std::span<const T> data, unsigned offset,
			   TreatAsSingleElement treatAsSingleElement = TreatAsSingleElement_False) noexcept {
		Array(filter, tagIndex, data.size(), offset, treatAsSingleElement);
	}

	void Array(const PathFilter&, TagIndex tagIndex, Serializer& ser, TagType tagType, int count) {
		updateParams(tagIndex, count, 0);
		const KeyValueType kvt{tagType};
		for (int i = 0; i < count; ++i) {
			std::ignore = ser.GetRawVariant(kvt);
		}
	}

	void Put(const PathFilter& filter, const Variant& /*arg*/, int offset) noexcept {
		if (filter.Match()) {
			if (params_.index >= 0 && params_.length > 0 && offset == params_.index + params_.length) {
				// Concatenate fields from objects, nested in arrays
				params_.length += 1;
			} else {
				params_.index = offset;
				params_.length = 1;
			}
		}
	}

public:
	int TargetField() const noexcept { return params_.field; }
	bool IsHavingOffset() const noexcept { return params_.length >= 0 || params_.index >= 0; }
	void OnScopeEnd(int offset) noexcept {
		assertrx(!IsHavingOffset());
		params_.index = offset;
		params_.length = 0;
	}

private:
	void updateParams(TagIndex tagIndex, size_t count, int offset) noexcept {
		if (tagIndex.IsAll()) {
			if (params_.index >= 0 && params_.length > 0) {
				params_.length += count;
			} else {
				params_.index = offset;
				params_.length = count;
			}
		} else if (tagIndex.AsNumber() < count) {
			params_.index = tagIndex.AsNumber() + offset;
			params_.length = 1;
		}
	}

	FieldParams& params_;
};

using FieldArrayAnalizer = FieldsExplorer<FieldArrayAnalizerStrategy>;

}  // namespace reindexer::cjson
