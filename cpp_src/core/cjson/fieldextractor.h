#pragma once

#include "tagsmatcher.h"

namespace reindexer {
class FieldsExtractor {
public:
	FieldsExtractor() = default;
	FieldsExtractor(VariantArray *va, KeyValueType expectedType, int expectedPathDepth)
		: values_(va), expectedType_(expectedType), expectedPathDepth_(expectedPathDepth) {}
	FieldsExtractor(FieldsExtractor &&other) = default;
	FieldsExtractor(const FieldsExtractor &) = delete;
	FieldsExtractor &operator=(const FieldsExtractor &) = delete;
	FieldsExtractor &operator=(FieldsExtractor &&) = delete;

	void SetTagsMatcher(const TagsMatcher *) {}

	/// Start new object
	FieldsExtractor Object(int) { return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1); }
	FieldsExtractor Array(int) { return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1); }
	FieldsExtractor Object(string_view) { return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1); }
	FieldsExtractor Array(string_view) { return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1); }

	template <typename T>
	void Array(int /*tagName*/, span<T> data) {
		for (auto d : data) Put(0, Variant(d));
	}
	void Array(int /*tagName*/, Serializer &ser, int tagType, int count) {
		while (count--) Put(0, ser.GetRawVariant(KeyValueType(tagType)));
	}

	FieldsExtractor &Put(int, Variant arg) {
		if (expectedPathDepth_ > 0) return *this;
		if (expectedType_ != KeyValueUndefined && expectedType_ != KeyValueComposite) arg.convert(expectedType_);
		values_->push_back(arg);
		if (expectedPathDepth_ < 0) values_->MarkObject();
		return *this;
	}

	FieldsExtractor &Null(int) { return *this; }

protected:
	VariantArray *values_ = nullptr;
	KeyValueType expectedType_;
	int expectedPathDepth_ = 0;
};

}  // namespace reindexer
