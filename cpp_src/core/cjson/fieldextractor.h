#pragma once

#include "tagsmatcher.h"

namespace reindexer {
class FieldsExtractor {
public:
	FieldsExtractor(VariantArray *va = nullptr, KeyValueType expectedType = KeyValueUndefined) : values_(va), expectedType_(expectedType){}
	FieldsExtractor(const FieldsExtractor &) = delete;
	FieldsExtractor(FieldsExtractor &&other) : values_(other.values_), expectedType_(other.expectedType_) {}
	FieldsExtractor &operator=(const FieldsExtractor &) = delete;
	FieldsExtractor &operator=(FieldsExtractor &&) = delete;

	void SetTagsMatcher(const TagsMatcher *) {}

	/// Start new object
	FieldsExtractor Object(int) { return FieldsExtractor(values_, expectedType_); }
	FieldsExtractor Array(int) { return FieldsExtractor(values_, expectedType_); }
	FieldsExtractor Object(const char *) { return FieldsExtractor(values_, expectedType_); }
	FieldsExtractor Array(const char *) { return FieldsExtractor(values_, expectedType_); }

	template <typename T>
	void Array(int /*tagName*/, span<T> data) {
		for (auto d : data) Put(0, Variant(d));
	}
	void Array(int /*tagName*/, Serializer &ser, int tagType, int count) {
		while (count--) Put(0, ser.GetRawVariant(KeyValueType(tagType)));
	}

	FieldsExtractor &Put(int, Variant arg) {
		if (expectedType_ != KeyValueUndefined && expectedType_ != KeyValueComposite) arg.convert(expectedType_);
		values_->push_back(arg);
		return *this;
	}

	FieldsExtractor &Null(int) { return *this; }

protected:
	VariantArray *values_ = nullptr;
	KeyValueType expectedType_;
};  // namespace reindexer

}  // namespace reindexer
