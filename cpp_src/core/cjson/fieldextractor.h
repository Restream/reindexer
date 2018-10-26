#pragma once

#include "tagsmatcher.h"

namespace reindexer {
class FieldsExtractor {
public:
	FieldsExtractor(VariantArray *va = nullptr, KeyValueType expectedType = KeyValueUndefined) : values_(va), expectedType_(expectedType){};
	FieldsExtractor(const FieldsExtractor &) = delete;
	FieldsExtractor(FieldsExtractor &&other) : values_(other.values_), expectedType_(other.expectedType_) {}
	FieldsExtractor &operator=(const FieldsExtractor &) = delete;
	FieldsExtractor &operator=(FieldsExtractor &&) = delete;

	void SetTagsMatcher(const TagsMatcher *) {}

	/// Start new object
	FieldsExtractor Object(int) { return FieldsExtractor(values_, expectedType_); };
	FieldsExtractor Array(int, bool = false) { return FieldsExtractor(values_, expectedType_); };
	FieldsExtractor Object(const char *) { return FieldsExtractor(values_, expectedType_); };
	FieldsExtractor Array(const char *, bool = false) { return FieldsExtractor(values_, expectedType_); };

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
