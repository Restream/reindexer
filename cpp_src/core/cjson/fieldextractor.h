#pragma once

#include "core/payload/fieldsset.h"
#include "estl/span.h"
#include "tagsmatcher.h"

namespace reindexer {

class FieldsExtractor {
public:
	class FieldParams {
	public:
		int &index;
		int &length;
		int field;
	};

	FieldsExtractor() = default;
	FieldsExtractor(VariantArray *va, KeyValueType expectedType, int expectedPathDepth, FieldsSet *filter = nullptr,
					FieldParams *params = nullptr) noexcept
		: values_(va), expectedType_(expectedType), expectedPathDepth_(expectedPathDepth), filter_(filter), params_(params) {}
	FieldsExtractor(FieldsExtractor &&other) = default;
	FieldsExtractor(const FieldsExtractor &) = delete;
	FieldsExtractor &operator=(const FieldsExtractor &) = delete;
	FieldsExtractor &operator=(FieldsExtractor &&) = delete;

	void SetTagsMatcher(const TagsMatcher *) noexcept {}

	FieldsExtractor Object(int) noexcept { return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1, filter_, params_); }
	FieldsExtractor Array(int) noexcept {
		assertrx_throw(values_);
		return FieldsExtractor(&values_->MarkArray(), expectedType_, expectedPathDepth_ - 1, filter_, params_);
	}
	FieldsExtractor Object(std::string_view) noexcept {
		return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1, filter_, params_);
	}
	FieldsExtractor Object(std::nullptr_t) noexcept { return Object(std::string_view{}); }
	FieldsExtractor Array(std::string_view) noexcept {
		return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1, filter_, params_);
	}

	template <typename T>
	void Array(int, span<T> data, int offset) {
		const IndexedPathNode &pathNode = getArrayPathNode();
		const PathType ptype = pathNotToType(pathNode);
		if (ptype == PathType::Other) {
			throw Error(errLogic, "Unable to extract array value without index value");
		}
		if (params_) {
			if (ptype == PathType::WithIndex) {
				params_->index = pathNode.Index() + offset;
				params_->length = data.size();
			} else if (params_->index >= 0 && params_->length > 0) {
				params_->length += data.size();
			} else {
				params_->index = offset;
				params_->length = data.size();
			}
		}

		if (ptype == PathType::WithIndex) {
			int i = 0;
			for (const auto &d : data) {
				if (i++ == pathNode.Index()) {
					put(0, Variant(d));
				}
			}
		} else {
			for (const auto &d : data) {
				put(0, Variant(d));
			}
		}
		if (expectedPathDepth_ <= 0) {
			assertrx_throw(values_);
			values_->MarkArray();
		}
	}

	void Array(int, Serializer &ser, TagType tagType, int count) {
		const IndexedPathNode &pathNode = getArrayPathNode();
		const PathType ptype = pathNotToType(pathNode);
		if (ptype == PathType::Other) {
			throw Error(errLogic, "Unable to extract array value without index value");
		}
		if (params_) {
			if (ptype == PathType::WithIndex) {
				params_->index = pathNode.Index();
				params_->length = count;
			} else if (params_->index >= 0 && params_->length > 0) {
				params_->length += count;
			} else {
				params_->index = 0;
				params_->length = count;
			}
		}
		if (ptype == PathType::WithIndex) {
			for (int i = 0; i < count; ++i) {
				auto value = ser.GetRawVariant(KeyValueType(tagType));
				if (i == pathNode.Index()) {
					put(0, std::move(value));
				}
			}
		} else {
			for (int i = 0; i < count; ++i) {
				put(0, ser.GetRawVariant(KeyValueType(tagType)));
			}
		}
		if (expectedPathDepth_ <= 0) {
			assertrx_throw(values_);
			values_->MarkArray();
		}
	}

	FieldsExtractor &Put(int t, Variant arg, int offset) {
		if (expectedPathDepth_ > 0) return *this;
		if (params_) {
			if (params_->index >= 0 && params_->length > 0 && offset == params_->index + params_->length) {
				// Concatenate fields from objects, nested in arrays
				params_->length += 1;
			} else {
				params_->index = offset;
				params_->length = 1;
			}
		}
		return put(t, std::move(arg));
	}

	template <typename T>
	FieldsExtractor &Put(int tag, const T &arg, int offset) {
		return Put(tag, Variant{arg}, offset);
	}

	FieldsExtractor &Null(int) noexcept { return *this; }
	int TargetField() { return params_ ? params_->field : IndexValueType::NotSet; }
	bool IsHavingOffset() const noexcept { return params_ && (params_->length >= 0 || params_->index >= 0); }
	void OnScopeEnd(int offset) noexcept {
		if (expectedPathDepth_ <= 0) {
			assertrx(params_ && !IsHavingOffset());
			params_->index = offset;
			params_->length = 0;
		}
	}

private:
	enum class PathType { AllItems, WithIndex, Other };
	PathType pathNotToType(const IndexedPathNode &pathNode) noexcept {
		return pathNode.IsForAllItems()						  ? PathType::AllItems
			   : (pathNode.Index() == IndexValueType::NotSet) ? PathType::Other
															  : PathType::WithIndex;
	}
	FieldsExtractor &put(int, Variant arg) {
		if (expectedPathDepth_ > 0) return *this;
		expectedType_.EvaluateOneOf(
			[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::String,
					  KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Uuid>) { arg.convert(expectedType_); },
			[](OneOf<KeyValueType::Undefined, KeyValueType::Composite>) noexcept {});
		assertrx_throw(values_);
		values_->emplace_back(std::move(arg));
		if (expectedPathDepth_ < 0) values_->MarkObject();
		return *this;
	}

	const IndexedPathNode &getArrayPathNode() const {
		if (filter_ && filter_->getTagsPathsLength() > 0) {
			size_t lastItemIndex = filter_->getTagsPathsLength() - 1;
			if (filter_->isTagsPathIndexed(lastItemIndex)) {
				const IndexedTagsPath &path = filter_->getIndexedTagsPath(lastItemIndex);
				assertrx(path.size() > 0);
				if (path.back().IsArrayNode()) return path.back();
			}
		}
		static const IndexedPathNode commonNode{IndexedPathNode::AllItems};
		return commonNode;
	}

	VariantArray *values_ = nullptr;
	KeyValueType expectedType_{KeyValueType::Undefined{}};
	int expectedPathDepth_ = 0;
	FieldsSet *filter_;
	FieldParams *params_;
};

}  // namespace reindexer
