#pragma once

#include <span>
#include "tagsmatcher.h"

namespace reindexer {

class FieldsFilter;

class FieldsExtractor {
public:
	class FieldParams {
	public:
		int& index;
		int& length;
		int field;
	};

	FieldsExtractor() = default;
	FieldsExtractor(VariantArray* va, KeyValueType expectedType, int expectedPathDepth, const FieldsFilter* filter,
					FieldParams* params = nullptr) noexcept
		: values_(va), expectedType_(expectedType), expectedPathDepth_(expectedPathDepth), filter_(filter), params_(params) {}
	FieldsExtractor(FieldsExtractor&& other) = default;
	FieldsExtractor(const FieldsExtractor&) = delete;
	FieldsExtractor& operator=(const FieldsExtractor&) = delete;
	FieldsExtractor& operator=(FieldsExtractor&&) = delete;

	void SetTagsMatcher(const TagsMatcher*) noexcept {}

	FieldsExtractor Object(TagName) noexcept {
		if rx_unlikely (expectedPathDepth_ == 0) {
			values_->MarkObject();
		}
		return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1, filter_, params_);
	}
	FieldsExtractor Array(TagName) noexcept {
		assertrx_throw(values_);
		return FieldsExtractor(&values_->MarkArray(), expectedType_, expectedPathDepth_, filter_, params_);
	}
	FieldsExtractor Object(std::string_view = {}) noexcept {
		if rx_unlikely (expectedPathDepth_ == 0) {
			values_->MarkObject();
		}
		return FieldsExtractor(values_, expectedType_, expectedPathDepth_ - 1, filter_, params_);
	}
	FieldsExtractor Array(std::string_view) noexcept {
		return FieldsExtractor(&values_->MarkArray(), expectedType_, expectedPathDepth_, filter_, params_);
	}

	template <typename T>
	void Array(TagName, std::span<T> data, int offset) {
		const IndexedPathNode& pathNode = getArrayPathNode();
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
			for (const auto& d : data) {
				if (i++ == pathNode.Index()) {
					put(TagName::Empty(), Variant(d));
				}
			}
		} else {
			for (const auto& d : data) {
				put(TagName::Empty(), Variant(d));
			}
		}
		if (expectedPathDepth_ <= 0) {
			assertrx_throw(values_);
			values_->MarkArray();
		}
	}

	void Array(TagName, Serializer& ser, TagType tagType, int count) {
		const IndexedPathNode& pathNode = getArrayPathNode();
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
		const KeyValueType kvt{tagType};
		if (ptype == PathType::WithIndex) {
			for (int i = 0; i < count; ++i) {
				auto value = ser.GetRawVariant(kvt);
				if (i == pathNode.Index()) {
					put(TagName::Empty(), std::move(value));
				}
			}
		} else {
			for (int i = 0; i < count; ++i) {
				put(TagName::Empty(), ser.GetRawVariant(kvt));
			}
		}
		if (expectedPathDepth_ <= 0) {
			assertrx_throw(values_);
			values_->MarkArray();
		}
	}

	FieldsExtractor& Put(TagName t, Variant arg, int offset) {
		if (expectedPathDepth_ > 0) {
			return *this;
		}
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
	FieldsExtractor& Put(TagName tag, const T& arg, int offset) {
		return Put(tag, Variant{arg}, offset);
	}

	FieldsExtractor& Null(TagName = TagName::Empty()) noexcept { return *this; }
	int TargetField() { return params_ ? params_->field : IndexValueType::NotSet; }
	bool IsHavingOffset() const noexcept { return params_ && (params_->length >= 0 || params_->index >= 0); }
	void OnScopeEnd(int offset) noexcept {
		if (expectedPathDepth_ <= 0) {
			assertrx(params_ && !IsHavingOffset());
			params_->index = offset;
			params_->length = 0;
		}
	}

	template <typename... Args>
	void Object(int, Args...) = delete;
	template <typename... Args>
	void Object(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Array(int, Args...) = delete;
	template <typename... Args>
	void Array(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Put(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Put(int, Args...) = delete;
	template <typename... Args>
	void Null(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Null(int, Args...) = delete;

private:
	enum class PathType { AllItems, WithIndex, Other };
	PathType pathNotToType(const IndexedPathNode& pathNode) noexcept {
		return pathNode.IsForAllItems()						  ? PathType::AllItems
			   : (pathNode.Index() == IndexValueType::NotSet) ? PathType::Other
															  : PathType::WithIndex;
	}
	FieldsExtractor& put(TagName, Variant arg) {
		if (expectedPathDepth_ > 0) {
			return *this;
		}
		expectedType_.EvaluateOneOf(
			[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
					  KeyValueType::String, KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Uuid, KeyValueType::FloatVector>) {
				arg.convert(expectedType_);
			},
			[](OneOf<KeyValueType::Undefined, KeyValueType::Composite>) noexcept {});
		assertrx_throw(values_);
		values_->emplace_back(std::move(arg));
		return *this;
	}

	const IndexedPathNode& getArrayPathNode() const;

	VariantArray* values_ = nullptr;
	KeyValueType expectedType_{KeyValueType::Undefined{}};
	int expectedPathDepth_ = 0;
	const FieldsFilter* filter_;
	FieldParams* params_;
};

}  // namespace reindexer
