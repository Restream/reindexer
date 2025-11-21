#pragma once

#include "core/cjson/tagsmatcher.h"
namespace reindexer {

class [[nodiscard]] MultidimensionalArrayChecker {
public:
	MultidimensionalArrayChecker() noexcept = default;
	~MultidimensionalArrayChecker() {
		if (parent_ && result_) {
			parent_->result_ = true;
		}
	}
	bool Result() const noexcept { return result_; }
	void SetTagsMatcher(const TagsMatcher*) const noexcept {}
	MultidimensionalArrayChecker Object() noexcept { return {*this, false}; }
	MultidimensionalArrayChecker Object(concepts::TagNameOrIndex auto) noexcept { return Object(); }
	MultidimensionalArrayChecker Array(auto) noexcept {
		result_ = result_ || isArray_;
		return {*this, true};
	}
	template <typename T>
	void Array(concepts::TagNameOrIndex auto, std::span<T>, unsigned) {
		result_ = result_ || isArray_;
	}
	void Array(concepts::TagNameOrIndex auto, Serializer& ser, TagType tagType, int count) {
		result_ = result_ || isArray_;
		const KeyValueType kvt{tagType};
		for (int i = 0; i < count; ++i) {
			std::ignore = ser.GetRawVariant(kvt);
		}
	}
	void Null(concepts::TagNameOrIndex auto) noexcept {}
	void Put(concepts::TagNameOrIndex auto, const Variant&, int) noexcept {}

	void Object(int, auto...) = delete;
	void Object(std::nullptr_t, auto...) = delete;
	void Array(int, auto...) = delete;
	void Array(std::nullptr_t, auto...) = delete;
	void Put(std::nullptr_t, auto...) = delete;
	void Put(int, auto...) = delete;
	void Null(std::nullptr_t, auto...) = delete;
	void Null(int, auto...) = delete;

private:
	MultidimensionalArrayChecker(MultidimensionalArrayChecker& parent, bool isArray) noexcept : parent_{&parent}, isArray_{isArray} {}
	MultidimensionalArrayChecker* parent_{nullptr};
	bool isArray_{false};
	bool result_{false};
};

}  // namespace reindexer
