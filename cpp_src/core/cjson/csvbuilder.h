#pragma once

#include <span>
#include "core/enums.h"
#include "tagslengths.h"
#include "tagsmatcher.h"

namespace reindexer {
namespace builders {

class CsvBuilder;

struct [[nodiscard]] CsvOrdering {
	CsvOrdering(std::vector<TagName> ordering) : ordering_(std::move(ordering)) {
		if (ordering_.empty()) {
			return;
		}

		buf_.Reserve(kInitBufferSize);
	}

	auto begin() const noexcept { return ordering_.begin(); }
	auto end() const noexcept { return ordering_.end(); }

	friend CsvBuilder;

private:
	const size_t kInitBufferSize = 0x1000;
	std::vector<TagName> ordering_;
	WrSerializer buf_;
};

class [[nodiscard]] CsvBuilder {
public:
	CsvBuilder() = default;

	CsvBuilder(WrSerializer& ser, CsvOrdering& ordering);

	~CsvBuilder() noexcept(false);
	CsvBuilder& operator=(const CsvBuilder&) = delete;
	CsvBuilder& operator=(CsvBuilder&&) = delete;

	void SetTagsMatcher(const TagsMatcher* tm) { tm_ = tm; }

	/// Start new object
	CsvBuilder Object(std::string_view name = {}, int size = KUnknownFieldSize);
	CsvBuilder Object(concepts::TagNameOrIndex auto tag, int size = KUnknownFieldSize) { return Object(getNameByTag(tag), size); }

	CsvBuilder Array(std::string_view name, int size = KUnknownFieldSize);
	CsvBuilder Array(concepts::TagNameOrIndex auto tag, int size = KUnknownFieldSize) { return Array(getNameByTag(tag), size); }

	template <typename T>
	void Array(concepts::TagNameOrIndex auto tag, std::span<T> data, int /*offset*/ = 0) {
		CsvBuilder node = Array(tag);
		for (const auto& d : data) {
			node.Put(TagName::Empty(), d);
		}
	}
	template <typename T>
	void Array(std::string_view n, std::span<T> data, int /*offset*/ = 0) {
		CsvBuilder node = Array(n);
		for (const auto& d : data) {
			node.Put(TagName::Empty(), d);
		}
	}
	template <typename T>
	void Array(std::string_view n, std::initializer_list<T> data, int /*offset*/ = 0) {
		CsvBuilder node = Array(n);
		for (const auto& d : data) {
			node.Put(TagName::Empty(), d);
		}
	}

	void Array(concepts::TagNameOrIndex auto tag, Serializer& ser, TagType tagType, int count) {
		CsvBuilder node = Array(tag);
		while (count--) {
			node.Put(TagName::Empty(), ser.GetRawVariant(KeyValueType{tagType}));
		}
	}

	void Put(std::string_view name, const Variant& arg, int offset = 0);
	void Put(std::string_view name, std::string_view arg, int offset = 0);
	void Put(std::string_view name, Uuid arg, int offset = 0);
	void Put(std::string_view name, const char* arg, int offset = 0) { return Put(name, std::string_view(arg), offset); }
	template <typename T, typename std::enable_if<std::is_integral<T>::value || std::is_floating_point<T>::value>::type* = nullptr>
	void Put(std::string_view name, const T& arg, int /*offset*/ = 0) {
		putName(name);
		(*ser_) << arg;
	}
	template <typename T>
	void Put(concepts::TagNameOrIndex auto tag, const T& arg, int offset = 0) {
		Put(getNameByTag(tag), arg, offset);
	}

	void Raw(concepts::TagNameOrIndex auto tag, std::string_view arg) { Raw(getNameByTag(tag), arg); }
	void Raw(std::string_view name, std::string_view arg);
	void Raw(std::string_view arg) { Raw(std::string_view{}, arg); }
	void Json(std::string_view name, std::string_view arg) { Raw(name, arg); }
	void Json(std::string_view arg) { Raw(arg); }

	void Null(concepts::TagNameOrIndex auto tag) { Null(getNameByTag(tag)); }
	void Null(std::string_view name);

	void End();

	template <typename... Args>
	void Object(int, Args...) = delete;
	template <typename... Args>
	void Object(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Array(int, Args...) = delete;
	template <typename... Args>
	void Array(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Put(int, Args...) = delete;
	template <typename... Args>
	void Put(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Raw(int, Args...) = delete;
	template <typename... Args>
	void Raw(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Null(int, Args...) = delete;
	template <typename... Args>
	void Null(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Json(int, Args...) = delete;
	template <typename... Args>
	void Json(std::nullptr_t, Args...) = delete;

private:
	CsvBuilder(ObjType type, const CsvBuilder& parent);

	void putName(std::string_view name);
	[[nodiscard]] std::string_view getNameByTag(TagName tagName);
	[[nodiscard]] std::string_view getNameByTag(TagIndex) { return getNameByTag(TagName::Empty()); }
	void tmProcessing(std::string_view name);
	void postProcessing();

	WrSerializer* ser_ = nullptr;
	const TagsMatcher* tm_ = nullptr;
	ObjType type_ = ObjType::TypePlain;
	int count_ = 0;

	int level_ = 0;
	int startSerLen_ = 0;

	const std::vector<TagName>* ordering_ = nullptr;
	WrSerializer* buf_ = nullptr;

	// idx - pos in ordering, {startTagPosInSer, endTagPosInSer(post calculated after received next tag)}
	std::vector<std::pair<int, int>> positions_;
	int curTagPos_ = -1;
};
}  // namespace builders
using builders::CsvBuilder;
using builders::CsvOrdering;
}  // namespace reindexer
