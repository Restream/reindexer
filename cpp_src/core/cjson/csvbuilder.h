#pragma once

#include <optional>
#include "estl/span.h"
#include "objtype.h"
#include "tagslengths.h"
#include "tagsmatcher.h"

namespace reindexer {

class CsvBuilder;

struct CsvOrdering {
	CsvOrdering(std::vector<int> ordering) : ordering_(std::move(ordering)) {
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
	std::vector<int> ordering_;
	WrSerializer buf_;
};

class CsvBuilder {
public:
	CsvBuilder() = default;
	CsvBuilder(WrSerializer& ser, CsvOrdering& ordering);

	~CsvBuilder();
	CsvBuilder& operator=(const CsvBuilder&) = delete;
	CsvBuilder& operator=(CsvBuilder&&) = delete;

	void SetTagsMatcher(const TagsMatcher* tm) { tm_ = tm; }

	/// Start new object
	CsvBuilder Object(std::string_view name = {}, int size = KUnknownFieldSize);
	CsvBuilder Object(std::nullptr_t, int size = KUnknownFieldSize) { return Object(std::string_view{}, size); }
	CsvBuilder Object(int tagName, int size = KUnknownFieldSize) { return Object(getNameByTag(tagName), size); }

	CsvBuilder Array(std::string_view name, int size = KUnknownFieldSize);
	CsvBuilder Array(int tagName, int size = KUnknownFieldSize) { return Array(getNameByTag(tagName), size); }

	template <typename T>
	void Array(int tagName, span<T> data, int /*offset*/ = 0) {
		CsvBuilder node = Array(tagName);
		for (const auto& d : data) {
			node.Put({}, d);
		}
	}
	template <typename T>
	void Array(std::string_view n, span<T> data, int /*offset*/ = 0) {
		CsvBuilder node = Array(n);
		for (const auto& d : data) {
			node.Put({}, d);
		}
	}
	template <typename T>
	void Array(std::string_view n, std::initializer_list<T> data, int /*offset*/ = 0) {
		CsvBuilder node = Array(n);
		for (const auto& d : data) {
			node.Put({}, d);
		}
	}

	void Array(int tagName, Serializer& ser, TagType tagType, int count) {
		CsvBuilder node = Array(tagName);
		while (count--) {
			node.Put({}, ser.GetRawVariant(KeyValueType{tagType}));
		}
	}

	CsvBuilder& Put(std::string_view name, const Variant& arg, int offset = 0);
	CsvBuilder& Put(std::nullptr_t, const Variant& arg, int offset = 0) { return Put(std::string_view{}, arg, offset); }
	CsvBuilder& Put(std::string_view name, std::string_view arg, int offset = 0);
	CsvBuilder& Put(std::string_view name, Uuid arg, int offset = 0);
	CsvBuilder& Put(std::nullptr_t, std::string_view arg, int offset = 0) { return Put(std::string_view{}, arg, offset); }
	CsvBuilder& Put(std::string_view name, const char* arg, int offset = 0) { return Put(name, std::string_view(arg), offset); }
	template <typename T, typename std::enable_if<std::is_integral<T>::value || std::is_floating_point<T>::value>::type* = nullptr>
	CsvBuilder& Put(std::string_view name, const T& arg, [[maybe_unused]] int offset = 0) {
		putName(name);
		(*ser_) << arg;
		return *this;
	}
	template <typename T>
	CsvBuilder& Put(int tagName, const T& arg, int offset = 0) {
		return Put(getNameByTag(tagName), arg, offset);
	}

	CsvBuilder& Raw(int tagName, std::string_view arg) { return Raw(getNameByTag(tagName), arg); }
	CsvBuilder& Raw(std::string_view name, std::string_view arg);
	CsvBuilder& Raw(std::nullptr_t, std::string_view arg) { return Raw(std::string_view{}, arg); }
	CsvBuilder& Json(std::string_view name, std::string_view arg) { return Raw(name, arg); }
	CsvBuilder& Json(std::nullptr_t, std::string_view arg) { return Raw(std::string_view{}, arg); }

	CsvBuilder& Null(int tagName) { return Null(getNameByTag(tagName)); }
	CsvBuilder& Null(std::string_view name);

	CsvBuilder& End();

protected:
	CsvBuilder(ObjType type, const CsvBuilder& parent);

	void putName(std::string_view name);
	std::string_view getNameByTag(int tagName);
	void tmProcessing(std::string_view name);
	void postProcessing();

	WrSerializer* ser_ = nullptr;
	const TagsMatcher* tm_ = nullptr;
	ObjType type_ = ObjType::TypePlain;
	int count_ = 0;

	int level_ = 0;
	int startSerLen_ = 0;

	const std::vector<int>* ordering_ = nullptr;
	WrSerializer* buf_ = nullptr;

	// idx - pos in ordering, {startTagPosInSer, endTagPosInSer(post culculated after received next tag)}
	std::vector<std::pair<int, int>> positions_;
	int curTagPos_ = -1;
};

}  // namespace reindexer
