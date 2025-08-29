#pragma once

#include <unordered_map>
#include "core/cjson/cjsonbuilder.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

class Schema;
struct ProtobufValue;
struct ProtobufObject;
class FloatVectorsHolderVector;

class [[nodiscard]] ArraysStorage {
public:
	explicit ArraysStorage(TagsMatcher& tm) noexcept : tm_(tm) {}
	ArraysStorage(const ArraysStorage&) = delete;
	ArraysStorage(ArraysStorage&&) = delete;
	ArraysStorage& operator=(const ArraysStorage&) = delete;
	ArraysStorage& operator=(ArraysStorage&&) = delete;

	CJsonBuilder& GetArray(TagName, int field = IndexValueType::NotSet);
	void UpdateArraySize(TagName, int field);

	void onAddObject();
	void onObjectBuilt(CJsonBuilder& parent);

private:
	struct [[nodiscard]] ArrayData {
		ArrayData(TagsMatcher* tm, TagName tagName, int _field) : field(_field), size(0), builder(ser, ObjType::TypeArray, tm, tagName) {}
		ArrayData(const ArrayData&) = delete;
		ArrayData(ArrayData&&) = delete;
		ArrayData& operator=(const ArrayData&) = delete;
		ArrayData& operator=(ArrayData&&) = delete;
		int field = 0;
		int size = 0;
		WrSerializer ser;
		CJsonBuilder builder;
	};
	h_vector<h_vector<TagName, 1>, 1> indexes_;
	std::unordered_map<TagName, ArrayData, TagName::Hash> data_;
	TagsMatcher& tm_;
};

class [[nodiscard]] CJsonProtobufObjectBuilder {
public:
	CJsonProtobufObjectBuilder(ArraysStorage& arraysStorage, WrSerializer& ser, TagsMatcher* tm = nullptr,
							   TagName tagName = TagName::Empty())
		: builder_(ser, ObjType::TypeObject, tm, tagName), arraysStorage_(arraysStorage) {
		arraysStorage_.onAddObject();
	}
	CJsonProtobufObjectBuilder(CJsonBuilder& obj, TagName tagName, ArraysStorage& arraysStorage)
		: builder_(obj.Object(tagName)), arraysStorage_(arraysStorage) {
		arraysStorage_.onAddObject();
	}
	~CJsonProtobufObjectBuilder() { arraysStorage_.onObjectBuilt(builder_); }
	CJsonProtobufObjectBuilder(const CJsonProtobufObjectBuilder&) = delete;
	CJsonProtobufObjectBuilder(CJsonProtobufObjectBuilder&&) = delete;
	CJsonProtobufObjectBuilder& operator=(const CJsonProtobufObjectBuilder&) = delete;
	CJsonProtobufObjectBuilder& operator=(CJsonProtobufObjectBuilder&&) = delete;

	operator CJsonBuilder&() noexcept { return builder_; }
	CJsonBuilder* operator->() noexcept { return &builder_; }

private:
	CJsonBuilder builder_;
	ArraysStorage& arraysStorage_;
};

class [[nodiscard]] ProtobufDecoder {
public:
	ProtobufDecoder(TagsMatcher& tagsMatcher, std::shared_ptr<const Schema> schema) noexcept
		: tm_(tagsMatcher), schema_(std::move(schema)), arraysStorage_(tm_) {}
	ProtobufDecoder(const ProtobufDecoder&) = delete;
	ProtobufDecoder(ProtobufDecoder&&) = delete;
	ProtobufDecoder& operator=(const ProtobufDecoder&) = delete;
	ProtobufDecoder& operator=(ProtobufDecoder&&) = delete;

	Error Decode(std::string_view buf, Payload& pl, WrSerializer& wrser, FloatVectorsHolderVector&) noexcept;

private:
	template <typename Validator>
	void setValue(Payload&, CJsonBuilder&, ProtobufValue, const Validator&);
	void decode(Payload&, CJsonBuilder&, const ProtobufValue&, FloatVectorsHolderVector&);
	template <typename Validator>
	void decode(Payload&, CJsonBuilder&, const ProtobufValue&, FloatVectorsHolderVector&, const Validator&);
	void decodeObject(Payload&, CJsonBuilder&, ProtobufObject&, FloatVectorsHolderVector&);
	template <typename Validator>
	void decodeArray(Payload&, CJsonBuilder&, const ProtobufValue&, FloatVectorsHolderVector&, Validator&&);
	InArray isInArray() const noexcept { return InArray(arrayLevel_ > 0); }

	TagsMatcher& tm_;
	std::shared_ptr<const Schema> schema_;
	TagsPath tagsPath_;
	ArraysStorage arraysStorage_;
	ScalarIndexesSetT objectScalarIndexes_;
	int32_t arrayLevel_ = 0;
};

}  // namespace reindexer
