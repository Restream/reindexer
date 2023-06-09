#pragma once

#include <unordered_map>
#include "core/cjson/cjsonbuilder.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

class Schema;
struct ProtobufValue;
struct ProtobufObject;

class ArraysStorage {
public:
	explicit ArraysStorage(TagsMatcher& tm);
	ArraysStorage(const ArraysStorage&) = delete;
	ArraysStorage(ArraysStorage&&) = delete;
	ArraysStorage& operator=(const ArraysStorage&) = delete;
	ArraysStorage& operator=(ArraysStorage&&) = delete;

	CJsonBuilder& GetArray(int tagName, int field = IndexValueType::NotSet);
	void UpdateArraySize(int tagName, int field);

	void onAddObject();
	void onObjectBuilt(CJsonBuilder& parent);

private:
	struct ArrayData {
		ArrayData(TagsMatcher* _tm, int _tagName, int _field)
			: field(_field), size(0), ser(), builder(ser, ObjType::TypeArray, _tm, _tagName) {}
		ArrayData(const ArrayData&) = delete;
		ArrayData(ArrayData&&) = delete;
		ArrayData& operator=(const ArrayData&) = delete;
		ArrayData& operator=(ArrayData&&) = delete;
		int field = 0;
		int size = 0;
		WrSerializer ser;
		CJsonBuilder builder;
	};
	h_vector<h_vector<int, 1>, 1> indexes_;
	std::unordered_map<int, ArrayData> data_;
	TagsMatcher& tm_;
};

class CJsonProtobufObjectBuilder {
public:
	CJsonProtobufObjectBuilder(ArraysStorage& arraysStorage, WrSerializer& ser, TagsMatcher* tm = nullptr, int tagName = 0)
		: builder_(ser, ObjType::TypeObject, tm, tagName), arraysStorage_(arraysStorage) {
		arraysStorage_.onAddObject();
	}
	CJsonProtobufObjectBuilder(CJsonBuilder& obj, int tagName, ArraysStorage& arraysStorage)
		: builder_(obj.Object(tagName)), arraysStorage_(arraysStorage) {
		arraysStorage_.onAddObject();
	}
	~CJsonProtobufObjectBuilder() { arraysStorage_.onObjectBuilt(builder_); }
	CJsonProtobufObjectBuilder(const CJsonProtobufObjectBuilder&) = delete;
	CJsonProtobufObjectBuilder(CJsonProtobufObjectBuilder&&) = delete;
	CJsonProtobufObjectBuilder& operator=(const CJsonProtobufObjectBuilder&) = delete;
	CJsonProtobufObjectBuilder& operator=(CJsonProtobufObjectBuilder&&) = delete;

	operator CJsonBuilder&() { return builder_; }
	CJsonBuilder* operator->() { return &builder_; }

private:
	CJsonBuilder builder_;
	ArraysStorage& arraysStorage_;
};

class ProtobufDecoder {
public:
	ProtobufDecoder(TagsMatcher& tagsMatcher, std::shared_ptr<const Schema> schema);
	ProtobufDecoder(const ProtobufDecoder&) = delete;
	ProtobufDecoder(ProtobufDecoder&&) = delete;
	ProtobufDecoder& operator=(const ProtobufDecoder&) = delete;
	ProtobufDecoder& operator=(ProtobufDecoder&&) = delete;

	Error Decode(std::string_view buf, Payload& pl, WrSerializer& wrser);

private:
	void setValue(Payload& pl, CJsonBuilder& builder, ProtobufValue item);
	Error decode(Payload& pl, CJsonBuilder& builder, const ProtobufValue& val);
	Error decodeObject(Payload& pl, CJsonBuilder& builder, ProtobufObject& object);
	Error decodeArray(Payload& pl, CJsonBuilder& builder, const ProtobufValue& val);

	TagsMatcher& tm_;
	std::shared_ptr<const Schema> schema_;
	TagsPath tagsPath_;
	ArraysStorage arraysStorage_;
};

}  // namespace reindexer
