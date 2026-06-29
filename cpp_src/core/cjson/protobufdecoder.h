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
	ProtobufDecoder(TagsMatcher& tagsMatcher, std::shared_ptr<const Schema> schema, std::string_view data, Payload& pl, WrSerializer& wrser,
					FloatVectorsHolderVector& floatVectorsHolder, ScalarIndexesSetT& objectScalarIndexes) noexcept
		: tm_(tagsMatcher),
		  schema_(std::move(schema)),
		  arraysStorage_(tm_),
		  data_(data),
		  pl_(pl),
		  wrSer_(wrser),
		  floatVectorsHolder_(floatVectorsHolder),
		  objectScalarIndexes_(objectScalarIndexes) {}
	ProtobufDecoder(const ProtobufDecoder&) = delete;
	ProtobufDecoder(ProtobufDecoder&&) = delete;
	ProtobufDecoder& operator=(const ProtobufDecoder&) = delete;
	ProtobufDecoder& operator=(ProtobufDecoder&&) = delete;

	Error Decode() noexcept;

private:
	template <typename Validator>
	void setValue(CJsonBuilder&, ProtobufValue, const Validator&);
	void decode(CJsonBuilder&, const ProtobufValue&);
	template <typename Validator>
	void decode(CJsonBuilder&, const ProtobufValue&, const Validator&);
	void decodeObject(CJsonBuilder&, ProtobufObject&);
	template <typename Validator>
	void decodeArray(CJsonBuilder&, const ProtobufValue&, Validator&&);
	InArray isInArray() const noexcept { return InArray(arrayLevel_ > 0); }

	TagsMatcher& tm_;
	std::shared_ptr<const Schema> schema_;
	TagsPath tagsPath_;
	ArraysStorage arraysStorage_;
	int32_t arrayLevel_ = 0;

	std::string_view data_;
	Payload& pl_;
	WrSerializer& wrSer_;
	FloatVectorsHolderVector& floatVectorsHolder_;
	ScalarIndexesSetT& objectScalarIndexes_;
};

}  // namespace reindexer
