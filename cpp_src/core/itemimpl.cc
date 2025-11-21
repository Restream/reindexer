#include "core/itemimpl.h"

#include <span>
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/cjsonmodifier.h"
#include "core/cjson/cjsontools.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/jsondecoder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/msgpackdecoder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/cjson/protobufdecoder.h"
#include "core/embedding/embedder.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/p_string.h"
#include "core/rdxcontext.h"
#include "estl/gift_str.h"

namespace reindexer {

void ItemImpl::SetField(int field, const VariantArray& krs, NeedCreate needCopy) {
	validateModifyArray(krs);
	cjson_ = {};
	if (needCopy) {
		payloadValue_.Clone();
	}
	auto& ptField = payloadType_.Field(field);

	auto setFieldValueSafe = [this, &ptField, field](const VariantArray& newValue) {
		auto pl = GetPayload();
		VariantArray oldValues;
		pl.Get(field, oldValues);
		pl.Set(field, newValue, Append_False);
		try {
			// TODO: We should modify CJSON for any default value or array size change #1837
			bool modifyCjson = false;
			if (ptField.Type().Is<KeyValueType::FloatVector>()) {
				assertrx_throw(oldValues.size() == 1);
				assertrx_throw(newValue.size() <= 1);
				const auto oldSize = oldValues[0].As<ConstFloatVectorView>().Dimension();
				const auto newSize = newValue.empty() ? FloatVectorDimension() : newValue[0].As<ConstFloatVectorView>().Dimension();
				modifyCjson = (oldSize != newSize);
			}
			if (modifyCjson && ptField.JsonPaths().size()) {
				ModifyField(ptField.JsonPaths()[0], newValue, FieldModeSet);
			}
		} catch (...) {
			pl.Set(field, oldValues, Append_False);
			throw;
		}
	};

	if (!unsafe_ && !krs.empty() && ptField.Type().IsOneOf<KeyValueType::String, KeyValueType::FloatVector>()) {
		VariantArray krsCopy;
		krsCopy.reserve(krs.size());

		if (payloadType_.Field(field).Type().Is<KeyValueType::String>()) {
			if (!holder_) {
				holder_ = std::make_unique<ItemImplRawData::HolderT>();
			}
			for (auto& kr : krs) {
				auto& back = holder_->emplace_back(kr.As<key_string>());
				krsCopy.emplace_back(p_string{back});
			}
		} else {
			floatVectorsHolder_.reserve(floatVectorsHolder_.size() + krs.size());
			for (auto& kr : krs) {
				const ConstFloatVectorView vect{kr};
				if (floatVectorsHolder_.Add(vect)) {
					krsCopy.emplace_back(floatVectorsHolder_.Back());
				} else {
					krsCopy.emplace_back(vect);
				}
			}
		}
		setFieldValueSafe(krsCopy);
	} else {
		setFieldValueSafe(krs);
	}
}

ItemImpl::ItemImpl() = default;
ItemImpl::~ItemImpl() = default;

ItemImpl::ItemImpl(PayloadType type, const TagsMatcher& tagsMatcher, const FieldsSet& pkFields, std::shared_ptr<const Schema> schema)
	: ItemImplRawData(PayloadValue(type.TotalSize(), nullptr, type.TotalSize() + 0x100)),
	  payloadType_(std::move(type)),
	  tagsMatcher_(tagsMatcher),
	  pkFields_(pkFields),
	  schema_(std::move(schema)) {
	tagsMatcher_.clearUpdated();
}

ItemImpl::ItemImpl(ItemImpl&&) noexcept = default;
ItemImpl& ItemImpl::operator=(ItemImpl&&) noexcept = default;

ItemImpl::ItemImpl(PayloadType type, const TagsMatcher& tagsMatcher, const FieldsSet& pkFields, std::shared_ptr<const Schema> schema,
				   ItemImplRawData&& rawData)
	: ItemImplRawData(std::move(rawData)),
	  payloadType_(std::move(type)),
	  tagsMatcher_(tagsMatcher),
	  pkFields_(pkFields),
	  schema_(std::move(schema)) {}

ItemImpl::ItemImpl(PayloadType type, PayloadValue v, const TagsMatcher& tagsMatcher, std::shared_ptr<const Schema> schema)
	: ItemImplRawData(std::move(v)), payloadType_(std::move(type)), tagsMatcher_(tagsMatcher), schema_{std::move(schema)} {
	tagsMatcher_.clearUpdated();
}

ItemImpl::ItemImpl(PayloadType type, PayloadValue v, const TagsMatcher& tagsMatcher, std::shared_ptr<const Schema> schema,
				   const FieldsFilter& fieldsFilter)
	: ItemImpl{std::move(type), std::move(v), tagsMatcher, std::move(schema)} {
	fieldsFilter_ = &fieldsFilter;
}

void ItemImpl::ModifyField(std::string_view jsonPath, const VariantArray& keys, FieldModifyMode mode) {
	ModifyField(tagsMatcher_.path2indexedtag(jsonPath, CanAddField(mode != FieldModeDrop)), keys, mode);
}

void ItemImpl::ModifyField(const IndexedTagsPath& tagsPath, const VariantArray& keys, FieldModifyMode mode) {
	validateModifyArray(keys);
	payloadValue_.Clone();
	Payload pl = GetPayload();

	ser_.Reset();
	ser_.PutUInt32(0);
	WrSerializer generatedCjson;
	const auto cjsonV = pl.Get(0, 0);
	std::string_view cjson(cjsonV);
	if (cjson.empty()) {
		buildPayloadTuple(pl, &tagsMatcher_, generatedCjson);
		cjson = generatedCjson.Slice();
	}

	CJsonModifier cjsonModifier(tagsMatcher_, payloadType_);
	try {
		switch (mode) {
			case FieldModeSet:
				cjsonModifier.SetFieldValue(cjson, tagsPath, keys, ser_, pl, floatVectorsHolder_);
				break;
			case FieldModeSetJson:
				cjsonModifier.SetObject(cjson, tagsPath, keys, ser_, pl, floatVectorsHolder_);
				break;
			case FieldModeDrop:
				cjsonModifier.RemoveField(cjson, tagsPath, ser_);
				break;
			case FieldModeArrayPushBack:
			case FieldModeArrayPushFront:
				throw Error(errLogic, "Update mode is not supported: {}", int(mode));
		}
	} catch (const Error& e) {
		throw Error(e.code(), "Error modifying field value: '{}'", e.what());
	} catch (std::exception& e) {
		throw Error(errLogic, "Error modifying field value: '{}'", e.what());
	}

	initTupleFrom(std::move(pl), ser_);
}

void ItemImpl::SetField(std::string_view jsonPath, const VariantArray& keys) { ModifyField(jsonPath, keys, FieldModeSet); }
void ItemImpl::DropField(std::string_view jsonPath) { ModifyField(jsonPath, {}, FieldModeDrop); }
Variant ItemImpl::GetField(int field) { return GetPayload().Get(field, 0); }
void ItemImpl::GetField(int field, VariantArray& values) { GetPayload().Get(field, values); }

Error ItemImpl::FromMsgPack(std::string_view buf, size_t& offset) {
	payloadValue_.Clone();
	cjson_ = {};

	Payload pl = GetPayload();
	if (!msgPackDecoder_) {
		msgPackDecoder_.reset(new MsgPackDecoder(tagsMatcher_));
	}

	std::string_view data = createSafeDataCopy(buf);

	pl.Reset();
	ser_.Reset();
	ser_.PutUInt32(0);
	Error err = msgPackDecoder_->Decode(data, pl, ser_, offset, floatVectorsHolder_);
	if (err.ok()) {
		initTupleFrom(std::move(pl), ser_);
	}
	return err;
}

Error ItemImpl::FromProtobuf(std::string_view buf) {
	payloadValue_.Clone();
	cjson_ = {};

	Payload pl = GetPayload();
	ProtobufDecoder decoder(tagsMatcher_, schema_);

	std::string_view data = createSafeDataCopy(buf);

	pl.Reset();
	ser_.Reset();
	ser_.PutUInt32(0);
	Error err = decoder.Decode(data, pl, ser_, floatVectorsHolder_);
	if (err.ok()) {
		initTupleFrom(std::move(pl), ser_);
	}
	return err;
}

Error ItemImpl::GetMsgPack(WrSerializer& wrser) {
	int startTag = 0;
	ConstPayload pl = GetConstPayload();

	MsgPackEncoder msgpackEncoder(&tagsMatcher_, fieldsFilter_);
	const TagsLengths& tagsLengths = msgpackEncoder.GetTagsMeasures(pl);

	MsgPackBuilder msgpackBuilder(wrser, &tagsLengths, &startTag, ObjType::TypePlain, &tagsMatcher_);
	msgpackEncoder.Encode(pl, msgpackBuilder);
	return {};
}

std::string_view ItemImpl::GetMsgPack() {
	ser_.Reset();
	auto err = GetMsgPack(ser_);
	if (!err.ok()) {
		throw err;
	}
	return ser_.Slice();
}

Error ItemImpl::GetProtobuf(WrSerializer& wrser) {
	ConstPayload pl = GetConstPayload();
	ProtobufBuilder protobufBuilder(&wrser, ObjType::TypePlain, schema_.get(), &tagsMatcher_);
	ProtobufEncoder protobufEncoder(&tagsMatcher_, fieldsFilter_);
	protobufEncoder.Encode(pl, protobufBuilder);
	return {};
}

void ItemImpl::Clear() {
	static const TagsMatcher kEmptyTagsMatcher;
	tagsMatcher_ = kEmptyTagsMatcher;
	precepts_.clear();
	cjson_ = {};
	holder_.reset();
	floatVectorsHolder_ = FloatVectorsHolderVector();
	sourceData_.reset();
	largeJSONStrings_.clear();
	tupleData_.reset();
	ser_ = WrSerializer();

	GetPayload().Reset();
	payloadValue_.SetLSN(lsn_t());

	unsafe_ = false;
	ns_.reset();
	realValue_.Free();
}

// Construct item from compressed json
void ItemImpl::FromCJSON(std::string_view slice, bool pkOnly, Recoder* recoder) {
	payloadValue_.Clone();
	std::string_view data = createSafeDataCopy(slice);

	// check tags matcher update
	if (Serializer rdser(data); rdser.GetCTag() == kCTagEnd) {
		const auto tmOffset = rdser.GetUInt32();
		// read tags matcher update
		Serializer tser(data.substr(tmOffset));
		tagsMatcher_.deserialize(tser);
		tagsMatcher_.setUpdated();
		data = data.substr(1 + sizeof(uint32_t), tmOffset - 5);
	}
	cjson_ = data;
	Serializer rdser(data);

	Payload pl = GetPayload();
	pl.Reset();
	if (!holder_) {
		holder_ = std::make_unique<ItemImplRawData::HolderT>();
	}
	CJsonDecoder decoder(tagsMatcher_, *holder_);

	ser_.Reset();
	ser_.PutUInt32(0);
	if (pkOnly && !pkFields_.empty()) {
		if (recoder) [[unlikely]] {
			throw Error(errParams, "ItemImpl::FromCJSON: pkOnly mode is not compatible with non-null recoder");
		}
		decoder.Decode(pl, rdser, ser_, floatVectorsHolder_, CJsonDecoder::RestrictingFilter(pkFields_));
	} else {
		if (recoder) {
			decoder.Decode(pl, rdser, ser_, floatVectorsHolder_, CJsonDecoder::DefaultFilter(fieldsFilter_),
						   CJsonDecoder::CustomRecoder(*recoder));
		} else {
			decoder.Decode(pl, rdser, ser_, floatVectorsHolder_, CJsonDecoder::DefaultFilter(fieldsFilter_));
		}
	}

	if (!rdser.Eof()) {
		throw Error(errParseJson, "Internal error - left unparsed data {}", rdser.Pos());
	}

	initTupleFrom(std::move(pl), ser_);
}

Error ItemImpl::FromJSON(std::string_view slice, char** endp, bool pkOnly) {
	payloadValue_.Clone();
	std::string_view data = slice;
	cjson_ = {};

	if (!unsafe_) {
		if (endp) {
			size_t len = 0;
			try {
				gason::JsonParser parser(nullptr);
				parser.Parse(data, &len);
				*endp = const_cast<char*>(data.data()) + len;
				sourceData_.reset(new char[len]);
				std::copy(data.begin(), data.begin() + len, sourceData_.get());
				data = std::string_view(sourceData_.get(), len);
			} catch (const gason::Exception& e) {
				return Error(errParseJson, "Error parsing json: '{}'", e.what());
			}
		} else {
			data = createSafeDataCopy(slice);
		}
	}

	size_t len = 0;
	gason::JsonNode node;
	gason::JsonParser parser(&largeJSONStrings_);
	try {
		node = parser.Parse(giftStr(data), &len);
		if (node.value.getTag() != gason::JsonTag::OBJECT) {
			return Error(errParseJson, "Expected json object");
		}
		if (unsafe_ && endp) {
			*endp = const_cast<char*>(data.data()) + len;
		}
	} catch (gason::Exception& e) {
		return Error(errParseJson, "Error parsing json: '{}', pos: {}", e.what(), len);
	}

	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_, pkOnly && !pkFields_.empty() ? &pkFields_ : nullptr);
	Payload pl = GetPayload();
	pl.Reset();

	ser_.Reset();
	ser_.PutUInt32(0);
	auto err = decoder.Decode(pl, ser_, node.value, floatVectorsHolder_);
	if (err.ok()) {
		initTupleFrom(std::move(pl), ser_);
	}
	return err;
}

void ItemImpl::FromCJSON(ItemImpl& other, Recoder* recoder) {
	FromCJSON(other.GetCJSON(), false, recoder);
	cjson_ = {};
}

std::string_view ItemImpl::GetJSON() {
	ConstPayload pl(payloadType_, payloadValue_);

	JsonEncoder encoder(&tagsMatcher_, fieldsFilter_);
	JsonBuilder builder(ser_, ObjType::TypePlain);

	ser_.Reset();
	encoder.Encode(pl, builder);

	return ser_.Slice();
}

std::string_view ItemImpl::GetCJSON(bool withTagsMatcher) {
	withTagsMatcher = withTagsMatcher && tagsMatcher_.isUpdated();

	if (!cjson_.empty() && !withTagsMatcher) {
		return cjson_;
	}
	ser_.Reset();
	return GetCJSON(ser_, withTagsMatcher);
}

std::string_view ItemImpl::GetCJSON(WrSerializer& ser, bool withTagsMatcher) {
	withTagsMatcher = withTagsMatcher && tagsMatcher_.isUpdated();

	if (!cjson_.empty() && !withTagsMatcher) {
		ser.Write(cjson_);
		return ser.Slice();
	}

	ConstPayload pl(payloadType_, payloadValue_);

	CJsonBuilder builder(ser, ObjType::TypePlain);
	CJsonEncoder encoder(&tagsMatcher_, fieldsFilter_);

	if (withTagsMatcher) {
		ser.PutCTag(kCTagEnd);
		auto pos = ser.Len();
		ser.PutUInt32(0);
		encoder.Encode(pl, builder);
		uint32_t tmOffset = ser.Len();
		memcpy(ser.Buf() + pos, &tmOffset, sizeof(tmOffset));
		tagsMatcher_.serialize(ser);
	} else {
		encoder.Encode(pl, builder);
	}

	return ser.Slice();
}

VariantArray ItemImpl::GetValueByJSONPath(std::string_view jsonPath) {
	ConstPayload pl(payloadType_, payloadValue_);
	VariantArray krefs;
	pl.GetByJsonPath(jsonPath, tagsMatcher_, krefs, KeyValueType::Undefined{});
	return krefs;
}

void ItemImpl::validateModifyArray(const VariantArray& values) {
	for (const auto& v : values) {
		v.Type().EvaluateOneOf([](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
												  KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid, KeyValueType::Null,
												  KeyValueType::Undefined, KeyValueType::FloatVector> auto) {},
							   [](KeyValueType::Tuple) {
								   throw Error(errParams,
											   "Unable to use 'tuple'-value (array of arrays, array of points, etc) to modify item");
							   },
							   [](KeyValueType::Composite) {
								   throw Error(errParams, "Unable to use 'composite'-value (object, array of objects, etc) to modify item");
							   });
	}
}

void ItemImpl::BuildTupleIfEmpty() {
	if (!tupleData_) {
		WrSerializer ser;
		ser.PutUInt32(0);  // Empty lstring header
		auto pl = GetPayload();
		buildPayloadTuple(pl, &tagsMatcher_, ser);
		initTupleFrom(std::move(pl), ser);
	}
}

void ItemImpl::CopyIndexedVectorsValuesFrom(IdType id, const FloatVectorsIndexes& indexes) {
	if (id < 0) {
		throw Error(errLogic, "Unable to set vector values with incorrect ID: {}", id);
	}
	if (indexes.empty()) {
		return;
	}
	payloadValue_.Clone();
	floatVectorsHolder_.resize(0);
	Payload pl(payloadType_, payloadValue_);
	if (IsUnsafe()) {
		for (auto& indexP : indexes) {
			pl.Set(indexP.ptField, Variant{indexP.ptr->GetFloatVectorView(id)});
		}
	} else {
		floatVectorsHolder_.reserve(indexes.size());
		for (auto& indexP : indexes) {
			if (floatVectorsHolder_.Add(indexP.ptr->GetFloatVector(id))) {
				pl.Set(indexP.ptField, Variant{floatVectorsHolder_.Back()});
			}
		}
	}
}

namespace {
bool isFieldEmpty(const Payload& pl, int field) {
	auto vec = pl.Get(field, 0).As<ConstFloatVectorView>();
	return vec.IsEmpty();
}

bool checkEmbedderAllowed(const Payload& pl, int field, const UpsertEmbedder& embedder) {
	switch (embedder.Strategy()) {
		case EmbedderConfig::Strategy::Always:
			return true;
		case EmbedderConfig::Strategy::EmptyOnly:
			return isFieldEmpty(pl, field);
		case EmbedderConfig::Strategy::Strict:
			if (!isFieldEmpty(pl, field)) {
				throw Error{errConflict,
							"Vector field '{}' must not contain a non-empty data if strict strategy for auto-embedding is configured",
							embedder.FieldName()};
			}
			return true;
	}
	return false;
}

int checkEmbedderCalcField(const PayloadType& payloadType, std::string_view indexName, std::string_view fieldName) {
	int fieldId = 0;
	if (!payloadType.FieldByName(fieldName, fieldId)) {
		throw Error(errLogic,
					"Cannot automatically embed value in index field named '{}'. The auxiliary field '{}' was not found or is a composite "
					"or sparse field. Embedding is supported only for scalar index fields",
					indexName, fieldName);
	}
	return fieldId;
}
}  // namespace

void ItemImpl::Embed(const RdxContext& ctx) {
	try {
		payloadValue_.Clone();
		auto pl = GetPayload();

		const auto unsafe = unsafe_;
		unsafe_ = false;

		// one document - one input vector of VariantArray
		std::vector<std::pair<std::string, VariantArray>> source;

		for (int field = 1, numFields = payloadType_.NumFields(); field < numFields; ++field) {
			auto embedder = payloadType_.Field(field).UpsertEmbedder();
			if (!embedder) {
				continue;  // do nothing
			}

			ThrowOnCancel(ctx);

			int fieldId = payloadType_.FieldByName(embedder->FieldName());
			if (!checkEmbedderAllowed(pl, fieldId, *embedder)) {
				continue;  // skip embedder
			}

			source.resize(0);
			for (const auto& fld : embedder->Fields()) {
				int fldId = checkEmbedderCalcField(payloadType_, embedder->FieldName(), fld);

				VariantArray data;
				pl.Get(fldId, data);
				source.emplace_back(fld, std::move(data));
			}

			// ToDo in real life, work with several embedded devices requires asynchrony. Now we support only one
			h_vector<ConstFloatVector, 1> products;
			embedder->Calculate(ctx, std::span{&source, 1}, products);
			if (products.size() != 1) {
				throw Error(errLogic, "Unable to set vector values with incorrect embedding result");
			}

			VariantArray krs;
			krs.emplace_back(products.front().View());

			SetField(fieldId, krs, NeedCreate_False);
		}
		unsafe_ = unsafe;
	} catch (Error& err) {
		if (err.code() == errTimeout || err.code() == errCanceled) {
			throw Error{err.code(), "Embedding service is unavailable (request was canceled/timed out)"};
		}
		throw err;
	} catch (...) {
		throw Error{errLogic, "Unknown error in embedders"};
	}
}

void ItemImpl::initTupleFrom(Payload&& pl, WrSerializer& ser) {
	// Put tuple to field[0]
	tupleData_ = ser.DetachLStr();
	pl.Set(0, Variant(p_string(reinterpret_cast<l_string_hdr*>(tupleData_.get())), Variant::noHold));
}

std::string_view ItemImpl::createSafeDataCopy(std::string_view slice) {
	std::string_view data = slice;
	if (!unsafe_) {
		sourceData_.reset(new char[data.size()]);
		std::copy(data.begin(), data.end(), sourceData_.get());
		data = std::string_view(sourceData_.get(), data.size());
	}
	return data;
}

}  // namespace reindexer
