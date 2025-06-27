#pragma once

#include <vector>

#include "core/cjson/tagsmatcher.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "itemimplrawdata.h"
#include "namespace/float_vectors_indexes.h"
#include "tools/serializer.h"

namespace reindexer {

class MsgPackDecoder;
class Namespace;
class RdxContext;
class Recoder;
class Schema;

class ItemImpl : public ItemImplRawData {
	friend class Item;

public:
	ItemImpl();
	~ItemImpl();
	// Construct empty item
	ItemImpl(PayloadType type, const TagsMatcher& tagsMatcher, const FieldsSet& pkFields = {}, std::shared_ptr<const Schema> schema = {});
	// Construct empty item
	ItemImpl(PayloadType type, const TagsMatcher& tagsMatcher, const FieldsSet& pkFields, std::shared_ptr<const Schema> schema,
			 ItemImplRawData&& rawData);
	ItemImpl(PayloadType type, PayloadValue v, const TagsMatcher& tagsMatcher, std::shared_ptr<const Schema> schema = {});

	ItemImpl(const ItemImpl&) = delete;
	ItemImpl(ItemImpl&&) noexcept;
	ItemImpl& operator=(ItemImpl&&) noexcept;
	ItemImpl& operator=(const ItemImpl&) = delete;

	void ModifyField(std::string_view jsonPath, const VariantArray& keys, FieldModifyMode mode);
	void ModifyField(const IndexedTagsPath& tagsPath, const VariantArray& keys, FieldModifyMode mode);
	void SetField(int field, const VariantArray& krs, NeedCreate needCopy = NeedCreate_True);
	void SetField(std::string_view jsonPath, const VariantArray& keys);
	void DropField(std::string_view jsonPath);
	[[nodiscard]] Variant GetField(int field);
	void GetField(int field, VariantArray&);
	[[nodiscard]] FieldsSet PkFields() const { return pkFields_; }
	[[nodiscard]] TagName NameTag(std::string_view name) const { return tagsMatcher_.name2tag(name); }
	[[nodiscard]] int FieldIndex(std::string_view name) const {
		int field = IndexValueType::NotSet;
		if (payloadType_.FieldByName(name, field)) {
			return field;
		}
		return IndexValueType::NotSet;
	}

	[[nodiscard]] VariantArray GetValueByJSONPath(std::string_view jsonPath);

	[[nodiscard]] std::string_view GetJSON();
	[[nodiscard]] Error FromJSON(std::string_view slice, char** endp = nullptr, bool pkOnly = false);
	void FromCJSON(ItemImpl& other, Recoder*);

	[[nodiscard]] std::string_view GetCJSON(bool withTagsMatcher = false);
	std::string_view GetCJSON(WrSerializer& ser, bool withTagsMatcher = false);
	void FromCJSON(std::string_view slice, bool pkOnly = false, Recoder* = nullptr);
	[[nodiscard]] Error FromMsgPack(std::string_view sbuf, size_t& offset);
	[[nodiscard]] Error FromProtobuf(std::string_view sbuf);
	[[nodiscard]] Error GetMsgPack(WrSerializer& wrser);
	[[nodiscard]] std::string_view GetMsgPack();
	[[nodiscard]] Error GetProtobuf(WrSerializer& wrser);

	[[nodiscard]] const PayloadType& Type() const& noexcept { return payloadType_; }
	[[nodiscard]] PayloadValue& Value()& noexcept { return payloadValue_; }
	[[nodiscard]] PayloadValue& RealValue()& noexcept { return realValue_; }
	[[nodiscard]] Payload GetPayload() noexcept { return Payload(payloadType_, payloadValue_); }
	[[nodiscard]] ConstPayload GetConstPayload() const noexcept { return ConstPayload(payloadType_, payloadValue_); }
	[[nodiscard]] std::shared_ptr<const Schema> GetSchema() const noexcept { return schema_; }

	[[nodiscard]] TagsMatcher& tagsMatcher() noexcept { return tagsMatcher_; }
	[[nodiscard]] std::shared_ptr<const Schema>& schema()& noexcept { return schema_; }

	void SetPrecepts(std::vector<std::string>&& precepts) {
		precepts_ = std::move(precepts);
		cjson_ = std::string_view();
	}
	[[nodiscard]] const std::vector<std::string>& GetPrecepts() const& noexcept { return precepts_; }
	void Unsafe(bool enable) noexcept { unsafe_ = enable; }
	[[nodiscard]] bool IsUnsafe() const noexcept { return unsafe_; }
	void Clear();
	void SetNamespace(std::shared_ptr<Namespace> ns) noexcept { ns_ = std::move(ns); }
	[[nodiscard]] std::weak_ptr<Namespace> GetNamespace() const noexcept { return ns_; }
	static void validateModifyArray(const VariantArray& values);
	void BuildTupleIfEmpty();
	/**
	 * @brief Copies vectors' values from indexes into ItemImpl::payloadValue.
	 * Be default this method creates and stores full vector's data copy.
	 * If item is marked 'unsafe', then resulting payload value will contain view, pointing into index structures directly
	 * and any modification of those indexes may break the references.
	 */
	void CopyIndexedVectorsValuesFrom(IdType, const FloatVectorsIndexes&);

	void Embed(const RdxContext& ctx);

private:
	ItemImpl(PayloadType, PayloadValue, const TagsMatcher&, std::shared_ptr<const Schema>, const FieldsFilter&);

	void initTupleFrom(Payload&&, WrSerializer&);

	// Index fields payload data
	PayloadType payloadType_;
	PayloadValue realValue_;
	TagsMatcher tagsMatcher_;
	FieldsSet pkFields_;
	std::shared_ptr<const Schema> schema_;

	WrSerializer ser_;

	bool unsafe_ = false;
	std::string_view cjson_;
	std::weak_ptr<Namespace> ns_;
	std::unique_ptr<MsgPackDecoder> msgPackDecoder_;
	const FieldsFilter* fieldsFilter_{nullptr};
};

}  // namespace reindexer
