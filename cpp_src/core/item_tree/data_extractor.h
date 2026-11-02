#pragma once

#include <cstddef>
#include "core/cjson/tagspath.h"
#include "core/item_tree/item_tree.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/namespaceimpl.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"
#include "core/payload/payloadvalue.h"
#include "core/queryresults/fields_filter.h"
#include "estl/overloaded.h"
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] IItemValuesExtractor {
public:
	virtual ~IItemValuesExtractor() = default;
	virtual const PayloadTypeImpl& Type() const noexcept = 0;
	virtual bool ContainsMultidimensionalArray(std::string_view jsonIndexPath) const = 0;
	virtual void Get(int field, VariantArray& res) const = 0;
	virtual void GetByJsonPath(std::string_view jsonPath, VariantArray& kvs, KeyValueType expectedType) = 0;
	virtual void GetByFieldsSet(const FieldsSet& fields, VariantArray& kvs, KeyValueType expectedType,
								const h_vector<KeyValueType, 4>& expectedCompositeTypes) const = 0;
	virtual size_t GetFieldSize(int field) const = 0;
	virtual size_t GetFieldSize(std::string_view jsonPath) const = 0;
};

class [[nodiscard]] TreeDataExtractor : public IItemValuesExtractor {
public:
	TreeDataExtractor(TreeNode& root, const ConstPayload& pv, const TagsMatcher& tm, std::span<const std::unique_ptr<Index>> indexes)
		: pv_(pv), tm_(tm), indexes_(indexes), root_(root) {}
	const PayloadTypeImpl& Type() const noexcept final { return pv_.Type(); }
	bool ContainsMultidimensionalArray(std::string_view jsonIndexPath) const final {
		return root_.ContainsMultidimensionalArray(tm_.path2tag(jsonIndexPath));
	}
	void Get(int field, VariantArray& res) const final { res = GetIndexData<VariantArray>(root_, *indexes_[field], tm_, pv_); }
	void GetByJsonPath(std::string_view jsonPath, VariantArray& kvs, KeyValueType) final { kvs = GetDataByPath(jsonPath, root_, tm_); }
	void GetByFieldsSet(const FieldsSet&, VariantArray&, KeyValueType, const h_vector<KeyValueType, 4>&) const final {
		throw Error(errLogic, "Field bindings are not supported by TreeDataExtractor");
	}
	size_t GetFieldSize(int field) const final {
		size_t count = 0;
		const auto& fieldType = pv_.Type().Field(field);
		if (fieldType.IsFloatVector()) {
			VariantArray values;
			Get(field, values);

			size_t result = 0;
			for (const Variant& value : values) {
				if (!value.IsNullValue()) {
					result += ConstFloatVectorView{value}.Dimension().Value();
				}
			}
			return result;
		}
		const auto& paths = fieldType.JsonPaths();
		for (size_t i = 0; i < paths.size(); i++) {
			TagsPath path = tm_.path2tag(paths[i]);
			std::span<const TagName> pp{path};
			TagsPathCursor cursor{root_, pp};
			for (const TreeNode* n = cursor.Next(); n != nullptr; n = cursor.Next()) {
				std::visit(overloaded{[&](const ArrayNode* array) { count += array->Size(); }, [&](const ObjectNode*) { ++count; },
									  [&](const ValueNode* value) {
										  if (!value->Value().IsNullValue() || !n->Parent()) {
											  ++count;
										  }
									  }},
						   n->data_.value_);
			}
		}

		return count;
	}
	size_t GetFieldSize(std::string_view jsonPath) const final {
		if (const int field = pv_.Type().FieldByJsonPath(jsonPath); field >= 0 && pv_.Type().Field(field).IsFloatVector()) {
			return GetFieldSize(field);
		}
		size_t count = 0;
		IndexedTagsPath path = tm_.path2indexedtag(jsonPath);
		ConstExistingPathCursor cursor{root_, path};
		for (const TreeNode* node = cursor.Next(); node != nullptr; node = cursor.Next()) {
			std::visit(overloaded{[&](const ObjectNode*) { ++count; },
								  [&](const ArrayNode* array) {
									  for (const TreeNode* child : array->Values()) {
										  const auto* value = child->GetIf<ValueNode>();
										  if (value && value->Value().Type().Is<KeyValueType::FloatVector>()) {
											  count += ConstFloatVectorView{value->Value()}.Dimension().Value();
										  } else {
											  ++count;
										  }
									  }
								  },
								  [&](const ValueNode* value) {
									  const Variant& v = value->Value();
									  if (!v.IsNullValue()) {
										  count +=
											  v.Type().Is<KeyValueType::FloatVector>() ? ConstFloatVectorView{v}.Dimension().Value() : 1;
									  }
								  }},
					   node->data_.value_);
		}
		return count;
	}

private:
	ConstPayload pv_;
	const TagsMatcher& tm_;
	std::span<const std::unique_ptr<Index>> indexes_;
	TreeNode& root_;
};

class [[nodiscard]] PayloadDataExtractor : public IItemValuesExtractor {
public:
	PayloadDataExtractor(const PayloadValue& pv, const PayloadType& pt, const NamespaceImpl& ns, const TagsMatcher& tm)
		: pv_(pt, pv), ns_(ns), tm_(tm) {}
	const PayloadTypeImpl& Type() const noexcept final { return pv_.Type(); }
	bool ContainsMultidimensionalArray(std::string_view path) const final {
		return pv_.ContainsMultidimensionalArray(FieldsFilter{path, ns_});
	}

	void Get(int field, VariantArray& res) const final { pv_.Get(field, res); }
	void GetByJsonPath(std::string_view jsonPath, VariantArray& kvs, KeyValueType expectedType) final {
		pv_.GetByJsonPath(jsonPath, tm_, kvs, expectedType);
	}
	void GetByFieldsSet(const FieldsSet& fields, VariantArray& kvs, KeyValueType expectedType,
						const h_vector<KeyValueType, 4>& expectedCompositeTypes) const final {
		pv_.GetByFieldsSet(fields, kvs, expectedType, expectedCompositeTypes);
	}
	size_t GetFieldSize(int field) const final { return pv_.GetFieldSize(FieldsSet{field}); }
	size_t GetFieldSize(std::string_view jsonPath) const final {
		if (const int field = pv_.Type().FieldByJsonPath(jsonPath); field >= 0 && pv_.Type().Field(field).IsFloatVector()) {
			return GetFieldSize(field);
		}
		return pv_.GetFieldSize(jsonPath, tm_);
	}

private:
	ConstPayload pv_;
	const NamespaceImpl& ns_;
	const TagsMatcher& tm_;
};

}  // namespace reindexer
