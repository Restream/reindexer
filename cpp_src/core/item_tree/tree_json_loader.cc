
#include "tree_json_loader.h"
#include <variant>
#include "core/cjson/cjsontools.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/variant.h"
#include "core/tag_name_index.h"
#include "estl/overloaded.h"
#include "gason/gason.h"
#include "item_tree.h"
#include "tools/json2kv.h"

namespace reindexer {

void JsonTreeLoader::Load(std::string_view json) {
	gason::JsonParser parser;
	gason::JsonNode node = parser.Parse(json);
	if (node.isObject()) {
		for (const auto& elem : node.value) {
			loadJsonInternal(elem, treeNode_);
		}
	} else {
		throw Error(errLogic, "Error modifying field value: 'Unsupported JSON format. Unnamed field detected'");
	}
}

void JsonTreeLoader::addValue(TreeNode& node, TagName tag, Variant&& v, int index) {
	if (node.IsType<ObjectNode>()) {
		if (index > 0) {
			indexData_.SaveOldIndexData(index, oldPayload_, root_);
			std::ignore = node.AddValue(tag, std::move(v), index, true);
		} else if (tm_.tags2field(pathForIndexedField_).IsSparseIndex()) {
			TreeNode& target = node.AddObject(tag);
			indexData_.SaveOldIndexData(target.IsSparseIndex(), oldPayload_, root_);
			target.ChangeToValue();
			target.UpdateValue(std::move(v));
		} else {
			std::ignore = node.AddValue(tag, std::move(v), index, true);
		}
	} else if (node.IsType<ArrayNode>()) {
		std::ignore = node.AddValue(TagName::Empty(), std::move(v), index);
	} else {
		assertrx(false);
	}
}

Variant JsonTreeLoader::makeFloatVector(const gason::JsonValue& value, const FieldProperties& field, std::string_view fieldName) const {
	Variant vector = jsonValue2Variant(value, KeyValueType::FloatVector{}, fieldName, nullptr, ConvertToString_False, ConvertNull_True);
	const ConstFloatVectorView view{vector};
	validateArrayFieldRestrictions(fieldName, IsArray_True, field.ArrayDim(), view.Dimension().Value(), kJSONFmt);
	return vector;
}

void JsonTreeLoader::loadFloatVectorArray(const gason::JsonValue& value, TreeNode& nodeTree, const FieldProperties& field,
										  std::string_view fieldName) {
	const int indexNumber = field.IndexNumber();
	const auto beginIt = begin(value);
	const auto endIt = end(value);
	if (beginIt == endIt || (!beginIt->isArray() && !beginIt->isNull() && !beginIt->isObject())) {
		addValue(nodeTree, TagName::Empty(), makeFloatVector(value, field, fieldName), indexNumber);
		return;
	}

	for (auto it = beginIt; it != endIt; ++it) {
		if (it->isArray() && begin(it->value) != end(it->value) &&
			(begin(it->value)->isArray() || begin(it->value)->isNull() || begin(it->value)->isObject())) {
			TreeNode& nestedNode = nodeTree.AddArray(TagName::Empty(), 0);
			nestedNode.SetHomogeneousArray(false);
			loadFloatVectorArray(it->value, nestedNode, field, fieldName);
		} else {
			addValue(nodeTree, TagName::Empty(), makeFloatVector(it->value, field, fieldName), indexNumber);
		}
	}
}

void JsonTreeLoader::processFloatVectorTag(const gason::JsonNode& node, TreeNode& nodeTree, const FieldProperties& field, TagName tag) {
	const int indexNumber = field.IndexNumber();
	const std::string fieldName = tm_.Path2Name(pathForIndexedField_);

	if (node.value.isNull()) {
		addValue(nodeTree, tag, field.IsArray() ? Variant{} : Variant{ConstFloatVectorView{}}, indexNumber);
		return;
	}
	if (!node.value.isArray()) {
		std::ignore =
			jsonValue2Variant(node.value, KeyValueType::FloatVector{}, fieldName, nullptr, ConvertToString_False, ConvertNull_True);
	}

	const auto beginIt = begin(node.value);
	const auto endIt = end(node.value);
	const bool isSingleVector = beginIt != endIt && !beginIt->isArray() && !beginIt->isNull() && !beginIt->isObject();
	if (!field.IsArray() || isSingleVector) {
		addValue(nodeTree, tag, makeFloatVector(node.value, field, fieldName), indexNumber);
		return;
	}

	TreeNode& arrayNode = nodeTree.AddArray(tag, 0);
	arrayNode.SetHomogeneousArray(false);
	indexData_.SaveOldIndexData(indexNumber, oldPayload_, root_);
	loadFloatVectorArray(node.value, arrayNode, field, fieldName);
}

void JsonTreeLoader::processJsonTag(const gason::JsonNode& node, TreeNode& nodeTree, const FieldProperties& field, TagName tag) {
	const int indexNumber = field.IndexNumber() > 0 ? field.IndexNumber() : 0;
	if (field.IsRegularIndex() && field.ValueType().Is<KeyValueType::FloatVector>()) {
		processFloatVectorTag(node, nodeTree, field, tag);
		return;
	}
	auto jsonTag = node.value.getTag();
	switch (jsonTag) {
		case gason::JsonTag::NUMBER:
			addValue(nodeTree, tag, Variant(node.value.toNumber()), indexNumber);
			break;
		case gason::JsonTag::DOUBLE:
			addValue(nodeTree, tag, Variant(node.value.toDouble()), indexNumber);
			break;
		case gason::JsonTag::STRING:
			addValue(nodeTree, tag, Variant(node.value.toString()), indexNumber);
			break;
		case gason::JsonTag::JTRUE:
			addValue(nodeTree, tag, Variant(true), indexNumber);
			break;
		case gason::JsonTag::JFALSE:
			addValue(nodeTree, tag, Variant(false), indexNumber);
			break;
		case gason::JsonTag::JSON_NULL:
			addValue(nodeTree, tag, Variant{}, indexNumber);
			break;
		case gason::JsonTag::ARRAY: {
			if (field.IsIndexed() && !field.IsArray()) [[unlikely]] {
				const std::string_view fieldName =
					field.IsRegularIndex() ? indexData_.IndexName(field.IndexNumber()) : tm_.SparseName(field.SparseNumber());
				throwUnexpectedArrayError(fieldName, field.ValueType(), kCJSONFmt);
			}
			TreeNode& arr = nodeTree.AddArray(tag, 0);
			int changedIndex = indexNumber;
			if (changedIndex > 0) {
				arr.SetHomogeneousArray(true);
				arr.SetIndex(changedIndex);
			} else {
				changedIndex = arr.IsSparseIndex();
			}
			if (changedIndex > 0) {
				indexData_.SaveOldIndexData(changedIndex, oldPayload_, root_);
			}
			for (const auto& elem : node.value) {
				loadJsonInternal(elem, arr);
			}
			break;
		}
		case gason::JsonTag::OBJECT: {
			TreeNode& newNode = [&]() -> TreeNode& {
				if (nodeTree.IsType<ObjectNode>()) {
					return nodeTree.AddObject(tag);
				}
				if (nodeTree.IsType<ArrayNode>()) {
					return nodeTree.AddObject(TagName::Empty());
				}
				throw Error(errLogic, "Cannot add an object to a non-container node");
			}();
			for (const auto& elem : node.value) {
				loadJsonInternal(elem, newNode);
			}

			break;
		}
		case gason::JsonTag::EMPTY:
		default:
			throw Error(errLogic, "Unexpected '{}' tag", jsonTag);
	}
}

void JsonTreeLoader::loadJsonInternal(const gason::JsonNode& node, TreeNode& nodeTree) {
	TagName tag = TagName::Empty();
	if (node.key.size()) {
		tag = tm_.name2tag(std::string_view(node.key.data(), node.key.size()), CanAddField_True);

		TagsPathLevel tpl(pathForIndexedField_, tag);

		FieldProperties fp = tm_.tags2field(pathForIndexedField_);
		processJsonTag(node, nodeTree, fp, tag);
	} else {
		FieldProperties fp = tm_.tags2field(pathForIndexedField_);
		processJsonTag(node, nodeTree, fp, tag);
	}
}
}  // namespace reindexer
