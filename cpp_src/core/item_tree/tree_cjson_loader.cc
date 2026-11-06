#include "tree_cjson_loader.h"
#include <cstdint>
#include <string_view>
#include <tuple>

#include "core/item_tree/item_tree.h"
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/variant.h"
#include "core/tag_name_index.h"
#include "core/type_consts.h"
#include "tools/assertrx.h"

namespace reindexer {

void CjsonTreeLoader::Load(std::string_view cjson, const ConstPayload& pv, TreeNode& node) {
	Serializer ser(cjson);
	const ctag tag = ser.GetCTag();
	if (tag.Type() != TAG_OBJECT) {
		throw Error(errParseJson, "Expected root CJSON object, got {}", int(tag.Type()));
	}
	fieldsoutcnt_.fill(0);
	loadObject(ser, pv, node);
}

void CjsonTreeLoader::loadObject(Serializer& rdser, const ConstPayload& pv, TreeNode& node) {
	while (true) {
		const ctag tag = rdser.GetCTag();
		if (tag == kCTagEnd) {
			return;
		}
		processTag(tag, rdser, pv, node, true);
	}
}

template <typename T>
void CjsonTreeLoader::loadIndexedArrayValues(TreeNode& node, int indexNum, const ConstPayload& pv, TagName tagName, uint64_t count) {
	TreeNode& nodeArray = node.AddArray(tagName, count);
	nodeArray.SetHomogeneousArray(true);
	nodeArray.SetIndex(indexNum);

	auto vals = pv.GetView<T>(indexNum).subspan(fieldsoutcnt_[indexNum], count);
	for (const auto& v : vals) {
		std::ignore = nodeArray.AddValue(TagName::Empty(), Variant{v}, indexNum);
	}
}

void CjsonTreeLoader::loadArrayIndexed(Serializer& rdser, const ConstPayload& pv, TreeNode& node, int indexNum, TagName tagName) {
	const auto& f = pv.Type().Field(indexNum);

	auto count = rdser.GetVarUInt();
	int& cnt = fieldsoutcnt_[indexNum];
	f.Type().EvaluateOneOf(
		[&](KeyValueType::Bool) { loadIndexedArrayValues<bool>(node, indexNum, pv, tagName, count); },
		[&](KeyValueType::Int) { loadIndexedArrayValues<int>(node, indexNum, pv, tagName, count); },
		[&](KeyValueType::Int64) { loadIndexedArrayValues<int64_t>(node, indexNum, pv, tagName, count); },
		[&](KeyValueType::Double) { loadIndexedArrayValues<double>(node, indexNum, pv, tagName, count); },
		[&](KeyValueType::String) {
			TreeNode& nodeArray = node.AddArray(tagName, 0);
			nodeArray.SetHomogeneousArray(true);
			nodeArray.SetIndex(indexNum);

			auto vals = pv.GetView<p_string>(indexNum).subspan(cnt, count);
			for (const auto& v : vals) {
				std::ignore = nodeArray.AddValue(TagName::Empty(), Variant{v}, indexNum);
			}
		},
		[&](KeyValueType::Uuid) { loadIndexedArrayValues<Uuid>(node, indexNum, pv, tagName, count); },
		[&](KeyValueType::FloatVector) {
			const int payloadPos = f.IsArray() ? cnt : 0;
			auto view = ConstFloatVectorView(pv.Get(indexNum, payloadPos));
			assertrx_dbg(unsigned(view.Dimension()) == count);
			Variant v(view);
			if (node.IsType<ObjectNode>()) {
				std::ignore = node.AddValue(tagName, std::move(v), indexNum, false);
			} else if (node.IsType<ArrayNode>()) {
				std::ignore = node.AddValue(TagName::Empty(), std::move(v), indexNum);
			} else {
				assertrx_throw(false);
			}
		},
		[&](KeyValueType::Float) {
			// Indexed field can not contain float array now
			assertrx(false);
			abort();
		},
		[](concepts::OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite> auto) noexcept {
			assertrx(false);
			abort();
		});

	if (f.Type().Is<KeyValueType::FloatVector>()) {
		if (f.IsArray()) {
			++cnt;
		}
	} else {
		cnt += int(count);
	}
}

void CjsonTreeLoader::loadArray(Serializer& rdser, const ConstPayload& pv, TreeNode& node) {
	carraytag atag = rdser.GetCArrayTag();
	uint32_t count = atag.Count();
	switch (atag.Type()) {
		case TAG_VARINT: {
			node.SetHomogeneousArray(true);
			readHomogeneousArray<int64_t>(node, count, [&rdser]() { return rdser.GetVarint(); });
			break;
		}
		case TAG_DOUBLE: {
			node.SetHomogeneousArray(true);
			readHomogeneousArray<double>(node, count, [&rdser]() { return rdser.GetDouble(); });
			break;
		}
		case TAG_BOOL: {
			node.SetHomogeneousArray(true);
			readHomogeneousArray<bool>(node, count, [&rdser]() { return rdser.GetBool(); });
			break;
		}
		case TAG_FLOAT: {
			node.SetHomogeneousArray(true);
			readHomogeneousArray<float>(node, count, [&rdser]() { return rdser.GetFloat(); });
			break;
		}
		case TAG_NULL: {
			node.SetHomogeneousArray(true);
			for (unsigned int i = 0; i < count; i++) {
				std::ignore = node.AddValue(TagName::Empty(), Variant{}, 0);
			}
			break;
		}
		case TAG_UUID: {
			node.SetHomogeneousArray(true);
			readHomogeneousArray<Uuid>(node, count, [&rdser]() { return rdser.GetUuid(); });
			break;
		}
		case TAG_STRING: {
			node.SetHomogeneousArray(true);
			readHomogeneousArray<p_string>(node, count, [&rdser]() { return rdser.GetPVString(); });
			break;
		}
		case TAG_OBJECT: {
			node.SetHomogeneousArray(false);
			for (unsigned int i = 0; i < count; i++) {
				const ctag tag = rdser.GetCTag();
				processTag(tag, rdser, pv, node, false);
			}
			break;
		}
		case TAG_ARRAY:
			assertrx(false);
			break;
		case TAG_END:
			break;
	}
}

void CjsonTreeLoader::processTag(ctag tag, Serializer& rdser, const ConstPayload& pv, TreeNode& nodeTree, bool isObject) {
	int indexNum = tag.Field();
	switch (tag.Type()) {
		case TAG_VARINT:
		case TAG_DOUBLE:
		case TAG_BOOL:
		case TAG_NULL:
		case TAG_FLOAT:
		case TAG_UUID:
		case TAG_STRING: {
			if (isObject) {
				readValue(tag, rdser, pv, nodeTree);
			} else {
				if (indexNum > 0) {
					Variant val = ReadValueFromPvArray(indexNum, pv);
					std::ignore = nodeTree.AddValue(TagName::Empty(), std::move(val), indexNum);
				} else {
					Variant val = rdser.GetRawVariant(KeyValueType{tag.Type()});
					std::ignore = nodeTree.AddValue(TagName::Empty(), std::move(val), 0);
				}
			}
			break;
		}
		case TAG_ARRAY: {
			int indexNum = tag.Field();

			if (indexNum > 0) {
				loadArrayIndexed(rdser, pv, nodeTree, indexNum, tag.Name());
			} else if (indexNum == 0) {
				assertrx(false);
			} else {
				TreeNode& nodeArray = nodeTree.AddArray(tag.Name(), 0);
				loadArray(rdser, pv, nodeArray);
			}
			break;
		}
		case TAG_OBJECT: {
			if (isObject) {
				TreeNode& objectNode = nodeTree.AddObject(tag.Name());
				loadObject(rdser, pv, objectNode);
			} else {
				TreeNode& objectNode = nodeTree.AddObject(TagName::Empty());
				loadObject(rdser, pv, objectNode);
			}
			break;
		}
		case TAG_END:
			break;
	}
}

Variant CjsonTreeLoader::ReadValueFromPvArray(int indexNum, const ConstPayload& pv) {
	Variant val;
	int& cnt = fieldsoutcnt_[indexNum];
	int count = 1;
	const auto& f = pv.Type().Field(indexNum);
	f.Type().EvaluateOneOf(
		[&](KeyValueType::Bool) { val = Variant{pv.GetView<bool>(indexNum).subspan(cnt, count)[0]}; },
		[&](KeyValueType::Int) { val = Variant{pv.GetView<int>(indexNum).subspan(cnt, count)[0]}; },
		[&](KeyValueType::Int64) { val = Variant{pv.GetView<int64_t>(indexNum).subspan(cnt, count)[0]}; },
		[&](KeyValueType::Double) { val = Variant{pv.GetView<double>(indexNum).subspan(cnt, count)[0]}; },
		[&](KeyValueType::String) { val = Variant{pv.GetView<p_string>(indexNum).subspan(cnt, count)[0]}; },
		[&](KeyValueType::Uuid) { val = Variant{pv.GetView<Uuid>(indexNum).subspan(cnt, count)[0]}; },
		[&](KeyValueType::FloatVector) { assertrx(false); },
		[&](KeyValueType::Float) {
			// Indexed field can not contain float array now
			assertrx(false);
			abort();
		},
		[](concepts::OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite> auto) noexcept {
			assertrx(false);
			abort();
		});

	cnt += int(count);
	return val;
}

void CjsonTreeLoader::readValue(ctag tag, Serializer& rdser, const ConstPayload& pv, TreeNode& node) {
	int indexNum = tag.Field();
	Variant val;
	if (indexNum >= 0) {
		if (pv.Type().Field(indexNum).IsArray()) {
			if (tag.Type() != TAG_NULL) {
				val = ReadValueFromPvArray(indexNum, pv);
			} else if (pv.Type().Field(indexNum).Type().Is<KeyValueType::FloatVector>()) {
				val = Variant{ConstFloatVectorView{}};
			}
		} else {
			val = pv.Field(indexNum).Get();
		}
	} else {
		val = rdser.GetRawVariant(KeyValueType{tag.Type()});
		indexNum = 0;
	}
	TreeNode& valueNode = node.AddValue(tag.Name(), std::move(val), indexNum, false);
	if (tag.Type() != TAG_NULL) {
		return;
	}

	// For nested payload indexes, the index number is determined by the JSON path
	valueNode.ResolveIndexByPath();
}

}  // namespace reindexer
