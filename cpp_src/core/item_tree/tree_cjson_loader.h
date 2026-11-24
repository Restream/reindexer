#pragma once

#include <string_view>
#include "core/item_tree/item_tree.h"
#include "core/type_consts.h"

namespace reindexer {

class TreeNode;

class [[nodiscard]] CjsonTreeLoader {
public:
	CjsonTreeLoader() {}
	void Load(std::string_view cjson, const ConstPayload& pv, TreeNode& node);

private:
	void loadObject(Serializer& rdser, const ConstPayload& pv, TreeNode& node);
	void loadArray(Serializer& rdser, const ConstPayload& pv, TreeNode& node);
	void loadArrayIndexed(Serializer& rdser, const ConstPayload& pv, TreeNode& node, int indexNum, TagName tagName);
	void processTag(ctag tag, Serializer& rdser, const ConstPayload& pv, TreeNode& node, bool isObject);
	void readValue(ctag tag, Serializer& rdser, const ConstPayload& pv, TreeNode& node);
	template <typename T, typename ReadValueT>
	void readHomogeneousArray(TreeNode& node, uint32_t count, ReadValueT read) {
		for (unsigned int i = 0; i < count; i++) {
			T val = read();
			std::ignore = node.AddValue(TagName::Empty(), Variant{val}, 0);
		}
	}

	template <typename T>
	void loadIndexedArrayValues(TreeNode& node, int indexNum, const ConstPayload& pv, TagName tagName, uint64_t count);

	Variant ReadValueFromPvArray(int indexNum, const ConstPayload& pv);

	std::array<int, kMaxIndexes> fieldsoutcnt_;
};

}  // namespace reindexer