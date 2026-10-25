#pragma once
#include <array>
#include <cassert>
#include <cstdint>
#include <ctime>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "core/cjson/tagsmatcher.h"
#include "core/cjson/tagspath.h"
#include "core/item_tree/intermediate_indexes_data.h"
#include "core/item_tree/node_arena.h"
#include "core/key_value_type.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"
#include "core/tag_name_index.h"
#include "core/type_consts.h"
#include "estl/concepts.h"
#include "estl/h_vector.h"
#include "tools/assertrx.h"
#include "tools/errors.h"

namespace reindexer {

class TreeNode;

class NodeFactory;
class TreeDataExtractor;
class UpdatePathCursor;

VariantArray GetDataByPath(std::string_view path, const TreeNode& root, const TagsMatcher& tm);
class ValueNode;

class [[nodiscard]] ItemsTreeContext {
public:
	ItemsTreeContext(const TagsMatcher& tm, const PayloadType& pt, std::span<const std::unique_ptr<Index>> indexes,
					 IntermediateIndexesData& indexData, std::function<int(std::string_view)> getIndexByName)
		: tm_(tm), pt_(pt), indexes_(indexes), indexData_(indexData), getIndexByName_(std::move(getIndexByName)) {}
	const TagsMatcher& tm_;
	const PayloadType& pt_;
	std::span<const std::unique_ptr<Index>> indexes_;
	IntermediateIndexesData& indexData_;
	std::function<int(std::string_view)> getIndexByName_;
};

class [[nodiscard]] ObjectNode {
public:
	explicit ObjectNode(NodeArena& arena) : values_{NodeAllocator<std::pair<TreeNode*, TagName>>{arena}} { values_.reserve(4); }

	TreeNode& Push(TagName name, TreeNode& node) {
		if (Get(name)) {
			throw Error(errLogic, "Cannot add a duplicate field to an object node");
		}

		return *values_.emplace_back(&node, name).first;
	}

	TreeNode* Get(TagName n) const {
		for (const auto& [node, tag] : values_) {
			if (node && tag == n) {
				return node;
			}
		}
		return nullptr;
	}

	TreeNode& GetOrCreate(TagName n, TreeNode& parent, ItemsTreeContext& ic);

	void Erase(TagName tag) {
		for (auto& [node, name] : values_) {
			if (node && name == tag) {
				node = nullptr;
				return;
			}
		}
	}
	void Clear() { values_.clear(); }

	std::vector<std::pair<TreeNode*, TagName>, NodeAllocator<std::pair<TreeNode*, TagName>>>::const_iterator begin() const {
		return values_.begin();
	}
	std::vector<std::pair<TreeNode*, TagName>, NodeAllocator<std::pair<TreeNode*, TagName>>>::const_iterator end() const {
		return values_.end();
	}

private:
	std::vector<std::pair<TreeNode*, TagName>, NodeAllocator<std::pair<TreeNode*, TagName>>> values_;  // node, tagName
};

class [[nodiscard]] ArrayNode {
public:
	explicit ArrayNode(NodeArena& arena) : values_{NodeAllocator<TreeNode*>{arena}} {}
	void Clear() {
		values_.resize(0);
		isHomogeneous_ = false;
		needsNormalization_ = false;
	}
	void Erase(size_t index) {
		if (index >= values_.size()) {
			throw Error(errLogic, "Array index {} is out of range [0, {})", index, values_.size());
		}
		values_.erase(values_.begin() + index);
	}
	size_t Size() const noexcept { return values_.size(); }
	void Reserve(size_t c) { values_.reserve(c); }

	const std::vector<TreeNode*, NodeAllocator<TreeNode*>>& Values() const noexcept { return values_; }
	bool Homogeneous() const noexcept { return isHomogeneous_; }
	void SetHomogeneous(bool v) noexcept { isHomogeneous_ = v; }
	bool NeedsNormalization() const noexcept { return needsNormalization_; }
	void MarkForNormalization() noexcept { needsNormalization_ = true; }
	void ResetNormalizationFlag() noexcept { needsNormalization_ = false; }
	void PushBack(TreeNode& n) { values_.push_back(&n); }

private:
	std::vector<TreeNode*, NodeAllocator<TreeNode*>> values_;
	bool isHomogeneous_ = false;
	bool needsNormalization_ = false;
};

class [[nodiscard]] ValueNode {
public:
	ValueNode() = default;
	ValueNode(Variant&& value) : value_(std::move(value)) {}

	void SetValue(const Variant& value) { value_ = value; }
	void SetValue(Variant&& value) { value_ = std::move(value); }

	bool IsValueSet() const noexcept { return value_.has_value(); }

	const Variant& Value() const {
		assertrx_throw(value_.has_value());
		return *value_;
	}

	void Clear() noexcept { value_.reset(); }

private:
	std::optional<Variant> value_;
};

class [[nodiscard]] NodeData {
public:
	NodeData(ItemsTreeContext& contextIndex) : contextIndex_(contextIndex) {}
	std::variant<ObjectNode*, ArrayNode*, ValueNode*> value_;
	int index_ = 0;
	ItemsTreeContext& contextIndex_;
};

template <concepts::OneOf<VariantArray, IntermediateIndexesData::IndexNodeData, h_vector<TreeNode*, 4>> V>
V GetIndexData(TreeNode& root, const Index& index, const TagsMatcher& tm, const ConstPayload& plSave);

struct [[nodiscard]] IndexedArrayData {
	void Reset() noexcept {
		isPresent = false;
		values.clear<false>();
	}

	bool isPresent = false;
	VariantArray values;
};

/**
 * @brief Mutable node of the arena-backed tree used to decode, inspect, and update an item.
 *
 * A node represents exactly one of three runtime kinds: an object, an array, or a scalar value. Object children are
 * addressed by TagName, while array children are addressed by their position. Each non-root node stores a link to its
 * parent and the identifier assigned by that parent, which allows tag paths and indexed tag paths to be reconstructed.
 *
 * TreeNode does not own its type-specific storage, context, parent, or children. Nodes and their ObjectNode, ArrayNode,
 * and ValueNode data are allocated and reused by NodeFactory; all related pointers and references are valid only until
 * that factory is reset or destroyed. The ItemsTreeContext supplied at construction must outlive the entire tree.
 */
class [[nodiscard]] TreeNode {
public:
	/** @brief Constructs an uninitialized reusable node tied to a tree context and a node factory.
	 * @param contextIndex Metadata and intermediate index data shared by the whole tree.
	 * @param creator Factory that owns all type-specific storage created for this node.
	 */
	TreeNode(ItemsTreeContext& contextIndex, NodeFactory& creator) : data_(contextIndex), creator_(creator) {}

	/** @brief Reinitializes this pooled node as an empty object.
	 * @param parentId Object tag number or array position assigned by parent; -1 for the root.
	 */
	void Init(TreeNode* parent, int parentId, ObjectNode* obj) {
		nodeParent_ = parent;
		parentId_ = parentId;
		data_.value_ = obj;
		data_.index_ = 0;
	}
	/** @brief Reinitializes this pooled node as an empty array.
	 * @param count Initial child capacity; it does not set the logical array size.
	 * @param parentId Object tag number or array position assigned by parent; -1 for the root.
	 */
	void Init(uint32_t count, TreeNode* parent, int parentId, ArrayNode* array) {
		nodeParent_ = parent;
		parentId_ = parentId;
		array->Reserve(count);
		data_.value_ = array;
		data_.index_ = 0;
	}
	/** @brief Reinitializes this pooled node as a scalar value.
	 * @param index Payload field number associated with the value; 0 means that the value is not bound to a regular index.
	 * @param parentId Object tag number or array position assigned by parent; -1 for the root.
	 */
	void Init(Variant&& val, int index, TreeNode* parent, int parentId, ValueNode* valNode) {
		nodeParent_ = parent;
		parentId_ = parentId;
		valNode->SetValue(std::move(val));
		data_.value_ = valNode;
		data_.index_ = index;
	}

	friend class UpdatePathCursor;
	friend class TreeDataExtractor;
	template <concepts::OneOf<VariantArray, IntermediateIndexesData::IndexNodeData, h_vector<TreeNode*, 4>> V>
	friend void GetNextIndexData(std::span<const TagName>, TreeNode&, V&, IsSparse, KeyValueType, NullAllowed, const ConstPayload&);
	friend void NextByPath(std::span<const IndexedPathNode> path, const TreeNode& node, VariantArray& data);

	/// @brief Returns the tag-only path from the root, omitting array positions.
	TagsPath GetNodePath() const;
	/// @brief Returns the path from the root including both object tags and array positions.
	IndexedTagsPath GetNodeIndexedPath() const;

	/** @brief Creates an object child in an object or appends one to an array.
	 * @param key Field tag for an object parent; must be empty for an array parent.
	 */
	TreeNode& AddObject(TagName key);

	/// @brief Finds an object field, returning nullptr when the field is absent.
	const TreeNode* Object(TagName key) const {
		auto* object = GetIf<ObjectNode>();
		if (!object) {
			throw Error(errLogic, "Cannot access an object field on a non-object node");
		}
		return object->Get(key);
	}

	/// @brief Finds an object field, returning nullptr when the field is absent.
	TreeNode* Object(TagName key) {
		auto* object = GetIf<ObjectNode>();
		if (!object) {
			throw Error(errLogic, "Cannot access an object field on a non-object node");
		}
		return object->Get(key);
	}
	/** @brief Creates an array child in an object or appends one to an array.
	 * @param key Field tag for an object parent; must be empty for an array parent.
	 * @param count Initial child capacity; it does not set the logical array size.
	 */
	TreeNode& AddArray(TagName key, uint32_t count);

	/// @brief Returns the ordered children of an array node.
	const std::vector<TreeNode*, NodeAllocator<TreeNode*>>& Array() const {
		auto* array = GetIf<ArrayNode>();
		if (!array) {
			throw Error(errLogic, "Cannot access array elements on a non-array node");
		}
		return array->Values();
	}

	/** @brief Creates a scalar child in an object or appends one to an array.
	 * @param key Field tag for an object parent; must be empty for an array parent.
	 * @param index Payload field number associated with the value; 0 means no regular-index binding.
	 * @param isUpdated Convert an object field to the bound payload field type before storing it.
	 */
	TreeNode& AddValue(TagName key, Variant&& val, unsigned int index, bool isUpdated = false);

	/// @brief Returns the scalar stored by a value node.
	const Variant& Value() const {
		auto* value = GetIf<ValueNode>();
		if (!value) {
			throw Error(errLogic, "Cannot access a value on a non-value node");
		}
		return value->Value();
	}

	/// @brief Returns mutable access to the type-specific value storage.
	ValueNode& GetValueNode() {
		auto* value = GetIf<ValueNode>();
		if (!value) {
			throw Error(errLogic, "Cannot access a value on a non-value node");
		}
		return *value;
	}
	/// @brief Returns read-only access to the type-specific value storage.
	const ValueNode& GetValueNode() const {
		auto* value = GetIf<ValueNode>();
		if (!value) {
			throw Error(errLogic, "Cannot access a value on a non-value node");
		}
		return *value;
	}

	/// @brief Marks whether this array uses the flat homogeneous CJSON representation.
	void SetHomogeneousArray(bool v) {
		auto* arr = GetIf<ArrayNode>();
		if (!arr) {
			throw Error(errLogic, "Cannot set the homogeneous flag on a non-array node");
		}
		arr->SetHomogeneous(v);
	}
	/** @brief Associates this node with a regular payload field.
	 * @param index Payload field number; 0 removes the association.
	 */
	void SetIndex(int index) { data_.index_ = index; }
	/// @brief Returns the associated payload field number, or 0 when the node is not bound to a regular index.
	int GetIndex() const noexcept { return data_.index_; }

	/// @brief Replaces a scalar value, applying the bound field type and invalidating parent array normalization as needed.
	void UpdateValue(Variant val);

	/// @brief Replaces all array children, converts indexed values, and schedules affected arrays for normalization.
	void UpdateArray(VariantArray& newValues);
	/// @brief Recursively marks every array in this subtree for representation and index-binding normalization.
	void MarkArraysForNormalization();
	/// @brief Recursively normalizes marked arrays, processing children before their parents.
	void NormalizeMarkedArrays();

	/// @brief Returns the sparse-index number matching this node path, or 0 when the path is not sparse-indexed.
	int IsSparseIndex() const {
		TagsPath path = GetNodePath();
		FieldProperties fp = data_.contextIndex_.tm_.tags2field(path);
		if (fp.IsSparseIndex()) {
			const auto& sparseIndexData = data_.contextIndex_.tm_.SparseIndex(fp.SparseNumber());
			int indexId = data_.contextIndex_.getIndexByName_(sparseIndexData.name);
			return indexId;
		}
		return 0;
	}

	/// @brief Binds an unbound node to the regular payload field matching its tag path, if one exists.
	void ResolveIndexByPath() {
		if (data_.index_ != 0) {
			return;
		}

		const auto path = GetNodePath();
		const int index = data_.contextIndex_.tm_.tags2field(path).IndexNumber();
		if (index > 0) {
			data_.index_ = index;
		}
	}

	/// @brief Removes all children from an object or array; value nodes are left unchanged.
	void Clear() {
		std::visit(overloaded{
					   [](ObjectNode* object) { object->Clear(); },
					   [](ArrayNode* array) { array->Clear(); },
					   [](auto) {},
				   },
				   data_.value_);
	}

	/// @brief Reports whether this node must change its runtime kind to T.
	template <typename T>
	bool NeedsTypeChange() const noexcept {
		return !IsType<T>();
	}

	/// @brief Reports whether this node currently uses the type-specific storage T.
	template <typename T>
	bool IsType() const noexcept {
		return std::holds_alternative<T*>(data_.value_);
	}
	/// @brief Returns the type-specific storage when this node has kind T, otherwise nullptr.
	template <typename T>
	T* GetIf() noexcept {
		auto value = std::get_if<T*>(&data_.value_);
		return value ? *value : nullptr;
	}
	/// @brief Returns the type-specific storage when this node has kind T, otherwise nullptr.
	template <typename T>
	const T* GetIf() const noexcept {
		auto value = std::get_if<T*>(&data_.value_);
		return value ? *value : nullptr;
	}
	/// @brief Changes the runtime kind to an empty array and invalidates normalization of parent arrays.
	void ChangeToArray();
	/// @brief Changes the runtime kind to an empty object and invalidates normalization of parent arrays.
	void ChangeToObject();
	/// @brief Changes the runtime kind to an unset scalar value and invalidates normalization of parent arrays.
	void ChangeToValue();

	/** @brief Saves original data for the regular or sparse index associated with this node.
	 * @param plSave Payload containing the values from before the update.
	 * @param root Root used to extract JSON-path index values.
	 */
	void saveOldIndexData(IntermediateIndexesData& indexData, ConstPayload& plSave, TreeNode& root) const {
		int indexNum = GetIndex();
		if (indexNum > 0) {
			indexData.SaveOldIndexData(indexNum, plSave, root);
		} else {
			int index = IsSparseIndex();
			if (index > 0) {
				indexData.SaveOldIndexData(index, plSave, root);
			}
		}
	}

	/// @brief Reports whether the regular or sparse index associated with this node is an array index.
	bool IsArrayIndex() const;

	/// @brief Recursively saves original index data for every indexed value and array in this subtree.
	void SaveOldIndexDataForSubtree(IntermediateIndexesData& indexData, ConstPayload& plSave, TreeNode& root) const;

	/// @brief Appends a JSON-like diagnostic representation of this subtree, including index-binding markers.
	void Dump(std::string& outPut, const TagsMatcher& tm) const;

	/// @brief Returns the parent node, or nullptr for the root.
	TreeNode* Parent() { return nodeParent_; }
	/// @brief Returns the parent node, or nullptr for the root.
	const TreeNode* Parent() const { return nodeParent_; }
	/// @brief Returns the object tag number or array position assigned by the parent; -1 for the root.
	int ParentIndex() const noexcept { return parentId_; }

	/** @brief Removes an array child and renumbers the following child positions.
	 * @param id Zero-based position of the child to remove.
	 */
	void Drop(int id);
	/// @brief Removes the field with the specified tag from an object node.
	void Drop(TagName id);

	/// @brief Writes the untagged CJSON payload of a supported scalar value.
	void PutVariant(const Variant& kv, WrSerializer& wrSer) const;
	/** @brief Serializes this subtree to CJSON and rebuilds the payload fields represented by indexed nodes.
	 * @param tagName Tag assigned to this subtree by its parent; empty for the root.
	 * @param pvSave Payload value from before the update, used for unchanged fields.
	 * @param indexData Intermediate values for changed indexes not materialized directly in the tree.
	 * @param arrayBuf Scratch storage used to collect array-index payload values.
	 */
	void GetCJSON(WrSerializer& wrser, Payload& pl, TagName tagName, const PayloadValue& pvSave, const IntermediateIndexesData& indexData,
				  std::array<IndexedArrayData, kMaxIndexes>& arrayBuf) const;
	/** @brief Reports whether any node matching a tag path is nested through consecutive array levels.
	 * @param tp Tag-only path selecting nodes to inspect.
	 */
	bool ContainsMultidimensionalArray(const TagsPath& tp) const;
	/** @brief Performs recursive CJSON serialization and records payload fields encountered in the tree.
	 * @param tagName Tag assigned to this node by its parent.
	 * @param arrayBuf Scratch storage for array-index payload values.
	 * @param indexLoaded Bitmap of payload fields already written during traversal.
	 */
	void getCJSONInternal(WrSerializer& wrser, Payload& pl, TagName tagName, std::array<IndexedArrayData, kMaxIndexes>& arrayBuf,
						  std::bitset<kMaxIndexes>& indexLoaded) const;

private:
	/// @brief Switches to the supplied type-specific storage and invalidates normalization of parent arrays.
	template <typename T>
	void changeType(T* t) {
		if (!IsType<T>()) {
			MarkParentArraysForNormalization();
		}
		data_.value_.emplace<T*>(t);
	}

	/// @brief Marks every ancestor array for representation and index-binding normalization.
	void MarkParentArraysForNormalization();
	/// @brief Recursively binds scalar values or flat arrays to a regular payload field.
	void BindToIndex(int index);
	/// @brief Recomputes homogeneous representation and regular-index bindings for one array.
	void NormalizeCurrentArray(ArrayNode& array);

	TreeNode* nodeParent_ = nullptr;
	int parentId_ = -1;
	NodeData data_;
	NodeFactory& creator_;
};

class [[nodiscard]] NodeFactory {
public:
	NodeFactory() : treeNodes_{arena_}, objectNodes_{arena_}, arrayNodes_{arena_}, valueNodes_{arena_} {}
	void Reset() {
		treeNodes_.Reset();
		objectNodes_.Reset();
		arrayNodes_.Reset();
		valueNodes_.Reset();
	}

	TreeNode* CreateObject(TreeNode* parent, int parentId, ItemsTreeContext& contextIndex) {
		auto* object = objectNodes_.Get(true, arena_);
		auto* node = treeNodes_.Get(false, contextIndex, *this);
		node->Init(parent, parentId, object);
		return node;
	}
	TreeNode* CreateArray(size_t count, TreeNode* parent, int parentId, ItemsTreeContext& contextIndex) {
		auto* array = arrayNodes_.Get(true, arena_);
		auto* node = treeNodes_.Get(false, contextIndex, *this);
		node->Init(count, parent, parentId, array);
		return node;
	}
	TreeNode* CreateValue(Variant&& val, int index, TreeNode* parent, int parentId, ItemsTreeContext& contextIndex) {
		auto* value = valueNodes_.Get(true);
		auto* node = treeNodes_.Get(false, contextIndex, *this);
		node->Init(std::move(val), index, parent, parentId, value);
		return node;
	}

	ObjectNode* GetObjectNode() { return objectNodes_.Get(true, arena_); }
	ArrayNode* GetArrayNode() { return arrayNodes_.Get(true, arena_); }
	ValueNode* GetValueNode() { return valueNodes_.Get(true); }

private:
	NodeArena arena_;
	ReusablePool<TreeNode> treeNodes_;
	ReusablePool<ObjectNode> objectNodes_;
	ReusablePool<ArrayNode> arrayNodes_;
	ReusablePool<ValueNode> valueNodes_;
};

class [[nodiscard]] UpdatePathCursor {
public:
	UpdatePathCursor(TreeNode& root, std::span<const IndexedPathNode> path, ItemsTreeContext& ci) noexcept
		: root_{root}, path_{path}, ci_{ci} {}
	TreeNode* Next();

private:
	TreeNode* findFirst(TreeNode& node, std::span<const IndexedPathNode> path);

	TreeNode& root_;
	std::span<const IndexedPathNode> path_;
	ItemsTreeContext& ci_;
	TreeNode* current_ = nullptr;
	bool started_ = false;
	bool iterable_ = false;
};

template <concepts::OneOf<TreeNode, const TreeNode> Node>
class [[nodiscard]] ExistingPathCursor {
public:
	ExistingPathCursor(Node& root, std::span<const IndexedPathNode> path) noexcept : root_{root}, path_{path} {}
	Node* Next();

private:
	Node* findFirst(Node& node, std::span<const IndexedPathNode> path);

	Node& root_;
	std::span<const IndexedPathNode> path_;
	Node* current_ = nullptr;
	bool started_ = false;
	bool iterable_ = false;
};

using MutableExistingPathCursor = ExistingPathCursor<TreeNode>;
using ConstExistingPathCursor = ExistingPathCursor<const TreeNode>;

class [[nodiscard]] TagsPathCursor {
public:
	TagsPathCursor(const TreeNode& root, std::span<const TagName> path) noexcept : root_{root}, path_{path} {}
	const TreeNode* Next();

private:
	const TreeNode* findFirst(const TreeNode& node, std::span<const TagName> path);
	const TreeNode* findNext();

	const TreeNode& root_;
	std::span<const TagName> path_;
	const TreeNode* current_ = nullptr;
	bool started_ = false;
	bool iterable_ = false;
};

TreeNode* GetRootVectorIndexNode(TreeNode& root, const Index& index, int indexNum, const TagsMatcher& tm);

}  // namespace reindexer
