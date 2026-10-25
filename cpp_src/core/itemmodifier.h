#pragma once

#include <optional>
#include <utility>
#include "core/id_type.h"
#include "core/item_tree/intermediate_indexes_data.h"
#include "core/item_tree/item_tree.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/namespaceimpl.h"
#include "core/payload/payloadiface.h"
#include "function/expression_ast.h"
#include "function/precomputed_values.h"
#include "tools/serilize/wrserializer.h"
#include "updates/updaterecord.h"

namespace reindexer {

class NsContext;
class UpdateEntry;

enum class [[nodiscard]] IndexUpdateState { Unchanged, OldValueDeleted, NewValueInserted };

class [[nodiscard]] IndexRollBack {
public:
	IndexRollBack(int indexCount) : data_(indexCount), insertedKeys_(indexCount) {}
	void Reset(const PayloadValue& pv);
	void SetOldValueDeleted(size_t index, IsPk isPk) noexcept {
		assertrx_dbg(index < data_.size());
		assertrx_dbg(data_[index] == IndexUpdateState::Unchanged || data_[index] == IndexUpdateState::NewValueInserted);
		data_[index] = IndexUpdateState::OldValueDeleted;
		pkModified_ = pkModified_ || isPk;
	}
	void SetNewValueInserted(size_t index, IsPk isPk) noexcept {
		assertrx_dbg(index < data_.size());
		assertrx_dbg(data_[index] == IndexUpdateState::OldValueDeleted);
		data_[index] = IndexUpdateState::NewValueInserted;
		pkModified_ = pkModified_ || isPk;
	}
	void SetNewValueInserted(size_t index, IsPk isPk, VariantArray&& insertedKeys) noexcept {
		assertrx_dbg(index < insertedKeys_.size());
		insertedKeys_[index] = std::move(insertedKeys);
		SetNewValueInserted(index, isPk);
	}
	PayloadValue& GetPayloadValueBackup() noexcept { return pvSave_; }
	const std::vector<IndexUpdateState>& IndexStatus() const noexcept { return data_; }
	const VariantArray& InsertedKeys(size_t index) const noexcept {
		assertrx_dbg(index < insertedKeys_.size());
		return insertedKeys_[index];
	}
	bool IsPkModified() const noexcept { return pkModified_; }

private:
	std::vector<IndexUpdateState> data_;
	std::vector<VariantArray> insertedKeys_;
	PayloadValue pvSave_;
	bool pkModified_ = false;
};

class [[nodiscard]] ItemModifier {
public:
	ItemModifier(const std::vector<UpdateEntry>&, NamespaceImpl& ns, UpdatesContainer& replUpdates, const NsContext& ctx,
				 const functions::PrecomputedValues& precomputedValues);
	ItemModifier(const ItemModifier&) = delete;
	ItemModifier& operator=(const ItemModifier&) = delete;
	ItemModifier(ItemModifier&&) = delete;
	ItemModifier& operator=(ItemModifier&&) = delete;

	bool Modify(IdType itemId, const NsContext& ctx, UpdatesContainer& pendedRepl);
	PayloadValue& GetPayloadValueBackup() { return rollBackIndexData_.GetPayloadValueBackup(); }
	FloatVectorsIndexesValues TakeVectorsBeforePkChange() {
		return rollBackIndexData_.IsPkModified() ? std::move(vectorsBeforePkChange_) : FloatVectorsIndexesValues{};
	}

private:
	using CompositeFlags = h_vector<bool, 32>;
	class [[nodiscard]] FieldData {
	public:
		struct [[nodiscard]] BoundExpression {
			ExpressionAst ast;
			std::vector<int> indexBindings;
		};

		FieldData(const UpdateEntry& entry, NamespaceImpl& ns, CompositeFlags& affectedComposites);
		const UpdateEntry& Details() const noexcept { return entry_; }
		const IndexedTagsPath& Tagspath() const noexcept { return tagsPath_; }
		const IndexedTagsPath& TagspathWithLastIndex() const noexcept {
			return tagsPathWithLastIndex_ ? *tagsPathWithLastIndex_ : tagsPath_;
		}
		std::optional<TagIndex> ArrayIndex() const noexcept { return arrayIndex_; }
		int Index() const noexcept { return fieldIndex_; }
		bool IsIndex() const noexcept { return isIndex_; }
		bool IsUpdateArrayPart() const noexcept { return isUpdateArrayPart_; }
		std::string_view Name() const noexcept;
		BoundExpression& Expression(const NamespaceImpl& ns);

	private:
		struct [[nodiscard]] PathData {
			std::string jsonPath;
			TagsPath tagsPath;
			unsigned indexesCountInPath = 0;
			unsigned tailIndexesInPath = 0;
		};

		PathData preprocIndexedTagsPath(const NamespaceImpl& ns, const IndexedTagsPath& tp);
		bool initFromSimpleIndexName(std::string_view name, NamespaceImpl& ns);
		bool initFromSimpleJsonPath(std::string_view name, NamespaceImpl& ns);
		bool initFromIndexedIndexName(const NamespaceImpl& ns, const PathData& pathData, const IndexedTagsPath& indexedTagsPath);
		void initFromIndexedPath(std::string_view name, NamespaceImpl& ns);
		static TagsPath getTagsPathByIndexField(int& field, const NamespaceImpl& ns);

		void appendAffectedIndexes(const NamespaceImpl& ns, CompositeFlags& affectedComposites) const;

		const UpdateEntry& entry_;
		std::optional<BoundExpression> expression_;
		IndexedTagsPath tagsPath_;
		std::optional<IndexedTagsPath> tagsPathWithLastIndex_;
		int fieldIndex_{IndexValueType::SetByJsonPath};
		std::optional<TagIndex> arrayIndex_;
		bool isIndex_{false};
		bool isUpdateArrayPart_{false};
	};

	struct [[nodiscard]] VectorIndex {
		int vecField;
		h_vector<int, 2> autoEmbeddingFields;
	};

	struct [[nodiscard]] EmbeddingIndex {
		int autoEmbeddingField;
		h_vector<int, 2> vecFields;
	};

	class [[nodiscard]] EmbedderHelper {
	public:
		explicit EmbedderHelper(NamespaceImpl& ns) noexcept : ns_(ns) {}
		void Init(const std::vector<FieldData>& fieldsToModify) {
			prepareVectorIndexInfo();
			prepareSkipVectorIndexes(fieldsToModify);
		}
		const h_vector<int, 2>& GetVectorIndexForAutoEmbeddingField(int index) const {
			assertrx_throw(index < kMaxIndexes);
			assertrx_throw(index >= 0);
			for (const auto& k : autoEmbeddingIndexesList_) {
				if (k.autoEmbeddingField == index) {
					return k.vecFields;
				}
			}
			static const h_vector<int, 2> emptyVector{};
			return emptyVector;
		}
		bool TestSkipVectorIndex(int vectorIndex) const noexcept { return skipVectorIndexes_.test(vectorIndex); }
		const h_vector<int, 4>& GetVectorIndexesToModify() const noexcept { return vectorIndexesToModify_; }
		void AddVectorIndexToModify(const h_vector<int, 2>& indexes) {
			for (const auto ii : indexes) {
				if (std::find(vectorIndexesToModify_.begin(), vectorIndexesToModify_.end(), ii) == vectorIndexesToModify_.end()) {
					vectorIndexesToModify_.emplace_back(ii);
				}
			}
		}

		void AddVectorIndexToModify(int index) {
			if (std::find(vectorIndexesToModify_.begin(), vectorIndexesToModify_.end(), index) == vectorIndexesToModify_.end()) {
				vectorIndexesToModify_.emplace_back(index);
			}
		}

	private:
		void prepareVectorIndexInfo();
		void prepareSkipVectorIndexes(const std::vector<FieldData>& fieldsToModify);
		template <typename I>
		void addToIndex(int index, int vectorIndex, h_vector<I, 2>& indexContainer) {
			auto it = std::find_if(indexContainer.begin(), indexContainer.end(), [index](const auto& v) {
				auto& [key, val] = v;
				return key == index;
			});
			if (it == indexContainer.end()) {
				indexContainer.emplace_back(I{index, {vectorIndex}});
			} else {
				auto& [key, val] = *it;
				auto itVec = std::find(val.begin(), val.end(), vectorIndex);
				if (itVec == val.end()) {
					val.emplace_back(vectorIndex);
				}
			}
		}
		const h_vector<int, 2>& autoEmbeddingFieldsForVectorIndex(int field) const noexcept;

		h_vector<EmbeddingIndex, 2> autoEmbeddingIndexesList_;
		h_vector<VectorIndex, 2> vectorIndexesList_;

		std::bitset<kMaxIndexes> skipVectorIndexes_;

		h_vector<int, 4> vectorIndexesToModify_;

		NamespaceImpl& ns_;
	};

	void deleteItemFromComposite(IdType itemId, auto& indexesCacheCleaner);
	void insertItemIntoComposite(IdType itemId, auto& indexesCacheCleaner);

	void updateEmbedding(IdType itemId, const RdxContext& rdxContext, TreeNode& root, ConstPayload& oldPayload,
						 NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner, StringsHolder& strHolder);

	bool isPayloadModify(Payload& pl);
	void directPayloadModify(Payload& pl, IdType itemId, NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner, StringsHolder& strHolder);
	void cjsonTreeModify(Payload& pl, IdType itemId, NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner, StringsHolder& strHolder,
						 UpdatesContainer& replUpdates, const NsContext& ctx);
	void processFieldModeSet(TreeNode& root, FieldData& field, ConstPayload& cpl, VariantArray& values, IdType itemId);
	void processFieldModeSetJson(TreeNode& root, FieldData& field, ConstPayload& cpl, const VariantArray& values, IdType itemId);
	void processFieldModeDrop(TreeNode& root, FieldData& field, ConstPayload& cpl, IdType itemId);
	void materializeFloatVectorIndex(TreeNode& root, ConstPayload& cpl, IdType itemId, int index);
	void materializeAffectedFloatVectorNodes(TreeNode& root, const FieldData& field, ConstPayload& cpl, IdType itemId);
	void materializeChangedFloatVectorNodes(TreeNode& root, ConstPayload& cpl, IdType itemId);
	void materializeChangedFloatVectorIndexes(ConstPayload& cpl, IdType itemId);

	void processNodeSet(TreeNode& node, TreeNode& root, VariantArray& values, ConstPayload& cpl, IdType itemId);

	void updateIndexes(TreeNode& root, ConstPayload& cpl, IdType itemId, NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner,
					   StringsHolder& strHolder);
	void updateIndex(IdType itemId, int index, const VariantArray& oldKeys, const VariantArray& newKeys, VariantArray& res,
					 NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner, StringsHolder& strHolder);
	void saveUpdatedFloatVector(int index, const VariantArray& values);
	void saveVectorsBeforePkChange(IdType itemId);

	void updatePayload(Payload& pl, TreeNode& root, IdType itemId, NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner,
					   StringsHolder& strHolder, Variant cjsonV);

	NamespaceImpl& ns_;
	const std::vector<UpdateEntry>& updateEntries_;
	std::vector<FieldData> fieldsToModify_;

	class RollBack_ModifiedPayload;

	IndexRollBack rollBackIndexData_;
	CompositeFlags affectedComposites_;
	bool affectedCompositePK_ = false;
	int nonCompositePKIndex_ = -1;
	FloatVectorsIndexesValues vectorsBeforePkChange_;

	EmbedderHelper embedderHelper_;

	IntermediateIndexesData indexData_;
	const functions::PrecomputedValues& precomputedValues_;

	NodeFactory creator_;
	std::array<IndexedArrayData, kMaxIndexes> arrayBuf_{};	//{} - init to false
	ItemsTreeContext ci_;
	WrSerializer wrser_;
};

}  // namespace reindexer
