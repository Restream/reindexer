#include "itemmodifier.h"

#include <span>
#include "core/cjson/tagsmatcher.h"
#include "core/embedding/embedder.h"
#include "core/itemimpl.h"
#include "core/namespace/namespaceimpl.h"
#include "core/query/expression/expression_evaluator.h"
#include "core/query/expression/function_executor.h"
#include "core/query/expression/function_parser.h"
#include "estl/tokenizer.h"
#include "index/float_vector/float_vector_index.h"
#include "index/index.h"

namespace reindexer {

std::string_view ItemModifier::FieldData::Name() const noexcept { return entry_.Column(); }

ItemModifier::FieldData::PathData ItemModifier::FieldData::preprocIndexedTagsPath(const NamespaceImpl& ns,
																				  const IndexedTagsPath& tagsPath) {
	PathData pd;
	for (size_t i = 0; i < tagsPath.size(); ++i) {
		const auto& tag = tagsPath[i];
		if (tag.IsTagName()) {
			if (!pd.jsonPath.empty()) {
				pd.jsonPath += '.';
			}
			const TagName tagName = tag.GetTagName();
			pd.tagsPath.emplace_back(tagName);
			pd.jsonPath += ns.tagsMatcher_.tag2name(tagName);
			pd.tailIndexesInPath = 0;
		} else {
			++pd.indexesCountInPath;
			++pd.tailIndexesInPath;
		}
	}
	return pd;
}

bool ItemModifier::FieldData::initFromSimpleIndexName(std::string_view name, NamespaceImpl& ns) {
	if (ns.tryGetIndexByName(name, fieldIndex_)) {
		isIndex_ = true;
		tagsPath_ = IndexedTagsPath(getTagsPathByIndexField(fieldIndex_, ns));
		return true;
	}
	return false;
}

bool ItemModifier::FieldData::initFromSimpleJsonPath(std::string_view name, NamespaceImpl& ns) {
	if (fieldIndex_ = ns.payloadType_.FieldByJsonPath(name); fieldIndex_ > 0) {
		isIndex_ = true;
		tagsPath_ = ns.tagsMatcher_.path2indexedtag(name, CanAddField_True);
		return true;
	}
	return false;
}

bool ItemModifier::FieldData::initFromIndexedIndexName(const NamespaceImpl& ns, const PathData& pathData,
													   const IndexedTagsPath& indexedTagsPath) {
	// Handle 'idx_name[1]' for cases, when index name does not equal to jsonpath
	int fieldIndex = SetByJsonPath;
	if (ns.tryGetIndexByName(pathData.jsonPath, fieldIndex)) {
		auto tmpTagsPath = getTagsPathByIndexField(fieldIndex, ns);
		if ((tmpTagsPath.size() == 1) && (pathData.tailIndexesInPath == pathData.indexesCountInPath)) {
			IndexedTagsPath tmpIndexedTagsPath(tmpTagsPath);
			for (auto tagIt = (indexedTagsPath.end() - pathData.tailIndexesInPath); tagIt != indexedTagsPath.end(); ++tagIt) {
				tmpIndexedTagsPath.emplace_back(*tagIt);
			}
			tagsPath_ = std::move(tmpIndexedTagsPath);
			isIndex_ = true;
			fieldIndex_ = fieldIndex;
			return true;
		} else {
			const auto idxNameTagsPath = ns.tagsMatcher_.path2tag(ns.indexes_[fieldIndex]->Name());
			const bool idxNameAndJsonPathAreEqual = !idxNameTagsPath.empty() && (idxNameTagsPath == tmpTagsPath);
			if (!idxNameAndJsonPathAreEqual) [[unlikely]] {
				throw Error(
					errParams,
					"Unable to update index '{}' by name, because array structure is ambiguous. Use JSON-path('{}') instead of index name",
					pathData.jsonPath, ns.tagsMatcher_.Path2Name(tmpTagsPath));
			}
		}
	}
	return false;
}

void ItemModifier::FieldData::initFromIndexedPath(std::string_view name, NamespaceImpl& ns) {
	IndexedTagsPath indexedTagsPath = ns.tagsMatcher_.path2indexedtag(name, CanAddField_True);
	auto pathData = preprocIndexedTagsPath(ns, indexedTagsPath);

	if (!initFromIndexedIndexName(ns, pathData, indexedTagsPath)) {
		// Handle indexed jsonpath
		const auto field = ns.tagsMatcher_.tags2field(pathData.tagsPath);
		if (field.IsRegularIndex()) {
			fieldIndex_ = field.IndexNumber();
			isIndex_ = true;
		} else {
			isIndex_ = ns.tryGetIndexByJsonPath(pathData.jsonPath, fieldIndex_);
		}
		tagsPath_ = std::move(indexedTagsPath);
	}
	if (isIndex_) {
		if (fieldIndex_ < ns.payloadType_.NumFields()) {
			auto isForAll = false;
			for (const auto& pathNode : tagsPath_) {
				if (pathNode.IsTagIndex()) {
					if (pathNode.GetTagIndex().IsAll()) {
						isForAll = true;
					} else if (isForAll) [[unlikely]] {
						throw Error(errParams,
									"Expressions like 'field[*].field[1]=10' are supported for sparse indexes/non-index fields only");
					}
				}
			}
		}

		if (!tagsPath_.empty() && tagsPath_.back().IsTagIndexNotAll()) {
			tagsPathWithLastIndex_ = tagsPath_;
			arrayIndex_ = tagsPath_.back().GetTagIndex();
			tagsPath_.pop_back();
		}
	}
}

TagsPath ItemModifier::FieldData::getTagsPathByIndexField(int& field, const NamespaceImpl& ns) {
	assertrx_throw(field > 0);

	TagsPath tagsPath;
	const auto& idx = *ns.indexes_[field];
	auto totalJsonPaths = (idx.Opts().IsSparse() || static_cast<int>(field) >= ns.payloadType_.NumFields())
							  ? idx.Fields().size()
							  : ns.payloadType_.Field(field).JsonPaths().size();

	if (totalJsonPaths != 1) {
		assertrx_dbg(totalJsonPaths);
		throw Error(errParams, "Ambiguity when updating field with several json paths by index name: '{}'", idx.Name());
	}

	const auto& fields{idx.Fields()};
	if (fields[0] == IndexValueType::SetByJsonPath) {
		tagsPath = fields.getTagsPath(0);
	} else {
		field = fields[0];	// 'Composite' index with single subindex
		const auto& jsonPath = ns.payloadType_.Field(field).JsonPaths()[0];
		tagsPath = ns.tagsMatcher_.path2tag(jsonPath);
		if (tagsPath.empty()) {
			throw Error(errParams, "Cannot find field by json: '{}'", jsonPath);
		}
	}
	return tagsPath;
}

void ItemModifier::FieldData::appendAffectedIndexes(const NamespaceImpl& ns, CompositeFlags& affectedComposites) const {
	const auto firstCompositePos = ns.indexes_.firstCompositePos();
	const auto firstSparsePos = ns.indexes_.firstSparsePos();
	const auto totalIndexes = ns.indexes_.totalSize();
	const bool isRegularIndex = IsIndex() && Index() < firstSparsePos;
	const bool isSparseIndex = IsIndex() && Index() >= firstSparsePos && Index() < firstCompositePos;
	if (isSparseIndex) {
		// Composite indexes can not be created over sparse indexes, so just skipping rest of the checks for them
		return;
	}
	const bool isCompositeIndex = IsIndex() && Index() >= firstCompositePos;
	if (isCompositeIndex) {
		// Composite indexes can not be created over another composite indexes, so just skipping rest of the checks for them
		return;
	}
	std::bitset<kMaxIndexes> affected;
	if (isRegularIndex) {
		affected.set(Index());
	} else {
		for (int i = 0; i < firstSparsePos; ++i) {
			const auto& ptField = ns.payloadType_.Field(i);
			for (const auto& jpath : ptField.JsonPaths()) {
				auto tp = ns.tagsMatcher_.path2tag(jpath);
				if (Tagspath().IsNestedOrEqualTo(tp)) {
					affected.set(i);
					break;
				}
			}
		}
	}

	for (int i = firstCompositePos; i < totalIndexes; ++i) {
		const auto& fields = ns.indexes_[i]->Fields();
		const auto idxId = i - firstCompositePos;

		for (const auto f : fields) {
			if (f == IndexValueType::SetByJsonPath) {
				continue;
			}
			if (affected.test(f)) {
				affectedComposites[idxId] = true;
				break;
			}
		}
		if (affectedComposites[idxId]) {
			continue;
		}

		if (!IsIndex()) {
			// Fulltext composites may be created over non-index fields
			for (size_t tp = 0, end = fields.getTagsPathsLength(); tp < end; ++tp) {
				if (Tagspath().IsNestedOrEqualTo(fields.getTagsPath(tp))) {
					affectedComposites[idxId] = true;
					break;
				}
			}
		}
	}
}

class [[nodiscard]] ItemModifier::RollBack_ModifiedPayload final : private RollBackBase {
public:
	RollBack_ModifiedPayload(ItemModifier& modifier, IdType id) noexcept : modifier_{modifier}, itemId_{id} {}
	RollBack_ModifiedPayload(RollBack_ModifiedPayload&&) noexcept = default;
	~RollBack_ModifiedPayload() override { RollBack(); }

	// NOLINTNEXTLINE(bugprone-exception-escape) Termination here is better, than inconsistent state of the user's data
	void RollBack() noexcept {
		if (IsDisabled()) {
			return;
		}
		auto indexesCacheCleaner{modifier_.ns_.GetIndexesCacheCleaner()};
		const std::vector<bool>& data = modifier_.rollBackIndexData_.IndexStatus();
		PayloadValue& plValue = modifier_.ns_.items_[itemId_];
		NamespaceImpl::IndexesStorage& indexes = modifier_.ns_.indexes_;

		Payload plSave(modifier_.ns_.payloadType_, modifier_.rollBackIndexData_.GetPayloadValueBackup());

		Payload plCur(modifier_.ns_.payloadType_, plValue);
		VariantArray cjsonKref;
		plCur.Get(0, cjsonKref);

		VariantArray res;

		for (size_t i = indexes.firstCompositePos(); i < size_t(indexes.totalSize()) && i < data.size(); ++i) {
			if (data[i]) {
				bool needClearCache{false};
				indexes[i]->Delete(Variant(plValue), itemId_, MustExist_False, *modifier_.ns_.strHolder(), needClearCache);
				if (needClearCache) {
					indexesCacheCleaner.Add(*indexes[i]);
				}
			}
		}

		for (size_t i = 1; i < data.size() && i < size_t(indexes.firstCompositePos()); i++) {
			if (data[i]) {
				bool needClearCache{false};
				ConstPayload cpl = ConstPayload(modifier_.ns_.payloadType_, plValue);
				VariantArray vals;
				if (indexes[i]->Opts().IsSparse()) {
					try {
						cpl.GetByJsonPath(indexes[i]->Fields().getTagsPath(0), vals, indexes[i]->KeyType());
					} catch (const Error&) {
						vals.resize(0);
					}
				} else {
					cpl.Get(i, vals);
				}

				indexes[i]->Delete(vals, itemId_, MustExist_False, *modifier_.ns_.strHolder(), needClearCache);
				if (needClearCache) {
					indexesCacheCleaner.Add(*indexes[i]);
				}

				VariantArray oldData;
				if (indexes[i]->Opts().IsSparse()) {
					try {
						plSave.GetByJsonPath(indexes[i]->Fields().getTagsPath(0), oldData, indexes[i]->KeyType());
					} catch (const Error&) {
						oldData.resize(0);
					}
				} else {
					plSave.Get(i, oldData);
				}
				VariantArray result;
				indexes[i]->Upsert(result, oldData, itemId_, needClearCache);
				if (!indexes[i]->Opts().IsSparse()) {
					Payload pl{modifier_.ns_.payloadType_, modifier_.ns_.items_[itemId_]};
					pl.Set(i, result);
				}
				if (needClearCache) {
					indexesCacheCleaner.Add(*indexes[i]);
				}
			}
		}

		// Compare cjson tuples
		Variant oldTuple = plSave.Get(0, 0);
		if (const Variant& v = cjsonKref.front(); oldTuple != v) {
			bool needClearCache{false};
			indexes[0]->Delete(v, itemId_, MustExist_True, *modifier_.ns_.strHolder(), needClearCache);
			indexes[0]->Upsert(res, VariantArray{std::move(oldTuple)}, itemId_, needClearCache);
			plCur.Set(0, res);
		}

		for (size_t i = indexes.firstCompositePos(); i < size_t(indexes.totalSize()) && i < data.size(); ++i) {
			if (data[i]) {
				bool needClearCache{false};
				std::ignore = indexes[i]->Upsert(Variant(modifier_.rollBackIndexData_.GetPayloadValueBackup()), itemId_, needClearCache);
				if (needClearCache) {
					indexesCacheCleaner.Add(*indexes[i]);
				}
			}
		}
	}
	using RollBackBase::Disable;

	RollBack_ModifiedPayload(const RollBack_ModifiedPayload&) = delete;
	RollBack_ModifiedPayload operator=(const RollBack_ModifiedPayload&) = delete;
	RollBack_ModifiedPayload operator=(RollBack_ModifiedPayload&&) = delete;

private:
	ItemModifier& modifier_;
	IdType itemId_;
};

ItemModifier::FieldData::FieldData(const UpdateEntry& entry, NamespaceImpl& ns, CompositeFlags& affectedComposites)
	: entry_(entry), tagsPathWithLastIndex_{std::nullopt} {
	if (!initFromSimpleIndexName(entry_.Column(), ns)) {
		if (!initFromSimpleJsonPath(entry_.Column(), ns)) {
			initFromIndexedPath(entry_.Column(), ns);
		}
	}
	if (tagsPath_.empty()) [[unlikely]] {
		throw Error(errParams, "Cannot find field by json: '{}'", entry_.Column());
	}
	appendAffectedIndexes(ns, affectedComposites);
}

ItemModifier::ItemModifier(const std::vector<UpdateEntry>& updateEntries, NamespaceImpl& ns, UpdatesContainer& replUpdates,
						   const NsContext& ctx)
	: ns_(ns),
	  updateEntries_(updateEntries),
	  rollBackIndexData_(ns_.indexes_.totalSize()),
	  affectedComposites_(ns_.indexes_.totalSize() - ns_.indexes_.firstCompositePos(), false),
	  vectorIndexes_(ns_.getVectorIndexes()),
	  embedderHelper_(ns_) {
	const auto oldTmV = ns_.tagsMatcher_.version();
	for (const UpdateEntry& updateField : updateEntries_) {
		for (const auto& v : updateField.Values()) {
			v.Type().EvaluateOneOf([](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
													  KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid, KeyValueType::Null,
													  KeyValueType::Undefined, KeyValueType::FloatVector> auto) {},
								   [](KeyValueType::Tuple) {
									   throw Error(
										   errParams,
										   "Unable to use 'tuple'-value (array of arrays, array of points, etc) in UPDATE-query. Only "
										   "single dimensional arrays and arrays of objects are supported");
								   },
								   [](KeyValueType::Composite) {
									   throw Error(errParams,
												   "Unable to use 'composite'-value (object, array of objects, etc) in UPDATE-query. "
												   "Probably 'object'/'json' type was not explicitly set in the query");
								   });
		}
		fieldsToModify_.emplace_back(updateField, ns_, affectedComposites_);
	}
	embedderHelper_.Init(fieldsToModify_);
	ns_.replicateTmUpdateIfRequired(replUpdates, oldTmV, ctx);
}

void ItemModifier::EmbedderHelper::prepareVectorIndexInfo() {
	for (int fieldIndex = 1, numFields = ns_.payloadType_.NumFields(); fieldIndex < numFields; ++fieldIndex) {
		std::shared_ptr<const UpsertEmbedder> embedder = ns_.payloadType_.Field(fieldIndex).UpsertEmbedder();
		if (embedder) {
			for (const auto& field : embedder->Fields()) {
				int idx = 0;
				if (!ns_.tryGetIndexByName(field, idx)) {
					throw Error{errConflict, "Configuration 'embedding:upsert_embedder' configured with invalid base field name '{}'",
								field};
				}
				if (idx >= ns_.indexes_.firstSparsePos()) [[unlikely]] {
					throw Error(errParams,
								"Auto embedding not supported for composite and sparce indexes."
								"Field '{}' for embedding is invalid in namespace {}.",
								field, ns_.name_);
				}
				addToIndex(idx, fieldIndex, autoEmbeddingIndexesList_);
				addToIndex(fieldIndex, idx, vectorIndexesList_);
			}
		}
	}
}

const h_vector<int, 2>& ItemModifier::EmbedderHelper::autoEmbeddingFieldsForVectorIndex(int field) const noexcept {
	for (const auto& v : vectorIndexesList_) {
		if (v.vecField == field) {
			return v.autoEmbeddingFields;
		}
	}
	static const h_vector<int, 2> empty;
	return empty;
}

void ItemModifier::EmbedderHelper::RecalcVectorIndexesToModify(const Payload& pl, const ConstPayload& plOld) {
	for (const auto& [field, _] : vectorIndexesList_) {
		if (std::find(vectorIndexesToModify_.begin(), vectorIndexesToModify_.end(), field) != vectorIndexesToModify_.end()) {
			continue;
		}
		std::shared_ptr<const UpsertEmbedder> embedder = ns_.payloadType_.Field(field).UpsertEmbedder();
		if (!embedder) {
			continue;
		}
		for (int f : autoEmbeddingFieldsForVectorIndex(field)) {
			VariantArray fldDataNew;
			pl.Get(f, fldDataNew);
			VariantArray fldDataOld;
			plOld.Get(f, fldDataOld);
			if (fldDataNew != fldDataOld) {
				AddVectorIndexToModify(field);
				break;
			}
		}
	}
}

void ItemModifier::EmbedderHelper::prepareSkipVectorIndexes(const std::vector<FieldData>& fieldsToModify) {
	for (const FieldData& field : fieldsToModify) {
		if (field.IsIndex() && !autoEmbeddingFieldsForVectorIndex(field.Index()).empty()) {
			if (field.Details().IsExpression()) {
				assertrx_throw(field.Details().Values().size() > 0);
				std::string v = field.Details().Values().front().As<std::string>();
				Tokenizer tokenizer(v);
				Token tok = tokenizer.NextToken();
				auto parsedFunction = QueryFunctionParser::ParseFunction(tokenizer, tok);
				if (parsedFunction.funcName == "skip_embedding") {
					skipVectorIndexes_.set(field.Index());
					continue;
				} else {
					throw Error(errParams, "Incorrect value {}", field.Details().Values().front().As<std::string>());
				}
			}
			if (field.Details().Mode() == FieldModeSet) {  // value is set, embedding_strategy is ignored
				skipVectorIndexes_.set(field.Index());
			}
		}
	}
}

bool ItemModifier::Modify(IdType itemId, const NsContext& ctx, UpdatesContainer& replUpdates) {
	PayloadValue& pv = ns_.items_[itemId];
	Payload pl(ns_.payloadType_, pv);
	pv.Clone(pl.RealSize());

	rollBackIndexData_.Reset(itemId, ns_.payloadType_, pv, ns_.getVectorIndexes());
	RollBack_ModifiedPayload rollBack = RollBack_ModifiedPayload(*this, itemId);
	FunctionExecutor funcExecutor(ns_, replUpdates);
	ExpressionEvaluator ev(ns_, funcExecutor);

	auto indexesCacheCleaner = ns_.GetIndexesCacheCleaner();

	deleteItemFromComposite(itemId, indexesCacheCleaner);
	try {
		VariantArray values;
		bool recalcEmbeddersOnSetObject = false;
		for (FieldData& field : fieldsToModify_) {
			if (field.IsIndex() && field.Details().IsExpression() && embedderHelper_.TestSkipVectorIndex(field.Index())) {
				continue;
			}
			// values must be assigned a value in if else below
			if (field.Details().IsExpression()) {
				assertrx(field.Details().Values().size() > 0);
				values = ev.Evaluate(static_cast<std::string_view>(field.Details().Values().front()), pv, field.Name(), ctx);
			} else {
				values = field.Details().Values();
			}

			if (values.IsArrayValue() && field.TagspathWithLastIndex().back().IsTagIndex()) {
				throw Error(errParams, "Array items are supposed to be updated with a single value, not an array");
			}

			if (field.Details().Mode() == FieldModeSetJson || !field.IsIndex()) {
				modifyCJSON(itemId, field, values, replUpdates, ctx);
				if (field.Details().Mode() == FieldModeSetJson) {
					recalcEmbeddersOnSetObject = true;
				}
			} else {
				const auto& vectorIndex = embedderHelper_.GetVectorIndexForAutoEmbeddingField(field.Index());
				if (!vectorIndex.empty()) {
					VariantArray fldData;
					pl.Get(field.Index(), fldData);
					if (fldData != values) {
						embedderHelper_.AddVectorIndexToModify(vectorIndex);
					}
				}
				modifyField(itemId, field, pl, values);
			}
		}
		if (recalcEmbeddersOnSetObject) {
			ConstPayload plOld(ns_.payloadType_, rollBackIndexData_.GetPayloadValueBackup());
			embedderHelper_.RecalcVectorIndexesToModify(pl, plOld);
		}
		updateEmbedding(itemId, ctx.rdxContext, pl);
	} catch (...) {
		insertItemIntoComposite(itemId, indexesCacheCleaner);
		throw;
	}

	insertItemIntoComposite(itemId, indexesCacheCleaner);
	if (rollBackIndexData_.IsPkModified()) {
		ns_.checkUniquePK(ConstPayload(ns_.payloadType_, pv), ctx.IsInTransaction(), ctx.rdxContext);
	}
	rollBack.Disable();

	ns_.markUpdated(IndexOptimization::Partial);

	return rollBackIndexData_.IsPkModified();
}

void ItemModifier::modifyCJSON(IdType id, FieldData& field, VariantArray& values, UpdatesContainer& replUpdates, const NsContext& ctx) {
	PayloadValue& plData = ns_.items_[id];
	const PayloadTypeImpl& pti(*ns_.payloadType_.get());
	Payload pl(pti, plData);
	VariantArray cjsonKref;
	pl.Get(0, cjsonKref);
	cjsonCache_.Reset();

	const Variant& v = cjsonKref.front();
	if (v.Type().Is<KeyValueType::String>()) {
		cjsonCache_.Assign(std::string_view(p_string(v)));
	}

	ItemImpl itemimpl(ns_.payloadType_, plData, ns_.tagsMatcher_);
	itemimpl.Unsafe(true);
	itemimpl.CopyIndexedVectorsValuesFrom(id, vectorIndexes_);
	itemimpl.ModifyField(field.Tagspath(), values, field.Details().Mode());

	Item item = ns_.newItem();
	Error err = item.Unsafe().FromCJSON(itemimpl.GetCJSON(true));
	if (!err.ok()) {
		pl.Set(0, cjsonKref);
		throw err;
	}

	item.setID(id);
	ItemImpl* impl = item.impl_;
	ns_.setFieldsBasedOnPrecepts(impl, replUpdates, ctx);
	ns_.updateTagsMatcherFromItem(impl, ctx);

	Payload plNew = impl->GetPayload();
	plData.Clone(pl.RealSize());

	auto strHolder = ns_.strHolder();
	auto indexesCacheCleaner{ns_.GetIndexesCacheCleaner()};
	assertrx(ns_.indexes_.firstCompositePos() != 0);
	const int borderIdx = ns_.indexes_.totalSize() > 1 ? 1 : 0;
	int fieldIdx = borderIdx;
	do {
		// update the indexes, and then tuple (1,2,...,0)
		fieldIdx %= ns_.indexes_.firstCompositePos();
		Index& index = *(ns_.indexes_[fieldIdx]);
		const IsSparse isIndexSparse = index.Opts().IsSparse();
		assertrx(!isIndexSparse || (isIndexSparse && index.Fields().getTagsPathsLength() > 0));

		if (isIndexSparse) {
			assertrx(index.Fields().getTagsPathsLength() > 0);
			try {
				plNew.GetByJsonPath(index.Fields().getTagsPath(0), ns_.skrefs, index.KeyType());
			} catch (const Error&) {
				ns_.skrefs.resize(0);
			}
		} else {
			plNew.Get(fieldIdx, ns_.skrefs);
		}

		if (index.Opts().GetCollateMode() == CollateUTF8) {
			for (auto& key : ns_.skrefs) {
				key.EnsureUTF8();
			}
		}

		if ((fieldIdx == 0) && (cjsonCache_.Size() > 0)) {
			bool needClearCache{false};
			index.Delete(Variant(cjsonCache_.Get()), id, MustExist_True, *strHolder, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(index);
			}
		} else {
			if (isIndexSparse) {
				try {
					pl.GetByJsonPath(index.Fields().getTagsPath(0), ns_.krefs, index.KeyType());
				} catch (const Error&) {
					ns_.krefs.resize(0);
				}
			} else if (index.Opts().IsArray()) {
				pl.Get(fieldIdx, ns_.krefs, Variant::hold);
			} else if (index.Opts().IsFloatVector()) {
				const FloatVectorIndex& fvIdx = static_cast<FloatVectorIndex&>(index);
				const ConstFloatVectorView fvView = fvIdx.GetFloatVectorView(id);
				ns_.krefs.clear<false>();
				ns_.krefs.emplace_back(fvView);
			} else {
				pl.Get(fieldIdx, ns_.krefs);
			}
			if (ns_.krefs == ns_.skrefs) {
				// Do not modify indexes, if documents content was not changed
				continue;
			}

			bool needClearCache{false};
			rollBackIndexData_.IndexChanged(fieldIdx, index.Opts().IsPK());
			index.Delete(ns_.krefs, id, MustExist_True, *strHolder, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(index);
			}
		}

		ns_.krefs.resize(0);
		bool needClearCache{false};
		rollBackIndexData_.IndexChanged(fieldIdx, index.Opts().IsPK());
		index.Upsert(ns_.krefs, ns_.skrefs, id, needClearCache);
		if (needClearCache) {
			indexesCacheCleaner.Add(index);
		}

		if (!isIndexSparse) {
			pl.Set(fieldIdx, ns_.krefs);
		}
	} while (++fieldIdx != borderIdx);

	impl->RealValue() = plData;
}

void ItemModifier::deleteItemFromComposite(IdType itemId, auto& indexesCacheCleaner) {
	auto strHolder = ns_.strHolder();
	const auto firstCompositePos = ns_.indexes_.firstCompositePos();
	const auto totalIndexes = firstCompositePos + ns_.indexes_.compositeIndexesSize();
	for (int i = firstCompositePos; i < totalIndexes; ++i) {
		if (affectedComposites_[i - firstCompositePos]) {
			bool needClearCache{false};
			const auto& compositeIdx = ns_.indexes_[i];
			rollBackIndexData_.IndexChanged(i, compositeIdx->Opts().IsPK());
			compositeIdx->Delete(Variant(ns_.items_[itemId]), itemId, MustExist_True, *strHolder, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(*compositeIdx);
			}
		}
	}
}

void ItemModifier::insertItemIntoComposite(IdType itemId, auto& indexesCacheCleaner) {
	const auto totalIndexes = ns_.indexes_.totalSize();
	const auto firstCompositePos = ns_.indexes_.firstCompositePos();
	for (int i = firstCompositePos; i < totalIndexes; ++i) {
		if (affectedComposites_[i - firstCompositePos]) {
			bool needClearCache{false};
			auto& compositeIdx = ns_.indexes_[i];
			rollBackIndexData_.IndexChanged(i, compositeIdx->Opts().IsPK());
			std::ignore = compositeIdx->Upsert(Variant(ns_.items_[itemId]), itemId, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(*compositeIdx);
			}
		}
	}
}

void convertToFloatVector(FloatVectorDimension dim, VariantArray& values) {
	if (values.empty()) {
		values.emplace_back(ConstFloatVectorView{});
		std::ignore = values.MarkArray(false);
		return;
	} else if (values.IsNullValue()) {
		assertrx_dbg(values.size() == 1);
		values[0] = Variant{ConstFloatVectorView{}};
		return;
	}
	if (values.size() != dim.Value()) {
		throw Error(errParams, "It's not possible to Update float vector index of dimension {} with array of size {}", dim.Value(),
					values.size());
	}
	auto vect = FloatVector::CreateNotInitialized(dim);
	for (size_t i = 0, s = values.size(); i != s; ++i) {
		vect.RawData()[i] = values[i].As<double>();
	}
	values.clear();
	values.emplace_back(std::move(vect));
}

void ItemModifier::modifyField(IdType itemId, FieldData& field, Payload& pl, VariantArray& values) {
	assertrx_throw(field.IsIndex());
	Index& index = *(ns_.indexes_[field.Index()]);
	if (!index.Opts().IsSparse() && field.Details().Mode() == FieldModeDrop /*&&
		!(field.ArrayIndex() != IndexValueType::NotSet || field.tagspath().back().IsArrayNode())*/) {	 // TODO #1218 allow to drop array fields
		throw Error(errLogic, "It's only possible to drop sparse or non-index fields via UPDATE statement!");
	}

	if (index.IsFloatVector() && (values.IsArrayValue() || values.IsNullValue() || values.empty())) {
		convertToFloatVector(static_cast<FloatVectorIndex&>(index).Dimension(), values);
	}

	assertrx_throw(!index.Opts().IsSparse() || index.Fields().getTagsPathsLength() > 0);
	if (!index.Opts().IsArray()) {
		if (values.IsArrayValue()) {
			throw Error(errParams, "It's not possible to Update single index fields with arrays!");
		}

		if (!index.IsFloatVector() && field.TagspathWithLastIndex().back().IsTagIndex()) {
			throw Error(errParams, "Can't Update non-array index fields using array index");
		}
	}

	if (index.Opts().GetCollateMode() == CollateUTF8) {
		for (const Variant& key : values) {
			key.EnsureUTF8();
		}
	}

	auto strHolder = ns_.strHolder();
	auto indexesCacheCleaner{ns_.GetIndexesCacheCleaner()};

	bool fvRequiresTupleUpdate = false;
	if (index.IsFloatVector()) {
		// TODO: We should modify CJSON for any default value #1837
		// If float vector is changing it's size, than we have to perform tuple update, to handle null and missing values properly
		for (Variant& key : values) {
			std::ignore = key.convert(KeyValueType::FloatVector{});
		}
		assertrx_throw(values.size() <= 1);
		const auto oldSize = pl.Get(field.Index(), 0).As<ConstFloatVectorView>().Dimension();
		const auto newSize = values.empty() ? FloatVectorDimension() : values[0].As<ConstFloatVectorView>().Dimension();
		fvRequiresTupleUpdate = (oldSize != newSize);
	}

	modifyIndexValues(itemId, field, values, pl);

	if (index.Opts().IsSparse() || index.Opts().IsArray() || index.KeyType().Is<KeyValueType::Uuid>() || fvRequiresTupleUpdate) {
		ItemImpl item(ns_.payloadType_, *(pl.Value()), ns_.tagsMatcher_);
		Variant oldTupleValue = item.GetField(0);
		std::ignore = oldTupleValue.EnsureHold();

		item.ModifyField(field.TagspathWithLastIndex(), values, field.Details().Mode());

		bool needClearCache{false};
		auto& tupleIdx = ns_.indexes_[0];
		tupleIdx->Delete(oldTupleValue, itemId, MustExist_True, *strHolder, needClearCache);
		auto tupleValue = tupleIdx->Upsert(item.GetField(0), itemId, needClearCache);
		if (needClearCache) {
			indexesCacheCleaner.Add(*tupleIdx);
		}
		pl.Set(0, std::move(tupleValue));
	}
}

void ItemModifier::modifyIndexValues(IdType itemId, const FieldData& field, VariantArray& values, Payload& pl) {
	Index& index = *(ns_.indexes_[field.Index()]);
	if (values.IsNullValue() && !index.Opts().IsArray()) {
		throw Error(errParams, "Non-array index fields cannot be set to null!");
	}
	auto strHolder = ns_.strHolder();
	auto indexesCacheCleaner{ns_.GetIndexesCacheCleaner()};
	bool updateArrayPart = field.ArrayIndex() && !field.ArrayIndex()->IsAll();
	bool isForAllItems = false;
	for (const auto& tag : field.Tagspath()) {
		if (tag.IsTagIndex()) {
			updateArrayPart = true;
			if (tag.GetTagIndex().IsAll()) {
				isForAllItems = true;
				break;
			}
		}
	}

	ns_.krefs.resize(0);
	for (Variant& key : values) {
		std::ignore = key.convert(index.KeyType());
	}

	if (index.Opts().IsArray() && updateArrayPart && !index.Opts().IsSparse()) {
		if (!values.IsArrayValue() && values.empty()) {
			throw Error(errParams, "Cannot update array item with an empty value");	 // TODO #1218 maybe delete this
		}
		int offset = -1, length = -1;
		ns_.skrefs = pl.GetIndexedArrayData(field.TagspathWithLastIndex(), field.Index(), offset, length);
		if (offset < 0 || length < 0) {
			const auto& path = field.TagspathWithLastIndex();
			std::string indexesStr;
			for (const auto& p : path) {
				if (p.IsTagIndexNotAll()) {
					indexesStr += '[';
					indexesStr.append(std::to_string(p.GetTagIndex().AsNumber()));
					indexesStr += ']';
				}
			}
			throw Error(errParams, "Requested array's index was not found: {}", indexesStr);
		}

		if (ns_.skrefs == values) {
			// Index value was not changed
			return;
		}

		if (!ns_.skrefs.empty()) {
			bool needClearCache{false};
			rollBackIndexData_.IndexChanged(field.Index(), index.Opts().IsPK());

			index.Delete(ns_.skrefs, itemId, MustExist_True, *strHolder, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(index);
			}
		}

		bool needClearCache{false};
		rollBackIndexData_.IndexChanged(field.Index(), index.Opts().IsPK());
		index.Upsert(ns_.krefs, values, itemId, needClearCache);
		if (needClearCache) {
			indexesCacheCleaner.Add(index);
		}

		if (isForAllItems) {
			for (int i = offset, end = offset + length; i < end; ++i) {
				pl.Set(field.Index(), i, ns_.krefs.front());
			}
		} else {
			VariantArray v;
			pl.Get(field.Index(), v);
			size_t sizeDiff = 0;
			// Array may be resized
			if (size_t(length) < ns_.krefs.size()) {
				sizeDiff = ns_.krefs.size() - length;
				std::ignore = v.insert(v.begin() + offset, ns_.krefs.begin(), ns_.krefs.begin() + sizeDiff);
			} else if (size_t(length) > ns_.krefs.size()) {
				std::ignore = v.erase(v.cbegin() + offset, v.cbegin() + offset + length - ns_.krefs.size());
			}
			std::ignore = std::copy(ns_.krefs.begin() + sizeDiff, ns_.krefs.end(), v.begin() + offset + sizeDiff);
			pl.Set(field.Index(), v);
		}
	} else {
		if (index.Opts().IsSparse()) {
			pl.GetByJsonPath(field.TagspathWithLastIndex(), ns_.skrefs, index.KeyType());
		} else if (index.Opts().IsFloatVector()) {
			const FloatVectorIndex& fvIdx = static_cast<FloatVectorIndex&>(index);
			const ConstFloatVectorView fvView = fvIdx.GetFloatVectorView(itemId);
			ns_.skrefs.clear<false>();
			ns_.skrefs.emplace_back(fvView);
		} else {
			pl.Get(field.Index(), ns_.skrefs, Variant::hold);
		}

		// Required when updating index array field with several tagpaths
		VariantArray concatValues;
		int offset = -1, length = -1;
		if (index.Opts().IsArray() && !updateArrayPart) {
			std::ignore = pl.GetIndexedArrayData(field.TagspathWithLastIndex(), field.Index(), offset, length);
		}
		const bool kConcatIndexValues =
			index.Opts().IsArray() && !updateArrayPart &&
			(length < int(ns_.skrefs.size()));	//  (length < int(ns_.skrefs.size()) - condition to avoid copying
												//  when length and ns_.skrefs.size() are equal; then concatValues == values
		if (kConcatIndexValues) {
			if (offset < 0 || length < 0) {
				concatValues = ns_.skrefs;
				std::ignore = concatValues.insert(concatValues.cend(), values.begin(), values.end());
			} else {
				std::ignore = concatValues.insert(concatValues.cend(), ns_.skrefs.begin(), ns_.skrefs.begin() + offset);
				std::ignore = concatValues.insert(concatValues.cend(), values.begin(), values.end());
				std::ignore = concatValues.insert(concatValues.cend(), ns_.skrefs.begin() + offset + length, ns_.skrefs.end());
			}
		}

		if (ns_.skrefs == (kConcatIndexValues ? concatValues : values)) {
			// Index value was not changed
			return;
		}

		if (!ns_.skrefs.empty()) {
			bool needClearCache{false};
			rollBackIndexData_.IndexChanged(field.Index(), index.Opts().IsPK());
			index.Delete(ns_.skrefs, itemId, MustExist_True, *strHolder, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(index);
			}
		}

		bool needClearCache{false};
		rollBackIndexData_.IndexChanged(field.Index(), index.Opts().IsPK());
		index.Upsert(ns_.krefs, kConcatIndexValues ? concatValues : values, itemId, needClearCache);
		if (needClearCache) {
			indexesCacheCleaner.Add(index);
		}
		if (!index.Opts().IsSparse()) {
			pl.Set(field.Index(), ns_.krefs);
		}
	}
}

void ItemModifier::getEmbeddingData(const Payload& pl, const UpsertEmbedder& embedder, std::vector<VariantArray>& data) const {
	VariantArray fldData;
	data.resize(0);
	ns_.verifyEmbeddingFields(embedder.Fields(), embedder.FieldName(), "automatically embed value in");
	for (const auto& field : embedder.Fields()) {
		int idx = 0;
		if (!ns_.tryGetIndexByName(field, idx)) {
			throw Error{errConflict, "Configuration 'embedding:upsert_embedder' configured with invalid base field name '{}'", field};
		}
		pl.Get(idx, fldData);
		data.push_back(std::move(fldData));
	}
}

std::vector<std::pair<int, std::vector<VariantArray>>> ItemModifier::getEmbeddersSourceData(const Payload& pl) const {
	std::vector<std::pair<int, std::vector<VariantArray>>> data;
	for (int field = 1, numFields = ns_.payloadType_.NumFields(); field < numFields; ++field) {
		if (ns_.payloadType_.Field(field).UpsertEmbedder()) {
			data.push_back({field, {}});
			getEmbeddingData(pl, *ns_.payloadType_.Field(field).UpsertEmbedder(), data.back().second);
		}
	}
	return data;
}

void ItemModifier::updateEmbedding(IdType itemId, const RdxContext& rdxContext, Payload& pl) {
	std::vector<std::pair<std::string, VariantArray>> source;
	std::vector<VariantArray> updatedEmbeddingData;

	const auto& vectorIndexToModify = embedderHelper_.GetVectorIndexesToModify();

	for (auto index : vectorIndexToModify) {
		if (embedderHelper_.TestSkipVectorIndex(index)) {
			continue;
		}
		const auto& embedder = *ns_.payloadType_.Field(index).UpsertEmbedder();

		source.resize(0);

		for (const auto& fld : embedder.Fields()) {
			VariantArray data;
			pl.Get(fld, data);
			source.emplace_back(fld, std::move(data));
		}

		// ToDo in real life, work with several embedded devices requires asynchrony. Now we support only one
		h_vector<ConstFloatVector, 1> products;
		embedder.Calculate(rdxContext, std::span{&source, 1}, products);

		VariantArray krs;
		krs.emplace_back(ConstFloatVectorView{products.front()});

		UpdateEntry entry(embedder.FieldName(), {}, FieldModifyMode::FieldModeSet);
		FieldData fldData(entry, ns_, affectedComposites_);
		modifyField(itemId, fldData, pl, krs);
	}
}

void ItemModifier::IndexRollBack::Reset(IdType itemId, const PayloadType& pt, const PayloadValue& pv, FloatVectorsIndexes&& fvIndexes) {
	pvSave_ = pv;
	pvSave_.Clone();
	floatVectorsHolder_ = FloatVectorsHolderVector();
	Payload pl{pt, pvSave_};
	for (auto& fvIdx : fvIndexes) {
		auto fv = fvIdx.ptr->GetFloatVector(itemId);
		if (floatVectorsHolder_.Add(std::move(fv))) {
			pl.Set(fvIdx.ptField, Variant{floatVectorsHolder_.Back()});
		} else {
			pl.Set(fvIdx.ptField, Variant{ConstFloatVectorView{}});
		}
	}
	std::fill(data_.begin(), data_.end(), false);
	pkModified_ = false;
}

}  // namespace reindexer
