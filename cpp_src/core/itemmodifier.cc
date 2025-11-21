#include "itemmodifier.h"

#include <span>
#include "core/cjson/tagsmatcher.h"
#include "core/embedding/embedder.h"
#include "core/itemimpl.h"
#include "core/namespace/namespaceimpl.h"
#include "core/query/expression/expression_evaluator.h"
#include "core/query/expression/function_executor.h"
#include "index/float_vector/float_vector_index.h"
#include "index/index.h"

namespace reindexer {

std::string_view ItemModifier::FieldData::Name() const noexcept { return entry_.Column(); }

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
					modifier_.rollBackIndexData_.CjsonChanged();
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
		if (modifier_.rollBackIndexData_.IsCjsonChanged()) {
			const Variant& v = cjsonKref.front();
			bool needClearCache{false};
			indexes[0]->Delete(v, itemId_, MustExist_True, *modifier_.ns_.strHolder(), needClearCache);
			VariantArray keys;
			plSave.Get(0, keys);
			indexes[0]->Upsert(res, keys, itemId_, needClearCache);
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
	if (ns.tryGetIndexByName(entry_.Column(), fieldIndex_)) {
		isIndex_ = true;
		const auto& idx = *ns.indexes_[fieldIndex_];
		auto jsonPathsSize = (idx.Opts().IsSparse() || static_cast<int>(fieldIndex_) >= ns.payloadType_.NumFields())
								 ? idx.Fields().size()
								 : ns.payloadType_.Field(fieldIndex_).JsonPaths().size();

		if (jsonPathsSize != 1) {
			throw Error(errParams, "Ambiguity when updating field with several json paths by index name: '{}'", entry_.Column());
		}

		const auto& fields{idx.Fields()};
		if (fields.size() != 1) {
			throw Error(errParams, "Cannot update composite index: '{}'", entry_.Column());
		}
		if (fields[0] == IndexValueType::SetByJsonPath) {
			if (fields.isTagsPathIndexed(0)) {
				tagsPath_ = fields.getIndexedTagsPath(0);
			} else {
				tagsPath_ = IndexedTagsPath{fields.getTagsPath(0)};
			}
		} else {
			fieldIndex_ = fields[0];  // 'Composite' index with single subindex
			tagsPath_ = ns.tagsMatcher_.path2indexedtag(ns.payloadType_.Field(fieldIndex_).JsonPaths()[0], CanAddField_True);
		}
		if (tagsPath_.empty()) {
			throw Error(errParams, "Cannot find field by json: '{}'", entry_.Column());
		}
		if (tagsPath_.back().IsTagIndexNotAll()) {
			arrayIndex_ = tagsPath_.back().GetTagIndex();
			tagsPath_.pop_back();
		}
	} else if (fieldIndex_ = ns.payloadType_.FieldByJsonPath(entry_.Column()); fieldIndex_ > 0) {
		isIndex_ = true;
		tagsPath_ = ns.tagsMatcher_.path2indexedtag(entry_.Column(), CanAddField_True);
		if (tagsPath_.empty()) {
			throw Error(errParams, "Cannot find field by json: '{}'", entry_.Column());
		}
	} else {
		TagsPath tp;
		IndexedTagsPath tagsPath = ns.tagsMatcher_.path2indexedtag(entry_.Column(), CanAddField_True);
		std::string jsonPath;
		for (size_t i = 0; i < tagsPath.size(); ++i) {
			const auto& tag = tagsPath[i];
			if (tag.IsTagName()) {
				if (!jsonPath.empty()) {
					jsonPath += '.';
				}
				const TagName tagName = tag.GetTagName();
				tp.emplace_back(tagName);
				jsonPath += ns.tagsMatcher_.tag2name(tagName);
			}
		}
		const auto field = ns.tagsMatcher_.tags2field(tp);
		if (field.IsRegularIndex()) {
			fieldIndex_ = field.IndexNumber();
			isIndex_ = true;
			auto isForAll = false;
			for (const auto& pathNode : tagsPath) {
				if (pathNode.IsTagIndex()) {
					if (pathNode.GetTagIndex().IsAll()) {
						isForAll = true;
					} else if (isForAll) {
						throw Error(errParams,
									"Expressions like 'field[*].field[1]=10' are supported for sparse indexes/non-index fields only");
					}
				}
			}
		} else {
			fieldIndex_ = 0;
			isIndex_ = ns.tryGetIndexByNameOrJsonPath(jsonPath, fieldIndex_);
		}
		tagsPath_ = std::move(tagsPath);
		if (tagsPath_.empty()) {
			throw Error(errParams, "Cannot find field by json: '{}'", entry_.Column());
		}
		if (isIndex_) {
			if (tagsPath_.back().IsTagIndexNotAll()) {
				tagsPathWithLastIndex_ = tagsPath_;
				arrayIndex_ = tagsPath_.back().GetTagIndex();
				tagsPath_.pop_back();
			}
		}
	}
	appendAffectedIndexes(ns, affectedComposites);
}

ItemModifier::ItemModifier(const std::vector<UpdateEntry>& updateEntries, NamespaceImpl& ns, UpdatesContainer& replUpdates,
						   const NsContext& ctx)
	: ns_(ns),
	  updateEntries_(updateEntries),
	  rollBackIndexData_(ns_.indexes_.totalSize()),
	  affectedComposites_(ns_.indexes_.totalSize() - ns_.indexes_.firstCompositePos(), false),
	  vectorIndexes_(ns_.getVectorIndexes()) {
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
	ns_.replicateTmUpdateIfRequired(replUpdates, oldTmV, ctx);
}

bool ItemModifier::Modify(IdType itemId, const NsContext& ctx, UpdatesContainer& replUpdates) {
	PayloadValue& pv = ns_.items_[itemId];
	Payload pl(ns_.payloadType_, pv);
	pv.Clone(pl.RealSize());

	auto embeddersData = getEmbeddersSourceData(pl);

	rollBackIndexData_.Reset(itemId, ns_.payloadType_, pv, ns_.getVectorIndexes());
	RollBack_ModifiedPayload rollBack = RollBack_ModifiedPayload(*this, itemId);
	FunctionExecutor funcExecutor(ns_, replUpdates);
	ExpressionEvaluator ev(ns_, funcExecutor);

	auto indexesCacheCleaner = ns_.GetIndexesCacheCleaner();

	deleteItemFromComposite(itemId, indexesCacheCleaner);
	try {
		VariantArray values;
		for (FieldData& field : fieldsToModify_) {
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
			} else {
				modifyField(itemId, field, pl, values);
			}
		}

		updateEmbedding(itemId, ctx.rdxContext, pl, embeddersData);
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
			rollBackIndexData_.CjsonChanged();
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
			rollBackIndexData_.IndexAndCJsonChanged(i, compositeIdx->Opts().IsPK());
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

bool ItemModifier::skipEmbedder(const UpsertEmbedder& embedder) const {
	// auto-embed disabled when requesting with special values for embedded field
	const auto embedderIdx = ns_.payloadType_.FieldByName(embedder.FieldName());
	for (const FieldData& field : fieldsToModify_) {
		if ((field.Index() == embedderIdx) && (field.Details().Mode() == FieldModeSet)) {
			return true;
		}
	}
	return false;
}

void ItemModifier::updateEmbedding(IdType itemId, const RdxContext& rdxContext, Payload& pl,
								   const std::vector<std::pair<int, std::vector<VariantArray>>>& embeddersData) {
	if (embeddersData.empty() || std::all_of(embeddersData.begin(), embeddersData.end(),
											 [](const std::pair<int, std::vector<VariantArray>>& item) { return item.second.empty(); })) {
		return;
	}

	VariantArray data;
	std::vector<std::pair<std::string, VariantArray>> source;

	std::vector<VariantArray> updatedEmbeddingData;
	for (size_t i = 0, s = embeddersData.size(); i < s; ++i) {
		if (embeddersData[i].second.empty()) {
			continue;
		}

		assertrx_dbg(ns_.payloadType_.Field(embeddersData[i].first).UpsertEmbedder());
		const auto& embedder = *ns_.payloadType_.Field(embeddersData[i].first).UpsertEmbedder();
		if (skipEmbedder(embedder)) {
			continue;
		}
		ns_.verifyEmbeddingFields(embedder.Fields(), embedder.FieldName(), "automatically embed value in");

		source.resize(0);
		getEmbeddingData(pl, embedder, updatedEmbeddingData);
		if (embeddersData[i].second != updatedEmbeddingData) {
			for (const auto& fld : embedder.Fields()) {
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
	cjsonChanged_ = false;
	pkModified_ = false;
}

}  // namespace reindexer
