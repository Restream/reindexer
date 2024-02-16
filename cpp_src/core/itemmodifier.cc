#include "itemmodifier.h"
#include "core/itemimpl.h"
#include "core/namespace/namespaceimpl.h"
#include "core/query/expressionevaluator.h"
#include "core/selectfunc/functionexecutor.h"
#include "index/index.h"

namespace reindexer {

class ItemModifier::RollBack_ModifiedPayload : private RollBackBase {
public:
	RollBack_ModifiedPayload(ItemModifier &modifier, IdType id) noexcept : modifier_{modifier}, itemId_{id} {}
	RollBack_ModifiedPayload(RollBack_ModifiedPayload &&) noexcept = default;
	~RollBack_ModifiedPayload() { RollBack(); }

	void RollBack() {
		if (IsDisabled()) return;
		auto indexesCacheCleaner{modifier_.ns_.GetIndexesCacheCleaner()};
		const std::vector<bool> &data = modifier_.rollBackIndexData_.IndexStatus();
		PayloadValue &plValue = modifier_.ns_.items_[itemId_];
		NamespaceImpl::IndexesStorage &indexes = modifier_.ns_.indexes_;

		Payload plSave(modifier_.ns_.payloadType_, modifier_.rollBackIndexData_.GetPayloadValueBackup());

		Payload plCur(modifier_.ns_.payloadType_, plValue);
		VariantArray cjsonKref;
		plCur.Get(0, cjsonKref);

		VariantArray res;

		for (size_t i = indexes.firstCompositePos(); i < size_t(indexes.totalSize()) && i < data.size(); ++i) {
			if (data[i]) {
				bool needClearCache{false};
				indexes[i]->Delete(Variant(plValue), itemId_, *modifier_.ns_.strHolder(), needClearCache);
				if (needClearCache && indexes[i]->IsOrdered()) {
					indexesCacheCleaner.Add(indexes[i]->SortId());
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
					} catch (const Error &) {
						vals.resize(0);
					}
					modifier_.rollBackIndexData_.CjsonChanged();
				} else {
					cpl.Get(i, vals);
				}
				indexes[i]->Delete(vals, itemId_, *modifier_.ns_.strHolder(), needClearCache);
				if (needClearCache && indexes[i]->IsOrdered()) {
					indexesCacheCleaner.Add(indexes[i]->SortId());
				}

				VariantArray oldData;
				if (indexes[i]->Opts().IsSparse()) {
					try {
						plSave.GetByJsonPath(indexes[i]->Fields().getTagsPath(0), oldData, indexes[i]->KeyType());
					} catch (const Error &) {
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
				if (needClearCache && indexes[i]->IsOrdered()) {
					indexesCacheCleaner.Add(indexes[i]->SortId());
				}
			}
		}
		if (modifier_.rollBackIndexData_.IsCjsonChanged()) {
			const Variant &v = cjsonKref.front();
			bool needClearCache{false};
			indexes[0]->Delete(v, itemId_, *modifier_.ns_.strHolder(), needClearCache);
			VariantArray keys;
			plSave.Get(0, keys);
			indexes[0]->Upsert(res, keys, itemId_, needClearCache);
			plCur.Set(0, res);
		}

		for (size_t i = indexes.firstCompositePos(); i < size_t(indexes.totalSize()) && i < data.size(); ++i) {
			if (data[i]) {
				bool needClearCache{false};
				indexes[i]->Upsert(Variant(modifier_.rollBackIndexData_.GetPayloadValueBackup()), itemId_, needClearCache);
				if (needClearCache && indexes[i]->IsOrdered()) {
					indexesCacheCleaner.Add(indexes[i]->SortId());
				}
			}
		}
	}
	using RollBackBase::Disable;

	RollBack_ModifiedPayload(const RollBack_ModifiedPayload &) = delete;
	RollBack_ModifiedPayload operator=(const RollBack_ModifiedPayload &) = delete;
	RollBack_ModifiedPayload operator=(RollBack_ModifiedPayload &&) = delete;

private:
	ItemModifier &modifier_;
	IdType itemId_;
};

ItemModifier::FieldData::FieldData(const UpdateEntry &entry, NamespaceImpl &ns)
	: entry_(entry), tagsPathWithLastIndex_{std::nullopt}, arrayIndex_(IndexValueType::NotSet), isIndex_(false) {
	if (ns.tryGetIndexByName(entry_.Column(), fieldIndex_)) {
		isIndex_ = true;
		auto jsonPathsSize = (ns.indexes_[fieldIndex_]->Opts().IsSparse() || static_cast<int>(fieldIndex_) >= ns.payloadType_.NumFields())
								 ? ns.indexes_[fieldIndex_]->Fields().size()
								 : ns.payloadType_.Field(fieldIndex_).JsonPaths().size();

		if (jsonPathsSize != 1) {
			throw Error(errParams, "Ambiguity when updating field with several json paths by index name: '%s'", entry_.Column());
		}

		if (!entry.IsExpression()) {
			const auto &fields{ns.indexes_[fieldIndex_]->Fields()};
			if (fields.size() != 1) {
				throw Error(errParams, "Cannot update composite index: '%s'", entry_.Column());
			}
			if (fields[0] == IndexValueType::SetByJsonPath) {
				if (fields.isTagsPathIndexed(0)) {
					tagsPath_ = fields.getIndexedTagsPath(0);
				} else {
					tagsPath_ = IndexedTagsPath{fields.getTagsPath(0)};
				}
			} else {
				tagsPath_ = ns.tagsMatcher_.path2indexedtag(ns.payloadType_.Field(fieldIndex_).JsonPaths()[0], nullptr, true);
			}
			if (tagsPath_.empty()) {
				throw Error(errParams, "Cannot find field by json: '%s'", entry_.Column());
			}
			if (tagsPath_.back().IsWithIndex()) {
				arrayIndex_ = tagsPath_.back().Index();
				tagsPath_.back().SetIndex(IndexValueType::NotSet);
			}
		}
	} else if (fieldIndex_ = ns.payloadType_.FieldByJsonPath(entry_.Column()); fieldIndex_ > 0) {
		isIndex_ = true;
		if (!entry.IsExpression()) {
			tagsPath_ = ns.tagsMatcher_.path2indexedtag(entry_.Column(), nullptr, true);
			if (tagsPath_.empty()) {
				throw Error(errParams, "Cannot find field by json: '%s'", entry_.Column());
			}
		}
	} else {
		TagsPath tp;
		IndexedTagsPath tagsPath = ns.tagsMatcher_.path2indexedtag(entry_.Column(), nullptr, true);
		std::string jsonPath;
		for (size_t i = 0; i < tagsPath.size(); ++i) {
			if (i) jsonPath += '.';
			const auto tagName = tagsPath[i].NameTag();
			tp.emplace_back(tagName);
			jsonPath += ns.tagsMatcher_.tag2name(tagName);
		}
		fieldIndex_ = ns.tagsMatcher_.tags2field(tp.data(), tp.size());
		if (fieldIndex_ >= 0) {
			isIndex_ = true;
		} else {
			fieldIndex_ = 0;
			isIndex_ = ns.getIndexByNameOrJsonPath(jsonPath, fieldIndex_) || ns.getSparseIndexByJsonPath(jsonPath, fieldIndex_);
		}
		if (!entry.IsExpression()) {
			tagsPath_ = std::move(tagsPath);
			if (tagsPath_.empty()) {
				throw Error(errParams, "Cannot find field by json: '%s'", entry_.Column());
			}
			if (isIndex_) {
				auto &lastTag = tagsPath_.back();
				if (lastTag.IsWithIndex()) {
					tagsPathWithLastIndex_ = tagsPath_;
					arrayIndex_ = lastTag.Index();
					lastTag.SetIndex(IndexValueType::NotSet);
				}
			}
		}
	}
}

void ItemModifier::FieldData::updateTagsPath(TagsMatcher &tm, const IndexExpressionEvaluator &ev) {
	if (tagsPath_.empty()) {
		tagsPath_ = tm.path2indexedtag(entry_.Column(), ev, true);
	}
	for (size_t i = 0; i < tagsPath_.size(); ++i) {
		if (tagsPath_[i].IsWithExpression()) {
			IndexedPathNode &node = tagsPath_[i];
			VariantArray vals = ev(node.Expression());
			if (vals.size() != 1) {
				throw Error(errParams, "Index expression has wrong syntax: '%s'", node.Expression());
			}
			vals.front().Type().EvaluateOneOf([](OneOf<KeyValueType::Double, KeyValueType::Int, KeyValueType::Int64>) noexcept {},
											  [&](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Null, KeyValueType::Tuple,
														KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Uuid>) {
												  throw Error(errParams, "Wrong type of index: '%s'", node.Expression());
											  });
			node.SetIndex(vals.front().As<int>());
		}
	}
	if (tagsPath_.size()) {
		auto &lastTag = tagsPath_.back();
		if (lastTag.IsWithIndex()) {
			arrayIndex_ = lastTag.Index();
			tagsPathWithLastIndex_ = tagsPath_;
			lastTag.SetIndex(IndexValueType::NotSet);
		}
	}
}

ItemModifier::ItemModifier(const std::vector<UpdateEntry> &updateEntries, NamespaceImpl &ns,
						   h_vector<cluster::UpdateRecord, 2> &replUpdates, const NsContext &ctx)
	: ns_(ns), updateEntries_(updateEntries), rollBackIndexData_(ns_.indexes_.totalSize()) {
	const auto oldTmV = ns_.tagsMatcher_.version();
	for (const UpdateEntry &updateField : updateEntries_) {
		fieldsToModify_.emplace_back(updateField, ns_);
	}
	ns_.replicateTmUpdateIfRequired(replUpdates, oldTmV, ctx);
}

[[nodiscard]] bool ItemModifier::Modify(IdType itemId, const NsContext &ctx, h_vector<cluster::UpdateRecord, 2> &replUpdates) {
	PayloadValue &pv = ns_.items_[itemId];
	Payload pl(ns_.payloadType_, pv);
	pv.Clone(pl.RealSize());

	rollBackIndexData_.Reset(pv);
	RollBack_ModifiedPayload rollBack = RollBack_ModifiedPayload(*this, itemId);
	FunctionExecutor funcExecutor(ns_, replUpdates);
	ExpressionEvaluator ev(ns_.payloadType_, ns_.tagsMatcher_, funcExecutor);

	VariantArray values;
	for (FieldData &field : fieldsToModify_) {
		values.clear<false>();
		if (field.details().IsExpression()) {
			assertrx(field.details().Values().size() > 0);
			values = ev.Evaluate(static_cast<std::string_view>(field.details().Values().front()), pv, field.name(), ctx);
			field.updateTagsPath(ns_.tagsMatcher_, [&ev, &pv, &field, &ctx](std::string_view expression) {
				return ev.Evaluate(expression, pv, field.name(), ctx);
			});
		} else {
			values = field.details().Values();
		}

		if (values.IsArrayValue() && field.tagspathWithLastIndex().back().IsArrayNode()) {
			throw Error(errParams, "Array items are supposed to be updated with a single value, not an array");
		}

		if (field.details().Mode() == FieldModeSetJson || !field.isIndex()) {
			modifyCJSON(itemId, field, values, replUpdates, ctx);
		} else {
			modifyField(itemId, field, pl, values);
		}
	}
	if (rollBackIndexData_.IsPkModified()) {
		ns_.checkUniquePK(ConstPayload(ns_.payloadType_, pv), ctx.inTransaction, ctx.rdxContext);
	}
	rollBack.Disable();

	ns_.markUpdated(false);

	return rollBackIndexData_.IsPkModified();
}

void ItemModifier::modifyCJSON(IdType id, FieldData &field, VariantArray &values, h_vector<cluster::UpdateRecord, 2> &replUpdates,
							   const NsContext &ctx) {
	PayloadValue &plData = ns_.items_[id];
	Payload pl(*ns_.payloadType_.get(), plData);
	VariantArray cjsonKref;
	pl.Get(0, cjsonKref);
	cjsonCache_.Reset();

	const Variant &v = cjsonKref.front();
	if (v.Type().Is<KeyValueType::String>()) {
		cjsonCache_.Assign(std::string_view(p_string(v)));
	}

	ItemImpl itemimpl(ns_.payloadType_, plData, ns_.tagsMatcher_);
	itemimpl.ModifyField(field.tagspath(), values, field.details().Mode());

	Item item = ns_.newItem();
	Error err = item.FromCJSON(itemimpl.GetCJSON(true));
	if (!err.ok()) {
		pl.Set(0, cjsonKref);
		throw err;
	}

	item.setID(id);
	ItemImpl *impl = item.impl_;
	ns_.setFieldsBasedOnPrecepts(impl, replUpdates, ctx);
	ns_.updateTagsMatcherFromItem(impl, ctx);

	Payload plNew = impl->GetPayload();
	plData.Clone(pl.RealSize());

	auto strHolder = ns_.strHolder();
	auto indexesCacheCleaner{ns_.GetIndexesCacheCleaner()};
	h_vector<bool, 32> needUpdateCompIndexes(ns_.indexes_.compositeIndexesSize(), false);
	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		const auto &fields = ns_.indexes_[i]->Fields();
		if (field.isIndex()) {
			for (const auto f : fields) {
				if (f == IndexValueType::SetByJsonPath) continue;
				if (f == field.index()) {
					needUpdateCompIndexes[i - ns_.indexes_.firstCompositePos()] = true;
					break;
				}
			}
		}
		if (!needUpdateCompIndexes[i - ns_.indexes_.firstCompositePos()]) {
			for (size_t tp = 0, end = fields.getTagsPathsLength(); tp < end; ++tp) {
				if (field.tagspath().Compare(fields.getTagsPath(tp))) {
					needUpdateCompIndexes[i - ns_.indexes_.firstCompositePos()] = true;
					break;
				}
			}
			if (!needUpdateCompIndexes[i - ns_.indexes_.firstCompositePos()]) continue;
		}
		bool needClearCache{false};
		rollBackIndexData_.IndexAndCJsonChanged(i, ns_.indexes_[i]->Opts().IsPK());
		ns_.indexes_[i]->Delete(Variant(plData), id, *strHolder, needClearCache);
		if (needClearCache && ns_.indexes_[i]->IsOrdered()) indexesCacheCleaner.Add(ns_.indexes_[i]->SortId());
	}

	assertrx(ns_.indexes_.firstCompositePos() != 0);
	const int borderIdx = ns_.indexes_.totalSize() > 1 ? 1 : 0;
	int fieldIdx = borderIdx;
	do {
		// update the indexes, and then tuple (1,2,...,0)
		fieldIdx %= ns_.indexes_.firstCompositePos();
		Index &index = *(ns_.indexes_[fieldIdx]);
		bool isIndexSparse = index.Opts().IsSparse();
		assertrx(!isIndexSparse || (isIndexSparse && index.Fields().getTagsPathsLength() > 0));

		if (isIndexSparse) {
			assertrx(index.Fields().getTagsPathsLength() > 0);
			try {
				plNew.GetByJsonPath(index.Fields().getTagsPath(0), ns_.skrefs, index.KeyType());
			} catch (const Error &) {
				ns_.skrefs.resize(0);
			}
		} else {
			plNew.Get(fieldIdx, ns_.skrefs);
		}

		if (index.Opts().GetCollateMode() == CollateUTF8) {
			for (auto &key : ns_.skrefs) key.EnsureUTF8();
		}

		if ((fieldIdx == 0) && (cjsonCache_.Size() > 0)) {
			bool needClearCache{false};
			rollBackIndexData_.CjsonChanged();
			index.Delete(Variant(cjsonCache_.Get()), id, *strHolder, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		} else {
			if (isIndexSparse) {
				try {
					pl.GetByJsonPath(index.Fields().getTagsPath(0), ns_.krefs, index.KeyType());
				} catch (const Error &) {
					ns_.krefs.resize(0);
				}
			} else if (index.Opts().IsArray()) {
				pl.Get(fieldIdx, ns_.krefs, Variant::hold_t{});
			} else {
				pl.Get(fieldIdx, ns_.krefs);
			}
			if (ns_.krefs == ns_.skrefs) continue;
			bool needClearCache{false};
			rollBackIndexData_.IndexChanged(fieldIdx, index.Opts().IsPK());
			index.Delete(ns_.krefs, id, *strHolder, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		}

		ns_.krefs.resize(0);
		bool needClearCache{false};
		rollBackIndexData_.IndexChanged(fieldIdx, index.Opts().IsPK());
		index.Upsert(ns_.krefs, ns_.skrefs, id, needClearCache);
		if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());

		if (!isIndexSparse) {
			pl.Set(fieldIdx, ns_.krefs);
		}
	} while (++fieldIdx != borderIdx);

	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		if (!needUpdateCompIndexes[i - ns_.indexes_.firstCompositePos()]) continue;
		bool needClearCache{false};
		ns_.indexes_[i]->Upsert(Variant(plData), id, needClearCache);
		if (needClearCache && ns_.indexes_[i]->IsOrdered()) indexesCacheCleaner.Add(ns_.indexes_[i]->SortId());
	}

	impl->RealValue() = plData;
}

void ItemModifier::modifyField(IdType itemId, FieldData &field, Payload &pl, VariantArray &values) {
	Index &index = *(ns_.indexes_[field.index()]);
	if (field.isIndex() && !index.Opts().IsSparse() && field.details().Mode() == FieldModeDrop /*&&
		!(field.arrayIndex() != IndexValueType::NotSet || field.tagspath().back().IsArrayNode())*/) {	 // TODO #1218 allow to drop array fields
		throw Error(errLogic, "It's only possible to drop sparse or non-index fields via UPDATE statement!");
	}

	assertrx(!index.Opts().IsSparse() || (index.Opts().IsSparse() && index.Fields().getTagsPathsLength() > 0));
	if (field.isIndex() && !index.Opts().IsArray() && values.IsArrayValue()) {
		throw Error(errParams, "It's not possible to Update single index fields with arrays!");
	}

	if (index.Opts().GetCollateMode() == CollateUTF8) {
		for (const Variant &key : values) key.EnsureUTF8();
	}

	auto strHolder = ns_.strHolder();
	auto indexesCacheCleaner{ns_.GetIndexesCacheCleaner()};
	h_vector<bool, 32> needUpdateCompIndexes(ns_.indexes_.compositeIndexesSize(), false);
	const auto firstCompositePos = ns_.indexes_.firstCompositePos();
	const auto totalIndexes = ns_.indexes_.totalSize();
	for (int i = firstCompositePos; i < totalIndexes; ++i) {
		auto &compositeIdx = ns_.indexes_[i];
		const auto &fields = compositeIdx->Fields();
		const auto idxId = i - firstCompositePos;
		for (const auto f : fields) {
			if (f == IndexValueType::SetByJsonPath) continue;
			if (f == field.index()) {
				needUpdateCompIndexes[idxId] = true;
				break;
			}
		}
		if (!needUpdateCompIndexes[idxId]) {
			for (size_t tp = 0, end = fields.getTagsPathsLength(); tp < end; ++tp) {
				if (field.tagspath().Compare(fields.getTagsPath(tp))) {
					needUpdateCompIndexes[idxId] = true;
					break;
				}
			}
			if (!needUpdateCompIndexes[idxId]) continue;
		}
		bool needClearCache{false};
		rollBackIndexData_.IndexAndCJsonChanged(i, compositeIdx->Opts().IsPK());
		compositeIdx->Delete(Variant(ns_.items_[itemId]), itemId, *strHolder, needClearCache);
		if (needClearCache && compositeIdx->IsOrdered()) indexesCacheCleaner.Add(compositeIdx->SortId());
	}

	const auto insertItemIntoCompositeIndexes = [&] {
		for (int i = firstCompositePos; i < totalIndexes; ++i) {
			if (!needUpdateCompIndexes[i - firstCompositePos]) continue;
			bool needClearCache{false};
			auto &compositeIdx = ns_.indexes_[i];
			rollBackIndexData_.IndexChanged(i, compositeIdx->Opts().IsPK());
			compositeIdx->Upsert(Variant(ns_.items_[itemId]), itemId, needClearCache);
			if (needClearCache && compositeIdx->IsOrdered()) indexesCacheCleaner.Add(compositeIdx->SortId());
		}
	};

	try {
		if (field.isIndex()) {
			modifyIndexValues(itemId, field, values, pl);
		}

		if (index.Opts().IsSparse() || index.Opts().IsArray() || index.KeyType().Is<KeyValueType::Uuid>() || !field.isIndex()) {
			ItemImpl item(ns_.payloadType_, *(pl.Value()), ns_.tagsMatcher_);
			Variant oldTupleValue = item.GetField(0);
			oldTupleValue.EnsureHold();
			bool needClearCache{false};
			auto &tupleIdx = ns_.indexes_[0];
			tupleIdx->Delete(oldTupleValue, itemId, *strHolder, needClearCache);
			Variant tupleValue;
			std::exception_ptr exception;
			try {
				item.ModifyField(field.tagspathWithLastIndex(), values, field.details().Mode());
			} catch (...) {
				exception = std::current_exception();
			}
			tupleValue = tupleIdx->Upsert(item.GetField(0), itemId, needClearCache);
			if (needClearCache && tupleIdx->IsOrdered()) indexesCacheCleaner.Add(tupleIdx->SortId());
			pl.Set(0, std::move(tupleValue));
			if (exception) {
				std::rethrow_exception(exception);
			}
		}
	} catch (...) {
		// Insert item back, even if it was not modified
		insertItemIntoCompositeIndexes();
		throw;
	}

	insertItemIntoCompositeIndexes();
}

void ItemModifier::modifyIndexValues(IdType itemId, const FieldData &field, VariantArray &values, Payload &pl) {
	Index &index = *(ns_.indexes_[field.index()]);
	if (values.IsNullValue() && !index.Opts().IsArray()) {
		throw Error(errParams, "Non-array index fields cannot be set to null!");
	}
	auto strHolder = ns_.strHolder();
	auto indexesCacheCleaner{ns_.GetIndexesCacheCleaner()};
	bool updateArrayPart = field.arrayIndex() >= 0;
	bool isForAllItems = false;
	for (const auto &tag : field.tagspath()) {
		if (tag.IsArrayNode()) {
			updateArrayPart = true;
		}
		if (tag.IsForAllItems()) {
			isForAllItems = true;
			continue;
		}
		if (isForAllItems && tag.IsWithIndex()) {
			throw Error(errParams, "Expressions like 'field[*].field[1]=10' are supported for sparse indexes/non-index fields only");
		}
	}

	ns_.krefs.resize(0);
	for (Variant &key : values) {
		key.convert(index.KeyType());
	}

	if (index.Opts().IsArray() && updateArrayPart && !index.Opts().IsSparse()) {
		if (!values.IsArrayValue() && values.empty()) {
			throw Error(errParams, "Cannot update array item with an empty value");	 // TODO #1218 maybe delete this
		}
		int offset = -1, length = -1;
		bool isForAllItems = false;
		for (const auto &tag : field.tagspath()) {	// TODO: Move to FieldEntry?
			if (tag.IsForAllItems()) {
				isForAllItems = true;
				continue;
			}
			if (isForAllItems && tag.IsWithIndex()) {
				throw Error(errParams, "Expressions like 'field[*].field[1]=10' are supported for sparse indexes/non-index fields only");
			}
		}

		ns_.skrefs = pl.GetIndexedArrayData(field.tagspathWithLastIndex(), field.index(), offset, length);
		if (offset < 0 || length < 0) {
			const auto &path = field.tagspathWithLastIndex();
			std::string indexesStr;
			for (auto &p : path) {
				if (p.Index() >= 0) {
					if (indexesStr.size()) {
						indexesStr.append(",");
					}
					indexesStr.append(std::to_string(p.Index()));
				}
			}
			throw Error(errParams, "Requested array's index was not found: [%s]", indexesStr);
		}
		if (field.arrayIndex() != IndexValueType::NotSet && field.arrayIndex() >= length) {
			throw Error(errLogic, "Array index is out of range: [%d/%d]", field.arrayIndex(), length);
		}

		if (!ns_.skrefs.empty()) {
			bool needClearCache{false};
			rollBackIndexData_.IndexChanged(field.index(), index.Opts().IsPK());
			index.Delete(ns_.skrefs.front(), itemId, *strHolder, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		}

		bool needClearCache{false};
		rollBackIndexData_.IndexChanged(field.index(), index.Opts().IsPK());
		index.Upsert(ns_.krefs, values, itemId, needClearCache);
		if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());

		if (isForAllItems) {
			for (int i = offset, end = offset + length; i < end; ++i) {
				pl.Set(field.index(), i, ns_.krefs.front());
			}
		} else if (field.arrayIndex() == IndexValueType::NotSet) {
			// Array may be resized
			VariantArray v;
			pl.Get(field.index(), v);
			v.erase(v.begin() + offset, v.begin() + offset + length);
			v.insert(v.begin() + offset, ns_.krefs.begin(), ns_.krefs.end());
			pl.Set(field.index(), v);
		} else {
			// Exactly one value was changed
			pl.Set(field.index(), offset, ns_.krefs.front());
		}
	} else {
		if (index.Opts().IsSparse()) {
			pl.GetByJsonPath(field.tagspathWithLastIndex(), ns_.skrefs, index.KeyType());
		} else {
			pl.Get(field.index(), ns_.skrefs, Variant::hold_t{});
		}

		// Required when updating index array field with several tagpaths
		VariantArray concatValues;
		int offset = -1, length = -1;
		pl.GetIndexedArrayData(field.tagspathWithLastIndex(), field.index(), offset, length);
		const bool kConcatIndexValues =
			index.Opts().IsArray() && !updateArrayPart &&
			(length < int(ns_.skrefs.size()));	//  (length < int(ns_.skrefs.size()) - condition to avoid coping
												//  when length and ns_.skrefs.size() are equal; then concatValues == values
		if (kConcatIndexValues) {
			if (offset < 0 || length < 0) {
				concatValues = ns_.skrefs;
				concatValues.insert(concatValues.end(), values.begin(), values.end());
			} else {
				concatValues.insert(concatValues.end(), ns_.skrefs.begin(), ns_.skrefs.begin() + offset);
				concatValues.insert(concatValues.end(), values.begin(), values.end());
				concatValues.insert(concatValues.end(), ns_.skrefs.begin() + offset + length, ns_.skrefs.end());
			}
		}

		if (!ns_.skrefs.empty()) {
			bool needClearCache{false};
			rollBackIndexData_.IndexChanged(field.index(), index.Opts().IsPK());
			index.Delete(ns_.skrefs, itemId, *strHolder, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		}

		bool needClearCache{false};
		rollBackIndexData_.IndexChanged(field.index(), index.Opts().IsPK());
		index.Upsert(ns_.krefs, kConcatIndexValues ? concatValues : values, itemId, needClearCache);
		if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		if (!index.Opts().IsSparse()) {
			pl.Set(field.index(), ns_.krefs);
		}
	}
}

}  // namespace reindexer
