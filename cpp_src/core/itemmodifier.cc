#include "itemmodifier.h"
#include "core/namespace/namespaceimpl.h"
#include "core/query/expressionevaluator.h"
#include "core/selectfunc/functionexecutor.h"
#include "index/index.h"
#include "tools/logger.h"

namespace reindexer {

ItemModifier::FieldData::FieldData(const UpdateEntry &entry, NamespaceImpl &ns)
	: entry_(entry), tagsPath_(), fieldIndex_(0), arrayIndex_(IndexValueType::NotSet), isIndex_(false) {
	if (ns.getIndexByName(entry_.Column(), fieldIndex_)) {
		jsonPath_ = entry.Column();
		isIndex_ = true;
		if (!entry.IsExpression()) {
			tagsPath_ = ns.tagsMatcher_.path2indexedtag(entry_.Column(), nullptr, true);
			if (tagsPath_.empty()) {
				throw Error(errParams, "Cannot find field by json: '%s'", entry_.Column());
			}
			if (tagsPath_.back().IsWithIndex()) {
				arrayIndex_ = tagsPath_.back().Index();
				tagsPath_.back().SetIndex(IndexValueType::NotSet);
			}
		}
	} else {
		IndexedTagsPath tagsPath = ns.tagsMatcher_.path2indexedtag(entry_.Column(), nullptr, true);
		for (size_t i = 0; i < tagsPath.size(); ++i) {
			if (i) jsonPath_ += ".";
			jsonPath_ += ns.tagsMatcher_.tag2name(tagsPath[i].NameTag());
		}
		isIndex_ = ns.getIndexByName(jsonPath_, fieldIndex_);
		if (!entry.IsExpression()) {
			tagsPath_ = std::move(tagsPath);
			if (tagsPath_.empty()) {
				throw Error(errParams, "Cannot find field by json: '%s'", entry_.Column());
			}
			if (isIndex_ && tagsPath_.back().IsWithIndex()) {
				arrayIndex_ = tagsPath_.back().Index();
				tagsPath_.back().SetIndex(IndexValueType::NotSet);
			}
		}
	}
}

void ItemModifier::FieldData::updateTagsPath(TagsMatcher &tm, const IndexExpressionEvaluator &ev) {
	if (tagsPath_.empty()) {
		tagsPath_ = tm.path2indexedtag(entry_.Column(), ev, true);
	}
	for (size_t i = 0; i < tagsPath_.size(); ++i) {
		bool isLast = (i == tagsPath_.size() - 1);
		if (tagsPath_[i].IsWithExpression()) {
			IndexedPathNode &node = tagsPath_[i];
			VariantArray vals = ev(node.Expression());
			if (vals.size() != 1) {
				throw Error(errParams, "Index expression has wrong syntax: '%s'", node.Expression());
			}
			if (vals.front().Type() != KeyValueDouble && vals.front().Type() != KeyValueInt && vals.front().Type() != KeyValueInt64) {
				throw Error(errParams, "Wrong type of index: '%s'", node.Expression());
			}
			node.SetIndex(vals.front().As<int>());
		}
		if (isLast && isIndex_ && tagsPath_[i].IsWithIndex()) {
			arrayIndex_ = tagsPath_[i].Index();
			tagsPath_[i].SetIndex(IndexValueType::NotSet);
		}
	}
}

ItemModifier::ItemModifier(const std::vector<UpdateEntry> &updateEntries, NamespaceImpl &ns) : ns_(ns), updateEntries_(updateEntries) {
	for (const UpdateEntry &updateField : updateEntries_) {
		fieldsToModify_.emplace_back(updateField, ns_);
	}
}

void ItemModifier::Modify(IdType itemId, const NsContext &ctx) {
	assertrx(ctx.noLock);
	PayloadValue &pv = ns_.items_[itemId];
	Payload pl(ns_.payloadType_, pv);
	pv.Clone(pl.RealSize());

	FunctionExecutor funcExecutor(ns_);
	ExpressionEvaluator ev(ns_.payloadType_, ns_.tagsMatcher_, funcExecutor);

	for (FieldData &field : fieldsToModify_) {
		VariantArray values;
		if (field.details().IsExpression()) {
			assertrx(field.details().Values().size() > 0);
			values = ev.Evaluate(static_cast<std::string_view>(field.details().Values().front()), pv, field.name());
			field.updateTagsPath(ns_.tagsMatcher_,
								 [&ev, &pv, &field](std::string_view expression) { return ev.Evaluate(expression, pv, field.name()); });
		} else {
			values = field.details().Values();
		}

		if (field.details().Mode() == FieldModeSetJson) {
			modifyCJSON(pv, itemId, field, values, ctx);
		} else {
			modifyField(itemId, field, pl, values, ctx);
		}
	}

	ns_.markUpdated(false);
}

void ItemModifier::modifyCJSON(PayloadValue &pv, IdType id, FieldData &field, VariantArray &values, const NsContext &ctx) {
	PayloadValue &plData = ns_.items_[id];
	Payload pl(ns_.payloadType_, plData);
	VariantArray cjsonKref;
	pl.Get(0, cjsonKref);
	cjsonCache_.Reset();
	if (cjsonKref.size() > 0) {
		Variant v = cjsonKref.front();
		if (v.Type() == KeyValueString) {
			cjsonCache_.Assign(std::string_view(p_string(v)));
		}
	}

	ItemImpl itemimpl(ns_.payloadType_, pv, ns_.tagsMatcher_);
	itemimpl.ModifyField(field.tagspath(), values, field.details().Mode());

	Item item = ns_.NewItem(ctx);
	Error err = item.FromCJSON(itemimpl.GetCJSON(true));
	if (!err.ok()) throw err;
	item.setID(id);
	ItemImpl *impl = item.impl_;
	ns_.setFieldsBasedOnPrecepts(impl);
	ns_.updateTagsMatcherFromItem(impl);

	Payload plNew = impl->GetPayload();
	plData.Clone(pl.RealSize());

	auto strHolder = ns_.StrHolder(ctx);
	auto indexesCacheCleaner{ns_.GetIndexesCacheCleaner()};
	h_vector<bool, 32> needUpdateCompIndexes(ns_.indexes_.compositeIndexesSize(), false);
	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		const auto &fields = ns_.indexes_[i]->Fields();
		for (const auto f : fields) {
			if (f == IndexValueType::SetByJsonPath) continue;
			if (f == field.index()) {
				needUpdateCompIndexes[i - ns_.indexes_.firstCompositePos()] = true;
				break;
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
		ns_.indexes_[i]->Delete(Variant(plData), id, *strHolder, needClearCache);
		if (needClearCache && ns_.indexes_[i]->IsOrdered()) indexesCacheCleaner.Add(ns_.indexes_[i]->SortId());
	}

	assertrx(ns_.indexes_.firstCompositePos() != 0);
	const int borderIdx = ns_.indexes_.totalSize() > 1 ? 1 : 0;
	int fieldIdx = borderIdx;
	do {
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
			index.Delete(Variant(cjsonCache_.Get()), id, *strHolder, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		} else {
			if (isIndexSparse) {
				try {
					pl.GetByJsonPath(index.Fields().getTagsPath(0), ns_.krefs, index.KeyType());
				} catch (const Error &) {
					ns_.krefs.resize(0);
				}
			} else {
				pl.Get(fieldIdx, ns_.krefs, index.Opts().IsArray());
			}
			if (ns_.krefs == ns_.skrefs) continue;
			bool needClearCache{false};
			index.Delete(ns_.krefs, id, *strHolder, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		}

		ns_.krefs.resize(0);
		bool needClearCache{false};
		index.Upsert(ns_.krefs, ns_.skrefs, id, needClearCache);
		if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());

		if (!isIndexSparse) {
			pl.Set(fieldIdx, ns_.krefs);
		}
	} while (++fieldIdx != borderIdx);

	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		if (!needUpdateCompIndexes[i - ns_.indexes_.firstCompositePos()]) continue;
		bool needClearCache{false};
		ns_.indexes_[i]->Upsert(Variant(pv), id, needClearCache);
		if (needClearCache && ns_.indexes_[i]->IsOrdered()) indexesCacheCleaner.Add(ns_.indexes_[i]->SortId());
	}

	impl->RealValue() = pv;
}

void ItemModifier::modifyField(IdType itemId, FieldData &field, Payload &pl, VariantArray &values, const NsContext &ctx) {
	Index &index = *(ns_.indexes_[field.index()]);
	if (field.isIndex() && !index.Opts().IsSparse() && field.details().Mode() == FieldModeDrop /*&&
		!(field.arrayIndex() != IndexValueType::NotSet || field.tagspath().back().IsArrayNode())*/) {	 // TODO #1218 allow to drop array fields
		throw Error(errLogic, "It's only possible to drop sparse or non-index fields via UPDATE statement!");
	}

	assertrx(!index.Opts().IsSparse() || (index.Opts().IsSparse() && index.Fields().getTagsPathsLength() > 0));
	if (field.isIndex() && !index.Opts().IsArray() && values.IsArrayValue()) {
		throw Error(errParams, "It's not possible to Update single index fields with arrays!");
	}

	if (index.Opts().IsSparse()) {
		pl.GetByJsonPath(index.Fields().getTagsPath(0), ns_.skrefs, index.KeyType());
	} else if (!index.Opts().IsArray()) {
		pl.Get(field.index(), ns_.skrefs);
	}

	if (index.Opts().GetCollateMode() == CollateUTF8) {
		for (const Variant &key : values) key.EnsureUTF8();
	}

	auto strHolder = ns_.StrHolder(ctx);
	auto indexesCacheCleaner{ns_.GetIndexesCacheCleaner()};
	h_vector<bool, 32> needUpdateCompIndexes(ns_.indexes_.compositeIndexesSize(), false);
	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		const auto &fields = ns_.indexes_[i]->Fields();
		for (const auto f : fields) {
			if (f == IndexValueType::SetByJsonPath) continue;
			if (f == field.index()) {
				needUpdateCompIndexes[i - ns_.indexes_.firstCompositePos()] = true;
				break;
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
		ns_.indexes_[i]->Delete(Variant(ns_.items_[itemId]), itemId, *strHolder, needClearCache);
		if (needClearCache && ns_.indexes_[i]->IsOrdered()) indexesCacheCleaner.Add(ns_.indexes_[i]->SortId());
	}

	if (field.isIndex()) {
		modifyIndexValues(itemId, field, values, pl, ctx);
	}

	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		if (!needUpdateCompIndexes[i - ns_.indexes_.firstCompositePos()]) continue;
		bool needClearCache{false};
		ns_.indexes_[i]->Upsert(Variant(ns_.items_[itemId]), itemId, needClearCache);
		if (needClearCache && ns_.indexes_[i]->IsOrdered()) indexesCacheCleaner.Add(ns_.indexes_[i]->SortId());
	}

	if (index.Opts().IsSparse() || index.Opts().IsArray() || !field.isIndex()) {
		ItemImpl item(ns_.payloadType_, *(pl.Value()), ns_.tagsMatcher_);
		Variant oldTupleValue = item.GetField(0);
		oldTupleValue.EnsureHold();
		bool needClearCache{false};
		ns_.indexes_[0]->Delete(oldTupleValue, itemId, *strHolder, needClearCache);
		item.ModifyField(field.tagspath(), values, field.details().Mode());
		Variant tupleValue = ns_.indexes_[0]->Upsert(item.GetField(0), itemId, needClearCache);
		if (needClearCache && ns_.indexes_[0]->IsOrdered()) indexesCacheCleaner.Add(ns_.indexes_[0]->SortId());
		pl.Set(0, {tupleValue});
		ns_.tagsMatcher_.try_merge(item.tagsMatcher());
	}
}

void ItemModifier::modifyIndexValues(IdType itemId, const FieldData &field, VariantArray &values, Payload &pl, const NsContext &ctx) {
	Index &index = *(ns_.indexes_[field.index()]);
	if (values.IsNullValue() && !index.Opts().IsArray()) {
		throw Error(errParams, "Non-array index fields cannot be set to null!");
	}
	bool isArrayItem = (field.tagspath().back().IsForAllItems() || field.arrayIndex() != IndexValueType::NotSet);
	if (index.Opts().IsArray() && values.IsArrayValue() && isArrayItem) {
		throw Error(errParams, "Array items are supposed to be updated with a single value, not an array");
	}
	auto strHolder = ns_.StrHolder(ctx);
	auto indexesCacheCleaner{ns_.GetIndexesCacheCleaner()};
	if (index.Opts().IsArray() && !values.IsArrayValue() && !values.IsNullValue()) {
		if (values.empty()) {
			throw Error(errParams, "Cannot update array item with an empty value");	 // TODO #1218 maybe delete this
		}
		int offset = 0, length = 0;
		ns_.krefs.resize(0);
		ns_.krefs.emplace_back(values.front().convert(index.KeyType()));
		if (field.tagspath().back().IsForAllItems()) {
			ns_.skrefs = pl.GetIndexedArrayData(field.tagspath(), offset, length);
			if (!ns_.skrefs.empty()) {
				bool needClearCache{false};
				index.Delete(ns_.skrefs, itemId, *strHolder, needClearCache);
				if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
			}
			if (!index.Opts().IsSparse()) {
				for (int i = offset; i < offset + length; ++i) {
					pl.Set(field.index(), i, ns_.krefs.front());
				}
			}
		} else {
			if (field.arrayIndex() == IndexValueType::NotSet) {
				throw Error(errParams, "Array index is not set");
			}
			IndexedTagsPath arrayPath = field.tagspath();
			arrayPath.back().SetIndex(field.arrayIndex());
			ns_.skrefs = pl.GetIndexedArrayData(arrayPath, offset, length);
			if (field.arrayIndex() < length) {
				if (!ns_.skrefs.empty()) {
					bool needClearCache{false};
					index.Delete(ns_.skrefs.front(), itemId, *strHolder, needClearCache);
					if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
				}
				if (!index.Opts().IsSparse()) {
					pl.Set(field.index(), offset, ns_.krefs.front());
				}
			} else {
				throw Error(errLogic, "Array index is out of range: [%d/%d]", field.arrayIndex(), length);
			}
		}
		bool needClearCache{false};
		index.Upsert(ns_.krefs.front(), itemId, needClearCache);
		if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		if (!index.Opts().IsSparse()) values.resize(length);
	} else {
		if (index.Opts().IsArray() && !index.Opts().IsSparse()) {
			pl.Get(field.index(), ns_.skrefs, true);
		}
		if (!ns_.skrefs.empty()) {
			bool needClearCache{false};
			index.Delete(ns_.skrefs, itemId, *strHolder, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		}
		ns_.krefs.resize(0);
		ns_.krefs.reserve(values.size());
		for (Variant &key : values) {
			key.convert(index.KeyType());
		}
		bool needClearCache{false};
		index.Upsert(ns_.krefs, values, itemId, needClearCache);
		if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		if (!index.Opts().IsSparse()) {
			pl.Set(field.index(), ns_.krefs);
		}
	}
}

}  // namespace reindexer
