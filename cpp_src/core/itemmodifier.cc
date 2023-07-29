#include "itemmodifier.h"
#include "core/namespace/namespaceimpl.h"
#include "core/query/expressionevaluator.h"
#include "core/selectfunc/functionexecutor.h"
#include "index/index.h"
#include "tools/logger.h"

namespace reindexer {

ItemModifier::FieldData::FieldData(const UpdateEntry &entry, NamespaceImpl &ns)
	: entry_(entry), tagsPathWithLastIndex_{std::nullopt}, arrayIndex_(IndexValueType::NotSet), isIndex_(false) {
	if (ns.getIndexByName(entry_.Column(), fieldIndex_)) {
		isIndex_ = true;
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
				const auto &fld{ns.payloadType_.Field(fieldIndex_)};
				for (const auto &jp : fld.JsonPaths()) {
					tagsPath_ = ns.tagsMatcher_.path2indexedtag(jp, nullptr, true);
					if (!tagsPath_.empty()) break;
				}
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

	VariantArray values;
	for (FieldData &field : fieldsToModify_) {
		values.clear<false>();
		if (field.details().IsExpression()) {
			assertrx(field.details().Values().size() > 0);
			values = ev.Evaluate(static_cast<std::string_view>(field.details().Values().front()), pv, field.name());
			field.updateTagsPath(ns_.tagsMatcher_,
								 [&ev, &pv, &field](std::string_view expression) { return ev.Evaluate(expression, pv, field.name()); });
		} else {
			values = field.details().Values();
		}

		if (values.IsArrayValue() && field.tagspathWithLastIndex().back().IsArrayNode()) {
			throw Error(errParams, "Array items are supposed to be updated with a single value, not an array");
		}

		if (field.details().Mode() == FieldModeSetJson || !field.isIndex()) {
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
		if (v.Type().Is<KeyValueType::String>()) {
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

	if (index.Opts().GetCollateMode() == CollateUTF8) {
		for (const Variant &key : values) key.EnsureUTF8();
	}

	auto strHolder = ns_.StrHolder(ctx);
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
		compositeIdx->Delete(Variant(ns_.items_[itemId]), itemId, *strHolder, needClearCache);
		if (needClearCache && compositeIdx->IsOrdered()) indexesCacheCleaner.Add(compositeIdx->SortId());
	}

	const auto insertItemIntoCompositeIndexes = [&] {
		for (int i = firstCompositePos; i < totalIndexes; ++i) {
			if (!needUpdateCompIndexes[i - firstCompositePos]) continue;
			bool needClearCache{false};
			auto &compositeIdx = ns_.indexes_[i];
			compositeIdx->Upsert(Variant(ns_.items_[itemId]), itemId, needClearCache);
			if (needClearCache && compositeIdx->IsOrdered()) indexesCacheCleaner.Add(compositeIdx->SortId());
		}
	};

	try {
		if (field.isIndex()) {
			modifyIndexValues(itemId, field, values, pl, ctx);
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
			ns_.tagsMatcher_.try_merge(item.tagsMatcher());
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

void ItemModifier::modifyIndexValues(IdType itemId, const FieldData &field, VariantArray &values, Payload &pl, const NsContext &ctx) {
	Index &index = *(ns_.indexes_[field.index()]);
	if (values.IsNullValue() && !index.Opts().IsArray()) {
		throw Error(errParams, "Non-array index fields cannot be set to null!");
	}
	auto strHolder = ns_.StrHolder(ctx);
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
			index.Delete(ns_.skrefs.front(), itemId, *strHolder, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		}

		bool needClearCache{false};
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
			pl.Get(field.index(), ns_.skrefs, true);
		}
		if (!ns_.skrefs.empty()) {
			bool needClearCache{false};
			index.Delete(ns_.skrefs, itemId, *strHolder, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
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
