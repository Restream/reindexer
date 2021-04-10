#include "itemmodifier.h"
#include "core/namespace/namespaceimpl.h"
#include "core/query/expressionevaluator.h"
#include "core/selectfunc/functionexecutor.h"
#include "index/index.h"
#include "tools/logger.h"

namespace reindexer {

ItemModifier::FieldData::FieldData(const UpdateEntry &entry, NamespaceImpl &ns)
	: entry_(entry), tagsPath_(), fieldIndex_(0), arrayIndex_(IndexValueType::NotSet), isIndex_(false) {
	if (ns.getIndexByName(entry_.column, fieldIndex_)) {
		jsonPath_ = entry.column;
		isIndex_ = true;
	} else {
		IndexedTagsPath tagsPath = ns.tagsMatcher_.path2indexedtag(entry_.column, nullptr, true);
		for (size_t i = 0; i < tagsPath.size(); ++i) {
			if (i) jsonPath_ += ".";
			jsonPath_ += ns.tagsMatcher_.tag2name(tagsPath[i].NameTag());
		}
		isIndex_ = ns.getIndexByName(jsonPath_, fieldIndex_);
	}
}

void ItemModifier::FieldData::updateTagsPath(TagsMatcher &tm, const IndexExpressionEvaluator &ev) {
	if (tagsPath_.empty()) {
		tagsPath_ = tm.path2indexedtag(entry_.column, ev, true);
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
		if (isLast && isIndex_ && tagsPath_[i].IsWithIndex() && !tagsPath_[i].IsForAllItems()) {
			arrayIndex_ = tagsPath_[i].Index();
			tagsPath_[i].SetIndex(IndexValueType::NotSet);
		}
	}
}

ItemModifier::ItemModifier(const h_vector<UpdateEntry, 0> &updateEntries, NamespaceImpl &ns) : ns_(ns), updateEntries_(updateEntries) {
	for (const UpdateEntry &updateField : updateEntries_) {
		fieldsToModify_.emplace_back(updateField, ns_);
	}
}

void ItemModifier::Modify(IdType itemId, const NsContext &ctx) {
	PayloadValue &pv = ns_.items_[itemId];
	Payload pl(ns_.payloadType_, pv);
	pv.Clone(pl.RealSize());

	FunctionExecutor funcExecutor(ns_);
	ExpressionEvaluator ev(ns_.payloadType_, ns_.tagsMatcher_, funcExecutor);

	for (FieldData &field : fieldsToModify_) {
		VariantArray values;
		if (field.details().isExpression) {
			assert(field.details().values.size() > 0);
			values = ev.Evaluate(static_cast<string_view>(field.details().values.front()), pv, field.name());
		} else {
			values = field.details().values;
		}

		field.updateTagsPath(ns_.tagsMatcher_,
							 [&ev, &pv, &field](string_view expression) { return ev.Evaluate(expression, pv, field.name()); });

		if (field.details().mode == FieldModeSetJson) {
			modifyCJSON(pv, field, values, ctx);
		} else {
			modifyField(itemId, field, pl, values);
		}
	}

	ns_.markUpdated();
}

void ItemModifier::modifyCJSON(PayloadValue &pv, FieldData &field, VariantArray &values, const NsContext &ctx) {
	ItemImpl itemimpl(ns_.payloadType_, pv, ns_.tagsMatcher_);
	itemimpl.ModifyField(field.tagspath(), values, field.details().mode);

	NsContext nsCtx{ctx.rdxContext};
	nsCtx.NoLock();

	Item item = ns_.NewItem(nsCtx);
	Error err = item.FromCJSON(itemimpl.GetCJSON(true));
	if (!err.ok()) throw err;

	ItemImpl *impl = item.impl_;
	ns_.setFieldsBasedOnPrecepts(impl);
	ns_.updateTagsMatcherFromItem(impl);

	auto originalItem = ns_.findByPK(impl, ctx.rdxContext);
	if (!originalItem.second) {
		item.setID(-1);
		return;
	}
	IdType id = originalItem.first;
	item.setID(id);

	auto &plData = ns_.items_[id];
	Payload pl(ns_.payloadType_, plData);
	Payload plNew = impl->GetPayload();
	plData.Clone(pl.RealSize());

	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		ns_.indexes_[i]->Delete(Variant(plData), id);
	}

	assert(ns_.indexes_.firstCompositePos() != 0);
	const int borderIdx = ns_.indexes_.totalSize() > 1 ? 1 : 0;
	int fieldIdx = borderIdx;
	do {
		fieldIdx %= ns_.indexes_.firstCompositePos();
		Index &index = *(ns_.indexes_[fieldIdx]);
		bool isIndexSparse = index.Opts().IsSparse();
		assert(!isIndexSparse || (isIndexSparse && index.Fields().getTagsPathsLength() > 0));

		if (isIndexSparse) {
			assert(index.Fields().getTagsPathsLength() > 0);
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

		if (isIndexSparse) {
			try {
				pl.GetByJsonPath(index.Fields().getTagsPath(0), ns_.krefs, index.KeyType());
			} catch (const Error &) {
				ns_.krefs.resize(0);
			}
		} else {
			pl.Get(fieldIdx, ns_.krefs, index.Opts().IsArray());
		}
		index.Delete(ns_.krefs, id);

		ns_.krefs.resize(0);
		index.Upsert(ns_.krefs, ns_.skrefs, id, !isIndexSparse);

		if (!isIndexSparse) {
			pl.Set(fieldIdx, ns_.krefs);
		}
	} while (++fieldIdx != borderIdx);

	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		ns_.indexes_[i]->Upsert(Variant(plData), id);
	}
	impl->RealValue() = pv;

	ns_.markUpdated();
}

void ItemModifier::modifyField(IdType itemId, FieldData &field, Payload &pl, VariantArray &values) {
	Index &index = *(ns_.indexes_[field.index()]);
	if (field.isIndex() && !index.Opts().IsSparse() && (field.details().mode == FieldModeDrop)) {
		throw Error(errLogic, "It's only possible to drop sparse or non-index fields via UPDATE statement!");
	}

	assert(!index.Opts().IsSparse() || (index.Opts().IsSparse() && index.Fields().getTagsPathsLength() > 0));
	if (field.isIndex() && !index.Opts().IsArray() && values.IsArrayValue()) {
		throw Error(errLogic, "It's not possible to Update single index fields with arrays!");
	}

	if (index.Opts().IsSparse()) {
		pl.GetByJsonPath(index.Fields().getTagsPath(0), ns_.skrefs, index.KeyType());
	} else if (!index.Opts().IsArray()) {
		pl.Get(field.index(), ns_.skrefs);
	}

	if (index.Opts().GetCollateMode() == CollateUTF8) {
		for (const Variant &key : values) key.EnsureUTF8();
	}

	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		ns_.indexes_[i]->Delete(Variant(ns_.items_[itemId]), itemId);
	}

	if (field.isIndex()) {
		modifyIndexValues(itemId, field, values, pl);
	}

	for (int i = ns_.indexes_.firstCompositePos(); i < ns_.indexes_.totalSize(); ++i) {
		ns_.indexes_[i]->Upsert(Variant(ns_.items_[itemId]), itemId);
	}

	if (index.Opts().IsSparse() || index.Opts().IsArray() || !field.isIndex()) {
		ItemImpl item(ns_.payloadType_, *(pl.Value()), ns_.tagsMatcher_);
		Variant oldTupleValue = item.GetField(0);
		oldTupleValue.EnsureHold();
		ns_.indexes_[0]->Delete(oldTupleValue, itemId);
		item.ModifyField(field.tagspath(), values, field.details().mode);
		Variant tupleValue = ns_.indexes_[0]->Upsert(item.GetField(0), itemId);
		pl.Set(0, {tupleValue});
		ns_.tagsMatcher_.try_merge(item.tagsMatcher());
	}
}

void ItemModifier::modifyIndexValues(IdType itemId, const FieldData &field, VariantArray &values, Payload &pl) {
	Index &index = *(ns_.indexes_[field.index()]);
	if (values.IsNullValue() && !index.Opts().IsArray()) {
		throw Error(errParams, "Non-array index fields cannot be set to null!");
	}
	bool isArrayItem = (field.tagspath().back().IsForAllItems() || field.arrayIndex() != IndexValueType::NotSet);
	if (index.Opts().IsArray() && values.IsArrayValue() && isArrayItem) {
		throw Error(errParams, "Array items are supposed to be updated with a single value, not an array");
	}
	if (index.Opts().IsArray() && !values.IsArrayValue() && !values.IsNullValue()) {
		if (values.empty()) {
			throw Error(errParams, "Cannot update array item with an empty value");
		}
		int offset = 0, length = 0;
		ns_.krefs.resize(0);
		ns_.krefs.emplace_back(values.front().convert(index.KeyType()));
		if (field.tagspath().back().IsForAllItems()) {
			ns_.skrefs = pl.GetIndexedArrayData(field.tagspath(), offset, length);
			if (!ns_.skrefs.empty()) {
				index.Delete(ns_.skrefs, itemId);
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
					index.Delete(ns_.skrefs.front(), itemId);
				}
				if (!index.Opts().IsSparse()) {
					pl.Set(field.index(), offset, ns_.krefs.front());
				}
			} else {
				throw Error(errLogic, "Array index is out of range: [%d/%d]", field.arrayIndex(), length);
			}
		}
		index.Upsert(ns_.krefs.front(), itemId);
		if (!index.Opts().IsSparse()) values.resize(length);
	} else {
		if (index.Opts().IsArray() && !index.Opts().IsSparse()) {
			pl.Get(field.index(), ns_.skrefs, true);
		}
		if (!ns_.skrefs.empty()) {
			index.Delete(ns_.skrefs, itemId);
		}
		ns_.krefs.resize(0);
		ns_.krefs.reserve(values.size());
		for (Variant &key : values) {
			key.convert(index.KeyType());
		}
		index.Upsert(ns_.krefs, values, itemId, true);
		if (!index.Opts().IsSparse()) {
			pl.Set(field.index(), ns_.krefs);
		}
	}
}

}  // namespace reindexer
