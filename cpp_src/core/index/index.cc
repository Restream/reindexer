#include "index.h"
#include "core/namespacedef.h"
#include "indexordered.h"
#include "indextext/fastindextext.h"
#include "indextext/fuzzyindextext.h"
#include "rtree/indexrtree.h"
#include "tools/logger.h"
#include "ttlindex.h"
#include "uuid_index.h"

namespace reindexer {

Index::Index(const IndexDef& idef, PayloadType payloadType, const FieldsSet& fields)
	: type_(idef.Type()), name_(idef.name_), opts_(idef.opts_), payloadType_(std::move(payloadType)), fields_(fields) {
	logPrintf(LogTrace, "Index::Index ('%s',%s,%s)  %s%s%s", idef.name_, idef.indexType_, idef.fieldType_, idef.opts_.IsPK() ? ",pk" : "",
			  idef.opts_.IsDense() ? ",dense" : "", idef.opts_.IsArray() ? ",array" : "");
}

Index::Index(const Index& obj)
	: type_(obj.type_),
	  name_(obj.name_),
	  sortOrders_(obj.sortOrders_),
	  sortId_(obj.sortId_),
	  opts_(obj.opts_),
	  payloadType_(obj.payloadType_),
	  fields_(obj.fields_),
	  keyType_(obj.keyType_),
	  selectKeyType_(obj.selectKeyType_),
	  sortedIdxCount_(obj.sortedIdxCount_) {}

std::unique_ptr<Index> Index::New(const IndexDef& idef, PayloadType payloadType, const FieldsSet& fields) {
	switch (idef.Type()) {
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexDoubleBTree:
		case IndexInt64BTree:
		case IndexCompositeBTree:
			return IndexOrdered_New(idef, std::move(payloadType), fields);
		case IndexStrHash:
		case IndexIntHash:
		case IndexInt64Hash:
		case IndexCompositeHash:
			return IndexUnordered_New(idef, std::move(payloadType), fields);
		case IndexIntStore:
		case IndexStrStore:
		case IndexInt64Store:
		case IndexDoubleStore:
		case IndexBool:
		case IndexUuidStore:
			return IndexStore_New(idef, std::move(payloadType), fields);
		case IndexFastFT:
		case IndexCompositeFastFT:
			return FastIndexText_New(idef, std::move(payloadType), fields);
		case IndexFuzzyFT:
		case IndexCompositeFuzzyFT:
			return FuzzyIndexText_New(idef, std::move(payloadType), fields);
		case IndexTtl:
			return TtlIndex_New(idef, std::move(payloadType), fields);
		case ::IndexRTree:
			return IndexRTree_New(idef, std::move(payloadType), fields);
		case IndexUuidHash:
			return IndexUuid_New(idef, std::move(payloadType), fields);
	}
	throw Error(errParams, "Ivalid index type %d for index '%s'", idef.Type(), idef.name_);
}

template <typename S>
void Index::dump(S& os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << "{\n"
	   << newOffset << "name: " << name_ << ",\n"
	   << newOffset << "type: " << type_ << ",\n"
	   << newOffset << "keyType: " << keyType_.Name() << ",\n"
	   << newOffset << "selectKeyType: " << selectKeyType_.Name() << ",\n"
	   << newOffset << "sortOrders: [";
	for (size_t i = 0; i < sortOrders_.size(); ++i) {
		if (i != 0) os << ", ";
		os << sortOrders_[i];
	}
	os << "],\n" << newOffset << "sortId: " << sortId_ << ",\n" << newOffset << "opts: ";
	opts_.Dump(os);
	os << ",\n" << newOffset << "payloadType: ";
	payloadType_.Dump(os, step, newOffset);
	if (IsComposite(type_)) {
		os << ",\n" << newOffset << "fields: ";
		fields_.Dump(os);
	}
	os << ",\n"
	   << newOffset << "sortedIdxCount: " << sortedIdxCount_ << ",\n"
	   << newOffset << "isBuilt: " << std::boolalpha << isBuilt_ << '\n'
	   << offset << '}';
}

template void Index::dump<std::ostream>(std::ostream&, std::string_view, std::string_view) const;

}  // namespace reindexer
