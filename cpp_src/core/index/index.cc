#include "index.h"
#include "core/namespacedef.h"
#include "indexordered.h"
#include "indextext/fastindextext.h"
#include "indextext/fuzzyindextext.h"
#include "tools/logger.h"

namespace reindexer {

Index::Index(IndexType type, const string& name, const IndexOpts& opts, const PayloadType payloadType, const FieldsSet& fields)
	: type_(type), name_(name), opts_(opts), payloadType_(payloadType), fields_(fields) {
	IndexDef def;
	def.FromType(type);
	logPrintf(LogTrace, "Index::Index (%s,%s,%s)  %s%s%s", def.indexType_.c_str(), def.fieldType_.c_str(), name.c_str(),
			  opts.IsPK() ? ",pk" : "", opts.IsDense() ? ",dense" : "", opts.IsArray() ? ",array" : "");
}

Index::~Index() {}

Index* Index::New(IndexType type, const string& name, const IndexOpts& opts, const PayloadType payloadType, const FieldsSet& fields) {
	switch (type) {
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexDoubleBTree:
		case IndexInt64BTree:
		case IndexCompositeBTree:
			return IndexOrdered_New(type, name, opts, payloadType, fields);
		case IndexStrHash:
		case IndexIntHash:
		case IndexInt64Hash:
		case IndexCompositeHash:
			return IndexUnordered_New(type, name, opts, payloadType, fields);
		case IndexIntStore:
		case IndexStrStore:
		case IndexInt64Store:
		case IndexDoubleStore:
		case IndexBool:
			return IndexStore_New(type, name, opts, payloadType, fields);
		case IndexFastFT:
		case IndexCompositeFastFT:
			return FastIndexText_New(type, name, opts, payloadType, fields);
		case IndexFuzzyFT:
		case IndexCompositeFuzzyFT:
			return FuzzyIndexText_New(type, name, opts, payloadType, fields);
		default:
			throw Error(errParams, "Ivalid index type %d for index '%s'", type, name.c_str());
	}
}

}  // namespace reindexer
