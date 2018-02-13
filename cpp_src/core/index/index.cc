#include "core/index/index.h"
#include "core/index/indexordered.tcc"
#include "core/index/indexstore.tcc"
#include "core/index/indextext/fasttextsearch.tcc"
#include "core/index/indextext/fuzzytextsearch.tcc"
#include "core/index/indexunordered.tcc"

namespace reindexer {

Index::Index(IndexType _type, const string& _name, const IndexOpts& opts, const PayloadType::Ptr payloadType, const FieldsSet& fields)
	: type(_type), name(_name), opts_(opts), payloadType_(payloadType), fields_(fields) {
	logPrintf(LogTrace, "Index::Index (%s)%s", _name.c_str(), opts.IsPK() ? ",pk" : "");
}

Index::~Index() { logPrintf(LogTrace, "Index::~Index (%s)", name.c_str()); }

Index* Index::New(IndexType type, const string& name, const IndexOpts& opts) {
	bool dense = opts.IsPK() || opts.IsDense();
	switch (type) {
		case IndexStrHash:
			return dense ? static_cast<Index*>(new IndexUnordered<unordered_str_map<KeyEntryPlain>>(type, name, opts))
						 : static_cast<Index*>(new IndexUnordered<unordered_str_map<KeyEntry>>(type, name, opts));
		case IndexIntHash:
			return dense ? static_cast<Index*>(new IndexUnordered<unordered_map<int, KeyEntryPlain>>(type, name, opts))
						 : static_cast<Index*>(new IndexUnordered<unordered_map<int, KeyEntry>>(type, name, opts));
		case IndexInt64Hash:
			return dense ? static_cast<Index*>(new IndexUnordered<unordered_map<int64_t, KeyEntryPlain>>(type, name, opts))
						 : static_cast<Index*>(new IndexUnordered<unordered_map<int64_t, KeyEntry>>(type, name, opts));
		case IndexIntBTree:
			return dense ? static_cast<Index*>(new IndexOrdered<btree_map<int, KeyEntryPlain>>(type, name, opts))
						 : static_cast<Index*>(new IndexOrdered<btree_map<int, KeyEntry>>(type, name, opts));
		case IndexInt64BTree:
			return dense ? static_cast<Index*>(new IndexOrdered<btree_map<int64_t, KeyEntryPlain>>(type, name, opts))
						 : static_cast<Index*>(new IndexOrdered<btree_map<int64_t, KeyEntry>>(type, name, opts));
		case IndexDoubleBTree:
			return dense ? static_cast<Index*>(new IndexOrdered<btree_map<double, KeyEntryPlain>>(type, name, opts))
						 : static_cast<Index*>(new IndexOrdered<btree_map<double, KeyEntry>>(type, name, opts));
		case IndexStrBTree:
			return dense ? static_cast<Index*>(new IndexOrdered<str_map<KeyEntryPlain>>(type, name, opts))
						 : static_cast<Index*>(new IndexOrdered<str_map<KeyEntry>>(type, name, opts));
		case IndexFullText:
			return new FastIndexText<unordered_str_map<KeyEntryPlain>>(type, name);
		case IndexNewFullText:
			return new FuzzyIndexText<unordered_str_map<KeyEntryPlain>>(type, name);
		case IndexIntStore:
			return new IndexStore<int>(type, name, opts);
		case IndexStrStore:
			return new IndexStore<key_string>(type, name, opts);
		case IndexInt64Store:
			return new IndexStore<int64_t>(type, name, opts);
		case IndexDoubleStore:
			return new IndexStore<double>(type, name, opts);
		case IndexBool:
			return new IndexStore<int>(type, name, opts);

		default:
			throw Error(errParams, "Ivalid index type %d for index '%s'", type, name.c_str());
	}
	return nullptr;
}

Index* Index::NewComposite(IndexType type, const string& name, const IndexOpts& opts, const PayloadType::Ptr payloadType,
						   const FieldsSet& fields) {
	logPrintf(LogInfo, "Index::NewComposite (%s,%d)\n", name.c_str(), type);
	bool dense = opts.IsPK() || opts.IsDense();

	switch (type) {
		case IndexCompositeText:
			return new FastIndexText<unordered_payload_map<KeyEntryPlain>>(type, name, opts, payloadType, fields);
		case IndexCompositeNewText:
			return new FuzzyIndexText<unordered_payload_map<KeyEntryPlain>>(type, name, opts, payloadType, fields);

		case IndexCompositeHash:
			return dense ? static_cast<Index*>(
							   new IndexUnordered<unordered_payload_map<KeyEntryPlain>>(type, name, opts, payloadType, fields))
						 : static_cast<Index*>(new IndexUnordered<unordered_payload_map<KeyEntry>>(type, name, opts, payloadType, fields));
		case IndexCompositeBTree:
			return dense ? static_cast<Index*>(new IndexOrdered<payload_map<KeyEntryPlain>>(type, name, opts, payloadType, fields))
						 : static_cast<Index*>(new IndexOrdered<payload_map<KeyEntry>>(type, name, opts, payloadType, fields));
		default:
			throw Error(errParams, "Ivalid index type %d for index '%s'", type, name.c_str());
	}
	return nullptr;
}

KeyValueType Index::KeyType() {
	switch (type) {
		case IndexFullText:
		case IndexNewFullText:
		case IndexStrHash:
		case IndexStrBTree:
		case IndexStrStore:
			return KeyValueString;
		case IndexIntBTree:
		case IndexBool:
		case IndexIntHash:
		case IndexIntStore:
			return KeyValueInt;
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexInt64Store:
			return KeyValueInt64;
		case IndexDoubleStore:
		case IndexDoubleBTree:
			return KeyValueDouble;
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexCompositeText:
		case IndexCompositeNewText:
			return KeyValueComposite;
	}
	return KeyValueUndefined;
}

string Index::TypeName() {
	switch (type) {
		case IndexFullText:
		case IndexNewFullText:
		case IndexStrHash:
		case IndexStrBTree:
		case IndexStrStore:
			return "string";
		case IndexBool:
			return "bool";
		case IndexIntBTree:
		case IndexIntHash:
		case IndexIntStore:
			return "int";
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexInt64Store:
			return "int64";
		case IndexDoubleStore:
		case IndexDoubleBTree:
			return "double";
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexCompositeText:
		case IndexCompositeNewText:
			return "composite";
	}
	return "undefined";
}

vector<string> Index::Conds() {
	switch (type) {
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexDoubleBTree:
		case IndexInt64BTree:
		case IndexStrHash:
		case IndexIntHash:
		case IndexInt64Hash:
		case IndexIntStore:
		case IndexStrStore:
		case IndexInt64Store:
		case IndexDoubleStore:
			return {"SET", "EQ", "ANY", "EMPTY", "LT", "LE", "GT", "GE", "RANGE"};
		case IndexBool:
			return {"SET", "EQ", "ANY", "EMPTY"};
		case IndexFullText:
		case IndexNewFullText:
			return {"EQ"};
		default:
			return {};
	}
	return {};
}

string Index::CollateMode() {
	switch (opts_.GetCollateMode()) {
		case CollateASCII:
			return "ascii";
		case CollateUTF8:
			return "utf8";
		case CollateNumeric:
			return "numeric";
		case CollateNone:
			return "none";
	}
	return "none";
}
}  // namespace reindexer
