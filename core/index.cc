
#include "core/index.h"
#include <algorithm>
#include "core/indexordered.h"
#include "core/indexstore.h"
#include "core/indextext.h"
#include "core/indexunordered.h"
#include "core/newindextext.h"
#include "core/payload.h"
#include "tools/errors.h"
#include "tools/logger.h"
#include "tools/strings.h"

using std::sort;

namespace reindexer {

void Index::KeyEntry::UpdateSortedIds(const UpdateSortedContext& ctx) {
	ids_.reserve((ctx.getSortedIdxCount() + 1) * ids_.size());
	assert(ctx.getCurSortId());

	auto idsAsc = Sorted(ctx.getCurSortId());

	size_t idx = 0;
	// For all ids of current key
	for (auto id : ids_) {
		assertf(id < (int)ctx.ids2Sorts().size(), "id=%d,ctx.ids2Sorts().size()=%d", id, (int)ctx.ids2Sorts().size());
		idsAsc[idx++] = ctx.ids2Sorts()[id];
	}
	sort(idsAsc.begin(), idsAsc.end());
}

Index::Index(IndexType _type, const string& _name, const IndexOpts& opts, const FieldsSet& fields)
	: type(_type), name(_name), opts_(opts), fields_(fields) {
	logPrintf(LogTrace, "Index::Index (%s)%s", _name.c_str(), opts.IsPK ? ",pk" : "");
}

Index::~Index() { logPrintf(LogTrace, "Index::~Index (%s)", name.c_str()); }

Index* Index::New(IndexType type, const string& name, const IndexOpts& opts) {
	switch (type) {
		case IndexHash:
			return new IndexUnordered<unordered_str_map<KeyEntry>>(type, name, opts);
		case IndexIntHash:
			return new IndexUnordered<unordered_map<int, KeyEntry>>(type, name, opts);
		case IndexBool:
			return new IndexStore<int>(type, name, opts);
		case IndexInt64Hash:
			return new IndexUnordered<unordered_map<int64_t, KeyEntry>>(type, name, opts);
		case IndexInt:
			return new IndexOrdered<map<int, KeyEntry>>(type, name, opts);
		case IndexInt64:
			return new IndexOrdered<map<int64_t, KeyEntry>>(type, name, opts);
		case IndexDouble:
			return new IndexOrdered<map<double, KeyEntry>>(type, name, opts);
		case IndexTree:
			return new IndexOrdered<str_map<KeyEntry>>(type, name, opts);
		case IndexFullText:
			return new IndexText<unordered_str_map<KeyEntry>>(type, name);
		case IndexNewFullText:
			return new NewIndexText<unordered_str_map<KeyEntry>>(type, name);
		case IndexIntStore:
			return new IndexStore<int>(type, name, opts);
		case IndexStrStore:
			return new IndexStore<key_string>(type, name, opts);
		case IndexInt64Store:
			return new IndexStore<int64_t>(type, name, opts);
		case IndexDoubleStore:
			return new IndexStore<double>(type, name, opts);
		default:
			throw Error(errParams, "Ivalid index type %d for index '%s'", type, name.c_str());
	}
	return nullptr;
}

Index* Index::NewComposite(IndexType type, const string& name, const IndexOpts& opts, const PayloadType& payloadType,
						   const FieldsSet& fields) {
	switch (type) {
		case IndexCompositeHash:
			return new IndexUnordered<unordered_map<PayloadData, KeyEntry, hash_composite, equal_composite>>(type, name, opts, payloadType,
																											 fields);
		case IndexComposite:
			return new IndexOrdered<map<PayloadData, KeyEntry, less_composite>>(type, name, opts, payloadType, fields);
		default:
			throw Error(errParams, "Ivalid index type %d for index '%s'", type, name.c_str());
	}
	return nullptr;
}

KeyValueType Index::KeyType() {
	switch (type) {
		case IndexFullText:
		case IndexNewFullText:
		case IndexHash:
		case IndexTree:
		case IndexStrStore:
			return KeyValueString;
		case IndexInt:
		case IndexBool:
		case IndexIntHash:
		case IndexIntStore:
			return KeyValueInt;
		case IndexInt64:
		case IndexInt64Hash:
		case IndexInt64Store:
			return KeyValueInt64;
		case IndexDoubleStore:
		case IndexDouble:
			return KeyValueDouble;
		case IndexComposite:
		case IndexCompositeHash:
			return KeyValueComposite;
	}
	return KeyValueUndefined;
}

}  // namespace reindexer
