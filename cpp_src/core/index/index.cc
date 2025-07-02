#include "index.h"
#include "indexordered.h"
#include "indextext/fastindextext.h"
#include "indextext/fuzzyindextext.h"
#include "rtree/indexrtree.h"
#include "tools/logger.h"
#include "ttlindex.h"
#include "uuid_index.h"

#if RX_WITH_BUILTIN_ANN_INDEXES
#include "float_vector/hnsw_index.h"
#endif
#if RX_WITH_FAISS_ANN_INDEXES
#include "float_vector/ivf_index.h"
#endif

namespace reindexer {

Index::Index(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields)
	: type_(idef.IndexType()), opts_(idef.Opts()), payloadType_(std::move(payloadType)), fields_(std::move(fields)) {
	reindexer::deepCopy(name_, idef.Name());  // Avoiding false positive TSAN-warning for COW strings on centos7
	logFmt(LogTrace, "Index::Index ('{}',{},{})  {}{}{}", idef.Name(), idef.IndexTypeStr(), idef.FieldType(),
		   idef.Opts().IsPK() ? ",pk" : "", idef.Opts().IsDense() ? ",dense" : "", idef.Opts().IsArray() ? ",array" : "");
}

Index::Index(const Index& obj)
	: type_(obj.type_),
	  sortOrders_(obj.sortOrders_),
	  sortId_(obj.sortId_),
	  opts_(obj.opts_),
	  payloadType_(obj.payloadType_),
	  fields_(obj.fields_),
	  keyType_(obj.keyType_),
	  selectKeyType_(obj.selectKeyType_),
	  sortedIdxCount_(obj.sortedIdxCount_) {
	reindexer::deepCopy(name_, obj.name_);	// Avoiding false positive TSAN-warning for COW strings on centos7
}

static std::unique_ptr<Index> createANNIfAvailable(const IndexDef& idef, [[maybe_unused]] PayloadType&& payloadType,
												   [[maybe_unused]] FieldsSet&& fields, [[maybe_unused]] size_t currentNsSize,
												   [[maybe_unused]] LogCreation log) {
	const auto itype = idef.IndexType();
	if (itype == IndexHnsw) {
#if RX_WITH_BUILTIN_ANN_INDEXES
		return HnswIndex_New(idef, std::move(payloadType), std::move(fields), currentNsSize, log);
#else	// RX_WITH_BUILTIN_ANN_INDEXES
		throw Error(errParams,
					"Reindexer was built without builtin ANN-indexes support and unable to create '{}' HNSW index (check BUILD_ANN_INDEXES "
					"cmake option)",
					idef.Name());
#endif	// RX_WITH_BUILTIN_ANN_INDEXES
	} else if (itype == IndexVectorBruteforce) {
#if RX_WITH_BUILTIN_ANN_INDEXES
		return BruteForceVectorIndex_New(idef, std::move(payloadType), std::move(fields), currentNsSize, log);
#else	// RX_WITH_BUILTIN_ANN_INDEXES
		throw Error(errParams,
					"Reindexer was built without builtin ANN-indexes support and unable to create '{}' bruteforce index (check "
					"BUILD_ANN_INDEXES cmake option)",
					idef.Name());
#endif	// RX_WITH_BUILTIN_ANN_INDEXES
	} else if (itype == IndexIvf) {
#if RX_WITH_FAISS_ANN_INDEXES
		return IvfIndex_New(idef, std::move(payloadType), std::move(fields), log);
#else	// RX_WITH_FAISS_ANN_INDEXES
		throw Error(errParams,
					"Reindexer was built without FAISS ANN-indexes support and unable to create '{}' IVF index (check BUILD_ANN_INDEXES "
					"cmake option)",
					idef.Name());
#endif	// RX_WITH_FAISS_ANN_INDEXES
	}
	throw Error(errLogic, "Unexpected ANN index type: {} for '{}'", int(itype), idef.Name());
}

std::unique_ptr<Index> Index::New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
								  const NamespaceCacheConfigData& cacheCfg, size_t currentNsSize, LogCreation log) {
	switch (idef.IndexType()) {
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexDoubleBTree:
		case IndexInt64BTree:
		case IndexCompositeBTree:
			return IndexOrdered_New(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexStrHash:
		case IndexIntHash:
		case IndexInt64Hash:
		case IndexCompositeHash:
			return IndexUnordered_New(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexIntStore:
		case IndexStrStore:
		case IndexInt64Store:
		case IndexDoubleStore:
		case IndexBool:
		case IndexUuidStore:
			return IndexStore_New(idef, std::move(payloadType), std::move(fields));
		case IndexFastFT:
		case IndexCompositeFastFT:
			return FastIndexText_New(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexFuzzyFT:
		case IndexCompositeFuzzyFT:
			return FuzzyIndexText_New(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexTtl:
			return TtlIndex_New(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case ::IndexRTree:
			return IndexRTree_New(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexUuidHash:
			return IndexUuid_New(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexHnsw:
		case IndexVectorBruteforce:
		case IndexIvf:
			return createANNIfAvailable(idef, std::move(payloadType), std::move(fields), currentNsSize, log);
		case IndexDummy:
			throw Error(errParams, "'Dummy' index type can not be used to create index ('{}')", idef.Name());
	}
	throw Error(errParams, "Invalid index type {} for index '{}'", int(idef.IndexType()), idef.Name());
}

bool Index::IsFloatVector() const noexcept {
	assertrx_dbg(bool(dynamic_cast<const FloatVectorIndex*>(this)) == opts_.IsFloatVector());
	return opts_.IsFloatVector();
}

IndexPerfStat Index::GetIndexPerfStat() {
	return IndexPerfStat(name_, selectPerfCounter_.Get<PerfStat>(), commitPerfCounter_.Get<PerfStat>());
}

void Index::ResetIndexPerfStat() {
	selectPerfCounter_.Reset();
	commitPerfCounter_.Reset();
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
		if (i != 0) {
			os << ", ";
		}
		os << sortOrders_[i];
	}
	os << "],\n" << newOffset << "sortId: " << sortId_ << ",\n" << newOffset << "opts: ";
	opts_.Dump(os);
	os << ",\n" << newOffset << "payloadType: ";
	payloadType_.Dump(os, step, newOffset);
	if (IsComposite(type_)) {
		os << ",\n" << newOffset << "fields: ";
		fields_.Dump(os, DumpWithMask_True);
	}
	os << ",\n"
	   << newOffset << "sortedIdxCount: " << sortedIdxCount_ << ",\n"
	   << newOffset << "isBuilt: " << std::boolalpha << isBuilt_ << '\n'
	   << offset << '}';
}

template void Index::dump<std::ostream>(std::ostream&, std::string_view, std::string_view) const;

}  // namespace reindexer
