#include "core/namespace/namespaceimpl.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include "core/cjson/jsonbuilder.h"
#include "core/index/index.h"
#include "core/itemimpl.h"
#include "core/itemmodifier.h"
#include "core/nsselecter/nsselecter.h"
#include "core/payload/payloadiface.h"
#include "core/rdxcontext.h"
#include "core/selectfunc/functionexecutor.h"
#include "core/storage/storagefactory.h"
#include "namespace.h"
#include "replicator/updatesobserver.h"
#include "replicator/walselecter.h"
#include "tools/errors.h"
#include "tools/flagguard.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
#include "tools/timetools.h"

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using std::make_shared;
using std::thread;
using std::to_string;
using std::defer_lock;

#define kStorageItemPrefix "I"
#define kStorageIndexesPrefix "indexes"
#define kStorageSchemaPrefix "schema"
#define kStorageReplStatePrefix "repl"
#define kStorageTagsPrefix "tags"
#define kStorageMetaPrefix "meta"
#define kStorageCachePrefix "cache"
#define kTupleName "-tuple"

static const string kPKIndexName = "#pk";
const int kWALStatementItemsThreshold = 5;

#define kStorageMagic 0x1234FEDC
#define kStorageVersion 0x8

namespace reindexer {

constexpr int64_t kStorageSerialInitial = 1;
constexpr uint8_t kSysRecordsBackupCount = 8;
constexpr uint8_t kSysRecordsFirstWriteCopies = 3;

NamespaceImpl::IndexesStorage::IndexesStorage(const NamespaceImpl &ns) : ns_(ns) {}

void NamespaceImpl::IndexesStorage::MoveBase(IndexesStorage &&src) { Base::operator=(move(src)); }

// private implementation and NOT THREADSAFE of copy CTOR
NamespaceImpl::NamespaceImpl(const NamespaceImpl &src)
	: indexes_{*this},
	  indexesNames_{src.indexesNames_},
	  items_{src.items_},
	  free_{src.free_},
	  name_{src.name_},
	  payloadType_{src.payloadType_},
	  tagsMatcher_{src.tagsMatcher_},
	  storage_{src.storage_},
	  updates_{src.updates_},
	  unflushedCount_{src.unflushedCount_.load(std::memory_order_acquire)},	 // 0
	  meta_{src.meta_},
	  dbpath_{src.dbpath_},
	  queryCache_{make_shared<QueryCache>()},
	  sparseIndexesCount_{src.sparseIndexesCount_},
	  krefs{src.krefs},
	  skrefs{src.skrefs},
	  sysRecordsVersions_{src.sysRecordsVersions_},
	  joinCache_{make_shared<JoinCache>()},
	  enablePerfCounters_{src.enablePerfCounters_.load()},
	  config_{src.config_},
	  wal_{src.wal_},
	  repl_{src.repl_},
	  observers_{src.observers_},
	  storageOpts_{src.storageOpts_},
	  lastSelectTime_{0},
	  cancelCommit_{false},
	  lastUpdateTime_{src.lastUpdateTime_.load(std::memory_order_acquire)},
	  itemsCount_{static_cast<uint32_t>(items_.size())},
	  itemsCapacity_{static_cast<uint32_t>(items_.capacity())},
	  nsIsLoading_{false},
	  serverId_{src.serverId_},
	  itemsDataSize_{src.itemsDataSize_},
	  optimizationState_{NotOptimized} {
	for (auto &idxIt : src.indexes_) indexes_.push_back(unique_ptr<Index>(idxIt->Clone()));

	markUpdated();
	logPrintf(LogTrace, "Namespace::CopyContentsFrom (%s)", name_);
}

NamespaceImpl::NamespaceImpl(const string &name, UpdatesObservers &observers)
	: indexes_(*this),
	  name_(name),
	  payloadType_(name),
	  tagsMatcher_(payloadType_),
	  unflushedCount_{0},
	  queryCache_(make_shared<QueryCache>()),
	  joinCache_(make_shared<JoinCache>()),
	  enablePerfCounters_(false),
	  wal_(config_.walSize),
	  observers_(&observers),
	  lastSelectTime_(0),
	  cancelCommit_(false),
	  lastUpdateTime_(0),
	  nsIsLoading_(false),
	  serverIdChanged_(false) {
	logPrintf(LogTrace, "NamespaceImpl::NamespaceImpl (%s)", name_);
	FlagGuardT nsLoadingGuard(nsIsLoading_);
	items_.reserve(10000);
	itemsCapacity_.store(items_.capacity());
	optimizationState_.store(NotOptimized);

	// Add index and payload field for tuple of non indexed fields
	IndexDef tupleIndexDef(kTupleName, {}, IndexStrStore, IndexOpts());
	addIndex(tupleIndexDef);
	updateSelectTime();
}

NamespaceImpl::~NamespaceImpl() {
	flushStorage(RdxContext());
	logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items", name_, items_.size());
}

void NamespaceImpl::OnConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx) {
	NamespaceConfigData configData;
	configProvider.GetNamespaceConfig(GetName(), configData);
	ReplicationConfigData replicationConf = configProvider.GetReplicationConfig();

	enablePerfCounters_ = configProvider.GetProfilingConfig().perfStats;

	auto wlck = wLock(ctx);

	config_ = configData;
	storageOpts_.LazyLoad(configData.lazyLoad);
	storageOpts_.noQueryIdleThresholdSec = configData.noQueryIdleThreshold;

	updateSortedIdxCount();

	if (wal_.Resize(config_.walSize)) {
		logPrintf(LogInfo, "[%s] WAL has been resized lsn #%s, max size %ld", name_, repl_.lastLsn, wal_.Capacity());
	}

	if (isSystem()) return;

	if (serverId_ != replicationConf.serverId) {
		if (itemsCount_ != 0) {
			serverIdChanged_ = true;
			repl_.slaveMode = true;
			repl_.replicatorEnabled = false;
			logPrintf(LogWarning, "Change serverId on non empty ns [%s]. Set read only mode.", name_);
		}
		serverId_ = replicationConf.serverId;
		logPrintf(LogWarning, "[repl:%s]:%d Change serverId", name_, serverId_);
	}

	ReplicationRole curRole;
	if (repl_.slaveMode && !repl_.replicatorEnabled) {	// read only
		curRole = ReplicationReadOnly;
	} else if (repl_.slaveMode && repl_.replicatorEnabled) {
		curRole = ReplicationSlave;
	} else if (!repl_.slaveMode && !repl_.replicatorEnabled) {
		curRole = ReplicationMaster;
	} else {
		curRole = ReplicationNone;
	}

	auto newRole = replicationConf.role;
	if (!replicationConf.namespaces.empty() && replicationConf.namespaces.find(name_) == replicationConf.namespaces.end()) {
		newRole = ReplicationMaster;
	}
	if (newRole == curRole) return;
	if (newRole == ReplicationNone && curRole == ReplicationMaster) return;
	if (curRole == ReplicationReadOnly && serverIdChanged_) return;

	// switch slave  -> master
	// switch master -> slave

	if (curRole == ReplicationSlave && newRole == ReplicationMaster) {
		repl_.slaveMode = false;
		repl_.replicatorEnabled = false;
		repl_.lastUpstreamLSN = lsn_t();
		repl_.originLSN = lsn_t(wal_.LSNCounter() - 1, serverId_);
		logPrintf(LogInfo, "[repl:%s]:%d Switch from slave to master '%s'", name_, serverId_, name_);
	} else if (curRole == ReplicationMaster && newRole == ReplicationSlave) {
		// real transition ns to slave in OpenNamespace
	} else if (curRole == ReplicationReadOnly) {
		repl_.slaveMode = false;
		repl_.replicatorEnabled = false;
		repl_.lastUpstreamLSN = lsn_t();
		repl_.originLSN = lsn_t(wal_.LSNCounter() - 1, serverId_);
		logPrintf(LogInfo, "[repl:%s]:%d Switch from read only to slave/master '%s'", name_, serverId_, name_);
	}

	unflushedCount_.fetch_add(1, std::memory_order_release);

	logPrintf(LogInfo, "Replication role changed '%s' %d", name_, replicationConf.role);
}

void NamespaceImpl::recreateCompositeIndexes(int startIdx, int endIdx) {
	for (int i = startIdx; i < endIdx; ++i) {
		std::unique_ptr<reindexer::Index> &index(indexes_[i]);
		if (isComposite(index->Type())) {
			IndexDef indexDef;
			indexDef.name_ = index->Name();
			indexDef.opts_ = index->Opts();
			indexDef.FromType(index->Type());

			index.reset(Index::New(indexDef, payloadType_, index->Fields()));
		}
	}
}

void NamespaceImpl::updateItems(PayloadType oldPlType, const FieldsSet &changedFields, int deltaFields) {
	logPrintf(LogTrace, "Namespace::updateItems(%s) delta=%d", name_, deltaFields);

	assert(oldPlType->NumFields() + deltaFields == payloadType_->NumFields());

	int compositeStartIdx = 0;
	if (deltaFields >= 0) {
		compositeStartIdx = indexes_.firstCompositePos();
	} else {
		compositeStartIdx = indexes_.firstCompositePos(oldPlType, sparseIndexesCount_);
	}
	int compositeEndIdx = indexes_.totalSize();
	recreateCompositeIndexes(compositeStartIdx, compositeEndIdx);

	for (auto &idx : indexes_) {
		idx->UpdatePayloadType(payloadType_);
	}

	VariantArray skrefsDel, skrefsUps;
	ItemImpl newItem(payloadType_, tagsMatcher_);
	newItem.Unsafe(true);
	int errCount = 0;
	Error lastErr = errOK;
	repl_.dataHash = 0;
	itemsDataSize_ = 0;
	for (size_t rowId = 0; rowId < items_.size(); rowId++) {
		if (items_[rowId].IsFree()) {
			continue;
		}
		PayloadValue &plCurr = items_[rowId];
		Payload oldValue(oldPlType, plCurr);
		ItemImpl oldItem(oldPlType, plCurr, tagsMatcher_);
		oldItem.Unsafe(true);
		auto err = newItem.FromCJSON(&oldItem);
		if (!err.ok()) {
			logPrintf(LogTrace, "Can't apply indexes: %s", err.what());
			errCount++;
			lastErr = err;
		}

		PayloadValue plNew = oldValue.CopyTo(payloadType_, deltaFields >= 0);
		plNew.SetLSN(plCurr.GetLSN());
		Payload newValue(payloadType_, plNew);

		for (auto fieldIdx : changedFields) {
			auto &index = *indexes_[fieldIdx];
			if ((fieldIdx == 0) || deltaFields <= 0) {
				oldValue.Get(fieldIdx, skrefsDel, true);
				index.Delete(skrefsDel, rowId);
			}

			if ((fieldIdx == 0) || deltaFields >= 0) {
				newItem.GetPayload().Get(fieldIdx, skrefsUps);
				krefs.resize(0);
				index.Upsert(krefs, skrefsUps, rowId, true);
				newValue.Set(fieldIdx, krefs);
			}
		}

		for (int fieldIdx = compositeStartIdx; fieldIdx < compositeEndIdx; ++fieldIdx) {
			indexes_[fieldIdx]->Upsert(Variant(plNew), rowId);
		}

		plCurr = std::move(plNew);
		repl_.dataHash ^= Payload(payloadType_, plCurr).GetHash();
		itemsDataSize_ += plCurr.GetCapacity() + sizeof(PayloadValue::dataHeader);
	}
	markUpdated();
	if (errCount != 0) {
		logPrintf(LogError, "Can't update indexes of %d items in namespace %s: %s", errCount, name_, lastErr.what());
	}
}

void NamespaceImpl::addToWAL(const IndexDef &indexDef, WALRecType type, const RdxContext &ctx) {
	WrSerializer ser;
	indexDef.GetJSON(ser);
	WALRecord wrec(type, ser.Slice());
	processWalRecord(wrec, ctx);
}

void NamespaceImpl::addToWAL(string_view json, WALRecType type, const RdxContext &ctx) {
	WALRecord wrec(type, json);
	processWalRecord(wrec, ctx);
}

void NamespaceImpl::AddIndex(const IndexDef &indexDef, const RdxContext &ctx) {
	auto wlck = wLock(ctx);
	addIndex(indexDef);
	saveIndexesToStorage();
	addToWAL(indexDef, WalIndexAdd, ctx);
}

void NamespaceImpl::UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx) {
	auto wlck = wLock(ctx);
	updateIndex(indexDef);
	saveIndexesToStorage();
	addToWAL(indexDef, WalIndexUpdate, ctx);
}

void NamespaceImpl::DropIndex(const IndexDef &indexDef, const RdxContext &ctx) {
	auto wlck = wLock(ctx);
	dropIndex(indexDef);
	saveIndexesToStorage();
	addToWAL(indexDef, WalIndexDrop, ctx);
}

void NamespaceImpl::SetSchema(string_view schema, const RdxContext &ctx) {
	auto wlck = wLock(ctx);
	schema_ = make_shared<Schema>(schema);
	auto fields = schema_->GetPaths();
	for (auto &field : fields) {
		tagsMatcher_.path2tag(field, true);
	}

	schema_->BuildProtobufSchema(tagsMatcher_, payloadType_);

	saveSchemaToStorage();
	addToWAL(schema, WalSetSchema, ctx);
}

std::string NamespaceImpl::GetSchema(int format, const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	WrSerializer ser;
	if (schema_) {
		if (format == JsonSchemaType) {
			schema_->GetJSON(ser);
		} else if (format == ProtobufSchemaType) {
			Error err = schema_->GetProtobufSchema(ser);
			if (!err.ok()) throw err;
		} else {
			throw Error(errParams, "Unknown schema type: %d", format);
		}
	}
	return std::string(ser.Slice());
}

void NamespaceImpl::dropIndex(const IndexDef &index) {
	auto itIdxName = indexesNames_.find(index.name_);
	if (itIdxName == indexesNames_.end()) {
		const char *errMsg = "Cannot remove index %s: doesn't exist";
		logPrintf(LogError, errMsg, index.name_);
		throw Error(errParams, errMsg, index.name_);
	}

	int fieldIdx = itIdxName->second;
	if (indexes_[fieldIdx]->Opts().IsSparse()) --sparseIndexesCount_;

	// Check, that index to remove is not a part of composite index
	for (int i = indexes_.firstCompositePos(); i < indexes_.totalSize(); ++i) {
		if (indexes_[i]->Fields().contains(fieldIdx))
			throw Error(LogError, "Cannot remove index %s : it's a part of a composite index %s", index.name_, indexes_[i]->Name());
	}
	for (auto &namePair : indexesNames_) {
		if (namePair.second >= fieldIdx) {
			namePair.second--;
		}
	}

	const unique_ptr<Index> &indexToRemove = indexes_[fieldIdx];
	if (indexToRemove->Opts().IsPK()) {
		indexesNames_.erase(kPKIndexName);
	}

	// Update indexes fields refs
	for (const unique_ptr<Index> &idx : indexes_) {
		FieldsSet fields = idx->Fields(), newFields;
		int jsonPathIdx = 0;
		for (auto field : fields) {
			if (field == IndexValueType::SetByJsonPath) {
				newFields.push_back(fields.getJsonPath(jsonPathIdx));
				newFields.push_back(fields.getTagsPath(jsonPathIdx));
				jsonPathIdx++;
			} else {
				newFields.push_back(field < fieldIdx ? field : field - 1);
			}
		}
		idx->SetFields(std::move(newFields));
	}

	if (!isComposite(indexToRemove->Type()) && !indexToRemove->Opts().IsSparse()) {
		PayloadType oldPlType = payloadType_;
		payloadType_.Drop(index.name_);
		tagsMatcher_.UpdatePayloadType(payloadType_);
		FieldsSet changedFields{0, fieldIdx};
		updateItems(oldPlType, changedFields, -1);
	}

	indexes_.erase(indexes_.begin() + fieldIdx);
	indexesNames_.erase(itIdxName);
	updateSortedIdxCount();
}

static void verifyConvertTypes(KeyValueType from, KeyValueType to, const PayloadType &payloadType, const FieldsSet &fields) {
	if ((from == KeyValueString || to == KeyValueString) && (from != to)) {
		throw Error(errParams, "Cannot convert key from type %s to %s", Variant::TypeName(from), Variant::TypeName(to));
	}
	static const std::string defaultStringValue;
	Variant value;
	switch (from) {
		case KeyValueInt64:
			value = Variant(int64_t(0));
			break;
		case KeyValueDouble:
			value = Variant(0.0);
			break;
		case KeyValueString:
			value = Variant(defaultStringValue);
			break;
		case KeyValueBool:
			value = Variant(false);
			break;
		case KeyValueNull:
			break;
		case KeyValueInt:
			value = Variant(0);
			break;
		default:
			if (to != from) throw Error(errParams, "Cannot convert key value types");
	}
	value.convert(to, &payloadType, &fields);
}

void NamespaceImpl::verifyUpdateIndex(const IndexDef &indexDef) const {
	const auto idxNameIt = indexesNames_.find(indexDef.name_);
	const auto currentPKIt = indexesNames_.find(kPKIndexName);

	if (idxNameIt == indexesNames_.end()) {
		throw Error(errParams, "Cannot update index %s: doesn't exist", indexDef.name_);
	}
	const auto &oldIndex = indexes_[idxNameIt->second];
	if (indexDef.opts_.IsPK() && !oldIndex->Opts().IsPK() && currentPKIt != indexesNames_.end()) {
		throw Error(errConflict, "Can't add PK index '%s.%s'. Already exists another PK index - '%s'", name_, indexDef.name_,
					indexes_[currentPKIt->second]->Name());
	}
	if (indexDef.opts_.IsArray() != oldIndex->Opts().IsArray()) {
		throw Error(errParams, "Can't update index '%s' in namespace '%s'. Can't convert array index to not array and vice versa",
					indexDef.name_, name_);
	}
	if (indexDef.opts_.IsPK() && indexDef.opts_.IsArray()) {
		throw Error(errParams, "Can't update index '%s' in namespace '%s'. PK field can't be array", indexDef.name_, name_);
	}
	if (indexDef.opts_.IsPK() && isStore(indexDef.Type())) {
		throw Error(errParams, "Can't add index '%s' in namespace '%s'. PK field can't have '-' type", indexDef.name_, name_);
	}

	if (isComposite(indexDef.Type())) {
		verifyUpdateCompositeIndex(indexDef);
		return;
	}

	const auto newIndex = unique_ptr<Index>(Index::New(indexDef, PayloadType(), FieldsSet()));
	if (indexDef.opts_.IsSparse()) {
		const auto newSparseIndex = std::unique_ptr<Index>(Index::New(indexDef, payloadType_, {}));
	} else {
		FieldsSet changedFields{idxNameIt->second};
		PayloadType newPlType = payloadType_;
		newPlType.Drop(indexDef.name_);
		newPlType.Add(PayloadFieldType(newIndex->KeyType(), indexDef.name_, indexDef.jsonPaths_, indexDef.opts_.IsArray()));
		verifyConvertTypes(oldIndex->KeyType(), newIndex->KeyType(), newPlType, changedFields);
	}
}

void NamespaceImpl::addIndex(const IndexDef &indexDef) {
	string indexName = indexDef.name_;

	auto idxNameIt = indexesNames_.find(indexName);
	int idxNo = payloadType_->NumFields();
	IndexOpts opts = indexDef.opts_;
	JsonPaths jsonPaths = indexDef.jsonPaths_;
	auto currentPKIndex = indexesNames_.find(kPKIndexName);

	if (idxNameIt != indexesNames_.end()) {
		IndexDef newIndexDef = indexDef;
		IndexDef oldIndexDef = getIndexDefinition(indexName);
		// reset config
		oldIndexDef.opts_.config = "";
		newIndexDef.opts_.config = "";
		if (newIndexDef == oldIndexDef) {
			return;
		} else {
			throw Error(errConflict, "Index '%s.%s' already exists with different settings", name_, indexName);
		}
	}

	if (!validateIndexName(indexName, indexDef.Type())) {
		throw Error(errParams,
					"Can't add index '%s' in namespace '%s'. Index name contains invalid characters. Only alphas, digits, '+' (for "
					"composite indexes only), '.', '_' "
					"and '-' are allowed",
					indexName, name_);
	}
	// New index case. Just add
	if (currentPKIndex != indexesNames_.end() && opts.IsPK()) {
		throw Error(errConflict, "Can't add PK index '%s.%s'. Already exists another PK index - '%s'", name_, indexName,
					indexes_[currentPKIndex->second]->Name());
	}
	if (opts.IsPK() && opts.IsArray()) {
		throw Error(errParams, "Can't add index '%s' in namespace '%s'. PK field can't be array", indexName, name_);
	}
	if (opts.IsPK() && isStore(indexDef.Type())) {
		throw Error(errParams, "Can't add index '%s' in namespace '%s'. PK field can't have '-' type", indexName, name_);
	}

	if (isComposite(indexDef.Type())) {
		addCompositeIndex(indexDef);
		return;
	}

	auto newIndex = unique_ptr<Index>(Index::New(indexDef, PayloadType(), FieldsSet()));
	FieldsSet fields;
	if (opts.IsSparse()) {
		for (const string &jsonPath : jsonPaths) {
			TagsPath tagsPath = tagsMatcher_.path2tag(jsonPath, true);
			assert(tagsPath.size() > 0);

			fields.push_back(jsonPath);
			fields.push_back(tagsPath);
		}

		++sparseIndexesCount_;
		insertIndex(Index::New(indexDef, payloadType_, fields), idxNo, indexName);
	} else {
		PayloadType oldPlType = payloadType_;

		payloadType_.Add(PayloadFieldType(newIndex->KeyType(), indexName, jsonPaths, newIndex->Opts().IsArray()));
		tagsMatcher_.UpdatePayloadType(payloadType_);
		newIndex->SetFields(FieldsSet{idxNo});
		newIndex->UpdatePayloadType(payloadType_);

		FieldsSet changedFields{0, idxNo};
		insertIndex(newIndex.release(), idxNo, indexName);
		updateItems(oldPlType, changedFields, 1);
	}
	updateSortedIdxCount();
}

void NamespaceImpl::updateIndex(const IndexDef &indexDef) {
	const string &indexName = indexDef.name_;

	IndexDef foundIndex = getIndexDefinition(indexName);

	if (indexDef.IsEqual(foundIndex, true)) {
		// Index has not been changed
		if (!indexDef.IsEqual(foundIndex, false)) {
			// Only index config changed
			// Just call SetOpts
			indexes_[getIndexByName(indexName)]->SetOpts(indexDef.opts_);
		}
		return;
	}

	verifyUpdateIndex(indexDef);
	dropIndex(indexDef);
	addIndex(indexDef);
}

IndexDef NamespaceImpl::getIndexDefinition(const string &indexName) const {
	NamespaceDef nsDef = getDefinition();

	auto indexes = nsDef.indexes;
	auto indexDefIt = std::find_if(indexes.begin(), indexes.end(), [&](const IndexDef &idxDef) { return idxDef.name_ == indexName; });
	if (indexDefIt == indexes.end()) {
		throw Error(errParams, "Index '%s' not found in '%s'", indexName, name_);
	}

	return *indexDefIt;
}

void NamespaceImpl::verifyUpdateCompositeIndex(const IndexDef &indexDef) const {
	IndexType type = indexDef.Type();

	for (auto &jsonPathOrSubIdx : indexDef.jsonPaths_) {
		auto idxNameIt = indexesNames_.find(jsonPathOrSubIdx);
		if (idxNameIt != indexesNames_.end() && !indexes_[idxNameIt->second]->Opts().IsSparse() &&
			indexes_[idxNameIt->second]->Opts().IsArray() && (type == IndexCompositeBTree || type == IndexCompositeHash)) {
			throw Error(errParams, "Can't add array subindex '%s' to composite index '%s'", jsonPathOrSubIdx, indexDef.name_);
		}
	}
	const auto newIndex = std::unique_ptr<Index>(Index::New(indexDef, payloadType_, {}));
}

void NamespaceImpl::addCompositeIndex(const IndexDef &indexDef) {
	string indexName = indexDef.name_;
	IndexType type = indexDef.Type();
	IndexOpts opts = indexDef.opts_;

	FieldsSet fields;

	for (auto &jsonPathOrSubIdx : indexDef.jsonPaths_) {
		auto idxNameIt = indexesNames_.find(jsonPathOrSubIdx);
		if (idxNameIt == indexesNames_.end()) {
			TagsPath tagsPath = tagsMatcher_.path2tag(jsonPathOrSubIdx, true);
			if (tagsPath.empty()) {
				throw Error(errParams, "Subindex '%s' for composite index '%s' does not exist", jsonPathOrSubIdx, indexName);
			}
			fields.push_back(tagsPath);
			fields.push_back(jsonPathOrSubIdx);
		} else if (indexes_[idxNameIt->second]->Opts().IsSparse() && !indexes_[idxNameIt->second]->Opts().IsArray()) {
			fields.push_back(jsonPathOrSubIdx);
			fields.push_back(indexes_[idxNameIt->second]->Fields().getTagsPath(0));
		} else {
			if (indexes_[idxNameIt->second]->Opts().IsArray() && (type == IndexCompositeBTree || type == IndexCompositeHash)) {
				throw Error(errParams, "Can't add array subindex '%s' to composite index '%s'", jsonPathOrSubIdx, indexName);
			}
			fields.push_back(idxNameIt->second);
		}
	}

	assert(fields.getJsonPathsLength() == fields.getTagsPathsLength());
	assert(indexesNames_.find(indexName) == indexesNames_.end());

	int idxPos = indexes_.size();
	insertIndex(Index::New(indexDef, payloadType_, fields), idxPos, indexName);

	for (IdType rowId = 0; rowId < int(items_.size()); rowId++) {
		if (!items_[rowId].IsFree()) {
			indexes_[idxPos]->Upsert(Variant(items_[rowId]), rowId);
		}
	}
	updateSortedIdxCount();
}

void NamespaceImpl::insertIndex(Index *newIndex, int idxNo, const string &realName) {
	indexes_.insert(indexes_.begin() + idxNo, unique_ptr<Index>(newIndex));

	for (auto &n : indexesNames_) {
		if (n.second >= idxNo) {
			n.second++;
		}
	}

	indexesNames_.insert({realName, idxNo});

	if (newIndex->Opts().IsPK()) {
		indexesNames_.insert({kPKIndexName, idxNo});
	}
}

int NamespaceImpl::getIndexByName(const string &index) const {
	auto idxIt = indexesNames_.find(index);

	if (idxIt == indexesNames_.end()) throw Error(errParams, "Index '%s' not found in '%s'", index, name_);

	return idxIt->second;
}

bool NamespaceImpl::getIndexByName(const string &name, int &index) const {
	auto it = indexesNames_.find(name);
	if (it == indexesNames_.end()) return false;
	index = it->second;
	return true;
}

void NamespaceImpl::Insert(Item &item, const NsContext &ctx) { modifyItem(item, ctx, ModeInsert); }

void NamespaceImpl::Update(Item &item, const NsContext &ctx) { modifyItem(item, ctx, ModeUpdate); }

void NamespaceImpl::Update(const Query &query, QueryResults &result, const NsContext &ctx) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	Locker::WLockT wlck;

	if (!ctx.noLock) {
		cancelCommit_ = true;
		wlck = wLock(ctx.rdxContext);
		cancelCommit_ = false;
	}
	calc.LockHit();

	checkApplySlaveUpdate(ctx.rdxContext.fromReplication_);	 // throw exception if false

	NsSelecter selecter(this);
	SelectCtx selCtx(query);
	SelectFunctionsHolder func;
	selCtx.functions = &func;
	selCtx.contextCollectingMode = true;
	selecter(result, selCtx, ctx.rdxContext);

	auto tmStart = high_resolution_clock::now();

	bool updateWithJson = false;
	bool withExpressions = false;
	for (const UpdateEntry &ue : query.UpdateFields()) {
		if (!withExpressions && ue.isExpression) withExpressions = true;
		if (!updateWithJson && ue.mode == FieldModeSetJson) updateWithJson = true;
		if (withExpressions && updateWithJson) break;
	}

	if (ctx.rdxContext.fromReplication_ && withExpressions)
		throw Error(errLogic, "Can't apply update query with expression to slave ns '%s'", name_);

	ThrowOnCancel(ctx.rdxContext);

	// If update statement is expression and contains function calls then we use
	// row-based replication (to preserve data inconsistency), otherwise we update
	// it via 'WalUpdateQuery' (statement-based replication). If Update statement
	// contains update of entire object (via JSON) then statement replication is not possible.
	bool statementReplication =
		(!updateWithJson && !withExpressions && !query.HasLimit() && !query.HasOffset() && (result.Count() >= kWALStatementItemsThreshold));

	ItemModifier itemModifier(query.UpdateFields(), *this);
	for (ItemRef &item : result.Items()) {
		assert(items_.exists(item.Id()));
		PayloadValue &pv(items_[item.Id()]);
		Payload pl(payloadType_, pv);
		uint64_t oldPlHash = pl.GetHash();
		size_t oldItemCapacity = pv.GetCapacity();
		pv.Clone(pl.RealSize());
		itemModifier.Modify(item.Id(), ctx);
		replicateItem(item.Id(), ctx, statementReplication, oldPlHash, oldItemCapacity);
		item.Value() = items_[item.Id()];
	}
	result.getTagsMatcher(0) = tagsMatcher_;
	result.lockResults();

	if (statementReplication) {
		WrSerializer ser;
		const_cast<Query &>(query).type_ = QueryUpdate;
		WALRecord wrec(WalUpdateQuery, query.GetSQL(ser).Slice(), ctx.inTransaction);
		lsn_t lsn(wal_.Add(wrec), serverId_);
		if (!ctx.rdxContext.fromReplication_) repl_.lastSelfLSN = lsn;
		for (ItemRef &item : result.Items()) {
			item.Value().SetLSN(int64_t(lsn));
		}
		if (!repl_.temporary)
			observers_->OnWALUpdate(LSNPair(lsn, ctx.rdxContext.fromReplication_ ? ctx.rdxContext.LSNs_.originLSN_ : lsn), name_, wrec);
		if (!ctx.rdxContext.fromReplication_) setReplLSNs(LSNPair(lsn_t(), lsn));
	}

	if (query.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Updated %d items in %d µs", result.Count(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
}

void NamespaceImpl::replicateItem(IdType itemId, const NsContext &ctx, bool statementReplication, uint64_t oldPlHash,
								  size_t oldItemCapacity) {
	PayloadValue &pv(items_[itemId]);
	Payload pl(payloadType_, pv);

	if (!statementReplication) {
		lsn_t lsn(wal_.Add(WALRecord(WalItemUpdate, itemId, ctx.inTransaction), lsn_t(items_[itemId].GetLSN())), serverId_);
		if (!ctx.rdxContext.fromReplication_) repl_.lastSelfLSN = lsn;
		pv.SetLSN(int64_t(lsn));
		ItemImpl item(payloadType_, pv, tagsMatcher_);
		string_view cjson = item.GetCJSON(false);
		if (!repl_.temporary)
			observers_->OnWALUpdate(LSNPair(lsn, ctx.rdxContext.fromReplication_ ? ctx.rdxContext.LSNs_.originLSN_ : lsn), name_,

									WALRecord(WalItemModify, cjson, tagsMatcher_.version(), ModeUpdate, ctx.inTransaction));
		if (!ctx.rdxContext.fromReplication_) setReplLSNs(LSNPair(lsn_t(), lsn));
	}

	repl_.dataHash ^= oldPlHash;
	repl_.dataHash ^= pl.GetHash();
	itemsDataSize_ -= oldItemCapacity;
	itemsDataSize_ += pl.Value()->GetCapacity();
	if (storage_) {
		if (tagsMatcher_.isUpdated()) {
			WrSerializer ser;
			ser.PutUInt64(sysRecordsVersions_.tagsVersion);
			tagsMatcher_.serialize(ser);
			tagsMatcher_.clearUpdated();
			writeSysRecToStorage(ser.Slice(), "tags", sysRecordsVersions_.tagsVersion, false);
			logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name_, tagsMatcher_.dump());
		}

		WrSerializer pk;
		WrSerializer data;
		pk << "I";
		pl.SerializeFields(pk, pkFields());
		data.PutUInt64(lsn_t(pv.GetLSN()).Counter());
		ItemImpl item(payloadType_, pv, tagsMatcher_);
		item.GetCJSON(data);
		writeToStorage(pk.Slice(), data.Slice());
	}
}

void NamespaceImpl::Upsert(Item &item, const NsContext &ctx) { modifyItem(item, ctx, ModeUpsert); }

void NamespaceImpl::Delete(Item &item, const NsContext &ctx) {
	ItemImpl *ritem = item.impl_;

	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	Locker::WLockT wlck;

	if (!ctx.noLock) {
		cancelCommit_ = true;
		wlck = wLock(ctx.rdxContext);
		cancelCommit_ = false;
	}
	calc.LockHit();

	checkApplySlaveUpdate(ctx.rdxContext.fromReplication_);

	updateTagsMatcherFromItem(ritem);

	auto itItem = findByPK(ritem, ctx.rdxContext);
	IdType id = itItem.first;

	if (!itItem.second) {
		return;
	}

	item.setID(id);

	WALRecord wrec{WalItemModify, ritem->GetCJSON(), ritem->tagsMatcher().version(), ModeDelete, ctx.inTransaction};
	doDelete(id);

	lsn_t itemLsn(item.GetLSN());
	processWalRecord(wrec, ctx.rdxContext, itemLsn, &item);
}

void NamespaceImpl::doDelete(IdType id) {
	assert(items_.exists(id));

	Payload pl(payloadType_, items_[id]);

	WrSerializer pk;
	pk << kStorageItemPrefix;
	pl.SerializeFields(pk, pkFields());

	repl_.dataHash ^= pl.GetHash();
	wal_.Set(WALRecord(), lsn_t(items_[id].GetLSN()).Counter());

	if (storage_) {
		try {
			unique_lock<std::mutex> lck(locker_.StorageLock());
			updates_->Remove(pk.Slice());
			unflushedCount_.fetch_add(1, std::memory_order_release);
		} catch (const Error &err) {
			if (err.code() != errNamespaceInvalidated) {
				throw;
			}
		}
	}

	// erase last item
	VariantArray skrefs;
	int field;

	// erase from composite indexes
	for (field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		indexes_[field]->Delete(Variant(items_[id]), id);
	}

	// Holder for tuple. It is required for sparse indexes will be valid
	VariantArray tupleHolder(pl.Get(0, skrefs));

	// Deleteing fields from dense and sparse indexes:
	// we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire
	// payload value (and not only 0 item).
	assert(indexes_.firstCompositePos() != 0);
	const int borderIdx = indexes_.totalSize() > 1 ? 1 : 0;
	field = borderIdx;
	do {
		field %= indexes_.firstCompositePos();

		Index &index = *indexes_[field];
		if (index.Opts().IsSparse()) {
			assert(index.Fields().getTagsPathsLength() > 0);
			pl.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} else {
			pl.Get(field, skrefs, index.Opts().IsArray());
		}
		// Delete value from index
		index.Delete(skrefs, id);
	} while (++field != borderIdx);

	// free PayloadValue
	itemsDataSize_ -= items_[id].GetCapacity() + sizeof(PayloadValue::dataHeader);
	items_[id].Free();
	free_.push_back(id);
	if (free_.size() == items_.size()) {
		free_.resize(0);
		items_.resize(0);
	}
	markUpdated();
}

void NamespaceImpl::Delete(const Query &q, QueryResults &result, const NsContext &ctx) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	Locker::WLockT wlck;
	if (!ctx.noLock) {
		cancelCommit_ = true;
		wlck = wLock(ctx.rdxContext);
		cancelCommit_ = false;
	}
	calc.LockHit();

	checkApplySlaveUpdate(ctx.rdxContext.fromReplication_);	 // throw exception if false

	NsSelecter selecter(this);
	SelectCtx selCtx(q);
	selCtx.contextCollectingMode = true;
	SelectFunctionsHolder func;
	selCtx.functions = &func;
	selecter(result, selCtx, ctx.rdxContext);
	result.lockResults();

	auto tmStart = high_resolution_clock::now();
	for (auto &r : result.Items()) {
		doDelete(r.Id());
	}

	if (!q.HasLimit() && !q.HasOffset() && result.Count() >= kWALStatementItemsThreshold) {
		WrSerializer ser;
		const_cast<Query &>(q).type_ = QueryDelete;
		WALRecord wrec(WalUpdateQuery, q.GetSQL(ser).Slice(), ctx.inTransaction);
		processWalRecord(wrec, ctx.rdxContext);
	} else if (result.Count() > 0) {
		for (auto it : result) {
			WrSerializer cjson;
			it.GetCJSON(cjson, false);
			const int id = it.GetItemRef().Id();
			const WALRecord wrec{WalItemModify, cjson.Slice(), tagsMatcher_.version(), ModeDelete, ctx.inTransaction};
			processWalRecord(wrec, ctx.rdxContext, lsn_t(items_[id].GetLSN()));
		}
	}
	if (q.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Deleted %d items in %d µs", result.Count(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
}

void NamespaceImpl::Truncate(const NsContext &ctx) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	Locker::WLockT wlck;
	if (!ctx.noLock) {
		cancelCommit_ = true;
		wlck = wLock(ctx.rdxContext);
		cancelCommit_ = false;
	}
	calc.LockHit();

	checkApplySlaveUpdate(ctx.rdxContext.fromReplication_);	 // throw exception if false

	if (storage_) {
		for (PayloadValue &pv : items_) {
			if (pv.IsFree()) continue;
			Payload pl(payloadType_, pv);
			WrSerializer pk;
			pk << kStorageItemPrefix;
			pl.SerializeFields(pk, pkFields());
			try {
				unique_lock<std::mutex> lck(locker_.StorageLock());
				updates_->Remove(pk.Slice());
				unflushedCount_.fetch_add(1, std::memory_order_release);
			} catch (const Error &err) {
				if (err.code() != errNamespaceInvalidated) {
					throw;
				}
			}
		}
	}
	items_.clear();
	free_.clear();
	repl_.dataHash = 0;
	itemsDataSize_ = 0;
	for (size_t i = 0; i < indexes_.size(); ++i) {
		const IndexOpts opts = indexes_[i]->Opts();
		unique_ptr<Index> newIdx{Index::New(getIndexDefinition(i), indexes_[i]->GetPayloadType(), indexes_[i]->Fields())};
		newIdx->SetOpts(opts);
		std::swap(indexes_[i], newIdx);
	}

	WrSerializer ser;
	WALRecord wrec(WalUpdateQuery, (ser << "TRUNCATE " << name_).Slice());

	lsn_t lsn(wal_.Add(wrec), serverId_);
	if (!ctx.rdxContext.fromReplication_) repl_.lastSelfLSN = lsn;
	markUpdated();
	if (!repl_.temporary)
		observers_->OnWALUpdate(LSNPair(lsn, ctx.rdxContext.fromReplication_ ? ctx.rdxContext.LSNs_.originLSN_ : lsn), name_, wrec);
	if (!ctx.rdxContext.fromReplication_) setReplLSNs(LSNPair(lsn_t(), lsn));
}

void NamespaceImpl::Refill(vector<Item> &items, const NsContext &ctx) {
	auto wlck = wLock(ctx.rdxContext);
	auto intCtx = ctx;
	intCtx.NoLock();
	Truncate(intCtx);
	for (Item &i : items) {
		Upsert(i, intCtx);
	}
}

ReplicationState NamespaceImpl::GetReplState(const RdxContext &ctx) const {
	auto rlck = rLock(ctx);
	return getReplState();
}

void NamespaceImpl::SetReplLSNs(LSNPair LSNs, const RdxContext &ctx) {
	auto wlck = wLock(ctx);
	setReplLSNs(LSNs);
	unflushedCount_.fetch_add(1, std::memory_order_release);
}

void NamespaceImpl::setReplLSNs(LSNPair LSNs) {
	repl_.originLSN = LSNs.originLSN_;
	repl_.lastUpstreamLSN = LSNs.upstreamLSN_;
	logPrintf(LogTrace, "[repl:%s:%s]:%d setReplLSNs originLSN = %s upstreamLSN=%s", name_, dbpath_, serverId_, LSNs.originLSN_,
			  LSNs.upstreamLSN_);
}

void NamespaceImpl::setSlaveMode(const RdxContext &ctx) {
	{
		auto wlck = wLock(ctx);
		repl_.slaveMode = true;
		repl_.replicatorEnabled = true;
		repl_.incarnationCounter++;
	}
	logPrintf(LogInfo, "Enable slave mode for namespace '%s'", name_);
}

ReplicationState NamespaceImpl::getReplState() const {
	ReplicationState ret = repl_;
	ret.dataCount = items_.size() - free_.size();
	ret.lastLsn = lsn_t(wal_.LSNCounter() - 1, serverId_);
	return ret;
}

void NamespaceImpl::SetSlaveReplStatus(ReplicationState::Status status, const Error &err, const RdxContext &ctx) {
	auto wlck = wLock(ctx);
	assert(repl_.replicatorEnabled);
	if (status == ReplicationState::Status::Idle || status == ReplicationState::Status::Syncing) {
		assert(err.code() == errOK);
	} else {
		assert(err.code() != errOK);
	}
	repl_.replError = err;
	repl_.status = status;
	unflushedCount_.fetch_add(1, std::memory_order_release);
}

void NamespaceImpl::SetSlaveReplMasterState(MasterState state, const RdxContext &ctx) {
	auto wlck = wLock(ctx);
	assert(repl_.replicatorEnabled);
	repl_.masterState = state;
	unflushedCount_.fetch_add(1, std::memory_order_release);
}

Transaction NamespaceImpl::NewTransaction(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return Transaction(name_, payloadType_, tagsMatcher_, pkFields(), schema_);
}

void NamespaceImpl::CommitTransaction(Transaction &tx, QueryResults &result, const NsContext &ctx) {
	logPrintf(LogTrace, "[repl:%s]:%d CommitTransaction start", name_, serverId_);
	Locker::WLockT wlck;
	if (!ctx.noLock) {
		PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
		cancelCommit_ = true;  // -V519
		wlck = wLock(ctx.rdxContext);
		cancelCommit_ = false;	// -V519
		calc.LockHit();
	}

	NsContext nsCtx{ctx};
	nsCtx.NoLock().InTransaction();

	WALRecord initWrec(WalInitTransaction, 0, true);
	lsn_t lsn(wal_.Add(initWrec), serverId_);
	if (!ctx.rdxContext.fromReplication_) repl_.lastSelfLSN = lsn;
	if (!repl_.temporary)
		observers_->OnWALUpdate(LSNPair(lsn, ctx.rdxContext.fromReplication_ ? ctx.rdxContext.LSNs_.originLSN_ : lsn), name_, initWrec);

	for (auto &step : tx.GetSteps()) {
		if (step.query_) {
			QueryResults qr;
			if (step.query_->type_ == QueryDelete) {
				Delete(*step.query_, qr, nsCtx);
			} else {
				Update(*step.query_, qr, nsCtx);
			}
		} else {
			Item item = tx.GetItem(std::move(step));
			if (step.modifyMode_ == ModeDelete) {
				Delete(item, nsCtx);
			} else {
				modifyItem(item, nsCtx, step.modifyMode_);
			}
			result.AddItem(item);
		}
	}

	WALRecord commitWrec(WalCommitTransaction, 0, true);
	processWalRecord(commitWrec, ctx.rdxContext);
	logPrintf(LogTrace, "[repl:%s]:%d CommitTransaction end", name_, serverId_);
}

void NamespaceImpl::doUpsert(ItemImpl *ritem, IdType id, bool doUpdate) {
	// Upsert fields to indexes
	assert(items_.exists(id));
	auto &plData = items_[id];

	// Inplace payload
	Payload pl(payloadType_, plData);
	Payload plNew = ritem->GetPayload();
	if (doUpdate) {
		repl_.dataHash ^= pl.GetHash();
		itemsDataSize_ -= plData.GetCapacity() + sizeof(PayloadValue::dataHeader);
		plData.Clone(pl.RealSize());

		// Delete from composite indexes first
		for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
			indexes_[field]->Delete(Variant(plData), id);
		}
	}

	plData.SetLSN(ritem->Value().GetLSN());

	// Upserting fields to dense and sparse indexes:
	// we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire
	// payload value (and not only 0 item).
	assert(indexes_.firstCompositePos() != 0);
	const int borderIdx = indexes_.totalSize() > 1 ? 1 : 0;
	int field = borderIdx;
	do {
		field %= indexes_.firstCompositePos();
		Index &index = *indexes_[field];
		bool isIndexSparse = index.Opts().IsSparse();
		assert(!isIndexSparse || (isIndexSparse && index.Fields().getTagsPathsLength() > 0));

		if (isIndexSparse) {
			assert(index.Fields().getTagsPathsLength() > 0);
			try {
				plNew.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
			} catch (const Error &) {
				skrefs.resize(0);
			}
		} else {
			plNew.Get(field, skrefs);
		}

		if (index.Opts().GetCollateMode() == CollateUTF8)
			for (auto &key : skrefs) key.EnsureUTF8();

		// Check for update
		if (doUpdate) {
			if (isIndexSparse) {
				try {
					pl.GetByJsonPath(index.Fields().getTagsPath(0), krefs, index.KeyType());
				} catch (const Error &) {
					krefs.resize(0);
				}
			} else {
				pl.Get(field, krefs, index.Opts().IsArray());
			}
			index.Delete(krefs, id);
		}
		// Put value to index
		krefs.resize(0);
		index.Upsert(krefs, skrefs, id, !isIndexSparse);

		if (!isIndexSparse) {
			// Put value to payload
			pl.Set(field, krefs);
		}
	} while (++field != borderIdx);

	// Upsert to composite indexes
	for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		indexes_[field]->Upsert(Variant(plData), id);
	}
	repl_.dataHash ^= pl.GetHash();
	itemsDataSize_ += plData.GetCapacity() + sizeof(PayloadValue::dataHeader);
	ritem->RealValue() = plData;
}

void NamespaceImpl::ReplaceTagsMatcher(const TagsMatcher &tm, const RdxContext &ctx) {
	assert(!items_.size() && repl_.replicatorEnabled);
	cancelCommit_ = true;
	auto wlck = wLock(ctx);
	cancelCommit_ = false;	// -V519
	tagsMatcher_ = tm;
	tagsMatcher_.UpdatePayloadType(payloadType_);
}

void NamespaceImpl::updateTagsMatcherFromItem(ItemImpl *ritem) {
	if (ritem->tagsMatcher().isUpdated()) {
		logPrintf(LogTrace, "Updated TagsMatcher of namespace '%s' on modify:\n%s", name_, ritem->tagsMatcher().dump());
	}

	if (ritem->Type().get() != payloadType_.get() || (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))) {
		string jsonSliceBuf(ritem->GetJSON());
		logPrintf(LogTrace, "Conflict TagsMatcher of namespace '%s' on modify: item:\n%s\ntm is\n%s\nnew tm is\n %s\n", name_, jsonSliceBuf,
				  tagsMatcher_.dump(), ritem->tagsMatcher().dump());

		ItemImpl tmpItem(payloadType_, tagsMatcher_);
		tmpItem.Value().SetLSN(ritem->Value().GetLSN());
		*ritem = std::move(tmpItem);

		auto err = ritem->FromJSON(jsonSliceBuf, nullptr);
		if (!err.ok()) throw err;

		if (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))
			throw Error(errLogic, "Could not insert item. TagsMatcher was not merged.");
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
	if (ritem->tagsMatcher().isUpdated()) {
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
}

void NamespaceImpl::modifyItem(Item &item, const NsContext &ctx, int mode) {
	// Item to doUpsert
	ItemImpl *itemImpl = item.impl_;
	Locker::WLockT wlck;
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	if (!ctx.noLock) {
		cancelCommit_ = true;  // -V519
		wlck = wLock(ctx.rdxContext);
		cancelCommit_ = false;	// -V519
	}
	calc.LockHit();

	checkApplySlaveUpdate(ctx.rdxContext.fromReplication_);

	setFieldsBasedOnPrecepts(itemImpl);
	updateTagsMatcherFromItem(itemImpl);
	auto newPl = itemImpl->GetPayload();

	auto realItem = findByPK(itemImpl, ctx.rdxContext);
	bool exists = realItem.second;

	if ((exists && mode == ModeInsert) || (!exists && mode == ModeUpdate)) {
		item.setID(-1);
		return;
	}

	IdType id = exists ? realItem.first : createItem(newPl.RealSize());

	lsn_t lsn(wal_.Add(WALRecord(WalItemUpdate, id, ctx.inTransaction), exists ? lsn_t(items_[id].GetLSN()) : lsn_t()), serverId_);
	if (!ctx.rdxContext.fromReplication_) repl_.lastSelfLSN = lsn;

	item.setLSN(int64_t(lsn));
	item.setID(id);
	doUpsert(itemImpl, id, exists);

	if (storage_) {
		if (tagsMatcher_.isUpdated()) {
			WrSerializer ser;
			ser.PutUInt64(sysRecordsVersions_.tagsVersion);
			tagsMatcher_.serialize(ser);
			tagsMatcher_.clearUpdated();
			writeSysRecToStorage(ser.Slice(), kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, false);
			logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name_, tagsMatcher_.dump());
		}

		WrSerializer pk, data;
		pk << kStorageItemPrefix;
		newPl.SerializeFields(pk, pkFields());
		data.PutUInt64(lsn.Counter());
		itemImpl->GetCJSON(data);
		writeToStorage(pk.Slice(), data.Slice());
	}

	if (!repl_.temporary) {
		// not send row with fromReplication=true and originLSN_= empty
		if (!ctx.rdxContext.fromReplication_ || !ctx.rdxContext.LSNs_.originLSN_.isEmpty())
			observers_->OnModifyItem(LSNPair(lsn, ctx.rdxContext.fromReplication_ ? ctx.rdxContext.LSNs_.originLSN_ : lsn), name_,
									 item.impl_, mode, ctx.inTransaction);
	}
	if (!ctx.rdxContext.fromReplication_) setReplLSNs(LSNPair(lsn_t(), lsn));
	markUpdated();
}

// find id by PK. NOT THREAD SAFE!
pair<IdType, bool> NamespaceImpl::findByPK(ItemImpl *ritem, const RdxContext &ctx) {
	auto pkIndexIt = indexesNames_.find(kPKIndexName);

	if (pkIndexIt == indexesNames_.end()) {
		throw Error(errLogic, "Trying to modify namespace '%s', but it doesn't contain PK index", name_);
	}
	Index *pkIndex = indexes_[pkIndexIt->second].get();

	Payload pl = ritem->GetPayload();
	// It is a faster alternative of "select ID from namespace where pk1 = 'item.pk1' and pk2 = 'item.pk2' "
	// Get pkey values from pk fields
	VariantArray krefs;
	if (isComposite(pkIndex->Type())) {
		krefs.push_back(Variant(*pl.Value()));
	} else if (pkIndex->Opts().IsSparse()) {
		auto f = pkIndex->Fields();
		pl.GetByJsonPath(f.getTagsPath(0), krefs, pkIndex->KeyType());
	} else
		pl.Get(pkIndexIt->second, krefs);
	assertf(krefs.size() == 1, "Pkey field must contain 1 key, but there '%d' in '%s.%s'", krefs.size(), name_, pkIndex->Name());

	SelectKeyResult res = pkIndex->SelectKey(krefs, CondEq, 0, Index::SelectOpts(), nullptr, ctx)[0];
	if (res.size() && res[0].ids_.size()) return {res[0].ids_[0], true};
	return {-1, false};
}

void NamespaceImpl::optimizeIndexes(const NsContext &ctx) {
	// This is read lock only atomics based implementation of rebuild indexes
	// If optimizationState_ == OptimizationCompleted is true, then indexes are completely built.
	// In this case reset optimizationState_ and/or any idset's and sort orders builds are allowed only protected by write lock
	if (optimizationState_ == OptimizationCompleted) return;
	int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	auto lastUpdateTime = lastUpdateTime_.load(std::memory_order_acquire);

	Locker::RLockT rlck;
	if (!ctx.noLock) {
		rlck = rLock(ctx.rdxContext);
	}

	if (isSystem()) return;
	if (!lastUpdateTime || !config_.optimizationTimeout || now - lastUpdateTime < config_.optimizationTimeout) {
		return;
	}

	if (!indexes_.size()) {
		return;
	}

	if (optimizationState_ == OptimizationCompleted || cancelCommit_) return;
	optimizationState_ = OptimizingIndexes;

	logPrintf(LogTrace, "Namespace::optimizeIndexes(%s) enter", name_);
	assert(indexes_.firstCompositePos() != 0);
	int field = indexes_.firstCompositePos();
	do {
		field %= indexes_.totalSize();
		PerfStatCalculatorMT calc(indexes_[field]->GetCommitPerfCounter(), enablePerfCounters_);
		calc.LockHit();
		indexes_[field]->Commit();
	} while (++field != indexes_.firstCompositePos() && !cancelCommit_);

	// Update sort orders and sort_id for each index
	optimizationState_ = OptimizingSortOrders;

	int i = 1;
	int maxIndexWorkers = std::min(int(std::thread::hardware_concurrency()), config_.optimizationSortWorkers);
	for (auto &idxIt : indexes_) {
		if (idxIt->IsOrdered() && maxIndexWorkers != 0) {
			NSUpdateSortedContext sortCtx(*this, i++);
			idxIt->MakeSortOrders(sortCtx);
			// Build in multiple threads
			unique_ptr<thread[]> thrs(new thread[maxIndexWorkers]);
			auto indexes = &this->indexes_;

			for (int i = 0; i < maxIndexWorkers; i++) {
				thrs[i] = std::thread(
					[&](int i) {
						for (int j = i; j < int(indexes->size()) && !cancelCommit_; j += maxIndexWorkers)
							indexes->at(j)->UpdateSortedIds(sortCtx);
					},
					i);
			}
			for (int i = 0; i < maxIndexWorkers; i++) thrs[i].join();
		}
		if (cancelCommit_) break;
	}
	optimizationState_ = (!cancelCommit_ && maxIndexWorkers) ? OptimizationCompleted : NotOptimized;
	if (!cancelCommit_) {
		lastUpdateTime_.store(0, std::memory_order_release);
	}
	logPrintf(LogTrace, "Namespace::optimizeIndexes(%s) leave %s", name_, cancelCommit_ ? "(cancelled by concurent update)" : "");
}

void NamespaceImpl::markUpdated() {
	itemsCount_.store(items_.size(), std::memory_order_relaxed);
	itemsCapacity_.store(items_.capacity(), std::memory_order_relaxed);
	optimizationState_.store(NotOptimized);
	queryCache_->Clear();
	joinCache_->Clear();
	lastUpdateTime_.store(
		std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(),
		std::memory_order_release);
	if (!nsIsLoading_) {
		repl_.updatedUnixNano = getTimeNow("nsec"_sv);
	}
}

void NamespaceImpl::updateSelectTime() {
	lastSelectTime_ = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

int64_t NamespaceImpl::getLastSelectTime() const { return lastSelectTime_; }

void NamespaceImpl::Select(QueryResults &result, SelectCtx &params, const RdxContext &ctx) {
	if (params.query.IsWALQuery()) {
		WALSelecter selecter(this);
		selecter(result, params);
	} else {
		NsSelecter selecter(this);
		selecter(result, params, ctx);
	}
}

IndexDef NamespaceImpl::getIndexDefinition(size_t i) const {
	assert(i < indexes_.size());
	IndexDef indexDef;
	const Index &index = *indexes_[i];
	indexDef.name_ = index.Name();
	indexDef.opts_ = index.Opts();
	indexDef.FromType(index.Type());
	indexDef.expireAfter_ = index.GetTTLValue();

	if (index.Opts().IsSparse() || static_cast<int>(i) >= payloadType_.NumFields()) {
		int fIdx = 0;
		for (auto &f : index.Fields()) {
			if (f != IndexValueType::SetByJsonPath) {
				indexDef.jsonPaths_.push_back(indexes_[f]->Name());
			} else {
				indexDef.jsonPaths_.push_back(index.Fields().getJsonPath(fIdx++));
			}
		}
	} else {
		indexDef.jsonPaths_ = payloadType_->Field(i).JsonPaths();
	}
	return indexDef;
}

NamespaceDef NamespaceImpl::getDefinition() const {
	auto pt = this->payloadType_;
	NamespaceDef nsDef(name_, StorageOpts().Enabled(!dbpath_.empty()));
	nsDef.indexes.reserve(indexes_.size());
	for (size_t i = 1; i < indexes_.size(); ++i) {
		nsDef.AddIndex(getIndexDefinition(i));
	}
	nsDef.isTemporary = repl_.temporary;
	if (schema_) {
		WrSerializer ser;
		schema_->GetJSON(ser);
		nsDef.schemaJson = string(ser.Slice());
	}
	return nsDef;
}

NamespaceDef NamespaceImpl::GetDefinition(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return getDefinition();
}

NamespaceMemStat NamespaceImpl::GetMemStat(const RdxContext &ctx) {
	NamespaceMemStat ret;
	auto rlck = rLock(ctx);
	ret.name = name_;
	ret.joinCache = joinCache_->GetMemStat();
	ret.queryCache = queryCache_->GetMemStat();

	ret.itemsCount = items_.size() - free_.size();
	*(static_cast<ReplicationState *>(&ret.replication)) = getReplState();
	ret.replication.walCount = size_t(wal_.size());
	ret.replication.walSize = wal_.heap_size();

	ret.emptyItemsCount = free_.size();

	ret.Total.dataSize = itemsDataSize_ + items_.capacity() * sizeof(PayloadValue);
	ret.Total.cacheSize = ret.joinCache.totalSize + ret.queryCache.totalSize;

	ret.indexes.reserve(indexes_.size());
	for (auto &idx : indexes_) {
		ret.indexes.emplace_back(idx->GetMemStat());
		auto &istat = ret.indexes.back();
		istat.sortOrdersSize = idx->IsOrdered() ? (items_.size() * sizeof(IdType)) : 0;
		ret.Total.indexesSize += istat.idsetPlainSize + istat.idsetBTreeSize + istat.sortOrdersSize + istat.fulltextSize + istat.columnSize;
		ret.Total.dataSize += istat.dataSize;
		ret.Total.cacheSize += istat.idsetCache.totalSize;
	}

	ret.storageOK = storage_ != nullptr;
	ret.storagePath = dbpath_;
	ret.optimizationCompleted = (optimizationState_ == OptimizationCompleted);

	logPrintf(LogTrace,
			  "[GetMemStat:%s]:%d replication (dataHash=%ld  dataCount=%d  lastLsn=%s) replication.masterState (dataHash=%ld dataCount=%d "
			  "lastUpstreamLSN=%s",
			  ret.name, serverId_, ret.replication.dataHash, ret.replication.dataCount, ret.replication.lastLsn,
			  ret.replication.masterState.dataHash, ret.replication.masterState.dataCount, ret.replication.lastUpstreamLSN);

	return ret;
}

NamespacePerfStat NamespaceImpl::GetPerfStat(const RdxContext &ctx) {
	auto rlck = rLock(ctx);

	NamespacePerfStat ret;
	ret.name = name_;
	ret.selects = selectPerfCounter_.Get<PerfStat>();
	ret.updates = updatePerfCounter_.Get<PerfStat>();
	for (unsigned i = 1; i < indexes_.size(); i++) {
		ret.indexes.emplace_back(indexes_[i]->GetIndexPerfStat());
	}
	return ret;
}

void NamespaceImpl::ResetPerfStat(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	selectPerfCounter_.Reset();
	updatePerfCounter_.Reset();
	for (auto &i : indexes_) i->ResetIndexPerfStat();
}

Error NamespaceImpl::loadLatestSysRecord(string_view baseSysTag, uint64_t &version, string &content) {
	std::string key(baseSysTag);
	key.append(".");
	std::string latestContent;
	version = 0;
	Error err = errOK;
	for (int i = 0; i < kSysRecordsBackupCount; ++i) {
		Error status = storage_->Read(StorageOpts().FillCache(), string_view(key + std::to_string(i)), content);
		if (!status.ok() && status.code() != errNotFound) {
			logPrintf(LogTrace, "Error on namespace service info(tag: %s, id: %u) load '%s': %s", baseSysTag, i, name_, status.what());
			err = Error(errNotValid, "Error load namespace from storage '%s': %s", name_, status.what());
		}

		if (content.size()) {
			Serializer ser(content.data(), content.size());
			auto curVersion = ser.GetUInt64();
			if (curVersion >= version) {
				version = curVersion;
				latestContent = std::move(content);
				err = errOK;
			}
		}
	}

	if (latestContent.empty()) {
		Error status = storage_->Read(StorageOpts().FillCache(), baseSysTag, content);
		if (!content.empty()) {
			logPrintf(LogTrace, "Converting %s for %s to new format", baseSysTag, name_);
			WrSerializer ser;
			ser.PutUInt64(version);
			ser.Write(string_view(content));
			writeSysRecToStorage(ser.Slice(), baseSysTag, version, true);
		}
		if (!status.ok() && status.code() != errNotFound) {
			return Error(errNotValid, "Error load namespace from storage '%s': %s", name_, status.what());
		}
		return status;
	} else {
		version++;
	}
	latestContent.erase(0, sizeof(uint64_t));
	content = std::move(latestContent);
	return err;
}

bool NamespaceImpl::loadIndexesFromStorage() {
	// Check if indexes structures are ready.
	assert(indexes_.size() == 1);
	assert(items_.size() == 0);

	string def;
	Error status = loadLatestSysRecord(kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}
	if (def.size()) {
		Serializer ser(def.data(), def.size());
		tagsMatcher_.deserialize(ser);
		tagsMatcher_.clearUpdated();
		logPrintf(LogTrace, "Loaded tags(version: %lld) of namespace %s:\n%s",
				  sysRecordsVersions_.tagsVersion ? sysRecordsVersions_.tagsVersion - 1 : 0, name_, tagsMatcher_.dump());
	}

	def.clear();
	status = loadLatestSysRecord(kStorageSchemaPrefix, sysRecordsVersions_.schemaVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}
	if (def.size()) {
		schema_ = make_shared<Schema>();
		Serializer ser(def.data(), def.size());
		status = schema_->FromJSON(ser.GetSlice());
		if (!status.ok()) {
			throw status;
		}
		logPrintf(LogTrace, "Loaded schema(version: %lld) of namespace %s",
				  sysRecordsVersions_.schemaVersion ? sysRecordsVersions_.schemaVersion - 1 : 0, name_);
	}

	def.clear();
	status = loadLatestSysRecord(kStorageIndexesPrefix, sysRecordsVersions_.idxVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}

	if (def.size()) {
		Serializer ser(def.data(), def.size());
		const uint32_t dbMagic = ser.GetUInt32();
		const uint32_t dbVer = ser.GetUInt32();
		if (dbMagic != kStorageMagic) {
			logPrintf(LogError, "Storage magic mismatch. want %08X, got %08X", kStorageMagic, dbMagic);
			return false;
		}
		if (dbVer != kStorageVersion) {
			logPrintf(LogError, "Storage version mismatch. want %08X, got %08X", kStorageVersion, dbVer);
			return false;
		}

		int count = ser.GetVarUint();
		while (count--) {
			IndexDef indexDef;
			string_view indexData = ser.GetVString();
			Error err = indexDef.FromJSON(giftStr(indexData));
			if (err.ok()) {
				try {
					addIndex(indexDef);
				} catch (const Error &e) {
					err = e;
				}
			}
			if (!err.ok()) {
				logPrintf(LogError, "Error adding index '%s': %s", indexDef.name_, err.what());
			}
		}
	}

	if (schema_) schema_->BuildProtobufSchema(tagsMatcher_, payloadType_);

	logPrintf(LogTrace, "Loaded index structure(version %lld) of namespace '%s'\n%s",
			  sysRecordsVersions_.idxVersion ? sysRecordsVersions_.idxVersion - 1 : 0, name_, payloadType_->ToString());

	return true;
}

void NamespaceImpl::loadReplStateFromStorage() {
	string json;
	Error status = loadLatestSysRecord(kStorageReplStatePrefix, sysRecordsVersions_.replVersion, json);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}

	if (json.size()) {
		logPrintf(LogTrace, "[load_repl:%s]:%d Loading replication state(version %lld) of namespace %s: %s", name_, serverId_,
				  sysRecordsVersions_.replVersion ? sysRecordsVersions_.replVersion - 1 : 0, name_, json);
		repl_.FromJSON(giftStr(json));
	}
	{
		WrSerializer ser_log;
		JsonBuilder builder_log(ser_log, ObjType::TypePlain);
		repl_.GetJSON(builder_log);
		logPrintf(LogTrace, "[load_repl:%s]:%d Loading replication state %s", name_, serverId_, ser_log.c_str());
	}
}

void NamespaceImpl::saveIndexesToStorage() {
	// clear ItemImpl pool on payload change
	pool_.clear();

	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveIndexesToStorage (%s)", name_);

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.idxVersion);
	ser.PutUInt32(kStorageMagic);
	ser.PutUInt32(kStorageVersion);

	ser.PutVarUint(indexes_.size() - 1);
	NamespaceDef nsDef = getDefinition();

	for (const IndexDef &indexDef : nsDef.indexes) {
		WrSerializer wrser;
		indexDef.GetJSON(wrser);
		ser.PutVString(wrser.Slice());
	}

	writeSysRecToStorage(ser.Slice(), kStorageIndexesPrefix, sysRecordsVersions_.idxVersion, true);

	saveReplStateToStorage();
}

void NamespaceImpl::saveSchemaToStorage() {
	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveSchemaToStorage (%s)", name_);

	if (!schema_) return;

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.schemaVersion);
	{
		auto sliceHelper = ser.StartSlice();
		schema_->GetJSON(ser);
	}

	writeSysRecToStorage(ser.Slice(), kStorageSchemaPrefix, sysRecordsVersions_.schemaVersion, true);

	saveReplStateToStorage();
}

void NamespaceImpl::saveReplStateToStorage() {
	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveReplStateToStorage (%s)", name_);

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.replVersion);
	JsonBuilder builder(ser);
	ReplicationState st = getReplState();
	st.GetJSON(builder);
	builder.End();
	writeSysRecToStorage(ser.Slice(), kStorageReplStatePrefix, sysRecordsVersions_.replVersion, true);
}

void NamespaceImpl::EnableStorage(const string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx) {
	string dbpath = fs::JoinPath(path, name_);

	auto wlck = wLock(ctx);
	FlagGuardT nsLoadingGuard(nsIsLoading_);

	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '%s' on path '%s'", name_, path);
	}

	bool success = false;
	while (!success) {
		if (!opts.IsCreateIfMissing() && fs::Stat(dbpath) != fs::StatDir) {
			throw Error(errNotFound,
						"Storage directory doesn't exist for namespace '%s' on path '%s' and CreateIfMissing option is not set", name_,
						path);
		}
		storage_.reset(datastorage::StorageFactory::create(storageType));
		Error status = storage_->Open(dbpath, opts);
		if (!status.ok()) {
			if (!opts.IsDropOnFileFormatError()) {
				storage_.reset();
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s' - %s", name_, path, status.what());
			}
		} else {
			success = loadIndexesFromStorage();
			if (!success && !opts.IsDropOnFileFormatError()) {
				storage_.reset();
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s': format error", name_, dbpath);
			}
			loadReplStateFromStorage();
		}
		if (!success && opts.IsDropOnFileFormatError()) {
			logPrintf(LogWarning, "Dropping storage for namespace '%s' on path '%s' due to format error", name_, dbpath);
			opts.DropOnFileFormatError(false);
			storage_->Destroy(dbpath);
			storage_.reset();
		}
	}

	storageOpts_ = opts;
	updates_.reset(storage_->GetUpdatesCollection());
	dbpath_ = dbpath;
}

StorageOpts NamespaceImpl::GetStorageOpts(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return storageOpts_;
}

std::shared_ptr<const Schema> NamespaceImpl::GetSchemaPtr(const RdxContext &ctx) const {
	auto rlck = rLock(ctx);
	return schema_;
}

void NamespaceImpl::LoadFromStorage(const RdxContext &ctx) {
	auto wlck = wLock(ctx);
	FlagGuardT nsLoadingGuard(nsIsLoading_);

	StorageOpts opts;
	opts.FillCache(false);
	size_t ldcount = 0;
	logPrintf(LogTrace, "Loading items to '%s' from storage", name_);
	unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	ItemImpl item(payloadType_, tagsMatcher_);
	item.Unsafe(true);
	int errCount = 0;
	int64_t maxLSN = -1;
	int64_t minLSN = std::numeric_limits<int64_t>::max();
	Error lastErr = errOK;

	uint64_t dataHash = repl_.dataHash;
	repl_.dataHash = 0;
	itemsDataSize_ = 0;
	for (dbIter->Seek(kStorageItemPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), string_view(kStorageItemPrefix "\xFF")) < 0; dbIter->Next()) {
		string_view dataSlice = dbIter->Value();
		if (dataSlice.size() > 0) {
			if (!pkFields().size()) {
				throw Error(errLogic, "Can't load data storage of '%s' - there are no PK fields in ns", name_);
			}
			if (dataSlice.size() < sizeof(int64_t)) {
				lastErr = Error(errParseBin, "Not enougth data in data slice");
				logPrintf(LogTrace, "Error load item to '%s' from storage: '%s'", name_, lastErr.what());
				errCount++;
				continue;
			}

			// Read LSN
			int64_t lsn = *reinterpret_cast<const int64_t *>(dataSlice.data());
			assert(lsn >= 0);
			lsn_t l(lsn);
			if (!isSystem()) {
				if (l.Server() != serverId_) {
					l.SetServer(serverId_);
				}
			} else {
				l.SetServer(0);
			}

			maxLSN = std::max(maxLSN, l.Counter());
			minLSN = std::min(minLSN, l.Counter());
			dataSlice = dataSlice.substr(sizeof(lsn));

			auto err = item.FromCJSON(dataSlice);
			if (!err.ok()) {
				logPrintf(LogTrace, "Error load item to '%s' from storage: '%s'", name_, err.what());
				errCount++;
				lastErr = err;
				continue;
			}

			IdType id = items_.size();
			items_.emplace_back(PayloadValue(item.GetPayload().RealSize()));
			item.Value().SetLSN(int64_t(l));

			doUpsert(&item, id, false);
			ldcount += dataSlice.size();
		}
	}

	initWAL(minLSN, maxLSN);
	if (!isSystem()) {
		repl_.lastLsn.SetServer(serverId_);
		repl_.lastSelfLSN.SetServer(serverId_);
	}

	logPrintf(LogInfo, "[%s] Done loading storage. %d items loaded (%d errors %s), lsn #%s%s, total size=%dM, dataHash=%ld", name_,
			  items_.size(), errCount, lastErr.what(), repl_.lastLsn, repl_.slaveMode ? " (slave)" : "", ldcount / (1024 * 1024),
			  repl_.dataHash);
	if (dataHash != repl_.dataHash) {
		logPrintf(LogError, "[%s] Warning dataHash mismatch %lu != %lu", name_, dataHash, repl_.dataHash);
		unflushedCount_.fetch_add(1, std::memory_order_release);
	}

	markUpdated();
}

void NamespaceImpl::initWAL(int64_t minLSN, int64_t maxLSN) {
	wal_.Init(config_.walSize, minLSN, maxLSN, storage_);
	// Fill existing records
	for (IdType rowId = 0; rowId < IdType(items_.size()); rowId++) {
		if (!items_[rowId].IsFree()) {
			wal_.Set(WALRecord(WalItemUpdate, rowId), lsn_t(items_[rowId].GetLSN()).Counter());
		}
	}
	repl_.lastLsn = lsn_t(wal_.LSNCounter() - 1, serverId_);
	logPrintf(LogInfo, "[%s] WAL has been initalized lsn #%s, max size %ld", name_, repl_.lastLsn, wal_.Capacity());
}

void NamespaceImpl::removeExpiredItems(RdxActivityContext *ctx) {
	const RdxContext rdxCtx{ctx};
	auto wlck = wLock(rdxCtx);
	if (repl_.slaveMode) {
		return;
	}
	for (const std::unique_ptr<Index> &index : indexes_) {
		if ((index->Type() != IndexTtl) || (index->Size() == 0)) continue;
		int64_t expirationthreshold =
			std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count() -
			index->GetTTLValue();
		QueryResults qr;
		Delete(Query(name_).Where(index->Name(), CondLt, expirationthreshold), qr, NsContext(rdxCtx).NoLock());
	}
}

void NamespaceImpl::BackgroundRoutine(RdxActivityContext *ctx) {
	flushStorage(ctx);
	optimizeIndexes(NsContext(ctx));
	removeExpiredItems(ctx);
}

void NamespaceImpl::flushStorage(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	if (storage_) {
		if (unflushedCount_.load(std::memory_order_acquire) > 0) {
			try {
				unique_lock<std::mutex> lck(locker_.StorageLock());
				doFlushStorage();
			} catch (const Error &err) {
				if (err.code() != errNamespaceInvalidated) {
					throw;
				}
			}
		}
	}
}

void NamespaceImpl::doFlushStorage() {
	Error status = storage_->Write(StorageOpts(), *(updates_.get()));
	if (!status.ok()) throw Error(errLogic, "Error write ns '%s' to storage: %s", name_, status.what());
	unflushedCount_.store(0, std::memory_order_release);
	updates_->Clear();
	saveReplStateToStorage();
}

void NamespaceImpl::DeleteStorage(const RdxContext &ctx) {
	auto wlck = wLock(ctx);
	deleteStorage();
}

void NamespaceImpl::CloseStorage(const RdxContext &ctx) {
	flushStorage(ctx);
	auto wlck = wLock(ctx);
	dbpath_.clear();
	storage_.reset();
}

std::string NamespaceImpl::sysRecordName(string_view sysTag, uint64_t version) {
	std::string backupRecord(sysTag);
	static_assert(kSysRecordsBackupCount && ((kSysRecordsBackupCount & (kSysRecordsBackupCount - 1)) == 0),
				  "kBackupsCount has to be power of 2");
	backupRecord.append(".").append(std::to_string(version & (kSysRecordsBackupCount - 1)));
	return backupRecord;
}

void NamespaceImpl::writeSysRecToStorage(string_view data, string_view sysTag, uint64_t &version, bool direct) {
	size_t iterCount = (version > 0) ? 1 : kSysRecordsFirstWriteCopies;
	for (size_t i = 0; i < iterCount; ++i, ++version) {
		*(reinterpret_cast<uint64_t *>(const_cast<char *>(data.data()))) = version;
		if (direct) {
			storage_->Write(StorageOpts().FillCache().Sync(0 == version), sysRecordName(sysTag, version), data);
		} else {
			writeToStorage(sysRecordName(sysTag, version), data);
		}
	}
}

Item NamespaceImpl::NewItem(const NsContext &ctx) {
	Locker::RLockT rlck;
	if (!ctx.noLock) {
		rlck = rLock(ctx.rdxContext);
	}
	auto impl_ = pool_.get(ItemImpl(payloadType_, tagsMatcher_, pkFields(), schema_));
	impl_->tagsMatcher() = tagsMatcher_;
	impl_->tagsMatcher().clearUpdated();
	return Item(impl_);
}

void NamespaceImpl::ToPool(ItemImpl *item) {
	item->Clear();
	pool_.put(item);
}

// Get meta data from storage by key
string NamespaceImpl::GetMeta(const string &key, const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return getMeta(key);
}

string NamespaceImpl::getMeta(const string &key) const {
	auto it = meta_.find(key);
	if (it != meta_.end()) {
		return it->second;
	}

	if (storage_) {
		string data;
		Error status = storage_->Read(StorageOpts().FillCache(), string_view(kStorageMetaPrefix + key), data);
		if (status.ok()) {
			return data;
		}
	}

	return string();
}

// Put meta data to storage by key
void NamespaceImpl::PutMeta(const string &key, const string_view &data, const NsContext &ctx) {
	auto wlck = wLock(ctx.rdxContext);
	checkApplySlaveUpdate(ctx.rdxContext.fromReplication_);	 // throw exception if false
	putMeta(key, data, ctx.rdxContext);
}

// Put meta data to storage by key
void NamespaceImpl::putMeta(const string &key, const string_view &data, const RdxContext &ctx) {
	meta_[key] = string(data);

	if (storage_) {
		storage_->Write(StorageOpts().FillCache(), kStorageMetaPrefix + key, data);
	}

	WALRecord wrec(WalPutMeta, key, data);
	processWalRecord(wrec, ctx);
}

vector<string> NamespaceImpl::EnumMeta(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return enumMeta();
}

vector<string> NamespaceImpl::enumMeta() const {
	vector<string> ret;
	ret.reserve(meta_.size());
	for (auto &m : meta_) {
		ret.push_back(m.first);
	}
	if (!storage_) return ret;

	StorageOpts opts;
	opts.FillCache(false);
	unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	size_t prefixLen = strlen(kStorageMetaPrefix);

	for (dbIter->Seek(string_view(kStorageMetaPrefix));
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), string_view(kStorageMetaPrefix "\xFF")) < 0; dbIter->Next()) {
		string_view keySlice = dbIter->Key();
		if (keySlice.size() > prefixLen) {
			string key(keySlice.substr(prefixLen));
			if (meta_.find(key) == meta_.end()) {
				ret.push_back(key);
			}
		}
	}
	return ret;
}

int NamespaceImpl::getSortedIdxCount() const {
	if (!config_.optimizationSortWorkers) return 0;
	int cnt = 0;
	for (auto &it : indexes_)
		if (it->IsOrdered()) cnt++;
	return cnt;
}

void NamespaceImpl::updateSortedIdxCount() {
	int sortedIdxCount = getSortedIdxCount();
	for (auto &idx : indexes_) idx->SetSortedIdxCount(sortedIdxCount);
}

IdType NamespaceImpl::createItem(size_t realSize) {
	IdType id = 0;
	if (free_.size()) {
		id = free_.back();
		free_.pop_back();
		assert(id < IdType(items_.size()));
		assert(items_[id].IsFree());
		items_[id] = PayloadValue(realSize);
	} else {
		id = items_.size();
		items_.emplace_back(PayloadValue(realSize));
	}
	return id;
}

void NamespaceImpl::deleteStorage() {
	if (storage_) {
		storage_->Destroy(dbpath_);
		dbpath_.clear();
		storage_.reset();
	}
}

void NamespaceImpl::checkApplySlaveUpdate(bool fromReplication) {
	if (repl_.slaveMode && !repl_.replicatorEnabled)  // readOnly
	{
		throw Error(errLogic, "Can't modify read only ns '%s'", name_);
	} else if (repl_.slaveMode && repl_.replicatorEnabled)	// slave
	{
		if (!fromReplication) {
			logPrintf(LogTrace, "[repl:%s]:%d Can't modify slave ns '%s' repl_.slaveMode=%d repl_.replicatorenabled=%d fromReplication=%d",
					  name_, serverId_, name_, repl_.slaveMode, repl_.replicatorEnabled, fromReplication);
			throw Error(errLogic, "Can't modify slave ns '%s'", name_);
		} else if (repl_.status == ReplicationState::Status::Fatal) {
			throw Error(errLogic, "Can't modify slave ns '%s', ns has fatal replication error: %s", name_, repl_.replError.what());
		}
	} else if (!repl_.slaveMode && !repl_.replicatorEnabled)  // master
	{
		if (fromReplication) {
			throw Error(errLogic, "Can't modify master ns '%s' from replicator", name_);
		} else if (repl_.status == ReplicationState::Status::Fatal) {
			throw Error(errLogic, "Can't modify ns '%s', ns has fatal replication error: %s", name_, repl_.replError.what());
		}
	}
}

void NamespaceImpl::setFieldsBasedOnPrecepts(ItemImpl *ritem) {
	for (auto &precept : ritem->GetPrecepts()) {
		SelectFuncParser sqlFunc;
		SelectFuncStruct sqlFuncStruct = sqlFunc.Parse(precept);

		VariantArray krs;
		Variant field = ritem->GetPayload().Get(sqlFuncStruct.field, krs)[0];

		Variant value(make_key_string(sqlFuncStruct.value));
		if (sqlFuncStruct.isFunction) {
			value = FunctionExecutor(*this).Execute(sqlFuncStruct);
		}

		value.convert(field.Type());
		VariantArray refs{value};

		ritem->GetPayload().Set(sqlFuncStruct.field, refs, false);
	}
}

int64_t NamespaceImpl::GetSerial(const string &field) {
	int64_t counter = kStorageSerialInitial;

	string ser = getMeta("_SERIAL_" + field);
	if (ser != "") {
		counter = reindexer::stoll(ser) + 1;
	}

	string s = to_string(counter);
	putMeta("_SERIAL_" + field, string_view(s), RdxContext());

	return counter;
}

void NamespaceImpl::FillResult(QueryResults &result, IdSet::Ptr ids) const {
	for (auto &id : *ids) {
		result.Add({id, items_[id], 0, 0});
	}
}

void NamespaceImpl::getFromJoinCache(JoinCacheRes &ctx) const {
	if (config_.cacheMode == CacheModeOff || optimizationState_ != OptimizationCompleted) return;
	auto it = joinCache_->Get(ctx.key);
	ctx.needPut = false;
	ctx.haveData = false;
	if (it.valid) {
		if (!it.val.inited) {
			ctx.needPut = true;
		} else {
			ctx.haveData = true;
			ctx.it = std::move(it);
		}
	}
}

void NamespaceImpl::getIndsideFromJoinCache(JoinCacheRes &ctx) const {
	if (config_.cacheMode != CacheModeAggressive || optimizationState_ != OptimizationCompleted) return;
	auto it = joinCache_->Get(ctx.key);
	ctx.needPut = false;
	ctx.haveData = false;
	if (it.valid) {
		if (!it.val.inited) {
			ctx.needPut = true;
		} else {
			ctx.haveData = true;
			ctx.it = std::move(it);
		}
	}
}

void NamespaceImpl::putToJoinCache(JoinCacheRes &res, JoinPreResult::Ptr preResult) const {
	JoinCacheVal joinCacheVal;
	res.needPut = false;
	joinCacheVal.inited = true;
	joinCacheVal.preResult = preResult;
	joinCache_->Put(res.key, joinCacheVal);
}
void NamespaceImpl::putToJoinCache(JoinCacheRes &res, JoinCacheVal &val) const {
	val.inited = true;
	joinCache_->Put(res.key, val);
}

const FieldsSet &NamespaceImpl::pkFields() {
	auto it = indexesNames_.find(kPKIndexName);
	if (it != indexesNames_.end()) {
		return indexes_[it->second]->Fields();
	}

	static FieldsSet ret;
	return ret;
}

void NamespaceImpl::writeToStorage(const string_view &key, const string_view &data) {
	try {
		unique_lock<std::mutex> lck(locker_.StorageLock());
		updates_->Put(key, data);
		if (unflushedCount_.fetch_add(1, std::memory_order_release) > 20000) doFlushStorage();
	} catch (const Error &err) {
		if (err.code() != errNamespaceInvalidated) {
			throw;
		}
	}
}

void NamespaceImpl::processWalRecord(const WALRecord &wrec, const RdxContext &ctx, lsn_t itemLsn, Item *item) {
	lsn_t lsn(wal_.Add(wrec, itemLsn), serverId_);
	if (!ctx.fromReplication_) repl_.lastSelfLSN = lsn;
	if (item) item->setLSN(int64_t(lsn));
	if (!repl_.temporary) observers_->OnWALUpdate(LSNPair(lsn, ctx.fromReplication_ ? ctx.LSNs_.originLSN_ : lsn), name_, wrec);
	if (!ctx.fromReplication_) setReplLSNs(LSNPair(lsn_t(), lsn));
}

}  // namespace reindexer
