#include "ddl_transaction.h"

#include "composite_fields.h"
#include "core/cjson/cjsontools.h"
#include "core/cjson/uuid_recoders.h"
#include "core/formatters/namespacesname_fmt.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/index/index.h"
#include "core/index/indexfastupdate.h"
#include "core/index/indexing_condition.h"
#include "core/index/ttlindex.h"
#include "core/itemimpl.h"
#include "core/namespace/ann_storage_cache_helper.h"
#include "core/namespace/float_vector_data_access.h"
#include "core/namespace/migrations/pk_migration_service.h"
#include "core/namespace/namespaceimpl.h"
#include "core/payload/payloadfieldtype.h"
#include "tools/logger.h"
#include "tools/scope_guard.h"
#include "tools/stringstools.h"
#include "validator.h"

namespace reindexer::ns_indexes {

TransactionDDL::TransactionDDL(NamespaceImpl& ns) noexcept : ns_{ns}, registry_{ns.indexRegistry_} {}

void TransactionDDL::AddIndex(const IndexDef& indexDef, bool disableTmVersionInc, bool skipEqualityCheck) {
	if (RequireTtlUpdate requireTtlUpdate = RequireTtlUpdate_False;
		!skipEqualityCheck && Validator{registry_}.CheckIfSameIndexExists(indexDef, requireTtlUpdate)) {
		if (requireTtlUpdate) {
			int idxPos = 0;
			[[maybe_unused]] const bool found = registry_.TryGetIndexPos(indexDef.Name(), idxPos);
			assertrx(found);
			UpdateExpireAfter(registry_.Indexes()[idxPos].get(), indexDef.ExpireAfter());
		}
		return;
	}
	apply(Operation{.toAdd = &indexDef, .disableTmVersionInc = disableTmVersionInc});
}

void TransactionDDL::DropIndex(const IndexDef& indexDef, bool disableTmVersionInc) {
	apply(Operation{.toDrop = &indexDef, .disableTmVersionInc = disableTmVersionInc});
}

bool TransactionDDL::UpdateIndex(const IndexDef& indexDef, bool disableTmVersionInc) {
	const IndexDef foundIndex = registry_.GetIndexDefinition(indexDef.Name());
	const auto indexDiff = foundIndex.Compare(indexDef);
	if (indexDiff.Equal()) {
		return false;
	}
	const bool conditionOnly = IndexDef::DiffResult{indexDiff}.Skip(IndexDef::Diff::IndexingCondition).Equal();
	const bool checkCollation = !registry_.Indexes().ConditionalIndexes().empty() &&
								indexDiff.AnyOfIsDifferent(IndexOpts::ParamsDiff::CollateOpts) &&
								IndexFastUpdate::RelaxedEqual(foundIndex, indexDef);
	if (ns_.itemsCount() == 0 && (conditionOnly || checkCollation)) {
		// No items need rebuilding, and replacing the index in place preserves its payload field and references.
		registry_.CheckConsistency<Registry::ConsistencyState::BeforeModification>();
		const auto consistencyGuard =
			MakeScopeGuard([this] { registry_.CheckConsistency<Registry::ConsistencyState::AfterModification>(); });
		TargetState st{registry_};
		if (checkCollation) {
			Validator{registry_}.VerifyUpdateIndex(indexDef, st.GetTagsMatcher(), ns_.indexUpdateContext());
		} else {
			Validator{registry_}.VerifyUpsertIndex("update", indexDef, ns_.itemsCount());
		}
		const int pos = registry_.GetIndexPos(indexDef.Name());
		auto newIndex = Index::New(indexDef, PayloadType{st.GetPayloadType()}, FieldsSet{st.FieldsAt(pos)}, ns_.config_.cacheConfig, 0);
		ApplyIndexingConditionToIndex(*newIndex, indexDef, st);
		st.Replace(pos, std::move(newIndex));
		if (checkCollation) {
			// Prepare the new collation and the conditions using it together, before changing any live indexes.
			rebindIndexingConditions(st, st.TotalSize());
		}
		prepareCommit(st);
		commit(st, -1);
		if (checkCollation) {
			// The cleaner's span must refer to the index storage installed by commit().
			auto indexesCacheCleaner{ns_.getIndexesCacheCleaner()};
			indexesCacheCleaner.Add(*registry_.Indexes()[pos]);
			ns_.indexOptimizer_.UpdateSortedIdxCount(registry_.Indexes(), ns_.name_);
			ns_.markUpdated(IndexOptimization::Full);
		} else {
			ns_.clearNamespaceCaches();
		}
		return true;
	}

	if (checkCollation) {
		// Of the changes allowed by RelaxedEqual, only collation can affect condition evaluation.
		// Field bindings and types stay the same; VerifyIndexChange checks collation from the definitions,
		// so validation can reuse existing indexes without allocating a temporary index's storage.
		TargetState st{registry_};
		verifyIndexingConditions(st, indexDef, &foundIndex);
	}

	if (!IndexFastUpdate::Try(ns_, foundIndex, indexDef)) {
		apply(Operation{.toDrop = &indexDef, .toAdd = &indexDef, .disableTmVersionInc = disableTmVersionInc});
	}
	return true;
}

void TransactionDDL::verifyIndexingConditions(TargetState& st, const IndexDef& indexDef, const IndexDef* previous) const {
	assertrx_dbg(ns_.itemsCount() != 0);
	for (int pos = 1; pos < st.TotalSize(); ++pos) {
		const auto& index = st.At(pos);
		if (!index.HasIndexingCondition()) {
			continue;
		}
		try {
			if (iequals(index.Name(), indexDef.Name())) {
				auto condition = IndexingCondition::Parse(index.IndexingConditionStr(), st);
				if (!condition.has_value()) {
					throw condition.error();
				}
			} else {
				IndexingCondition::VerifyIndexChange(index.IndexingConditionStr(), st, indexDef, previous);
			}
		} catch (const Error& err) {
			throw Error(err.code(), "Cannot {} index '{}' in namespace '{}': indexing_condition of fulltext index '{}': {}",
						previous ? "update" : "add", indexDef.Name(), ns_.name_, index.Name(), err.what());
		}
	}
}

void TransactionDDL::apply(const Operation& op) {
	registry_.CheckConsistency<Registry::ConsistencyState::BeforeModification>();
	const auto consistencyGuard = MakeScopeGuard([this] { registry_.CheckConsistency<Registry::ConsistencyState::AfterModification>(); });

	TargetState st{registry_};
	PayloadFieldsChange change;

	if (op.toDrop && op.toAdd) {
		// The update is verified against the current state. Every JSON-path, which the verification registers, goes into
		// the tags matcher copy of the target state, so an aborted update never bumps the namespace tags matcher
		Validator{registry_}.VerifyUpdateIndex(*op.toAdd, st.GetTagsMatcher(), ns_.indexUpdateContext());
	}

	int droppedSrcPos = -1;
	std::optional<PayloadType> afterDropPlType;
	if (op.toDrop) {
		droppedSrcPos = prepareDrop(st, op, change);
		if (change.droppedPos >= 0) {
			st.UpdateTagsMatcherPayloadType(op.disableTmVersionInc ? NeedChangeTmVersion::No : NeedChangeTmVersion::Increment);
			afterDropPlType.emplace(st.GetPayloadType());
		}
	}

	AddedIndex added;
	if (op.toAdd) {
		added = prepareAdd(st, *op.toAdd, change);
		if (change.addedPos >= 0) {
			st.UpdateTagsMatcherPayloadType(op.disableTmVersionInc ? NeedChangeTmVersion::No : NeedChangeTmVersion::Increment);
		}
	}

	const bool hasItems = ns_.itemsCount() != 0;
	if (op.toAdd && hasItems) {
		const auto previous = op.toDrop ? std::optional{registry_.GetIndexDefinition(op.toDrop->Name())} : std::nullopt;
		verifyIndexingConditions(st, *op.toAdd, previous ? &*previous : nullptr);
	}

	if (change.Any()) {
		prepareComposites(st, change);
	}
	if (!hasItems) {
		// With no documents, changed bindings are safe. Still validate and install the new conditions.
		assertrx_dbg(ns_.itemsCount() == 0);
		rebindIndexingConditions(st, st.TotalSize());
	} else if (change.Any()) {
		if (change.droppedPos >= 0) {
			rebindIndexingConditions(st, st.FirstCompositePos());
		}
		prepareItems(st, change, afterDropPlType ? *afterDropPlType : registry_.GetPayloadType(), added);
	} else if (added.pos >= 0) {
		fillAddedIndex(st, added);
	}

	prepareCommit(st);
	commit(st, droppedSrcPos);

	// Everything below is out of the registry scope: the registry, the metadata and the items are already consistent
	if (!itemsRecoded_) {
		// Sparse/composite index changes can alter query results without recoding items and calling markUpdated below.
		ns_.clearNamespaceCaches();
	}
	if (rewriteStorage_) {
		rewriteStorage();
	}
	if (droppedIndex_) {
		ns_.removeIndex(std::move(droppedIndex_));
	}
	if (op.toDrop) {
		ns_.storage_.Remove(ann_storage_cache::GetStorageKey(op.toDrop->Name()));
	}
	ns_.indexOptimizer_.UpdateSortedIdxCount(registry_.Indexes(), ns_.name_);
	if (added.sparse) {
		ns_.indexOptimizer_.ScheduleOptimization(IndexOptimization::Partial);
	}
	if (itemsRecoded_) {
		ns_.markUpdated(IndexOptimization::Partial);
	}
	if (op.toAdd && op.toAdd->Opts().IsPK()) {
		int newPkPos = 0;
		[[maybe_unused]] const bool found = registry_.TryGetIndexPos(op.toAdd->Name(), newPkPos);
		assertrx(found);
		// PK migration is not rolled back by transactions - see the class comment for its crash-recovery guarantees
		if (!migrations::PKMigrationService{ns_}.MigrateToNewPK(registry_.Indexes()[newPkPos]->Fields())) {
			// The registry change is already committed and still needs replicating (same reasoning as
			// UpdateSortedIdxCount() above), so this can't fail outright - but it must not look silently successful
			// either. Further PK index changes stay blocked until a restart finishes the recovery
			logFmt(LogError,
				   "[{}] {} PK index '{}' left a PK migration incomplete. Further changes to the PK index are blocked "
				   "until the server is restarted to complete the pending storage recovery",
				   ns_.name_, op.toDrop ? "Updating" : "Adding", op.toAdd->Name());
		}
	}
}

int TransactionDDL::prepareDrop(TargetState& st, const Operation& op, PayloadFieldsChange& change) {
	const auto& indexDef = *op.toDrop;
	const int pos = Validator{registry_}.VerifyDropIndex(indexDef, ns_.itemsCount());
	if (!op.toAdd) {
		Validator{registry_}.VerifyIndexingConditionIndexRemoval(registry_.GetIndexDefinition(indexDef.Name()));
	}

	for (int i = st.FirstCompositePos(), total = st.TotalSize(); i < total; ++i) {
		if (st.FieldsAt(i).contains(pos)) [[unlikely]] {
			throw Error(errLogic, "Cannot remove index '{}': it's a part of a composite index '{}'", indexDef.Name(), st.At(i).Name());
		}
	}

	const Index& toRemove = st.At(pos);
	if (!IsComposite(toRemove.Type())) {
		if (toRemove.Opts().IsSparse()) {
			st.GetTagsMatcher().dropSparseIndex(toRemove.Name());
		} else {
			change.droppedPos = pos;
		}
	}
	st.Erase(pos);
	return pos;
}

TransactionDDL::AddedIndex TransactionDDL::prepareAdd(TargetState& st, const IndexDef& indexDef, PayloadFieldsChange& change) {
	const auto& indexName = indexDef.Name();
	if (const auto pkIt = st.names_.find(kPKIndexName); pkIt != st.names_.end() && indexDef.Opts().IsPK()) [[unlikely]] {
		throw Error(errConflict, "Cannot add PK index '{}.{}'. Already exists another PK index - '{}'", ns_.name_, indexName,
					st.At(pkIt->second).Name());
	}

	if (IsComposite(indexDef.IndexType())) {
		Validator{registry_}.VerifyCompositeIndex(indexDef);
		auto fields = CreateFieldsSetFromJsonPaths(indexDef, st.GetTagsMatcher(), [&st](std::string_view name, int& idx) noexcept {
			return st.TryGetNonCompositeIndexPos(name, idx);
		});
		auto newIndex = Index::New(indexDef, PayloadType{st.GetPayloadType()}, std::move(fields), ns_.config_.cacheConfig, ns_.itemsCount(),
								   LogCreation_True);
		ApplyIndexingConditionToIndex(*newIndex, indexDef, st);
		const int pos = st.TotalSize();
		st.Insert(pos, std::move(newIndex), indexName, std::nullopt);
		return AddedIndex{.pos = pos, .sparse = false, .jsonPath = {}};
	}

	const JsonPaths& jsonPaths = indexDef.JsonPaths();
	if (indexDef.Opts().IsSparse()) {
		if (jsonPaths.size() != 1) [[unlikely]] {
			throw Error(errParams, "Sparse index must have exactly 1 JSON-path, but {} paths found for '{}':'{}'", jsonPaths.size(),
						ns_.name_, indexName);
		}
		FieldsSet fields;
		fields.push_back(jsonPaths[0]);
		TagsPath tagsPath = st.GetTagsMatcher().path2tag(jsonPaths[0], CanAddField_True);
		assertrx(!tagsPath.empty());
		fields.push_back(std::move(tagsPath));
		auto newIndex = Index::New(indexDef, PayloadType{st.GetPayloadType()}, std::move(fields), ns_.config_.cacheConfig, ns_.itemsCount(),
								   LogCreation_True);
		ApplyIndexingConditionToIndex(*newIndex, indexDef, st);
		st.GetTagsMatcher().addSparseIndex(*newIndex);
		const int pos = st.FirstSparsePos();
		st.Insert(pos, std::move(newIndex), indexName, std::nullopt);
		return AddedIndex{.pos = pos, .sparse = true, .jsonPath = jsonPaths[0]};
	}

	const int idxNo = st.GetPayloadType().NumFields();
	if (idxNo >= kMaxIndexes) [[unlikely]] {
		throw Error(errConflict, "Cannot add index '{}.{}'. Too many non-composite indexes. {} non-composite indexes are allowed only",
					ns_.name_, indexName, kMaxIndexes - 1);
	}
	auto newIndex = Index::New(indexDef, PayloadType(), FieldsSet(), ns_.config_.cacheConfig, ns_.itemsCount(), LogCreation_True);
	ApplyIndexingConditionToIndex(*newIndex, indexDef, st);
	PayloadFieldType payloadField(ns_.name_.ToLower(), *newIndex, indexDef, ns_.embeddersCache_, ns_.enablePerfCounters_);
	newIndex->SetFields(FieldsSet{idxNo});
	st.Insert(idxNo, std::move(newIndex), indexName, std::move(payloadField));
	st.created(idxNo).UpdatePayloadType(PayloadType{st.GetPayloadType()});
	change.addedPos = idxNo;
	return AddedIndex{.pos = idxNo, .sparse = false, .jsonPath = {}};
}

void TransactionDDL::prepareComposites(TargetState& st, const PayloadFieldsChange& change) {
	for (int pos = st.FirstCompositePos(), total = st.TotalSize(); pos < total; ++pos) {
		const Index& current = st.At(pos);
		assertrx(IsComposite(current.Type()));
		IndexDef indexDef{current.Name(), {}, current.Type(), current.Opts(), 0, current.IndexingConditionStr()};

		FieldsSet fields;
		const auto& curFields = st.FieldsAt(pos);
		if (change.addedPos >= 0) {
			// The added index may have covered a JSON-path, which the composite index refers to
			size_t jsonPathIdx = 0;
			for (int field : curFields) {
				if (field == IndexValueType::SetByJsonPath) {
					const auto& jsonPath = curFields.getJsonPath(jsonPathIdx);
					if (!st.TryGetNonCompositeIndexPos(jsonPath, field)) {
						fields.push_back(curFields.getTagsPath(jsonPathIdx));
						fields.push_back(jsonPath);
					}
					++jsonPathIdx;
				}
				fields.push_back(field);
			}
			assertrx(fields.getJsonPathsLength() == fields.getTagsPathsLength());
		} else {
			fields = curFields;
		}

		auto newIndex =
			Index::New(indexDef, PayloadType{st.GetPayloadType()}, std::move(fields), ns_.config_.cacheConfig, ns_.itemsCount());
		ApplyIndexingConditionToIndex(*newIndex, indexDef, st);
		st.Replace(pos, std::move(newIndex));
	}
}

void TransactionDDL::rebindIndexingConditions(TargetState& st, int endPos) {
	for (int pos = 1; pos < endPos; ++pos) {
		const Index& current = st.At(pos);
		if (!current.HasIndexingCondition()) {
			continue;
		}
		IndexDef indexDef{current.Name(), {}, current.Type(), current.Opts(), 0, current.IndexingConditionStr()};
		if (st.slots_[pos].created) {
			ApplyIndexingConditionToIndex(st.created(pos), indexDef, st);
			continue;
		}
		auto newIndex = Index::New(indexDef, PayloadType{st.GetPayloadType()}, FieldsSet{st.FieldsAt(pos)}, ns_.config_.cacheConfig,
								   ns_.itemsCount(), LogCreation_True);
		ApplyIndexingConditionToIndex(*newIndex, indexDef, st);
		st.Replace(pos, std::move(newIndex));
	}
}

std::unique_ptr<Recoder> TransactionDDL::makeRecoder(const TargetState& st, const PayloadFieldsChange& change) const {
	const auto& oldPlType = registry_.GetPayloadType();
	const bool droppedUuid = change.droppedPos >= 0 && oldPlType.Field(change.droppedPos).Type().Is<KeyValueType::Uuid>();
	const bool addedUuid = change.addedPos >= 0 && st.GetPayloadType().Field(change.addedPos).Type().Is<KeyValueType::Uuid>();

	if (addedUuid && !droppedUuid) {
		if (st.GetPayloadType().Field(change.addedPos).IsArray()) {
			return std::make_unique<RecoderStringToUuidArray>(change.addedPos);
		}
		return std::make_unique<RecoderStringToUuid>(change.addedPos);
	}
	if (droppedUuid && !addedUuid) {
		const auto& jsonPaths = oldPlType.Field(change.droppedPos).JsonPaths();
		std::vector<TagsPath> tagsPaths;
		tagsPaths.reserve(jsonPaths.size());
		for (const auto& jp : jsonPaths) {
			tagsPaths.emplace_back(registry_.GetTagsMatcher().path2tag(jp));
		}
		return std::make_unique<RecoderUuidToString>(std::move(tagsPaths));
	}
	return nullptr;
}

uint64_t TransactionDDL::itemChecksum(const TargetState& st, const PayloadType& pt, const PayloadValue& pv, IdType rowId) const noexcept {
	struct [[nodiscard]] Ctx {
		const TransactionDDL* self;
		const TargetState* st;
		IdType rowId;
	} ctx{this, &st, rowId};
	return ConstPayload{pt, pv}.GetChecksum([&ctx](unsigned field, ConstFloatVectorView vec, unsigned arrayIndex) noexcept -> uint64_t {
		if (vec.IsStripped()) {
			const auto* idx = dynamic_cast<const FloatVectorIndex*>(&ctx.st->At(field));
			assertf(idx, "Expecting '{}' in '{}' being float vector index", ctx.st->At(field).Name(), ctx.self->ns_.name_);
			return idx->GetHash({ctx.rowId, arrayIndex});
		}
		return vec.Hash();
	});
}

void TransactionDDL::prepareItems(TargetState& st, const PayloadFieldsChange& change, const PayloadType& afterDropPlType,
								  const AddedIndex& added) {
	logFmt(LogTrace, "Namespace::prepareItems({}) dropped field={} added field={}", ns_.name_, change.droppedPos, change.addedPos);

	if (ns_.items_.empty()) {
		return;
	}

	const auto& oldPlType = registry_.GetPayloadType();
	const auto& oldTagsMatcher = registry_.GetTagsMatcher();
	const auto& newPlType = st.GetPayloadType();

	const FieldsSet* oldPkFields = ns_.pkFields();
	if (change.addedPos >= 0 && st.At(change.addedPos).IsFloatVector()) {
		rewriteStorage_ = true;
		if (!oldPkFields) {
			ns_.throwCannotModifyNsWithoutPK();
		}
	}
	const auto recoder = makeRecoder(st, change);

	// The tuple index holds the CJSON of every item, so it has to be rebuilt as well. The clone becomes the target
	// index, while the current one is left untouched until commit
	st.Replace(0, registry_.Indexes()[0]->Clone(0, IndexCloneKind::Snapshot));

	VariantArray skrefsDel, skrefsUps, krefs;
	ItemImpl newItem(newPlType, st.GetTagsMatcher());
	newItem.Unsafe(true);
	Index& tuple = st.created(0);
	const int firstComposite = st.FirstCompositePos();
	const int totalIndexes = st.TotalSize();

	newItems_.reserve(ns_.items_.size());
	newChecksum_ = {};
	newItemsDataSize_ = 0;
	for (size_t id = 0; id < ns_.items_.size(); ++id) {
		const IdType rowId = IdType::FromNumber(id);
		if (ns_.items_[rowId].IsFree()) {
			continue;
		}
		PayloadValue& plCurr = ns_.items_[rowId];
		Payload oldValue(oldPlType, plCurr);
		ItemImpl oldItem(oldPlType, plCurr, oldTagsMatcher);
		oldItem.Unsafe(true);
		oldItem.CopyIndexedVectorsValuesFrom(ns_.floatVectorsGetterFn(rowId, oldPkFields));
		if (recoder) {
			recoder->Prepare(rowId);
		}

		try {
			newItem.FromCJSON(oldItem, recoder.get());
		} catch (const std::exception& err) {
			ns_.throwIndexUpsertErrorWithPKInfo(ConstPayload(oldPlType, plCurr), err);
		}

		// The dropped field is removed by name and the added one is appended, so the two step conversion never mixes up
		// the old and the new field even when the updated index keeps its name
		PayloadValue plNew = [&] {
			if (change.droppedPos < 0) {
				return oldValue.CopyTo(newPlType, true);
			}
			PayloadValue withoutDropped = oldValue.CopyTo(afterDropPlType, false);
			if (change.addedPos < 0) {
				return withoutDropped;
			}
			return Payload{afterDropPlType, withoutDropped}.CopyTo(newPlType, true);
		}();
		plNew.SetLSN(plCurr.GetLSN());
		Payload newValue(newPlType, plNew);

		bool needClearCache{false};
		oldValue.Get(0, skrefsDel, Variant::hold);
		tuple.Delete(skrefsDel, rowId, MustExist_True, *ns_.strHolder_, needClearCache);
		newItem.GetPayload().Get(0, skrefsUps);
		tuple.Upsert(krefs, skrefsUps, rowId, needClearCache);
		newValue.Set(0, krefs);

		if (change.addedPos >= 0) {
			Index& addedIndex = st.created(change.addedPos);
			const ConstPayload cpNew{newPlType, plNew};
			newItem.GetPayload().Get(change.addedPos, skrefsUps);
			krefs.resize(0);
			addedIndex.Upsert(krefs, skrefsUps, rowId, addedIndex.EvaluateCondition(cpNew), needClearCache);
			newValue.Set(change.addedPos, krefs);
		}

		if (change.droppedPos >= 0) {
			for (int pos = 1; pos < firstComposite; ++pos) {
				if (pos == added.pos) {
					continue;
				}
				if (!st.At(pos).HasIndexingCondition()) {
					continue;
				}
				// The index was recreated by rebindIndexingConditions()
				Index& index = st.created(pos);
				if (index.Opts().IsSparse()) {
					newValue.GetByJsonPath(index.Fields().getTagsPath(0), skrefsUps, index.KeyType());
				} else {
					newValue.Get(pos, skrefsUps);
				}
				krefs.resize(0);
				index.Upsert(krefs, skrefsUps, rowId, index.EvaluateCondition(ConstPayload{newPlType, plNew}), needClearCache);
				if (!index.Opts().IsSparse()) {
					newValue.Set(pos, krefs);
				}
			}
		}

		if (added.sparse) {
			ConstPayload{newPlType, plNew}.GetByJsonPath(added.jsonPath, st.GetTagsMatcher(), skrefsUps, st.At(added.pos).KeyType());
			if (skrefsUps.IsObjectValue()) [[unlikely]] {
				throwUnexpectedObjectInIndex(st.At(added.pos).Name(), "sparse index");
			}
			krefs.resize(0);
			st.created(added.pos).Upsert(krefs, skrefsUps, rowId, st.At(added.pos).EvaluateCondition(ConstPayload{newPlType, plNew}),
										 needClearCache);
		}

		for (int pos = firstComposite; pos < totalIndexes; ++pos) {
			Index& compositeIndex = st.created(pos);
			std::ignore = compositeIndex.Upsert(Variant(plNew), rowId, compositeIndex.EvaluateCondition(ConstPayload{newPlType, plNew}),
												needClearCache);
		}

		newChecksum_ ^= itemChecksum(st, newPlType, plNew, rowId);
		newItemsDataSize_ += plNew.GetCapacity() + sizeof(PayloadValue::dataHeader);
		newItems_.emplace_back(rowId, std::move(plNew));
	}
	itemsRecoded_ = true;
}

void TransactionDDL::fillAddedIndex(TargetState& st, const AddedIndex& added) {
	const auto& plType = registry_.GetPayloadType();
	Index& index = st.created(added.pos);
	VariantArray krefs, skrefs;
	for (size_t id = 0; id < ns_.items_.size(); ++id) {
		const auto rowId = IdType::FromNumber(id);
		if (ns_.items_[rowId].IsFree()) {
			continue;
		}
		bool needClearCache{false};
		if (added.sparse) {
			try {
				const ConstPayload cp{plType, ns_.items_[rowId]};
				cp.GetByJsonPath(added.jsonPath, st.GetTagsMatcher(), skrefs, index.KeyType());
				if (skrefs.IsObjectValue()) [[unlikely]] {
					throwUnexpectedObjectInIndex(index.Name(), "sparse index");
				}
				krefs.resize(0);
				index.Upsert(krefs, skrefs, rowId, index.EvaluateCondition(cp), needClearCache);
			} catch (const std::exception& err) {
				ns_.throwIndexUpsertErrorWithPKInfo(ConstPayload(plType, ns_.items_[rowId]), err);
			}
		} else {
			// fillAddedIndex() is only reached for sparse or composite adds - a regular add always sets change.addedPos,
			// which routes through prepareItems() instead. The sparse case is handled above, so this branch is composite
			assertrx(IsComposite(index.Type()));
			std::ignore = index.Upsert(Variant(ns_.items_[rowId]), rowId, index.EvaluateCondition(ConstPayload{plType, ns_.items_[rowId]}),
									   needClearCache);
		}
	}
}

void TransactionDDL::rewriteStorage() const {
	// We have to rewrite the storage data in case, when have arrays with double values in storage, otherwise checksum
	// won't match
	const FieldsSet* pkFields = ns_.pkFields();
	assertrx(pkFields);

	// Item writes are always queued asynchronously (NamespaceImpl::replicateItem()) and never flushed on their own -
	// flush first, so a stale queued write for one of these keys can't later clobber the sync write below
	ns_.storage_.Flush(StorageFlushOpts{});

	WrSerializer pkBuf, itemBuf;
	for (const auto& [rowId, _] : newItems_) {
		(void)_;
		ItemImpl item(registry_.GetPayloadType(), ns_.items_[rowId], registry_.GetTagsMatcher());
		item.Unsafe(true);
		std::ignore = ns_.tryWriteItemIntoStorage(*pkFields, item, rowId, pkBuf, itemBuf);
	}

	// No-op in the healthy case (WriteSync() never queues on success) - only matters if a write degraded to async
	// (AsyncStorage::modifySync()); left unflushed, that risks the exact checksum mismatch noted above
	ns_.storage_.Flush(StorageFlushOpts{});
}

void TransactionDDL::prepareCommit(TargetState& st) {
	for (int pos = 1; pos < st.TotalSize(); ++pos) {
		if (st.At(pos).HasIndexingCondition()) {
			st.conditionalIndexes_.push_back(pos);
		}
	}
	newIndexes_.reserve(st.TotalSize());
	// commit() is noexcept: every container it grows has to be preallocated here. newIndexes_ takes at most
	// st.TotalSize() indexes; replacedIndexes_ takes at most every index the source registry currently has (whatever
	// commit() does not move into newIndexes_ and does not drop)
	replacedIndexes_.reserve(registry_.Indexes().totalSize());
}

void TransactionDDL::commit(TargetState& st, int droppedSrcPos) noexcept {
	auto& md = registry_.md_;
	md.plType_ = std::move(st.plType_);
	md.tagsMatcher_ = std::move(st.tm_);
	registry_.indexes_.sparseIndexesCount_ = st.sparseIndexesCount_;
	registry_.indexes_.conditionalIndexes_.swap(st.conditionalIndexes_);

	auto& srcIndexes = registry_.indexes_;
	for (auto& slot : st.slots_) {
		if (slot.created) {
			newIndexes_.emplace_back(std::move(slot.created));
		} else {
			if (slot.fields) {
				srcIndexes[slot.reusedFrom]->SetFields(std::move(*slot.fields));
			}
			newIndexes_.emplace_back(std::move(srcIndexes[slot.reusedFrom]));
		}
	}
	// Whatever is left in the source container has been replaced or dropped by this operation
	for (int pos = 0, total = srcIndexes.totalSize(); pos < total; ++pos) {
		if (!srcIndexes[pos]) {
			continue;
		}
		if (pos == droppedSrcPos) {
			droppedIndex_ = std::move(srcIndexes[pos]);
		} else {
			replacedIndexes_.emplace_back(std::move(srcIndexes[pos]));
		}
	}
	static_cast<IndexesStorage::Base&>(srcIndexes).swap(newIndexes_);
	newIndexes_.clear();

	registry_.names_.swap(st.names_);
	registry_.floatVectorsPositions_.swap(st.floatVectorsPositions_);
	registry_.PropagatePayloadType();
	registry_.ResetCompositesMapping();
	registry_.RebuildCompositesMapping();

	if (itemsRecoded_) {
		ns_.repl_.checksum = newChecksum_;
		ns_.itemsDataSize_ = newItemsDataSize_;
		for (auto& [rowId, pv] : newItems_) {
			std::swap(ns_.items_[rowId], pv);
		}
	}
}

}  // namespace reindexer::ns_indexes
