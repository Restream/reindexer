#include "validator.h"

#include <tuple>
#include "composite_fields.h"
#include "core/definitions/indexdef.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/index/index.h"
#include "core/key_value_type.h"
#include "core/keyvalue/uuid.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/namespacename.h"
#include "core/payload/payloadfieldtype.h"
#include "registry.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer::ns_indexes {

static void verifyConvertType(KeyValueType from, KeyValueType to, const PayloadType& payloadType, const FieldsSet& fields) {
	if (!from.IsSame(to) && (((from.Is<KeyValueType::String>() || from.Is<KeyValueType::Uuid>()) &&
							  !(to.Is<KeyValueType::String>() || to.Is<KeyValueType::Uuid>())) ||
							 ((to.Is<KeyValueType::String>() || to.Is<KeyValueType::Uuid>()) &&
							  !(from.Is<KeyValueType::String>() || from.Is<KeyValueType::Uuid>())) ||
							 from.Is<KeyValueType::FloatVector>() || to.Is<KeyValueType::FloatVector>())) {
		throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
	}
	static const std::string defaultStringValue;
	static const std::string nilUuidStringValue{Uuid{}};
	Variant value;
	from.EvaluateOneOf(
		[&](KeyValueType::Int64) noexcept { value = Variant(int64_t(0)); }, [&](KeyValueType::Double) noexcept { value = Variant(0.0); },
		[&](KeyValueType::Float) noexcept { value = Variant(0.0f); },
		[&](KeyValueType::String) { value = Variant{to.Is<KeyValueType::Uuid>() ? nilUuidStringValue : defaultStringValue}; },
		[&](KeyValueType::Bool) noexcept { value = Variant(false); }, [](KeyValueType::Null) noexcept {},
		[&](KeyValueType::Int) noexcept { value = Variant(0); }, [&](KeyValueType::Uuid) noexcept { value = Variant{Uuid{}}; },
		[&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::FloatVector> auto) {
			if (!to.IsSame(from)) {
				throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
			}
		});
	std::ignore = value.convert(to, &payloadType, &fields);
}

static void verifyConvertSparseType(KeyValueType from, KeyValueType to) {
	if (from.IsSame(to)) {
		return;
	}
	from.EvaluateOneOf(
		[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float> auto) {
			to.EvaluateOneOf([](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
												KeyValueType::Float> auto) noexcept {},
							 [&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::String, KeyValueType::Uuid, KeyValueType::Tuple,
												 KeyValueType::Composite, KeyValueType::Null, KeyValueType::FloatVector> auto) {
								 throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
							 });
		},
		[&](concepts::OneOf<KeyValueType::String, KeyValueType::Uuid> auto) {
			to.EvaluateOneOf([](concepts::OneOf<KeyValueType::String, KeyValueType::Uuid> auto) noexcept {},
							 [&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Bool,
												 KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Null,
												 KeyValueType::Float, KeyValueType::FloatVector> auto) {
								 throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
							 });
		},
		[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null,
							KeyValueType::FloatVector> auto) {
			throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
		});
}

std::string_view Validator::nsName() const noexcept { return registry_.GetPayloadType().Name(); }

bool Validator::CheckIfSameIndexExists(const IndexDef& indexDef, RequireTtlUpdate& requireTtlUpdate) const {
	if (int idxPos = 0; registry_.TryGetIndexPos(indexDef.Name(), idxPos)) {
		IndexDef oldIndexDef = registry_.GetIndexDefinition(indexDef.Name());
		if (oldIndexDef.IndexType() == IndexTtl && indexDef.IndexType() == IndexTtl) {
			if (oldIndexDef.ExpireAfter() != indexDef.ExpireAfter()) {
				requireTtlUpdate = RequireTtlUpdate_True;
			}
			oldIndexDef.SetExpireAfter(indexDef.ExpireAfter());
		}
		if (IndexDef::IsBasicCompatibility(indexDef.Compare(oldIndexDef))) {
			return true;
		}
		throw Error(errConflict, "Index '{}.{}' already exists with different settings", nsName(), indexDef.Name());
	}
	return false;
}

bool Validator::IsPkAffectingIndexChange(const IndexDef& indexDef) const noexcept {
	if (indexDef.Opts().IsPK()) {
		return true;
	}
	int pos = 0;
	return registry_.TryGetIndexPos(indexDef.Name(), pos) && registry_.Indexes()[pos]->Opts().IsPK();
}

int Validator::VerifyDropIndex(const IndexDef& index, size_t itemsCount) const {
	int pos = 0;
	if (!registry_.TryGetIndexPos(index.Name(), pos)) [[unlikely]] {
		constexpr auto errMsg = "Cannot remove index '{}': doesn't exist";
		logFmt(LogError, errMsg, index.Name());
		throw Error(errParams, errMsg, index.Name());
	}
	if (iequals(index.Name(), kTupleName)) [[unlikely]] {
		// The tuple index is not optional - the registry always expects it at position 0 (see Registry::CheckConsistency)
		throw Error(errParams, "Cannot remove index '{}': it's a system index", index.Name());
	}
	// Check, that index to remove is not a part of float index with auto embedding
	auto embeddedIndexName = registry_.GetPayloadType().CheckEmbeddersAuxiliaryField(index.Name());
	if (!embeddedIndexName.empty()) [[unlikely]] {
		throw Error(errLogic, "Cannot remove index '{}': it's a part of a auto embedding logic in index '{}'", index.Name(),
					embeddedIndexName);
	}
	if (registry_.Indexes()[pos]->Opts().IsPK() && itemsCount > 0) {
		for (const auto& idx : registry_.Indexes()) {
			if (idx->IsFloatVector()) [[unlikely]] {
				// TODO remove this after #2220
				throw Error(errLogic, "Cannot remove PK index '{}' from namespace '{}': the namespace contains float vector index '{}'",
							index.Name(), nsName(), idx->Name());
			}
		}
	}
	return pos;
}

void Validator::VerifyCompositeIndex(const IndexDef& indexDef) const {
	const auto type = indexDef.IndexType();
	if (indexDef.Opts().IsSparse()) [[unlikely]] {
		throw Error(errParams, "Composite index cannot be sparse. Use non-sparse composite instead");
	}
	int replacedIdx = -1;
	const bool replacing = registry_.TryGetIndexPos(indexDef.Name(), replacedIdx);
	for (const auto& jp : indexDef.JsonPaths()) {
		int idx;
		// Update keeps the old index in the live registry until commit. That index is being replaced, so it cannot
		// back a json-path of the new composite — after the update it will no longer be a non-composite index.
		if (!registry_.TryGetIndexPos(jp, idx) || (replacing && idx == replacedIdx)) {
			if (!IsFullText(indexDef.IndexType())) [[unlikely]] {
				throw Error(errParams,
							"Composite indexes over non-indexed field ('{}') are not supported yet (except for full-text indexes). Create "
							"at least column index('-') over each field inside the composite index",
							jp);
			}
		} else {
			const auto& index = *registry_.Indexes()[idx];
			if (index.Opts().IsFloatVector()) [[unlikely]] {
				throw Error(errParams, "Composite indexes over float vector indexed field ('{}') are not supported yet", jp);
			}
			if (index.Opts().IsSparse()) [[unlikely]] {
				throw Error(errParams, "Composite indexes over sparse indexed field ('{}') are not supported yet", jp);
			}
			if (type != IndexCompositeHash && index.IsUuid()) [[unlikely]] {
				throw Error{errParams, "Only hash index allowed on UUID field"};
			}
			if (index.Opts().IsArray() && !IsFullText(type)) [[unlikely]] {
				throw Error(errParams, "Cannot add array subindex '{}' to not fulltext composite index '{}'", jp, indexDef.Name());
			}
			if (IsComposite(index.Type())) [[unlikely]] {
				throw Error(errParams, "Cannot create composite index '{}' over the other composite '{}'", indexDef.Name(), index.Name());
			}
		}
	}
}

void Validator::VerifyEmbeddingFields(const h_vector<std::string, 1>& fields, std::string_view fieldName, std::string_view action) const {
	for (const auto& field : fields) {
		int idx = 0;
		if (!registry_.TryGetIndexPos(field, idx)) [[unlikely]] {
			throw Error(errLogic, "Cannot {} index field named '{}' in namespace '{}'. Auxiliary field '{}' not found", action, fieldName,
						nsName(), field);
		}
		if (idx >= registry_.Indexes().firstCompositePos()) [[unlikely]] {
			throw Error(errParams,
						"Cannot {} index field named '{}' in namespace '{}'. Support for embedding only for "
						"scalar index fields. Using composite field '{}' for embedding is invalid",
						action, fieldName, nsName(), field);
		}
		if (registry_.Indexes()[idx]->Opts().IsSparse()) [[unlikely]] {
			throw Error(errParams,
						"Cannot {} index field named '{}' in namespace '{}'. Support for embedding only for "
						"scalar index fields. Using field '{}' is sparse, so embedding is not supported",
						action, fieldName, nsName(), field);
		}
	}
}

void Validator::VerifyUpsertEmbedder(std::string_view action, const IndexDef& indexDef) const {
	const auto& indexDefOpts = indexDef.Opts();
	assertrx_throw(indexDefOpts.IsFloatVector());

	if (!iequals("update", action)) {
		return;	 // do only for update
	}

	auto embedding = indexDefOpts.FloatVector().Embedding();
	if (!embedding.has_value() || !embedding.value().upsertEmbedder.has_value()) {
		return;
	}

	VerifyEmbeddingFields(embedding.value().upsertEmbedder.value().fields, indexDef.Name(), action);
}

void Validator::VerifyUpsertQuantizationConfigHNSWIndex(std::string_view action, const IndexDef& indexDef) const {
	const auto& indexDefOpts = indexDef.Opts();
	if (!indexDefOpts.IsFloatVector()) {
		return;
	}

	const auto& config = indexDefOpts.FloatVector().QuantizationConfig();
	if (!config) {
		return;
	}

	if (indexDef.IndexType() != IndexType::IndexHnsw) {
		throw Error(errParams,
					"Cannot {} quantization config in index '{}' in namespace '{}'. Only the HNSW float vector index can have a "
					"quantization config.",
					action, indexDef.Name(), nsName());
	}

	if (config->quantizationType != hnswlib::QuantizationType::ScalarQuantization8bit) {
		throw Error(errParams, "Cannot {} quantization config in index '{}' in namespace '{}'. Unsupported quantization type - {}", action,
					indexDef.Name(), nsName(), int(config->quantizationType));
	}

	if (config->sampleSize == 0) {
		throw Error(errParams, "Cannot {} quantization config in index '{}' in namespace '{}'. The sampleSize value must be greater than 0",
					action, indexDef.Name(), nsName());
	}

	if (config->quantizationThreshold == 0) {
		throw Error(errParams,
					"Cannot {} quantization config in index '{}' in namespace '{}'. The quantizationThreshold value must be greater than 0",
					action, indexDef.Name(), nsName());
	}
}

void Validator::VerifyUpdateQuantizationConfigHNSWIndex(const Index& curIndex, const IndexDef& newIndexDef, size_t itemsCount) const {
	const auto& curIndexDefOpts = curIndex.Opts();

	if (curIndexDefOpts.IsFloatVector() != newIndexDef.Opts().IsFloatVector() && itemsCount > 0) {
		throw Error(errParams,
					"Cannot update index '{}' in namespace '{}'. Can't convert float vector index to not float vector index and vice versa "
					"in non-empty namespace",
					curIndex.Name(), nsName());
	}

	auto curFloatIndex = dynamic_cast<const FloatVectorIndex*>(&curIndex);
	if (!curFloatIndex) {
		return;
	}

	if (const auto& curConfig = curIndexDefOpts.FloatVector().QuantizationConfig();
		curConfig && curFloatIndex->Type() != IndexType::IndexHnsw) {
		logFmt(LogWarning,
			   "An incorrect float vector options were detected during update index '{}' in namespace '{}': the quantization config of "
			   "an index with a type other than 'hnsw'",
			   curFloatIndex->Name(), nsName());
	}

	if (curFloatIndex->IsQuantized() && newIndexDef.Opts().FloatVector().QuantizationConfig()) {
		throw Error(errParams,
					"Cannot update quantization config in index '{}' in namespace '{}'. Index was quantized already. "
					"To set a new quantization config, restore the original non-quantized index by resetting the current config, "
					"then apply the new one.",
					curFloatIndex->Name(), nsName());
	}
}

void Validator::VerifyUpsertIndex(std::string_view action, const IndexDef& indexDef, size_t itemsCount) const {
	const auto idxType = indexDef.IndexType();
	if (!ValidateIndexName(indexDef.Name(), idxType)) [[unlikely]] {
		throw Error(errParams,
					"Cannot {} index '{}' in namespace '{}'. Index name contains invalid characters. Only alphas, digits, '+' (for "
					"composite indexes only), '.', '_' and '-' are allowed",
					action, indexDef.Name(), nsName());
	}
	const auto& indexDefOpts = indexDef.Opts();
	if (indexDefOpts.IsPK()) {
		if (indexDefOpts.IsArray()) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be array", action, indexDef.Name(), nsName());
		} else if (indexDefOpts.IsSparse()) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be sparse", action, indexDef.Name(), nsName());
		} else if (IsStore(idxType)) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't have '-' type", action, indexDef.Name(),
						nsName());
		} else if (IsFullText(idxType)) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be fulltext index", action, indexDef.Name(),
						nsName());
		} else if (IsGeospatial(idxType)) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be geospatial index", action, indexDef.Name(),
						nsName());
		}
	}
	if ((idxType == IndexUuidHash || idxType == IndexUuidStore) && indexDefOpts.IsSparse()) [[unlikely]] {
		throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. UUID field can't be sparse", action, indexDef.Name(), nsName());
	}
	if (indexDef.JsonPaths().size() > 1) [[unlikely]] {
		if (!IsComposite(idxType) && !indexDefOpts.IsArray()) {
			throw Error(
				errParams,
				"Cannot {} index '{}' in namespace '{}'. Scalar (non-array and non-composite) index can not have multiple JSON-paths. "
				"Use array index instead",
				action, indexDef.Name(), nsName());
		}
		if (IsGeospatial(idxType)) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. Geospatial index can not have multiple JSON-paths", action,
						indexDef.Name(), nsName());
		}
	}
	if (indexDef.JsonPaths().empty()) [[unlikely]] {
		throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. JSON paths array can not be empty", action, indexDef.Name(),
					nsName());
	}
	for (const auto& jp : indexDef.JsonPaths()) {
		if (jp.empty()) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. JSON path can not be empty", action, indexDef.Name(), nsName());
		}
	}
	if (indexDefOpts.IsFloatVector()) {
		VerifyUpsertEmbedder(action, indexDef);
		if (int pkPos = 0; !registry_.TryGetIndexPos(kPKIndexName, pkPos) && itemsCount > 0) [[unlikely]] {
			// TODO remove this after #2220
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. The namespace does not have PK index", action, indexDef.Name(),
						nsName());
		}

		VerifyUpsertQuantizationConfigHNSWIndex(action, indexDef);
	}
}

void Validator::VerifyUpdateIndex(const IndexDef& indexDef, TagsMatcher& tm, const UpdateContext& ctx) const {
	int idxPos = 0;
	if (!registry_.TryGetIndexPos(indexDef.Name(), idxPos)) [[unlikely]] {
		throw Error(errParams, "Cannot update index '{}': doesn't exist", indexDef.Name());
	}
	const auto& oldIndex = registry_.Indexes()[idxPos];
	const auto& indexDefOpts = indexDef.Opts();
	if (int pkPos = 0; indexDefOpts.IsPK() && !oldIndex->Opts().IsPK() && registry_.TryGetIndexPos(kPKIndexName, pkPos)) [[unlikely]] {
		throw Error(errConflict, "Cannot add PK index '{}.{}'. Already exists another PK index - '{}'", nsName(), indexDef.Name(),
					registry_.Indexes()[pkPos]->Name());
	}
	if (indexDefOpts.IsArray() != oldIndex->Opts().IsArray() && ctx.itemsCount > 0) [[unlikely]] {
		// Array may be converted to scalar and scalar to array only if there are no items in namespace
		throw Error(
			errParams,
			"Cannot update index '{}' in namespace '{}'. Can't convert array index to not array and vice versa in non-empty namespace",
			indexDef.Name(), nsName());
	}

	VerifyUpsertIndex("update", indexDef, ctx.itemsCount);

	if (IsComposite(indexDef.IndexType())) {
		VerifyCompositeIndex(indexDef);
		// Composite text indexes require fair fields set to validate config
		auto fields = CreateFieldsSetFromJsonPaths(
			indexDef, tm, [this](std::string_view name, int& idx) noexcept { return registry_.TryGetNonCompositeIndexPos(name, idx); });
		const auto newIndex = std::unique_ptr<Index>(
			Index::New(indexDef, PayloadType{registry_.GetPayloadType()}, std::move(fields), ctx.cacheConfig, ctx.itemsCount));
	} else if (indexDefOpts.IsSparse()) {
		if (indexDef.JsonPaths().size() != 1) [[unlikely]] {
			throw Error(errParams, "Sparse index must have exactly 1 JSON-path, but {} paths found for '{}'", indexDef.JsonPaths().size(),
						indexDef.Name());
		}
		FieldsSet fields;
		fields.push_back(indexDef.JsonPaths()[0]);
		const auto newSparseIndex =
			Index::New(indexDef, PayloadType{registry_.GetPayloadType()}, std::move(fields), ctx.cacheConfig, ctx.itemsCount);
		if (ctx.itemsCount > 0) {
			verifyConvertSparseType(oldIndex->KeyType(), newSparseIndex->KeyType());
		}
	} else {
		const auto newIndex = std::unique_ptr<Index>(Index::New(indexDef, PayloadType(), FieldsSet(), ctx.cacheConfig, ctx.itemsCount));
		PayloadType newPlType = registry_.GetPayloadType();
		newPlType.Drop(indexDef.Name());
		newPlType.Add(PayloadFieldType(ctx.nsName.ToLower(), *newIndex, indexDef, ctx.embeddersCache, ctx.enablePerfCounters));

		if (ctx.itemsCount > 0) {
			FieldsSet changedFields{idxPos};
			verifyConvertType(oldIndex->KeyType(), newIndex->KeyType(), newPlType, changedFields);
		}
	}
	if (int pkPos = 0; indexDefOpts.IsFloatVector() && !registry_.TryGetIndexPos(kPKIndexName, pkPos) && ctx.itemsCount > 0) [[unlikely]] {
		// TODO remove this after #2220
		throw Error(errParams, "Cannot update index '{}' in namespace '{}'. The namespace does not have PK index", indexDef.Name(),
					nsName());
	}

	VerifyUpdateQuantizationConfigHNSWIndex(*oldIndex, indexDef, ctx.itemsCount);
}

}  // namespace reindexer::ns_indexes
