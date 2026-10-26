#include "itemmodifier.h"

#include <algorithm>
#include <ctime>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include "core/cjson/cjsontools.h"
#include "core/cjson/tagsmatcher.h"
#include "core/cjson/tagspath.h"
#include "core/embedding/embedder.h"
#include "core/enums.h"
#include "core/function/function_invoker.h"
#include "core/function/function_parser.h"
#include "core/item_tree/item_tree.h"
#include "core/item_tree/tree_cjson_loader.h"
#include "core/item_tree/tree_json_loader.h"
#include "core/itemimpl.h"
#include "core/key_value_type.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/float_vector_data_access.h"
#include "core/namespace/namespaceimpl.h"
#include "core/namespace/stringsholder.h"
#include "core/payload/payloadiface.h"
#include "core/query/queryentry.h"
#include "core/queryresults/fields_filter.h"
#include "core/tag_name_index.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "index/float_vector/float_vector_index.h"
#include "index/index.h"
#include "tools/assertrx.h"
#include "tools/errors.h"
#include "tools/logger.h"
#include "tools/scope_guard.h"

#include "core/function/expression_evaluator.h"
namespace reindexer {

std::string_view ItemModifier::FieldData::Name() const noexcept { return entry_.Column(); }

ItemModifier::FieldData::BoundExpression& ItemModifier::FieldData::Expression(const NamespaceImpl& ns) {
	if (!expression_) {
		assertrx_throw(entry_.IsExpression());
		assertrx_throw(!entry_.Values().empty());
		BoundExpression bound{ExpressionAst::Parse(static_cast<std::string_view>(entry_.Values().front()), false), {}};
		auto& bindings = bound.indexBindings;
		bindings.reserve(bound.ast.FieldNames().size());
		for (const auto& name : bound.ast.FieldNames()) {
			int indexNo = 0;
			if (ns.tryGetIndexByName(name, indexNo)) {
				if (indexNo >= ns.payloadType().NumFields()) {
					const auto& index = *ns.indexes()[indexNo];
					if (IsComposite(index.Type())) {
						throw Error(errParams, "Composite indexes are not supported in UPDATE expressions: {}", name);
					}
					if (index.KeyType().Is<KeyValueType::FloatVector>()) {
						throw Error(errParams, "Float vector indexes are not supported in UPDATE expressions: {}", name);
					}
				}
				bindings.emplace_back(indexNo);
			} else {
				bindings.emplace_back(IndexValueType::NotSet);
			}
		}
		expression_.emplace(std::move(bound));
	}
	return *expression_;
}

ItemModifier::FieldData::PathData ItemModifier::FieldData::preprocIndexedTagsPath(const NamespaceImpl& ns,
																				  const IndexedTagsPath& tagsPath) {
	PathData pd;
	for (size_t i = 0; i < tagsPath.size(); ++i) {
		const auto& tag = tagsPath[i];
		if (tag.IsTagName()) {
			if (!pd.jsonPath.empty()) {
				pd.jsonPath += '.';
			}
			const TagName tagName = tag.GetTagName();
			pd.tagsPath.emplace_back(tagName);
			pd.jsonPath += ns.tagsMatcher().tag2name(tagName);
			pd.tailIndexesInPath = 0;
		} else {
			++pd.indexesCountInPath;
			++pd.tailIndexesInPath;
		}
	}
	return pd;
}

bool ItemModifier::FieldData::initFromSimpleIndexName(std::string_view name, NamespaceImpl& ns) {
	if (!ns.tryGetIndexByName(name, fieldIndex_)) {
		return false;
	}
	isIndex_ = true;
	auto tagsPathIndex = getTagsPathByIndexField(fieldIndex_, ns);

	const std::string jsonPathIndex = ns.tagsMatcher().Path2Name(tagsPathIndex);

	if (ns.indexes()[fieldIndex_]->Opts().IsArray() && tagsPathIndex.size() > 1 && name != jsonPathIndex) {
		throw Error(errParams,
					"Unable to update index '{}' by name, because array structure "
					"is ambiguous. Use JSON-path('{}') instead of index name",
					name, ns.tagsMatcher().Path2Name(tagsPathIndex));
	}
	tagsPath_ = IndexedTagsPath(tagsPathIndex);
	if (IsComposite(ns.indexes()[fieldIndex_]->Type())) {
		isIndex_ = false;
		fieldIndex_ = SetByJsonPath;
	}
	return true;
}

bool ItemModifier::FieldData::initFromSimpleJsonPath(std::string_view name, NamespaceImpl& ns) {
	if (fieldIndex_ = ns.payloadType().FieldByJsonPath(name); fieldIndex_ > 0) {
		isIndex_ = true;
		tagsPath_ = ns.tagsMatcher().path2indexedtag(name);
		return true;
	}
	return false;
}

bool ItemModifier::FieldData::initFromIndexedIndexName(const NamespaceImpl& ns, const PathData& pathData,
													   const IndexedTagsPath& indexedTagsPath) {
	// Handle 'idx_name[1]' for cases, when index name does not equal to jsonpath
	int fieldIndex = SetByJsonPath;
	if (ns.tryGetIndexByName(pathData.jsonPath, fieldIndex)) {
		auto tmpTagsPath = getTagsPathByIndexField(fieldIndex, ns);
		if ((tmpTagsPath.size() == 1) && (pathData.tailIndexesInPath == pathData.indexesCountInPath)) {
			IndexedTagsPath tmpIndexedTagsPath(tmpTagsPath);
			for (auto tagIt = (indexedTagsPath.end() - pathData.tailIndexesInPath); tagIt != indexedTagsPath.end(); ++tagIt) {
				tmpIndexedTagsPath.emplace_back(*tagIt);
			}
			tagsPath_ = std::move(tmpIndexedTagsPath);
			isIndex_ = true;
			fieldIndex_ = fieldIndex;
			return true;
		} else {
			const auto idxNameTagsPath = ns.tagsMatcher().path2tag(ns.indexes()[fieldIndex]->Name());
			const bool idxNameAndJsonPathAreEqual = !idxNameTagsPath.empty() && (idxNameTagsPath == tmpTagsPath);
			if (!idxNameAndJsonPathAreEqual) [[unlikely]] {
				throw Error(
					errParams,
					"Unable to update index '{}' by name, because array structure is ambiguous. Use JSON-path('{}') instead of index name",
					pathData.jsonPath, ns.tagsMatcher().Path2Name(tmpTagsPath));
			}
		}
	}
	return false;
}

void ItemModifier::FieldData::initFromIndexedPath(std::string_view name, NamespaceImpl& ns) {
	IndexedTagsPath indexedTagsPath = ns.indexRegistry_.GetTagsMatcher().path2indexedtag(name, CanAddField_True);
	auto pathData = preprocIndexedTagsPath(ns, indexedTagsPath);

	if (!initFromIndexedIndexName(ns, pathData, indexedTagsPath)) {
		// Handle indexed jsonpath
		const auto field = ns.tagsMatcher().tags2field(pathData.tagsPath);
		if (field.IsRegularIndex()) {
			fieldIndex_ = field.IndexNumber();
			isIndex_ = true;
		} else {
			isIndex_ = ns.tryGetIndexByJsonPath(pathData.jsonPath, fieldIndex_);
		}
		tagsPath_ = std::move(indexedTagsPath);
	}
	if (isIndex_) {
		if (!tagsPath_.empty() && tagsPath_.back().IsTagIndexNotAll()) {
			tagsPathWithLastIndex_ = tagsPath_;
			arrayIndex_ = tagsPath_.back().GetTagIndex();
			tagsPath_.pop_back();
		}
		isUpdateArrayPart_ = ArrayIndex() && !ArrayIndex()->IsAll();
		for (const auto& tag : Tagspath()) {
			if (tag.IsTagIndex()) {
				isUpdateArrayPart_ = true;
				if (tag.GetTagIndex().IsAll()) {
					break;
				}
			}
		}
	}
}

TagsPath ItemModifier::FieldData::getTagsPathByIndexField(int& field, const NamespaceImpl& ns) {
	assertrx_throw(field > 0);

	TagsPath tagsPath;
	const auto& idx = *ns.indexes()[field];
	auto totalJsonPaths = (idx.Opts().IsSparse() || static_cast<int>(field) >= ns.payloadType().NumFields())
							  ? idx.Fields().size()
							  : ns.payloadType().Field(field).JsonPaths().size();

	if (totalJsonPaths != 1) [[unlikely]] {
		assertrx_dbg(totalJsonPaths);
		throw Error(errParams, "Ambiguity when updating field with several json paths by index name: '{}'", idx.Name());
	}

	const auto& fields{idx.Fields()};
	if (fields[0] == IndexValueType::SetByJsonPath) {
		tagsPath = fields.getTagsPath(0);
	} else {
		field = fields[0];	// 'Composite' index with single subindex
		const auto& jsonPath = ns.payloadType().Field(field).JsonPaths()[0];
		tagsPath = ns.tagsMatcher().path2tag(jsonPath);
		if (tagsPath.empty()) [[unlikely]] {
			throw Error(errParams, "Cannot find field by json: '{}'", jsonPath);
		}
	}
	return tagsPath;
}

void ItemModifier::FieldData::appendAffectedIndexes(const NamespaceImpl& ns, CompositeFlags& affectedComposites) const {
	const auto firstCompositePos = ns.indexes().firstCompositePos();
	const auto firstSparsePos = ns.indexes().firstSparsePos();
	const auto totalIndexes = ns.indexes().totalSize();
	const bool isRegularIndex = IsIndex() && Index() < firstSparsePos;
	std::bitset<kMaxIndexes> affected;
	if (isRegularIndex) {
		affected.set(Index());
	} else {
		for (int i = 0; i < firstSparsePos; ++i) {
			const auto& ptField = ns.payloadType().Field(i);
			for (const auto& jpath : ptField.JsonPaths()) {
				auto tp = ns.tagsMatcher().path2tag(jpath);
				if (Tagspath().IsNestedOrEqualTo(tp)) {
					affected.set(i);
					break;
				}
			}
		}
	}

	for (int i = firstCompositePos; i < totalIndexes; ++i) {
		const auto& fields = ns.indexes()[i]->Fields();
		const auto idxId = i - firstCompositePos;

		for (const auto f : fields) {
			if (f == IndexValueType::SetByJsonPath) {
				continue;
			}
			if (affected.test(f)) {
				affectedComposites[idxId] = true;
				break;
			}
		}
		if (affectedComposites[idxId]) {
			continue;
		}

		if (!isRegularIndex) {
			// Fulltext composites may be created over non-index and sparse fields
			for (size_t tp = 0, end = fields.getTagsPathsLength(); tp < end; ++tp) {
				if (Tagspath().IsNestedOrEqualTo(fields.getTagsPath(tp))) {
					affectedComposites[idxId] = true;
					break;
				}
			}
		}
	}
}

class [[nodiscard]] ItemModifier::RollBack_ModifiedPayload {
public:
	RollBack_ModifiedPayload(ItemModifier& modifier, IdType id) noexcept : modifier_{modifier}, itemId_{id} {}
	// The default move would copy disabled_ instead of transferring it, leaving both the moved-from and the moved-to
	// object able to roll back the same item from their destructors. Disable the source explicitly instead
	RollBack_ModifiedPayload(RollBack_ModifiedPayload&& other) noexcept
		: disabled_{other.disabled_}, modifier_{other.modifier_}, itemId_{other.itemId_} {
		other.disabled_ = true;
	}
	~RollBack_ModifiedPayload() { RollBack(); }

	void Disable() noexcept { disabled_ = true; }
	bool IsDisabled() const noexcept { return disabled_; }

	// NOLINTNEXTLINE(bugprone-exception-escape) Termination here is better, than inconsistent state of the user's data
	void RollBack() noexcept {
		if (IsDisabled()) {
			return;
		}
		auto indexesCacheCleaner{modifier_.ns_.GetIndexesCacheCleaner()};
		const std::vector<IndexUpdateState>& data = modifier_.rollBackIndexData_.IndexStatus();
		PayloadValue& plValue = modifier_.ns_.items_[itemId_];
		plValue.Clone();

		const NamespaceImpl::IndexesStorage& indexes = modifier_.ns_.indexes();

		Payload plCur(modifier_.ns_.payloadType(), plValue);

		for (size_t i = indexes.firstCompositePos(); i < size_t(indexes.totalSize()) && i < data.size(); ++i) {
			if (data[i] == IndexUpdateState::NewValueInserted) {
				bool needClearCache{false};
				indexes[i]->Delete(Variant(plValue), itemId_, MustExist_False, *modifier_.ns_.strHolder(), needClearCache);
				if (needClearCache) {
					indexesCacheCleaner.Add(*indexes[i]);
				}
			}
		}

		for (size_t i = 1; i < data.size() && i < size_t(indexes.firstCompositePos()); i++) {
			if (data[i] != IndexUpdateState::Unchanged) {
				bool needClearCache{false};
				if (data[i] == IndexUpdateState::NewValueInserted) {
					const VariantArray& vals = modifier_.rollBackIndexData_.InsertedKeys(i);
					indexes[i]->Delete(vals, itemId_, MustExist_False, *modifier_.ns_.strHolder(), needClearCache);
					if (needClearCache) {
						indexesCacheCleaner.Add(*indexes[i]);
					}
				}

				const VariantArray& oldData = modifier_.indexData_.IndexDataValue(i);
				VariantArray result;
				indexes[i]->Upsert(result, oldData, itemId_, needClearCache);
				if (!indexes[i]->Opts().IsSparse()) {
					Payload pl{modifier_.ns_.payloadType(), modifier_.ns_.items_[itemId_]};
					try {
						pl.Set(i, result);
					} catch (const Error& err) {
						assertf(false, "[{}]: Failed to set index {} value: {}", modifier_.ns_.name_, i, err.what());
						std::abort();
					}
				}
				if (needClearCache) {
					indexesCacheCleaner.Add(*indexes[i]);
				}
			}
		}

		const IndexUpdateState tupleState = data[0];
		if (tupleState != IndexUpdateState::Unchanged) {
			bool needClearCache{false};
			VariantArray res;
			if (tupleState == IndexUpdateState::NewValueInserted) {
				indexes[0]->Delete(modifier_.indexData_.IndexDataValueNew(0), itemId_, MustExist_True, *modifier_.ns_.strHolder(),
								   needClearCache);
			}
			indexes[0]->Upsert(res, modifier_.indexData_.IndexDataValue(0), itemId_, needClearCache);
			try {
				plCur.Set(0, res);
			} catch (const Error& err) {
				assertf(false, "[{}]: Failed to set cjson tuple: {}", modifier_.ns_.name_, err.what());
				std::abort();
			}
		}

		// Insert current payload value into affected composite indexes or refresh value in unaffected indexes
		const Variant pvCurVariant{plValue};
		for (size_t i = indexes.firstCompositePos(); i < size_t(indexes.totalSize()); ++i) {
			if (data[i] != IndexUpdateState::Unchanged) {
				bool needClearCache{false};
				std::ignore = indexes[i]->Upsert(pvCurVariant, itemId_, needClearCache);
				if (needClearCache) {
					indexesCacheCleaner.Add(*indexes[i]);
				}
			} else {
				bool refreshed = indexes[i]->RefreshCompositeKey(pvCurVariant, itemId_);
				assertrx_dbg(refreshed);
				if (!refreshed) [[unlikely]] {
					logFmt(LogError, "[{}]: Unable to refresh key for {} during update query", modifier_.ns_.name_, indexes[i]->Name());
				}
			}
		}
	}

	RollBack_ModifiedPayload(const RollBack_ModifiedPayload&) = delete;
	RollBack_ModifiedPayload operator=(const RollBack_ModifiedPayload&) = delete;
	RollBack_ModifiedPayload operator=(RollBack_ModifiedPayload&&) = delete;

private:
	bool disabled_ = false;
	ItemModifier& modifier_;
	IdType itemId_;
};

ItemModifier::FieldData::FieldData(const UpdateEntry& entry, NamespaceImpl& ns, CompositeFlags& affectedComposites)
	: entry_(entry), tagsPathWithLastIndex_{std::nullopt} {
	if (!initFromSimpleIndexName(entry_.Column(), ns)) {
		if (!initFromSimpleJsonPath(entry_.Column(), ns)) {
			initFromIndexedPath(entry_.Column(), ns);
		}
	}
	if (tagsPath_.empty()) [[unlikely]] {
		throw Error(errParams, "Cannot find field by json: '{}'", entry_.Column());
	}
	appendAffectedIndexes(ns, affectedComposites);
}

ItemModifier::ItemModifier(const std::vector<UpdateEntry>& updateEntries, NamespaceImpl& ns, UpdatesContainer& replUpdates,
						   const NsContext& ctx, const functions::PrecomputedValues& precomputedValues)
	: ns_(ns),
	  updateEntries_(updateEntries),
	  rollBackIndexData_(ns_.indexes().totalSize()),
	  affectedComposites_(ns_.indexes().totalSize() - ns_.indexes().firstCompositePos(), false),
	  embedderHelper_(ns_),
	  indexData_(ns_.indexes(), ns_.tagsMatcher()),
	  precomputedValues_(precomputedValues),
	  ci_(ns_.tagsMatcher(), ns_.payloadType(), ns_.indexes(), indexData_,
		  [this](std::string_view name) { return ns_.getIndexByName(name); }) {
	const auto oldTmV = ns_.tagsMatcher().version();
	for (const UpdateEntry& updateField : updateEntries_) {
		const auto& values = updateField.Values();
		for (const auto& v : updateField.Values()) {
			v.Type().EvaluateOneOf([](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
													  KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid, KeyValueType::Null,
													  KeyValueType::Undefined, KeyValueType::FloatVector> auto) {},
								   [&values](KeyValueType::Tuple) {
									   if (!values.IsArrayValue()) {
										   throw Error(
											   errParams,
											   "Unable to use 'tuple'-value (array of arrays, array of points, etc) in UPDATE-query. Only "
											   "single dimensional arrays and arrays of objects are supported");
									   }
								   },
								   [](KeyValueType::Composite) {
									   throw Error(errParams,
												   "Unable to use 'composite'-value (object, array of objects, etc) in UPDATE-query. "
												   "Probably 'object'/'json' type was not explicitly set in the query");
								   });
		}
		fieldsToModify_.emplace_back(updateField, ns_, affectedComposites_);
	}
	const int pkPos = ns_.getPkIdx().second;
	assertrx_dbg(pkPos >= 0);
	const int firstCompositePos = ns_.indexes().firstCompositePos();
	if (pkPos < firstCompositePos) {
		nonCompositePKIndex_ = pkPos;
	} else {
		affectedCompositePK_ = affectedComposites_[pkPos - firstCompositePos];
	}
	embedderHelper_.Init(fieldsToModify_);
	ns_.replicateTmUpdateIfRequired(replUpdates, oldTmV, ctx);
}

void ItemModifier::EmbedderHelper::prepareVectorIndexInfo() {
	for (int fieldIndex = 1, numFields = ns_.payloadType().NumFields(); fieldIndex < numFields; ++fieldIndex) {
		std::shared_ptr<const UpsertEmbedder> embedder = ns_.payloadType().Field(fieldIndex).UpsertEmbedder();
		if (embedder) {
			for (const auto& field : embedder->Fields()) {
				int idx = 0;
				if (!ns_.tryGetIndexByName(field, idx)) [[unlikely]] {
					throw Error{errConflict, "Configuration 'embedding:upsert_embedder' configured with invalid base field name '{}'",
								field};
				}
				if (idx >= ns_.indexes().firstSparsePos()) [[unlikely]] {
					throw Error(errParams,
								"Auto embedding not supported for composite and sparce indexes."
								"Field '{}' for embedding is invalid in namespace {}.",
								field, ns_.name_);
				}
				addToIndex(idx, fieldIndex, autoEmbeddingIndexesList_);
				addToIndex(fieldIndex, idx, vectorIndexesList_);
			}
		}
	}
}

const h_vector<int, 2>& ItemModifier::EmbedderHelper::autoEmbeddingFieldsForVectorIndex(int field) const noexcept {
	for (const auto& v : vectorIndexesList_) {
		if (v.vecField == field) {
			return v.autoEmbeddingFields;
		}
	}
	static const h_vector<int, 2> empty;
	return empty;
}

void ItemModifier::EmbedderHelper::prepareSkipVectorIndexes(const std::vector<FieldData>& fieldsToModify) {
	for (const FieldData& field : fieldsToModify) {
		if (field.IsIndex() && !autoEmbeddingFieldsForVectorIndex(field.Index()).empty()) {
			if (field.Details().IsExpression()) {
				assertrx_throw(field.Details().Values().size() > 0);
				std::string v = field.Details().Values().front().As<std::string>();
				Tokenizer tokenizer(v);
				Token tok = tokenizer.NextToken();
				auto parsedFunction = functions::FunctionParser::ParseFunction(tokenizer, tok);
				if (parsedFunction.funcName == "skip_embedding") [[likely]] {
					skipVectorIndexes_.set(field.Index());
					continue;
				} else {
					throw Error(errParams, "Incorrect value {}", field.Details().Values().front().As<std::string>());
				}
			}
			if (field.Details().Mode() == FieldModeSet) {  // value is set, embedding_strategy is ignored
				skipVectorIndexes_.set(field.Index());
			}
		}
	}
}

static void convertToFloatVector(FloatVectorDimension dim, VariantArray& values) {
	if (values.empty()) {
		values.emplace_back(ConstFloatVectorView{});
		std::ignore = values.MarkArray(false);
		return;
	} else if (values.IsNullValue()) {
		assertrx_dbg(values.size() == 1);
		values[0] = Variant{ConstFloatVectorView{}};
		return;
	}
	if (values.size() != dim.Value()) [[unlikely]] {
		throw Error(errParams, "It's not possible to Update float vector index of dimension {} with array of size {}", dim.Value(),
					values.size());
	}
	auto vect = FloatVector::CreateNotInitialized(dim);
	for (size_t i = 0, s = values.size(); i != s; ++i) {
		vect.RawData()[i] = values[i].As<double>();
	}
	values.clear();
	values.emplace_back(std::move(vect));
}

static void convertToFloatVectorArray(FloatVectorDimension dim, VariantArray& values) {
	if (values.empty()) {
		return;
	} else if (values.IsNullValue()) {
		values.Clear();
		return;
	}
	if (!values[0].Type().IsNumeric()) {
		for (Variant& v : values) {
			v.Type().EvaluateOneOf(
				[](KeyValueType::FloatVector) {},
				[&](KeyValueType::Tuple) {
					VariantArray nestedValues = v.As<VariantArray>();

					const bool isNumericArray =
						!nestedValues.empty() && std::all_of(nestedValues.begin(), nestedValues.end(),
															 [](const Variant& value) { return value.Type().IsNumeric(); });

					if (isNumericArray) {
						convertToFloatVector(dim, nestedValues);
						assertrx(nestedValues.size() == 1);
						assertrx(nestedValues.front().Type().Is<KeyValueType::FloatVector>());
						v = std::move(nestedValues.front());
					} else {
						convertToFloatVectorArray(dim, nestedValues);
						v = Variant{nestedValues};
					}
				},
				[](auto) { throw Error(errLogic, "Array of vectors allowed for update array float vector index only"); });
		}
	} else {
		if (values.IsArrayValue()) {
			return convertToFloatVector(dim, values);
		} else {
			throw Error(errParams, "Can't convert Variant from type '{}' to type 'float_vector'", values[0].Type().Name());
		}
	}
}

bool ItemModifier::isPayloadModify(Payload& pl) {
	bool isSimple = true;
	for (auto& field : fieldsToModify_) {
		if (!isSimple) {
			break;
		}
		if (field.IsIndex() && field.Details().Values().size() == 1 && !field.Details().Values().IsArrayValue() &&
			!field.Details().Values().IsNullValue() && !field.IsUpdateArrayPart() && field.Details().Mode() == FieldModeSet) {
			int indx = field.Index();
			if (indx < ns_.payloadType()->NumFields() && !ns_.payloadType()->Field(indx).IsArray() &&
				!ns_.payloadType()->Field(indx).IsFloatVector() && !field.Details().IsExpression()) {
				if (!pl.HasDefaultValue(indx)) {
					const auto& vectorIndex = embedderHelper_.GetVectorIndexForAutoEmbeddingField(indx);
					if (!vectorIndex.empty()) {
						isSimple = false;
						break;
					}
					continue;
				}
			}
		}
		isSimple = false;
	}
	return isSimple;
}

void ItemModifier::directPayloadModify(Payload& pl, IdType itemId, NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner,
									   StringsHolder& strHolder) {
	if (affectedCompositePK_) {
		saveVectorsBeforePkChange(itemId);
	}
	deleteItemFromComposite(itemId, indexesCacheCleaner);
	auto compositesGuard = MakeScopeGuard([this, itemId, &indexesCacheCleaner] { insertItemIntoComposite(itemId, indexesCacheCleaner); });

	for (FieldData& field : fieldsToModify_) {
		int index = field.Index();
		VariantArray keys;
		pl.Get(index, keys);
		indexData_.SaveOldIndexData(index, VariantArray{keys});
		VariantArray values;
		values = field.Details().Values();
		assertrx_throw(values.size() == 1);
		if (values.IsNullValue()) {
			throw Error(errParams, "Non-array index fields cannot be set to null!");
		}
		std::ignore = values[0].convert(ns_.payloadType()->Field(index).Type());
		IntermediateIndexesData::IndexNodeData newData;
		newData.Vals = values;
		indexData_.SetNewIndexData(index, std::move(newData), ns_.indexes()[index].get());

		if (index == nonCompositePKIndex_ && !rollBackIndexData_.IsPkModified()) {
			saveVectorsBeforePkChange(itemId);
		}
		VariantArray res;
		updateIndex(itemId, index, keys, values, res, indexesCacheCleaner, strHolder);
		assertrx_throw(res.size() == 1);
		pl.Set(index, res);
	}
}

static void writeFloatVectorArrayToTree(TreeNode& node, const VariantArray& values, int floatVectorIndex) {
	if (node.NeedsTypeChange<ArrayNode>()) {
		node.ChangeToArray();
	}

	node.Clear();
	node.SetIndex(0);
	node.SetHomogeneousArray(false);

	for (const Variant& value : values) {
		value.Type().EvaluateOneOf(
			[&](KeyValueType::FloatVector) {
				// The index is assigned directly to the FloatVector node.
				std::ignore = node.AddValue(TagName::Empty(), Variant{value}, floatVectorIndex);
			},
			[&](KeyValueType::Tuple) {
				VariantArray nestedValues = value.As<VariantArray>();

				TreeNode& nestedNode = node.AddArray(TagName::Empty(), nestedValues.size());

				writeFloatVectorArrayToTree(nestedNode, nestedValues, floatVectorIndex);
			},
			[&value](auto) { throw Error(errParams, "Unexpected value type '{}' in float vector array", value.Type().Name()); });
	}
}

void ItemModifier::processNodeSet(TreeNode& node, TreeNode& root, VariantArray& values, ConstPayload& cpl, IdType itemId) {
	node.ResolveIndexByPath();
	int nodeIndex = node.GetIndex();
	// FloatVector scalar type in Variant
	if (nodeIndex > 0 && ns_.indexes()[nodeIndex]->IsFloatVector()) {
		auto& index = *ns_.indexes()[nodeIndex];
		if (!indexData_.IsIndexChanged(nodeIndex)) {
			materializeFloatVectorIndex(root, cpl, itemId, nodeIndex);
			indexData_.MarkIndexChanged(nodeIndex);
		}

		if (index.Opts().IsArray()) {
			convertToFloatVectorArray(static_cast<FloatVectorIndex&>(index).Dimension(), values);

			bool isSingleVector = values.size() == 1 && values[0].Type().Is<KeyValueType::FloatVector>();

			if (isSingleVector) {
				if (node.NeedsTypeChange<ValueNode>()) {
					node.ChangeToValue();
				}

				node.SetIndex(nodeIndex);
				node.UpdateValue(values.front());
			} else {
				writeFloatVectorArrayToTree(node, values, nodeIndex);
			}

		} else {
			if (values.IsArrayValue() || values.IsNullValue() || values.empty()) {
				convertToFloatVector(static_cast<FloatVectorIndex&>(index).Dimension(), values);
			}

			assertrx_throw(values.size() == 1);

			if (node.NeedsTypeChange<ValueNode>()) {
				node.ChangeToValue();
			}

			node.SetIndex(nodeIndex);
			node.UpdateValue(values.front());
		}
	} else {
		if (values.IsArrayValue()) {
			int indx = node.GetIndex();
			if (indx > 0) {
				if (!ns_.indexes()[nodeIndex]->Opts().IsArray()) {
					if (values.IsArrayValue()) [[unlikely]] {
						throw Error(errParams, "It's not possible to Update single index fields with arrays!");
					}
				}
			} else {
				TagsPath tp = node.GetNodePath();
				FieldProperties fp = ns_.tagsMatcher().tags2field(tp);
				if (fp.IsIndexed()) {
					const auto& sparseIndexData = ns_.tagsMatcher().SparseIndex(fp.SparseNumber());
					int indexId = 0;
					if (ns_.tryGetIndexByName(sparseIndexData.name, indexId)) {
						if (!ns_.indexes()[indexId]->Opts().IsArray()) {
							if (values.IsArrayValue()) [[unlikely]] {
								throw Error(errParams, "It's not possible to Update single index fields with arrays!");
							}
						}
					}
				}
			}
			node.SaveOldIndexDataForSubtree(indexData_, cpl, root);
			materializeChangedFloatVectorNodes(root, cpl, itemId);
			if (node.NeedsTypeChange<ArrayNode>()) {
				node.ChangeToArray();
			}
			node.UpdateArray(values);
		} else {
			if (node.GetIndex() > 0 && node.IsType<ObjectNode>()) {	 // The node has just been created
				VariantArray vals;
				cpl.Get(node.GetIndex(), vals);
				indexData_.SaveOldIndexData(node.GetIndex(), std::move(vals));
			} else {
				node.SaveOldIndexDataForSubtree(indexData_, cpl, root);
			}
			materializeChangedFloatVectorNodes(root, cpl, itemId);
			if (node.IsType<ObjectNode>()) {  // Save an empty old value for a sparse index
				if (int sindex = node.IsSparseIndex()) {
					indexData_.SaveEmptyOldIndexData(sindex);
				}
			}
			if (node.NeedsTypeChange<ValueNode>()) {
				node.ChangeToValue();
			}
			if (values.empty()) {
				Variant nullValue;
				node.UpdateValue(nullValue);
			} else {
				node.UpdateValue(values.front());
			}
		}
	}
}

void ItemModifier::processFieldModeSet(TreeNode& root, FieldData& field, ConstPayload& cpl, VariantArray& values, IdType itemId) {
	UpdatePathCursor cursor{root, field.TagspathWithLastIndex(), ci_};
	TreeNode* node = cursor.Next();
	if (node == nullptr) {
		throw Error(errParams, "Unable to set field '{}': array item specified by the path was not found", field.Name());
	}

	for (; node != nullptr; node = cursor.Next()) {
		processNodeSet(*node, root, values, cpl, itemId);
	}
}

void ItemModifier::materializeFloatVectorIndex(TreeNode& root, ConstPayload& cpl, IdType itemId, int index) {
	if (indexData_.IsOldDataMaterialized(index)) {
		return;
	}

	const auto& floatVectorIndex = static_cast<const FloatVectorIndex&>(*ns_.indexes()[index]);
	auto vectors = ns_.getFloatVectorArray(itemId, cpl.GetFieldLen(index), floatVectorIndex);
	auto nodes = GetIndexData<h_vector<TreeNode*, 4>>(root, *ns_.indexes()[index], ns_.tagsMatcher(), cpl);
	const bool missingScalarVector = !floatVectorIndex.Opts().IsArray() && nodes.empty() && vectors.size() == 1 &&
									 vectors.front().Type().Is<KeyValueType::FloatVector>() &&
									 ConstFloatVectorView{vectors.front()}.IsEmpty();
	if (!missingScalarVector) {
		assertrx_throw(nodes.size() == vectors.size());
		for (size_t i = 0; i < nodes.size(); ++i) {
			TreeNode* node = nodes[i];
			assertrx_throw(node->IsType<ValueNode>());
			const Variant& value = node->GetValueNode().Value();
			assertrx_throw(value.Type().Is<KeyValueType::FloatVector>());
			if (ConstFloatVectorView{value}.IsStripped()) {
				node->GetValueNode().SetValue(vectors[i]);
			}
		}
	}
	VariantArray materialized;
	materialized.reserve(vectors.size());
	for (Variant& vector : vectors) {
		materialized.emplace_back(std::move(vector));
	}
	indexData_.SetMaterializedOldData(index, std::move(materialized));
}

void ItemModifier::materializeAffectedFloatVectorNodes(TreeNode& root, const FieldData& field, ConstPayload& cpl, IdType itemId) {
	const TagsPath updatePath = field.TagspathWithLastIndex().GetTagsPath();
	for (int index = 1; index < ns_.indexes().firstCompositePos(); ++index) {
		if (!ns_.indexes()[index]->IsFloatVector()) {
			continue;
		}
		for (const auto& jsonPath : ns_.payloadType().Field(index).JsonPaths()) {
			const TagsPath indexPath = ns_.tagsMatcher().path2tag(jsonPath);
			if (std::equal(updatePath.begin(), updatePath.begin() + std::min(updatePath.size(), indexPath.size()), indexPath.begin())) {
				materializeFloatVectorIndex(root, cpl, itemId, index);
				break;
			}
		}
	}
}

void ItemModifier::materializeChangedFloatVectorNodes(TreeNode& root, ConstPayload& cpl, IdType itemId) {
	for (int index = 1; index < ns_.indexes().firstCompositePos(); ++index) {
		if (indexData_.IsIndexChanged(index) && ns_.indexes()[index]->IsFloatVector()) {
			materializeFloatVectorIndex(root, cpl, itemId, index);
		}
	}
}

void ItemModifier::materializeChangedFloatVectorIndexes(ConstPayload& cpl, IdType itemId) {
	for (int index = 1; index < ns_.indexes().firstCompositePos(); ++index) {
		if (!indexData_.IsIndexChanged(index) || !ns_.indexes()[index]->IsFloatVector()) {
			continue;
		}
		if (indexData_.IsOldDataMaterialized(index)) {
			continue;
		}

		const auto& floatVectorIndex = static_cast<const FloatVectorIndex&>(*ns_.indexes()[index]);
		auto vectors = ns_.getFloatVectorArray(itemId, cpl.GetFieldLen(index), floatVectorIndex);
		VariantArray materialized;
		materialized.reserve(vectors.size());
		for (Variant& vector : vectors) {
			materialized.emplace_back(std::move(vector));
		}
		indexData_.SetMaterializedOldData(index, std::move(materialized));
	}
}

void ItemModifier::processFieldModeSetJson(TreeNode& root, FieldData& field, ConstPayload& cpl, const VariantArray& values, IdType itemId) {
	if (field.IsIndex()) {
		throw Error(errParams, "Cannot assign a JSON object to indexed field '{}'", field.Name());
	}

	UpdatePathCursor cursor{root, field.TagspathWithLastIndex(), ci_};
	std::vector<TreeNode*> nodes;
	for (TreeNode* node = cursor.Next(); node != nullptr; node = cursor.Next()) {
		nodes.emplace_back(node);
	}
	if (nodes.empty()) {
		throw Error(errParams, "Unable to update field '{}': node specified by the path was not found", field.Name());
	}
	materializeAffectedFloatVectorNodes(root, field, cpl, itemId);
	for (const TreeNode* node : nodes) {
		node->SaveOldIndexDataForSubtree(indexData_, cpl, root);
	}
	for (TreeNode* node : nodes) {
		if (values.IsArrayValue()) {
			if (node->NeedsTypeChange<ArrayNode>()) {
				node->ChangeToArray();
			}
			node->Clear();
			for (const auto& v : values) {
				TreeNode& arrayElement = node->AddObject(TagName::Empty());
				TagsPath path = field.TagspathWithLastIndex().GetTagsPath();
				JsonTreeLoader jsonLoader(arrayElement, root, cpl, ns_.indexRegistry_.GetTagsMatcher(), std::move(path), indexData_);
				jsonLoader.Load(std::string_view(v));
			}
		} else {
			if (node->NeedsTypeChange<ObjectNode>()) {
				node->ChangeToObject();
			}
			node->Clear();
			TagsPath path = field.TagspathWithLastIndex().GetTagsPath();
			JsonTreeLoader jsonLoader(*node, root, cpl, ns_.indexRegistry_.GetTagsMatcher(), std::move(path), indexData_);
			jsonLoader.Load(std::string_view(values.front()));
		}
		node->MarkArraysForNormalization();
	}
}

void ItemModifier::processFieldModeDrop(TreeNode& root, FieldData& field, ConstPayload& cpl, IdType itemId) {
	if (field.IsIndex()) {
		Index& index = *(ns_.indexes()[field.Index()]);
		if (!index.Opts().IsSparse() && field.Details().Mode() == FieldModeDrop) [[unlikely]] {	 // TODO #1218 allow to drop array fields
			throw Error(errLogic, "It's only possible to drop sparse or non-index fields via UPDATE statement!");
		}
	}

	MutableExistingPathCursor cursor{root, field.TagspathWithLastIndex()};
	std::vector<TreeNode*> nodes;
	for (TreeNode* node = cursor.Next(); node != nullptr; node = cursor.Next()) {
		nodes.emplace_back(node);
	}
	for (const TreeNode* n : nodes) {
		n->SaveOldIndexDataForSubtree(indexData_, cpl, root);
	}
	materializeChangedFloatVectorNodes(root, cpl, itemId);
	for (TreeNode* n : nodes) {
		if (n->Parent()->IsType<ObjectNode>()) {
			n->Parent()->Drop(TagName(n->ParentIndex()));
		} else if (n->Parent()->IsType<ArrayNode>()) {
			n->Parent()->Drop(n->ParentIndex());
		} else {
			assertrx_throw(false);
		}
	}
}

void ItemModifier::cjsonTreeModify(Payload& pl, IdType itemId, NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner,
								   StringsHolder& strHolder, UpdatesContainer& replUpdates, const NsContext& ctx) {
	functions::FunctionInvoker funcInvoker(precomputedValues_, ns_, ns_.payloadType(), replUpdates);
	ExpressionEvaluator ev(ns_, funcInvoker);

	TreeNode& root = *creator_.CreateObject(nullptr, -1, ci_);
	VariantArray cjsonKref;
	pl.Get(0, cjsonKref);

	Variant cjsonV = cjsonKref.front();
	std::string_view cjson(cjsonV);

	ConstPayload cpl(ns_.payloadType(), ns_.items_[itemId]);
	CjsonTreeLoader cjsonLoader{};
	cjsonLoader.Load(cjson, cpl, root);
	TreeDataExtractor de(root, cpl, ns_.tagsMatcher(), ns_.indexes());

	for (FieldData& field : fieldsToModify_) {
		if (field.IsIndex() && field.Details().IsExpression() && embedderHelper_.TestSkipVectorIndex(field.Index())) {
			continue;
		}
		VariantArray values;
		// values must be assigned a value in if else below
		if (field.Details().IsExpression()) {
			assertrx(field.Details().Values().size() > 0);
			auto& expression = field.Expression(ns_);
			values = ev.Evaluate(expression.ast, de, field.Name(), ctx, expression.indexBindings);
		} else {
			values = field.Details().Values();
		}

		if (field.IsIndex() && field.IsUpdateArrayPart() && values.IsNullValue()) [[unlikely]] {
			const Index& index = *ns_.indexes()[field.Index()];
			if (index.IsFloatVector() && index.Opts().IsArray()) {
				assertrx_throw(values.size() == 1);
				values[0] = Variant{ConstFloatVectorView{}};
			}
		}
		switch (field.Details().Mode()) {
			case FieldModeSet:
				processFieldModeSet(root, field, cpl, values, itemId);
				break;
			case FieldModeSetJson:
				processFieldModeSetJson(root, field, cpl, values, itemId);
				break;
			case FieldModeDrop:
				processFieldModeDrop(root, field, cpl, itemId);
				break;
		}
	}

	const bool pkAffected = affectedCompositePK_ || (nonCompositePKIndex_ >= 0 && indexData_.IsIndexChanged(nonCompositePKIndex_));
	if (pkAffected) {
		saveVectorsBeforePkChange(itemId);
	}

	root.NormalizeMarkedArrays();

	materializeChangedFloatVectorIndexes(cpl, itemId);
	updateIndexes(root, cpl, itemId, indexesCacheCleaner, strHolder);

	updateEmbedding(itemId, ctx.rdxContext, root, cpl, indexesCacheCleaner, strHolder);

	deleteItemFromComposite(itemId, indexesCacheCleaner);
	auto compositesGuard = MakeScopeGuard([this, itemId, &indexesCacheCleaner] { insertItemIntoComposite(itemId, indexesCacheCleaner); });
	updatePayload(pl, root, itemId, indexesCacheCleaner, strHolder, cjsonV);
}

void ItemModifier::updateIndexes(TreeNode& root, ConstPayload& cpl, IdType itemId, NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner,
								 StringsHolder& strHolder) {
	for (int i = 0; i < ns_.indexes().totalSize(); i++) {
		if (indexData_.IsIndexChanged(i)) {
			auto newData = GetIndexData<IntermediateIndexesData::IndexNodeData>(root, *ns_.indexes()[i], ns_.tagsMatcher(), cpl);
			if (ns_.indexes()[i]->Type() == IndexRTree && newData.Vals.IsArrayValue() && !newData.Vals.empty() &&
				newData.Vals.size() != 2) {
				throw Error(errParams, "Can't convert array of {} elements to Point", newData.Vals.size());
			}
			indexData_.SetNewIndexData(i, std::move(newData), ns_.indexes()[i].get());
			saveUpdatedFloatVector(i, indexData_.IndexDataValueNew(i));
			if (i < ns_.payloadType().NumFields()) {
				const auto& vectorIndex = embedderHelper_.GetVectorIndexForAutoEmbeddingField(i);
				if (!vectorIndex.empty()) {
					VariantArray fldData;
					cpl.Get(i, fldData);
					if (fldData != indexData_.IndexDataValueNew(i)) {
						embedderHelper_.AddVectorIndexToModify(vectorIndex);
					}
				}
			}
			VariantArray& keys = indexData_.IndexDataValue(i);
			if (ns_.indexes()[i]->Opts().IsSparse()) {
				for (auto& v : keys) {
					std::ignore = v.convert(ns_.indexes()[i]->KeyType());
				}
			}

			VariantArray res;
			updateIndex(itemId, i, keys, indexData_.IndexDataValueNew(i), res, indexesCacheCleaner, strHolder);
			if (indexData_.IsNewDataNode(i)) {
				auto& nodes = indexData_.IndexDataNodeNew(i);
				assertrx(nodes.size() == res.size());
				for (size_t k = 0; k < nodes.size(); k++) {
					nodes[k]->GetValueNode().SetValue(res[k]);
				}
			} else {
				assertrx(indexData_.IndexDataValueNew(i).size() == res.size());
				indexData_.SetUpsertedIndexData(i, std::move(res));
			}
		}
	}
}

void ItemModifier::updatePayload(Payload& pl, TreeNode& root, IdType itemId, NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner,
								 StringsHolder& strHolder, Variant cjsonV) {
	wrser_.Reset();
	PayloadValue newPayloadValue = *pl.Value();
	newPayloadValue.Clone();
	Payload newPayload(ns_.payloadType(), newPayloadValue);
	newPayload.Reset();
	root.GetCJSON(wrser_, newPayload, TagName::Empty(), rollBackIndexData_.GetPayloadValueBackup(), indexData_, arrayBuf_);
	Index& index = *(ns_.indexes()[0]);
	bool clearCache = false;
	Variant key{wrser_.Slice()};
	indexData_.SetTupleData(std::move(cjsonV), key);

	index.Delete(indexData_.IndexDataValue(0), itemId, reindexer::MustExist_True, strHolder, clearCache);
	rollBackIndexData_.SetOldValueDeleted(0, IsPk_False);
	if (clearCache) {
		indexesCacheCleaner.Add(index);
	}

	clearCache = false;
	Variant res = index.Upsert(indexData_.IndexDataValueNew(0).front(), itemId, clearCache);
	rollBackIndexData_.SetNewValueInserted(0, IsPk_False);
	if (clearCache) {
		indexesCacheCleaner.Add(index);
	}

	newPayload.Set(0, res);
	ns_.items_[itemId] = std::move(newPayloadValue);
}

bool ItemModifier::Modify(IdType itemId, const NsContext& ctx, UpdatesContainer& replUpdates) {
	creator_.Reset();
	indexData_.Reset();
	PayloadValue& pv = ns_.items_[itemId];
	vectorsBeforePkChange_.clear();
	Payload pl(ns_.payloadType(), pv);
	pv.Clone(pl.RealSize());

	rollBackIndexData_.Reset(pv);
	auto strHolder = ns_.strHolder();

	RollBack_ModifiedPayload rollBack = RollBack_ModifiedPayload(*this, itemId);

	auto indexesCacheCleaner = ns_.GetIndexesCacheCleaner();

	try {
		bool isSimple = isPayloadModify(pl);
		if (isSimple) {
			directPayloadModify(pl, itemId, indexesCacheCleaner, *strHolder);
		} else {
			cjsonTreeModify(pl, itemId, indexesCacheCleaner, *strHolder, replUpdates, ctx);
		}
	} catch (const DuplicatedItemIDError& err) {
		assertrx_throw(err.ID() < IdType::Max().ToNumber());
		IdType conflictingItemId = IdType::FromNumber(err.ID());
		PayloadValue& conflictingItemPV = ns_.items_[conflictingItemId];
		ns_.throwDuplicatePK(ConstPayload(ns_.payloadType(), conflictingItemPV), itemId, conflictingItemId);
	}
	rollBack.Disable();
	ns_.markUpdated(IndexOptimization::Partial, ctx);
	return rollBackIndexData_.IsPkModified();
}

void ItemModifier::saveVectorsBeforePkChange(IdType itemId) {
	// Quantized vectors may need to be read from storage under the old primary key.
	const PayloadValue& pv = ns_.items_[itemId];
	vectorsBeforePkChange_ = ns_.floatVectorsGetterFn(itemId)(ns_.payloadType(), pv, ns_.tagsMatcher());
}

void ItemModifier::saveUpdatedFloatVector(int index, const VariantArray& values) {
	if (vectorsBeforePkChange_.empty() || !ns_.indexes()[index]->IsFloatVector()) {
		return;
	}
	for (auto& [indexData, vectors] : vectorsBeforePkChange_) {
		if (indexData.ptField != size_t(index)) {
			continue;
		}
		vectors.clear();
		vectors.reserve(values.size());
		for (const auto& value : values) {
			vectors.emplace_back(FloatVector{value.As<ConstFloatVectorView>()});
		}
		break;
	}
}

void ItemModifier::deleteItemFromComposite(IdType itemId, auto& indexesCacheCleaner) {
	auto strHolder = ns_.strHolder();
	const auto firstCompositePos = ns_.indexes().firstCompositePos();
	const auto totalIndexes = firstCompositePos + ns_.indexes().compositeIndexesSize();
	for (int i = firstCompositePos; i < totalIndexes; ++i) {
		if (affectedComposites_[i - firstCompositePos]) {
			bool needClearCache{false};
			const auto& compositeIdx = ns_.indexes()[i];
			compositeIdx->Delete(Variant(ns_.items_[itemId]), itemId, MustExist_True, *strHolder, needClearCache);
			rollBackIndexData_.SetOldValueDeleted(i, compositeIdx->Opts().IsPK());
			if (needClearCache) {
				indexesCacheCleaner.Add(*compositeIdx);
			}
		}
	}
}

void ItemModifier::insertItemIntoComposite(IdType itemId, auto& indexesCacheCleaner) {
	const auto totalIndexes = ns_.indexes().totalSize();
	const auto firstCompositePos = ns_.indexes().firstCompositePos();
	for (int i = firstCompositePos; i < totalIndexes; ++i) {
		auto& compositeIdx = *ns_.indexes()[i];
		if (affectedComposites_[i - firstCompositePos]) {
			bool needClearCache{false};
			std::ignore = compositeIdx.Upsert(Variant(ns_.items_[itemId]), itemId, needClearCache);
			rollBackIndexData_.SetNewValueInserted(i, compositeIdx.Opts().IsPK());
			if (needClearCache) {
				indexesCacheCleaner.Add(compositeIdx);
			}
		} else {
			bool refreshed = compositeIdx.RefreshCompositeKey(Variant(ns_.items_[itemId]), itemId);
			assertrx_dbg(refreshed);
			if (!refreshed) [[unlikely]] {
				logFmt(LogError, "[{}]: Unable to refresh key for {} during update query", ns_.name_, compositeIdx.Name());
			}
		}
	}
}

void ItemModifier::updateIndex(IdType itemId, int index, const VariantArray& oldKeys, const VariantArray& newKeys, VariantArray& res,
							   NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner, StringsHolder& strHolder) {
	Index& idx = *ns_.indexes()[index];
	if (idx.Opts().GetCollateMode() == CollateUTF8) {
		for (const Variant& key : newKeys) {
			key.EnsureUTF8();
		}
	}
	VariantArray insertedKeys{newKeys};
	bool clearCache = false;
	idx.Delete(oldKeys, itemId, MustExist_False, strHolder, clearCache);
	rollBackIndexData_.SetOldValueDeleted(index, idx.Opts().IsPK());
	if (clearCache) {
		indexesCacheCleaner.Add(idx);
	}

	clearCache = false;
	idx.Upsert(res, newKeys, itemId, clearCache);
	rollBackIndexData_.SetNewValueInserted(index, idx.Opts().IsPK(), std::move(insertedKeys));
	if (clearCache) {
		indexesCacheCleaner.Add(idx);
	}
}

void ItemModifier::updateEmbedding(IdType itemId, const RdxContext& rdxContext, TreeNode& root, ConstPayload& oldPayload,
								   NamespaceImpl::IndexesCacheCleaner& indexesCacheCleaner, StringsHolder& strHolder) {
	std::vector<std::pair<std::string, VariantArray>> source;

	const auto& vectorIndexToModify = embedderHelper_.GetVectorIndexesToModify();

	for (auto index : vectorIndexToModify) {
		if (embedderHelper_.TestSkipVectorIndex(index)) {
			continue;
		}
		const auto& embedder = *ns_.payloadType().Field(index).UpsertEmbedder();

		source.resize(0);

		for (const auto& fld : embedder.Fields()) {
			int fieldIndex;
			VariantArray data;
			if (ns_.tryGetIndexByName(fld, fieldIndex)) {
				const Index& sourceIndex = *ns_.indexes()[fieldIndex];
				data = GetIndexData<VariantArray>(root, sourceIndex, ns_.tagsMatcher(), oldPayload);

				if (data.empty() && !sourceIndex.Opts().IsArray() && !sourceIndex.Opts().IsSparse()) {
					data.emplace_back(PayloadFieldValue::GetDefaultValue(sourceIndex.KeyType()));
				}
			} else {
				data = GetDataByPath(fld, root, ns_.tagsMatcher());
			}
			source.emplace_back(fld, std::move(data));
		}

		// ToDo in real life, work with several embedded devices requires asynchrony. Now we support only one
		h_vector<FloatVector, 1> products;
		embedder.Calculate(rdxContext, std::span{&source, 1}, products);
		if (products.size() != 1 && !ns_.payloadType().Field(index).IsArray()) {
			throw Error(errLogic, "Unable to set vector values with incorrect embedding result");
		}

		VariantArray keys;
		keys.reserve(products.size());
		for (auto& p : products) {
			keys.emplace_back(std::move(p));
		}
		products.clear<false>();

		materializeFloatVectorIndex(root, oldPayload, itemId, index);

		VariantArray oldKeys;
		oldPayload.Get(index, oldKeys);
		VariantArray savedOldKeys = oldKeys;
		indexData_.SaveOldIndexData(index, std::move(savedOldKeys));
		IntermediateIndexesData::IndexNodeData newData;
		newData.Vals = keys;
		indexData_.SetNewIndexData(index, std::move(newData), ns_.indexes()[index].get());
		saveUpdatedFloatVector(index, keys);

		VariantArray res;
		updateIndex(itemId, index, oldKeys, keys, res, indexesCacheCleaner, strHolder);
		TreeNode* node = GetRootVectorIndexNode(root, *ns_.indexes()[index], index, ns_.tagsMatcher());
		if (ns_.payloadType().Field(index).IsArray()) {
			if (res.empty()) {
				node->Clear();
				node->SetIndex(index);
			} else {
				node->Clear();
				node->SetIndex(0);
				for (auto& r : res) {
					std::ignore = node->AddValue(TagName::Empty(), std::move(r), index);
				}
			}
		} else {
			node->SetIndex(index);
			if (node->IsType<ValueNode>()) {
				node->GetValueNode().SetValue(res[0]);
			} else {
				throw Error(errLogic, "incorrect node type");
			}
		}
	}
}

void IndexRollBack::Reset(const PayloadValue& pv) {
	pvSave_ = pv;
	pvSave_.Clone();
	std::fill(data_.begin(), data_.end(), IndexUpdateState::Unchanged);
	for (VariantArray& keys : insertedKeys_) {
		keys.clear();
	}
	pkModified_ = false;
}

}  // namespace reindexer
