#include "indexing_condition.h"

#include <algorithm>
#include <type_traits>
#include "core/definitions/indexdef.h"
#include "core/formatters/tokenizer_range.h"
#include "core/function/function.h"
#include "core/index/index.h"
#include "core/namespace/indexes/target_state.h"
#include "core/payload/payloadiface.h"
#include "core/query/query.h"
#include "core/query/query_impl.h"
#include "core/type_consts.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace {

void validateIndexingConditionQuery(ConstQueryImpl query) {
	if (query.HasLimit()) {
		throw Error(errParams, "indexing_condition must not contain LIMIT");
	}
	if (query.HasOffset()) {
		throw Error(errParams, "indexing_condition must not contain OFFSET");
	}
	if (!query.GetSortingEntries().empty()) {
		throw Error(errParams, "indexing_condition must not contain ORDER BY");
	}
	if (!query.JoinQueries().empty()) {
		throw Error(errParams, "indexing_condition must not contain JOIN");
	}
	if (!query.SubQueries().empty()) {
		throw Error(errParams, "indexing_condition must not contain SUBQUERY");
	}
	if (!query.MergeQueries().empty()) {
		throw Error(errParams, "indexing_condition must not contain MERGE");
	}
	const auto& entries = query.Entries();
	if (!entries.equalPositions.empty()) {
		throw Error(errParams, "indexing_condition must not contain EQUAL_POSITION");
	}
	for (size_t i = 0; i < entries.Size(); ++i) {
		if (entries.IsSubTree(i) && !entries.Get<QueryEntriesBracket>(i).equalPositions.empty()) {
			throw Error(errParams, "indexing_condition must not contain EQUAL_POSITION");
		}
	}
}

template <typename State>
TagsPath resolvePath(State& st, std::string_view path) {
	if constexpr (std::is_const_v<State>) {
		return st.GetTagsMatcher().path2tag(path);
	} else {
		return st.GetTagsMatcher().path2tag(path, CanAddField_True);
	}
}

void verifyIndexingConditionField(const QueryField& qField, const ns_indexes::TargetState& st) {
	if (qField.IsFieldIndexed()) {
		const auto& index = st.At(qField.IndexNo());
		if (index.IsFulltext()) {
			throw Error(errParams, "indexing_condition must not reference fulltext index '{}'", qField.FieldName());
		}
		if (IsGeospatial(index.Type())) {
			throw Error(errParams, "indexing_condition must not reference geospatial index '{}'", qField.FieldName());
		}
		if (index.IsFloatVector()) {
			throw Error(errParams, "indexing_condition must not reference float vector index '{}'", qField.FieldName());
		}
	}
}

void setQueryIndex(QueryField& qField, int idxNo, const ns_indexes::TargetState& st) {
	const Index& idx = st.At(idxNo);
	QueryField::CompositeTypesVecT compositeFieldsTypes;
	if (idxNo >= st.FirstCompositePos()) {
		const auto& fields = st.FieldsAt(idxNo);
		compositeFieldsTypes.reserve(fields.size());
		for (const auto f : fields) {
			if (f != IndexValueType::SetByJsonPath) {
				assertrx_throw(f < st.FirstCompositePos());
				compositeFieldsTypes.emplace_back(st.At(f).SelectKeyType());
			} else {
				compositeFieldsTypes.emplace_back(KeyValueType::String{});
			}
		}
	}
	qField.SetIndexData(idxNo, idx.Name(), FieldsSet(st.FieldsAt(idxNo)), idx.KeyType(), idx.SelectKeyType(),
						std::move(compositeFieldsTypes));
}

void setQueryField(QueryField& qField, auto& st) {
	int idxNo = IndexValueType::SetByJsonPath;
	if (st.TryGetIndexByNameOrJsonPath(qField.FieldName(), idxNo)) {
		setQueryIndex(qField, idxNo, st);
	} else {
		qField.SetField({resolvePath(st, qField.FieldName())});
	}
}

[[noreturn]] void rejectIndexingConditionFunction(const QueryFunctionEntry& entry) {
	throw Error(errParams, "Invalid indexing_condition: Function '{}' is not supported", entry.Function().Name());
}

[[noreturn]] void rejectIndexingConditionArithmetic(const QueryArithmeticEntry&) {
	throw Error(errParams, "Invalid indexing_condition: arithmetic expressions are not supported");
}

void validateAndBindEntriesRange(QueryEntries::iterator begin, QueryEntries::iterator end, auto& st) {
	for (auto it = begin; it != end; ++it) {
		it->Visit([](const JoinQueryEntry&) { throw_as_assert; },
				  [&it, &st](QueryEntriesBracket&) { validateAndBindEntriesRange(it.begin(), it.end(), st); },
				  [&st](QueryEntry& qe) {
					  setQueryField(qe.FieldData(), st);
					  verifyIndexingConditionField(qe.FieldData(), st);
					  IndexingCondition::VerifyQueryEntry(qe, qe.IsFieldIndexed() ? &st.At(qe.IndexNo()) : nullptr, st.GetPayloadType());
				  },
				  [](const QueryFunctionEntry& entry) { rejectIndexingConditionFunction(entry); },
				  [](const QueryArithmeticEntry& entry) { rejectIndexingConditionArithmetic(entry); },
				  [&st](BetweenFieldsQueryEntry& qe) {
					  setQueryField(qe.LeftFieldData(), st);
					  setQueryField(qe.RightFieldData(), st);
					  verifyIndexingConditionField(qe.LeftFieldData(), st);
					  verifyIndexingConditionField(qe.RightFieldData(), st);
				  },
				  [](const AlwaysFalse&) {}, [](const AlwaysTrue&) {}, [](const MultiDistinctQueryEntry&) { throw_as_assert; },
				  [](const KnnQueryEntry&) { throw Error(errParams, "indexing_condition must not contain KNN conditions"); },
				  [](const AnySubQueryEntry auto&) { throw_as_assert; });
	}
}

void visitQueryEntries(QueryEntries::const_iterator begin, QueryEntries::const_iterator end, const auto& visitor) {
	for (auto it = begin; it != end; ++it) {
		it->Visit(
			[&](const JoinQueryEntry&) { throw Error(errParams, "indexing_condition must not contain join conditions"); },
			[&it, &visitor](const QueryEntriesBracket&) { visitQueryEntries(it.cbegin(), it.cend(), visitor); },
			[&](const QueryEntry& qe) { visitor(qe.FieldData()); },
			[](const QueryFunctionEntry& entry) { rejectIndexingConditionFunction(entry); },
			[](const QueryArithmeticEntry& entry) { rejectIndexingConditionArithmetic(entry); },
			[&](const BetweenFieldsQueryEntry& qe) {
				visitor(qe.LeftFieldData());
				visitor(qe.RightFieldData());
			},
			[&](const AlwaysFalse&) {}, [&](const AlwaysTrue&) {},
			[&](const MultiDistinctQueryEntry&) { throw Error(errParams, "indexing_condition must not contain distinct conditions"); },
			[&](const KnnQueryEntry&) { throw Error(errParams, "indexing_condition must not contain KNN conditions"); },
			[&](const AnySubQueryEntry auto&) { throw Error(errParams, "indexing_condition must not contain subqueries"); });
	}
}

std::vector<CollateOpts> collectCollateOpts(const QueryEntries& entries, const ns_indexes::TargetState& st) {
	std::vector<CollateOpts> collateOpts(entries.Size());
	for (size_t i = 0; i < entries.Size(); ++i) {
		const QueryField* field = nullptr;
		if (entries.Is<QueryEntry>(i)) {
			field = &entries.Get<QueryEntry>(i).FieldData();
		} else if (entries.Is<BetweenFieldsQueryEntry>(i)) {
			field = &entries.Get<BetweenFieldsQueryEntry>(i).LeftFieldData();
		}
		if (field && field->IsFieldIndexed()) {
			collateOpts[i] = st.At(field->IndexNo()).Opts().collateOpts_;
		}
	}
	return collateOpts;
}

void appendUniqueFieldName(std::vector<std::string>& out, std::string_view name) {
	if (name.empty()) {
		return;
	}
	const auto it = std::find_if(out.begin(), out.end(), [name](const std::string& f) { return iequals(f, name); });
	if (it == out.end()) {
		out.emplace_back(name);
	}
}

// The condition is parsed as `select * from _ where <sql>`, so reported columns do not refer to the condition text.
// FIXME: #2759 Refactor SqlParserError generation: we should be able to get original text without line/column right from the error object
std::string_view withoutSqlLocation(const SqlParserError& err) {
	std::string_view message = err.whatStr();
	for (const auto format : {", {}", "; {}", ": {}", " in query '{}'"}) {
		const auto suffix = fmt::format(fmt::runtime(format), err.Range());
		if (message.ends_with(suffix)) {
			message.remove_suffix(suffix.size());
			return message;
		}
	}
	if (message.ends_with(fmt::format("  location = {})", err.Range()))) {
		if (const auto pos = message.find(" (text = '"); pos != std::string_view::npos) {
			message = message.substr(0, pos);
		}
	}
	return message;
}

Expected<QueryEntries> parseIndexingConditionEntries(std::string_view sql) noexcept try {
	if (sql.empty()) {
		return QueryEntries{};
	}

	Query query;
	try {
		query = Query::FromSQL(std::string("select * from _ where ") + std::string(sql));
	} catch (const SqlParserError& err) {
		return Unexpected{Error{errParams, "Invalid indexing_condition: {}", withoutSqlLocation(err)}};
	} catch (const std::exception& err) {
		return Unexpected{Error{errParams, "Invalid indexing_condition: {}", err.what()}};
	}

	const ConstQueryImpl queryImpl{query};
	if (queryImpl.Entries().Size() == 0) {
		return Unexpected{Error{errParams, "indexing_condition must not be empty"}};
	}

	validateIndexingConditionQuery(queryImpl);
	return QueryEntries(queryImpl.Entries());
} catch (const std::exception& err) {
	return Unexpected{Error{err}};
} catch (...) {
	return Unexpected{Error{errAssert, "Unexpected exception"}};
}

}  // namespace

template <typename State>
Expected<IndexingCondition> IndexingCondition::parse(std::string_view sql, State& state) noexcept try {
	static_assert(std::is_same_v<std::remove_const_t<State>, ns_indexes::TargetState>);
	auto entries = parseIndexingConditionEntries(sql);
	if (!entries.has_value()) {
		return Unexpected{entries.error()};
	}
	if (entries->Size() == 0) {
		return IndexingCondition{};
	}
	IndexingCondition result;
	result.entries_ = std::move(*entries);
	validateAndBindEntriesRange(result.entries_.begin(), result.entries_.end(), state);
	result.collateOpts_ = collectCollateOpts(result.entries_, state);
	result.collectReferences(state.GetPayloadType(), state.GetTagsMatcher());
	return result;
} catch (const std::exception& err) {
	return Unexpected{Error{err}};
} catch (...) {
	return Unexpected{Error{errAssert, "Unexpected exception"}};
}

Expected<IndexingCondition> IndexingCondition::Parse(std::string_view sql, const ns_indexes::Registry& registry) noexcept try {
	// An unchanged TargetState exposes the current schema through the same interface used by DDL.
	// Keep it const so validation cannot register tags in this temporary copy.
	const ns_indexes::TargetState state{registry};
	return parse(sql, state);
} catch (const std::exception& err) {
	return Unexpected{Error{err}};
} catch (...) {
	return Unexpected{Error{errAssert, "Unexpected exception"}};
}

Expected<IndexingCondition> IndexingCondition::Parse(std::string_view sql, ns_indexes::TargetState& st) noexcept { return parse(sql, st); }

void IndexingCondition::VerifyIndexChange(std::string_view sql, ns_indexes::TargetState& st, const IndexDef& indexDef,
										  const IndexDef* previous) {
	auto entries = parseIndexingConditionEntries(sql);
	if (!entries.has_value()) {
		throw entries.error();
	}
	int changedPos = 0;
	[[maybe_unused]] const bool found = st.TryGetIndexByName(indexDef.Name(), changedPos);
	assertrx(found);
	struct [[nodiscard]] Context {
		const ns_indexes::TargetState& state;
		int changedPos;
		bool affected{false};
	} ctx{.state = st, .changedPos = changedPos};
	// Resolve the original SQL names before binding replaces aliases with index names.
	visitQueryEntries(entries->cbegin(), entries->cend(), [&ctx](const QueryField& field) {
		int pos = 0;
		if (ctx.state.TryGetIndexByNameOrJsonPath(field.FieldName(), pos) && pos == ctx.changedPos) {
			ctx.affected = true;
		}
	});

	IndexingCondition condition;
	condition.entries_ = std::move(*entries);
	validateAndBindEntriesRange(condition.entries_.begin(), condition.entries_.end(), st);
	condition.collectReferences(st.GetPayloadType(), st.GetTagsMatcher());

	if (previous &&
		!previous->Compare(indexDef).AnyOfIsDifferent(IndexDef::Diff::JsonPaths, IndexDef::Diff::FieldType,
													  IndexOpts::OptsDiff::kIndexOptArray, IndexOpts::OptsDiff::kIndexOptSparse,
													  IndexOpts::ParamsDiff::CollateOpts) &&
		IsFullText(previous->IndexType()) == IsFullText(indexDef.IndexType()) &&
		IsGeospatial(previous->IndexType()) == IsGeospatial(indexDef.IndexType()) &&
		IsFloatVector(previous->IndexType()) == IsFloatVector(indexDef.IndexType())) {
		return;
	}
	const auto affectsPaths = [&condition, &st](const IndexDef& def) {
		// Composite indexes only read their components; rebuilding them does not change the component fields.
		if (IsComposite(def.IndexType())) {
			return false;
		}
		const auto& paths = def.JsonPaths();
		return std::any_of(paths.begin(), paths.end(), [&condition, &st](const std::string& path) {
			return condition.IsAffectedBy(IndexedTagsPath{st.GetTagsMatcher().path2tag(path)});
		});
	};
	if (ctx.affected || affectsPaths(indexDef) || (previous && affectsPaths(*previous))) {
		throw Error(errLogic, "the change may alter indexing_condition results; rebuilding dependent fulltext indexes is not supported");
	}
}

void IndexingCondition::VerifyQueryEntry(QueryEntry& entry, const Index* index, const PayloadType& pt) {
	if (entry.Condition() == CondDWithin) {
		throw Error(errParams, "indexing_condition must not contain ST_DWithin conditions");
	}
	if (index && !index->Opts().IsSparse() && !index->Opts().IsArray()) {
		if (entry.Condition() == CondEmpty) {
			throw Error(errParams, "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes");
		}
		if (entry.Condition() == CondAny) {
			throw Error(errParams, "The 'NOT NULL' condition is supported only by 'sparse' or 'array' indexes");
		}
	}
	if (entry.SelectType().Is<KeyValueType::Composite>()) {
		// Payload evaluation compares tuples, not the composite index keys produced by conversion.
		auto validated = entry;
		validated.ConvertValuesToFieldType(pt);
	} else {
		entry.ConvertValuesToFieldType(pt);
	}
}

void IndexingCondition::collectReferences(const PayloadType& pt, const TagsMatcher& tm) {
	struct [[nodiscard]] Context {
		const PayloadType& payloadType;
		const TagsMatcher& tagsMatcher;
	} ctx{.payloadType = pt, .tagsMatcher = tm};
	visitQueryEntries(entries_.cbegin(), entries_.cend(), [this, &ctx](const QueryField& field) {
		appendUniqueFieldName(referencedFieldNames_, field.FieldName());
		const auto appendPath = [this](const auto& path) {
			if (!path.empty()) {
				referencedTagsPaths_.emplace_back(path);
			}
		};
		const auto& fields = field.Fields();
		for (const auto fieldNo : fields) {
			if (fieldNo == IndexValueType::SetByJsonPath) {
				continue;
			}
			for (const auto& jsonPath : ctx.payloadType.Field(fieldNo).JsonPaths()) {
				appendPath(ctx.tagsMatcher.path2tag(jsonPath));
			}
		}
		for (size_t i = 0; i < fields.getTagsPathsLength(); ++i) {
			if (fields.isTagsPathIndexed(i)) {
				appendPath(fields.getIndexedTagsPath(i));
			} else {
				appendPath(fields.getTagsPath(i));
			}
		}
	});
}

bool IndexingCondition::IsAffectedBy(const IndexedTagsPath& path) const noexcept {
	return !path.empty() && std::any_of(referencedTagsPaths_.begin(), referencedTagsPaths_.end(),
										[&path](const IndexedTagsPath& refPath) { return path.ComparePrefix(refPath); });
}

bool IndexingCondition::Evaluate(const ConstPayload& pl) const {
	if (Empty()) {
		return true;
	}
	return entries_.CheckIfSatisfyConditions(pl, collateOpts_);
}

bool IndexingCondition::UsesField(std::string_view fieldOrIndexName) const noexcept {
	return std::any_of(referencedFieldNames_.begin(), referencedFieldNames_.end(),
					   [fieldOrIndexName](const std::string& name) { return iequals(name, fieldOrIndexName); });
}

IndexingVerdicts::IndexingVerdicts(const ns_indexes::IndexesStorage& indexes, const ConstPayload& pl)
	: positions_{indexes.ConditionalIndexes()} {
	matches_.reserve(positions_.size());
	for (int pos : positions_) {
		matches_.emplace_back(indexes[pos]->EvaluateCondition(pl));
	}
}

bool IndexingVerdicts::HasCondition(int idx) const noexcept { return std::binary_search(positions_.begin(), positions_.end(), idx); }

MatchesCondition IndexingVerdicts::operator[](int idx) const noexcept {
	const auto it = std::lower_bound(positions_.begin(), positions_.end(), idx);
	return it != positions_.end() && *it == idx ? matches_[it - positions_.begin()] : MatchesCondition_True;
}

void ApplyIndexingConditionToIndex(Index& index, const IndexDef& indexDef, ns_indexes::TargetState& st) {
	if (indexDef.IndexingCondition().empty()) {
		return;
	}
	auto parsed = IndexingCondition::Parse(indexDef.IndexingCondition(), st);
	if (!parsed.has_value()) {
		const auto& err = parsed.error();
		throw Error(err.code(), "Cannot bind indexing_condition of fulltext index '{}' in namespace '{}': {}", indexDef.Name(),
					st.GetPayloadType().Name(), err.what());
	}
	index.SetIndexingCondition(std::move(*parsed), indexDef.IndexingCondition());
}

}  // namespace reindexer
