#include "query_field_resolver.h"

#include "core/query/queryentry.h"
#include "core/type_consts_helpers.h"

namespace reindexer {

void QueryFieldResolver::BindIndex(QueryField& qField, int idxNo) const {
	const auto idx = metadata_.GetIndexMetadata(idxNo);
	const auto composites = metadata_.GetCompositeIndexesRange();
	QueryField::CompositeTypesVecT compositeFieldsTypes;
	if (idxNo >= composites.begin) {
#ifndef NDEBUG
		const bool ftIdx = IsFullText(idx.Type());
#endif
		const auto& fields = idx.Fields();
		compositeFieldsTypes.reserve(fields.size());
		for (const auto f : fields) {
			if (f != IndexValueType::SetByJsonPath) [[likely]] {
				assertrx_throw(f <= composites.begin);
				compositeFieldsTypes.emplace_back(metadata_.GetIndexMetadata(f).SelectKeyType());
			} else {
				// not indexed fields allowed only in ft composite indexes
				assertrx_throw(ftIdx);
				compositeFieldsTypes.emplace_back(KeyValueType::String{});
			}
		}
	}
	qField.SetIndexData(idxNo, idx.Name(), FieldsSet(idx.Fields()), idx.KeyType(), idx.SelectKeyType(), std::move(compositeFieldsTypes));
}

void QueryFieldResolver::Resolve(QueryField& qField) const {
	int idxNo = IndexValueType::SetByJsonPath;
	if (metadata_.TryGetIndexPosByNameOrJsonPath(qField.FieldName(), idxNo)) {
		BindIndex(qField, idxNo);
	} else {
		qField.SetField({metadata_.GetTagsPath(qField.FieldName())});
	}
}

void QueryFieldResolver::VerifyJoinField(const QueryField& qField, StrictMode strictMode) const {
	if (qField.IsFieldIndexed()) {
		if (metadata_.IsFloatVectorIndex(qField.IndexNo())) [[unlikely]] {
			throw Error(errParams, "Float vector indexes are not allowed in ON statement: {}", qField.FieldName());
		}
		if (metadata_.IsFulltextIndex(qField.IndexNo())) [[unlikely]] {
			throw Error(errParams, "Fulltext indexes are not allowed in ON statement: {}", qField.FieldName());
		}
		return;
	}
	if (strictMode == StrictModeIndexes) [[unlikely]] {
		throw Error(errStrictMode, "Current query strict mode allows using only indexed fields in ON statement: {}", qField.FieldName());
	} else if (strictMode == StrictModeNames) {
		if (metadata_.GetTagsPath(qField.FieldName()).empty()) [[unlikely]] {
			throw Error(errStrictMode, "Current query strict mode allows using only existing fields in ON statement: {}",
						qField.FieldName());
		}
	}
}

}  // namespace reindexer
