#include "transactionsteps.h"
#include "core/rdxcontext.h"

namespace reindexer {

void TransactionSteps::Modify(Item&& item, ItemModifyMode mode, lsn_t lsn) {
	switch (mode) {
		case ModeUpdate:
			break;
		case ModeInsert:
		case ModeUpsert:
			++expectedInsertionsCount_;
			break;
		case ModeDelete:
			++deletionsCount_;
			break;
	}
	if (lsn.isEmpty() && (mode == ModeUpdate || mode == ModeInsert || mode == ModeUpsert)) {
		RdxContext ctx;	 // ToDo need real value - now stub
		item.Embed(ctx);
	}
	steps.emplace_back(std::move(item), mode, lsn);
}

void TransactionSteps::Modify(Query&& query, lsn_t lsn) {
	if (!query.GetJoinQueries().empty()) {
		throw Error(errParams, "Query in transaction can not contain JOINs");
	}
	if (!query.GetMergeQueries().empty()) {
		throw Error(errParams, "Query in transaction can not contain MERGEs");
	}
	if (!query.GetSubQueries().empty()) {
		throw Error(errParams, "Query in transaction can not contain subqueries");
	}
	switch (query.Type()) {
		case QueryUpdate:
			updateQueriesCount_ += 1;
			break;
		case QueryDelete:
			deleteQueriesCount_ += 1;
			break;
		case QuerySelect:
			throw Error(errParams, "Transactions does not support SELECT queries");
		case QueryTruncate:
			throw Error(errParams, "Transactions does not support TRUNCATE queries");
		default:;
	}
	steps.emplace_back(std::move(query), lsn);
}

// NOLINTNEXTLINE(bugprone-exception-escape)
size_t TransactionSteps::CalculateNewCapacity(size_t currentSize) const noexcept {
	size_t newCapacity = currentSize;
	bool haveDeleteQuery = false;
	bool haveTruncateQuery = false;
	for (const auto& step : steps) {
		std::visit(overloaded{[](const concepts::OneOf<TransactionMetaStep, TransactionTmStep, TransactionNopStep> auto&) noexcept {},
							  [&](const TransactionItemStep& s) noexcept {
								  if (haveDeleteQuery) {
									  return;
								  }
								  switch (s.mode) {
									  case ModeInsert:
									  case ModeUpsert:
										  ++newCapacity;
										  break;
									  case ModeDelete:
										  if (newCapacity > 0) {
											  --newCapacity;
										  }
										  break;
									  case ModeUpdate:
										  break;
								  }
							  },
							  [&](const TransactionQueryStep& s) noexcept {
								  if (s.query) {
									  switch (s.query->Type()) {
										  case QueryDelete:
											  if (!haveTruncateQuery) {
												  newCapacity = currentSize;
											  }
											  haveDeleteQuery = true;
											  break;
										  case QueryTruncate:
											  newCapacity = 0;
											  haveDeleteQuery = false;
											  haveTruncateQuery = false;
											  break;
										  case QueryUpdate:
										  case QuerySelect:
											  break;
									  }
								  }
							  }},
				   step.data_);
	}
	return newCapacity;
}

}  // namespace reindexer
