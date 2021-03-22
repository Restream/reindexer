#pragma once
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

class TransactionImpl;
class TransactionStep;
class PayloadType;
class TagsMatcher;
class FieldsSet;

class Transaction {
public:
	using time_point = std::chrono::time_point<std::chrono::high_resolution_clock>;

	Transaction(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf,
				std::shared_ptr<const Schema> schema, lsn_t lsn);
	Transaction(const Error &err);
	~Transaction();
	Transaction() = default;
	Transaction(Transaction &&) noexcept;
	Transaction &operator=(Transaction &&) noexcept;

	void Insert(Item &&item, lsn_t lsn = lsn_t());
	void Update(Item &&item, lsn_t lsn = lsn_t());
	void Upsert(Item &&item, lsn_t lsn = lsn_t());
	void Delete(Item &&item, lsn_t lsn = lsn_t());
	void Modify(Item &&item, ItemModifyMode mode, lsn_t lsn = lsn_t());
	void Modify(Query &&query, lsn_t lsn = lsn_t());
	void Nop(lsn_t lsn);
	bool IsFree() const noexcept { return impl_ == nullptr; }
	Item NewItem();
	Item GetItem(TransactionStep &&st);
	Error Status() const noexcept { return status_; }
	lsn_t GetLSN() const noexcept;

	const std::string &GetName();

	friend class ReindexerImpl;

	vector<TransactionStep> &GetSteps() noexcept;
	const vector<TransactionStep> &GetSteps() const noexcept;
	bool IsTagsUpdated() const noexcept;
	time_point GetStartTime() const noexcept;

protected:
	std::unique_ptr<TransactionImpl> impl_;
	Error status_;
};

}  // namespace reindexer
