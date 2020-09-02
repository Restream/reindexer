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

	Transaction(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf);
	Transaction(const Error &err);
	~Transaction();
	Transaction() = default;
	Transaction(Transaction &&) noexcept;
	Transaction &operator=(Transaction &&) noexcept;

	void Insert(Item &&item);
	void Update(Item &&item);
	void Upsert(Item &&item);
	void Delete(Item &&item);
	void Modify(Item &&item, ItemModifyMode mode);
	void Modify(Query &&query);
	bool IsFree() { return impl_ == nullptr; }
	Item NewItem();
	Item GetItem(TransactionStep &&st);
	Error Status() { return status_; }

	const std::string &GetName();

	friend class ReindexerImpl;

	vector<TransactionStep> &GetSteps();
	const vector<TransactionStep> &GetSteps() const;
	bool IsTagsUpdated() const;
	time_point GetStartTime() const;

protected:
	std::unique_ptr<TransactionImpl> impl_;
	Error status_;
};

}  // namespace reindexer
