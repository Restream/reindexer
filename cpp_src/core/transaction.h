#pragma once
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/query/queryresults.h"

namespace reindexer {

class TransactionImpl;
class ReindexerImpl;
class RdxContext;
class TransactionStep;

class Transaction {
public:
	/// Completion routine
	typedef std::function<void(const Error &err)> Completion;

	Transaction(const string &nsName, ReindexerImpl *rx, Completion cmpl = nullptr);
	~Transaction();
	Transaction() = default;
	Transaction(Transaction &&);
	Transaction &operator=(Transaction &&);

	void Insert(Item &&item);
	void Update(Item &&item);
	void Upsert(Item &&item);
	void Delete(Item &&item);
	void Modify(Item &&item, ItemModifyMode mode);
	void Modify(Query &&query);
	bool IsFree() { return impl_ == nullptr; }
	Item NewItem();
	Error Status() { return status_; }

	const std::string &GetName();

	friend class ReindexerImpl;

	vector<TransactionStep> &GetSteps();
	Completion GetCmpl();

protected:
	std::unique_ptr<TransactionImpl> impl_;
	Error status_;
};

}  // namespace reindexer
