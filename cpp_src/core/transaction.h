#pragma once
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/query/queryresults.h"

namespace reindexer {

class TransactionImpl;

using std::string;
class ReindexerImpl;
class RdxContext;

class Transaction {
public:
	/// Completion routine
	typedef std::function<void(const Error &err)> Completion;
	Transaction() : impl_(nullptr) {}

	Transaction(const string &nsName, ReindexerImpl *rx, Completion cmpl = nullptr);
	~Transaction();
	Transaction(const Transaction &) = delete;
	Transaction &operator=(const Transaction &) = delete;

	Transaction(Transaction &&) noexcept;
	Transaction &operator=(Transaction &&) noexcept;

	void Insert(Item &&item);
	void Update(Item &&item);
	void Upsert(Item &&item);
	void Delete(Item &&item);
	void Modify(Item &&item, ItemModifyMode mode);
	bool IsFree() { return impl_ == nullptr; }
	Item NewItem();

	const string &GetName();

	friend class ReindexerImpl;

protected:
	string empty_;
	TransactionImpl *impl_;
};

}  // namespace reindexer
