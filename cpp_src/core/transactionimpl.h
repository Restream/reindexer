#pragma once
#include "transaction.h"

namespace reindexer {

using std::move;
using std::string;
using std::vector;
using Completion = Transaction::Completion;

class TransactionStep {
public:
	TransactionStep(Item &&item, ItemModifyMode status) : item_(move(item)), status_(status) {}

	TransactionStep(const TransactionStep &) = delete;
	TransactionStep &operator=(const TransactionStep &) = delete;
	TransactionStep(TransactionStep && /*rhs*/) noexcept = default;
	TransactionStep &operator=(TransactionStep && /*rhs*/) = default;

	Item item_;
	ItemModifyMode status_;
};

class TransactionAccessor : public Transaction {
public:
	TransactionAccessor(const string &nsName, ReindexerImpl *rx, Completion cmpl = nullptr) : Transaction(nsName, rx, cmpl) {}
	TransactionAccessor(const Transaction &) = delete;
	TransactionAccessor &operator=(const Transaction &) = delete;

	TransactionAccessor(TransactionAccessor &&rhs) noexcept;
	TransactionAccessor &operator=(TransactionAccessor &&) noexcept;

	const string &GetName();
	vector<TransactionStep> &GetSteps();
	Completion GetCmpl();
};

class TransactionImpl {
public:
	TransactionImpl(const string &nsName, ReindexerImpl *rx, Completion cmpl = nullptr) : nsName_(nsName), cmpl_(cmpl), rx_(rx) {}

	void Insert(Item &&item);
	void Update(Item &&item);
	void Upsert(Item &&item);
	void Delete(Item &&item);
	void Modify(Item &&item, ItemModifyMode mode);
	const string &GetName() { return nsName_; }

	void checkTagsMatcher(Item &item);

	vector<TransactionStep> steps_;
	string nsName_;
	Completion cmpl_;
	ReindexerImpl *rx_;
};

}  // namespace reindexer
