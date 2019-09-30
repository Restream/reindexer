#include "transaction.h"
#include "transactionimpl.h"
namespace reindexer {

Transaction::Transaction(const string &nsName, ReindexerImpl *rx, Completion cmpl) : impl_(nullptr) {
	try {
		impl_.reset(new TransactionImpl(nsName, rx, cmpl));
	} catch (const Error &err) {
		status_ = err;
	}
}

Transaction::~Transaction() = default;
Transaction::Transaction(Transaction &&) = default;
Transaction &Transaction::operator=(Transaction &&) = default;

const string &Transaction::GetName() {
	static std::string empty;
	if (impl_)
		return impl_->nsName_;
	else
		return empty;
}

void Transaction::Insert(Item &&item) {
	if (impl_) impl_->Insert(move(item));
}
void Transaction::Update(Item &&item) {
	if (impl_) impl_->Update(move(item));
}
void Transaction::Upsert(Item &&item) {
	if (impl_) impl_->Upsert(move(item));
}
void Transaction::Delete(Item &&item) {
	if (impl_) impl_->Delete(move(item));
}
void Transaction::Modify(Item &&item, ItemModifyMode mode) {
	if (impl_) impl_->Modify(move(item), mode);
}

void Transaction::Modify(Query &&query) {
	if (impl_) impl_->Modify(move(query));
}

Item Transaction::NewItem() { return impl_->NewItem(); }

Completion Transaction::GetCmpl() { return impl_->cmpl_; }

vector<TransactionStep> &Transaction::GetSteps() { return impl_->steps_; }

}  // namespace reindexer
