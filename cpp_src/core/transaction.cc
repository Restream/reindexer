#include "transaction.h"
#include "transactionimpl.h"
namespace reindexer {

using std::string;
using std::move;

Transaction::~Transaction() {
	if (impl_) {
		delete impl_;
	}
}
Transaction::Transaction(const string &nsName, ReindexerImpl *rx, Completion cmpl) : impl_(new TransactionImpl(nsName, rx, cmpl)) {}

Transaction::Transaction(Transaction &&rhs) noexcept {
	impl_ = rhs.impl_;
	rhs.impl_ = nullptr;
}

Transaction &Transaction::operator=(Transaction &&rhs) noexcept {
	if (&rhs != this) {
		if (impl_) delete impl_;
		impl_ = rhs.impl_;
		rhs.impl_ = nullptr;
	}
	return *this;
}

const string &Transaction::GetName() {
	if (impl_)
		return impl_->nsName_;
	else
		return empty_;
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

Item Transaction::NewItem() { return impl_->NewItem(); }

}  // namespace reindexer
