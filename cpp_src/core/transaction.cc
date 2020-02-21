#include "transaction.h"
#include "transactionimpl.h"
namespace reindexer {

Transaction::Transaction(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf)
	: impl_(new TransactionImpl(nsName, pt, tm, pf)) {}

Transaction::Transaction(const Error &err) : status_(err) {}

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

vector<TransactionStep> &Transaction::GetSteps() {
	assert(impl_);
	return impl_->steps_;
}

const vector<TransactionStep> &Transaction::GetSteps() const {
	assert(impl_);
	return impl_->steps_;
}

Item Transaction::GetItem(TransactionStep &&st) {
	assert(impl_);
	return impl_->GetItem(std::move(st));
}

bool Transaction::IsTagsUpdated() const {
	assert(impl_);
	return impl_->tagsUpdated_;
}

Transaction::time_point Transaction::GetStartTime() const {
	assert(impl_);
	return impl_->startTime_;
}

}  // namespace reindexer
