#include "transaction.h"
#include "transactionimpl.h"
namespace reindexer {

Transaction::Transaction(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf,
						 std::shared_ptr<const Schema> schema, lsn_t lsn)
	: impl_(new TransactionImpl(nsName, pt, tm, pf, schema, lsn)) {}

Transaction::Transaction(const Error &err) : status_(err) {}

Transaction::~Transaction() = default;
Transaction::Transaction(Transaction &&) noexcept = default;
Transaction &Transaction::operator=(Transaction &&) noexcept = default;

const string &Transaction::GetName() {
	static std::string empty;
	if (impl_)
		return impl_->nsName_;
	else
		return empty;
}

void Transaction::Insert(Item &&item, lsn_t lsn) {
	if (impl_) impl_->Insert(move(item), lsn);
}
void Transaction::Update(Item &&item, lsn_t lsn) {
	if (impl_) impl_->Update(move(item), lsn);
}
void Transaction::Upsert(Item &&item, lsn_t lsn) {
	if (impl_) impl_->Upsert(move(item), lsn);
}
void Transaction::Delete(Item &&item, lsn_t lsn) {
	if (impl_) impl_->Delete(move(item), lsn);
}
void Transaction::Modify(Item &&item, ItemModifyMode mode, lsn_t lsn) {
	if (impl_) impl_->Modify(move(item), mode, lsn);
}
void Transaction::Modify(Query &&query, lsn_t lsn) {
	if (impl_) impl_->Modify(move(query), lsn);
}
void Transaction::Nop(lsn_t lsn) {
	if (impl_) impl_->Nop(lsn);
}

Item Transaction::NewItem() { return impl_->NewItem(); }

vector<TransactionStep> &Transaction::GetSteps() noexcept {
	assert(impl_);
	return impl_->steps_;
}

const vector<TransactionStep> &Transaction::GetSteps() const noexcept {
	assert(impl_);
	return impl_->steps_;
}

Item Transaction::GetItem(TransactionStep &&st) {
	assert(impl_);
	return impl_->GetItem(std::move(st));
}

lsn_t Transaction::GetLSN() const noexcept {
	assert(impl_);
	return impl_->GetLSN();
}

bool Transaction::IsTagsUpdated() const noexcept {
	assert(impl_);
	return impl_->tagsUpdated_;
}

Transaction::time_point Transaction::GetStartTime() const noexcept {
	assert(impl_);
	return impl_->startTime_;
}

}  // namespace reindexer
