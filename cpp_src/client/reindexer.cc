#include "client/reindexer.h"
#include "client/rpcclient.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

Reindexer::Reindexer(const ReindexerConfig& config) : impl_(new RPCClient(config)), owner_(true), ctx_() {}
Reindexer::~Reindexer() {
	if (owner_) {
		delete impl_;
	}
}
Reindexer::Reindexer(Reindexer&& rdx) noexcept : impl_(rdx.impl_), owner_(rdx.owner_), ctx_(std::move(rdx.ctx_)) { rdx.owner_ = false; }
Reindexer& Reindexer::operator=(Reindexer&& rdx) noexcept {
	if (this != &rdx) {
		if (owner_) {
			delete impl_;
		}
		impl_ = rdx.impl_;
		owner_ = rdx.owner_;
		ctx_ = rdx.ctx_;
		rdx.owner_ = false;
	}
	return *this;
}

Error Reindexer::Connect(const std::string& dsn, const client::ConnectOpts& opts) { return impl_->Connect(dsn, opts); }
Error Reindexer::Connect(const std::vector<std::pair<std::string, client::ConnectOpts>>& connectData) {
	return impl_->Connect(connectData);
}
Error Reindexer::Stop() { return impl_->Stop(); }
Error Reindexer::AddNamespace(const NamespaceDef& nsDef) { return impl_->AddNamespace(nsDef, ctx_); }
Error Reindexer::OpenNamespace(std::string_view nsName, const StorageOpts& storage) { return impl_->OpenNamespace(nsName, ctx_, storage); }
Error Reindexer::DropNamespace(std::string_view nsName) { return impl_->DropNamespace(nsName, ctx_); }
Error Reindexer::CloseNamespace(std::string_view nsName) { return impl_->CloseNamespace(nsName, ctx_); }
Error Reindexer::TruncateNamespace(std::string_view nsName) { return impl_->TruncateNamespace(nsName, ctx_); }
Error Reindexer::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName) {
	return impl_->RenameNamespace(srcNsName, dstNsName, ctx_);
}
Error Reindexer::Insert(std::string_view nsName, Item& item) { return impl_->Insert(nsName, item, ctx_); }
Error Reindexer::Update(std::string_view nsName, Item& item) { return impl_->Update(nsName, item, ctx_); }
Error Reindexer::Update(const Query& q, QueryResults& result) { return impl_->Update(q, result, ctx_); }
Error Reindexer::Upsert(std::string_view nsName, Item& item) { return impl_->Upsert(nsName, item, ctx_); }
Error Reindexer::Delete(std::string_view nsName, Item& item) { return impl_->Delete(nsName, item, ctx_); }
Item Reindexer::NewItem(std::string_view nsName) { return impl_->NewItem(nsName); }
Error Reindexer::GetMeta(std::string_view nsName, const std::string& key, std::string& data) {
	return impl_->GetMeta(nsName, key, data, ctx_);
}
Error Reindexer::PutMeta(std::string_view nsName, const std::string& key, std::string_view data) {
	return impl_->PutMeta(nsName, key, data, ctx_);
}
Error Reindexer::EnumMeta(std::string_view nsName, std::vector<std::string>& keys) { return impl_->EnumMeta(nsName, keys, ctx_); }
Error Reindexer::Delete(const Query& q, QueryResults& result) { return impl_->Delete(q, result, ctx_); }
Error Reindexer::Select(std::string_view query, QueryResults& result) { return impl_->Select(query, result, ctx_, nullptr); }
Error Reindexer::Select(const Query& q, QueryResults& result) { return impl_->Select(q, result, ctx_, nullptr); }
Error Reindexer::Commit(std::string_view nsName) { return impl_->Commit(nsName); }
Error Reindexer::AddIndex(std::string_view nsName, const IndexDef& idx) { return impl_->AddIndex(nsName, idx, ctx_); }
Error Reindexer::UpdateIndex(std::string_view nsName, const IndexDef& idx) { return impl_->UpdateIndex(nsName, idx, ctx_); }
Error Reindexer::DropIndex(std::string_view nsName, const IndexDef& index) { return impl_->DropIndex(nsName, index, ctx_); }
Error Reindexer::SetSchema(std::string_view nsName, std::string_view schema) { return impl_->SetSchema(nsName, schema, ctx_); }
Error Reindexer::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts) {
	return impl_->EnumNamespaces(defs, opts, ctx_);
}
Error Reindexer::EnumDatabases(std::vector<std::string>& dbList) { return impl_->EnumDatabases(dbList, ctx_); }
Error Reindexer::SubscribeUpdates(IUpdatesObserver* observer, const UpdatesFilters& filters, SubscriptionOpts opts) {
	return impl_->SubscribeUpdates(observer, filters, opts);
}
Error Reindexer::UnsubscribeUpdates(IUpdatesObserver* observer) { return impl_->UnsubscribeUpdates(observer); }
Error Reindexer::GetSqlSuggestions(const std::string_view sqlQuery, int pos, std::vector<std::string>& suggests) {
	return impl_->GetSqlSuggestions(sqlQuery, pos, suggests);
}
Error Reindexer::Status() { return impl_->Status(); }

Transaction Reindexer::NewTransaction(std::string_view nsName) { return impl_->NewTransaction(nsName, ctx_); }
Error Reindexer::CommitTransaction(Transaction& tr) { return impl_->CommitTransaction(tr, ctx_); }
Error Reindexer::RollBackTransaction(Transaction& tr) { return impl_->RollBackTransaction(tr, ctx_); }

}  // namespace client
}  // namespace reindexer
