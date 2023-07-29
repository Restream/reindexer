#include "client/cororeindexer.h"
#include "client/cororpcclient.h"
#include "tools/cpucheck.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

CoroReindexer::CoroReindexer(const ReindexerConfig& config) : impl_(new CoroRPCClient(config)), owner_(true), ctx_() {
	reindexer::CheckRequiredSSESupport();
}
CoroReindexer::~CoroReindexer() {
	if (owner_) {
		delete impl_;
	}
}
CoroReindexer::CoroReindexer(CoroReindexer&& rdx) noexcept : impl_(rdx.impl_), owner_(rdx.owner_), ctx_(std::move(rdx.ctx_)) {
	rdx.owner_ = false;
}
CoroReindexer& CoroReindexer::operator=(CoroReindexer&& rdx) noexcept {
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

Error CoroReindexer::Connect(const std::string& dsn, dynamic_loop& loop, const client::ConnectOpts& opts) {
	return impl_->Connect(dsn, loop, opts);
}
Error CoroReindexer::Stop() { return impl_->Stop(); }
Error CoroReindexer::AddNamespace(const NamespaceDef& nsDef) { return impl_->AddNamespace(nsDef, ctx_); }
Error CoroReindexer::OpenNamespace(std::string_view nsName, const StorageOpts& storage) {
	return impl_->OpenNamespace(nsName, ctx_, storage);
}
Error CoroReindexer::DropNamespace(std::string_view nsName) { return impl_->DropNamespace(nsName, ctx_); }
Error CoroReindexer::CloseNamespace(std::string_view nsName) { return impl_->CloseNamespace(nsName, ctx_); }
Error CoroReindexer::TruncateNamespace(std::string_view nsName) { return impl_->TruncateNamespace(nsName, ctx_); }
Error CoroReindexer::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName) {
	return impl_->RenameNamespace(srcNsName, dstNsName, ctx_);
}
Error CoroReindexer::Insert(std::string_view nsName, Item& item) { return impl_->Insert(nsName, item, ctx_); }
Error CoroReindexer::Update(std::string_view nsName, Item& item) { return impl_->Update(nsName, item, ctx_); }
Error CoroReindexer::Update(const Query& q, CoroQueryResults& result) { return impl_->Update(q, result, ctx_); }
Error CoroReindexer::Upsert(std::string_view nsName, Item& item) { return impl_->Upsert(nsName, item, ctx_); }
Error CoroReindexer::Delete(std::string_view nsName, Item& item) { return impl_->Delete(nsName, item, ctx_); }
Item CoroReindexer::NewItem(std::string_view nsName) { return impl_->NewItem(nsName); }
Error CoroReindexer::GetMeta(std::string_view nsName, const std::string& key, std::string& data) {
	return impl_->GetMeta(nsName, key, data, ctx_);
}
Error CoroReindexer::PutMeta(std::string_view nsName, const std::string& key, std::string_view data) {
	return impl_->PutMeta(nsName, key, data, ctx_);
}
Error CoroReindexer::EnumMeta(std::string_view nsName, std::vector<std::string>& keys) { return impl_->EnumMeta(nsName, keys, ctx_); }
Error CoroReindexer::Delete(const Query& q, CoroQueryResults& result) { return impl_->Delete(q, result, ctx_); }
Error CoroReindexer::Select(std::string_view query, CoroQueryResults& result) { return impl_->Select(query, result, ctx_); }
Error CoroReindexer::Select(const Query& q, CoroQueryResults& result) { return impl_->Select(q, result, ctx_); }
Error CoroReindexer::Commit(std::string_view nsName) { return impl_->Commit(nsName); }
Error CoroReindexer::AddIndex(std::string_view nsName, const IndexDef& idx) { return impl_->AddIndex(nsName, idx, ctx_); }
Error CoroReindexer::UpdateIndex(std::string_view nsName, const IndexDef& idx) { return impl_->UpdateIndex(nsName, idx, ctx_); }
Error CoroReindexer::DropIndex(std::string_view nsName, const IndexDef& index) { return impl_->DropIndex(nsName, index, ctx_); }
Error CoroReindexer::SetSchema(std::string_view nsName, std::string_view schema) { return impl_->SetSchema(nsName, schema, ctx_); }
Error CoroReindexer::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts) {
	return impl_->EnumNamespaces(defs, opts, ctx_);
}
Error CoroReindexer::EnumDatabases(std::vector<std::string>& dbList) { return impl_->EnumDatabases(dbList, ctx_); }
Error CoroReindexer::SubscribeUpdates(IUpdatesObserver* observer, const UpdatesFilters& filters, SubscriptionOpts opts) {
	return impl_->SubscribeUpdates(observer, filters, opts);
}
Error CoroReindexer::UnsubscribeUpdates(IUpdatesObserver* observer) { return impl_->UnsubscribeUpdates(observer); }
Error CoroReindexer::GetSqlSuggestions(const std::string_view sqlQuery, int pos, std::vector<std::string>& suggests) {
	return impl_->GetSqlSuggestions(sqlQuery, pos, suggests);
}
Error CoroReindexer::Status() { return impl_->Status(ctx_); }

CoroTransaction CoroReindexer::NewTransaction(std::string_view nsName) { return impl_->NewTransaction(nsName, ctx_); }
Error CoroReindexer::CommitTransaction(CoroTransaction& tr) { return impl_->CommitTransaction(tr, ctx_); }
Error CoroReindexer::RollBackTransaction(CoroTransaction& tr) { return impl_->RollBackTransaction(tr, ctx_); }

}  // namespace client
}  // namespace reindexer
