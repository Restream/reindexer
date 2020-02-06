#include "core/reindexer.h"
#include "core/reindexerimpl.h"

namespace reindexer {

Reindexer::Reindexer() : impl_(new ReindexerImpl), owner_(true) {}
Reindexer::~Reindexer() {
	if (owner_) {
		delete impl_;
	}
}
Reindexer::Reindexer(const Reindexer& rdx) noexcept : impl_(rdx.impl_), owner_(false), ctx_(rdx.ctx_) {}
Reindexer::Reindexer(Reindexer&& rdx) noexcept : impl_(rdx.impl_), owner_(rdx.owner_), ctx_(std::move(rdx.ctx_)) { rdx.owner_ = false; }

bool Reindexer::NeedTraceActivity() const { return impl_->NeedTraceActivity(); }

Error Reindexer::Connect(const string& dsn, ConnectOpts opts) { return impl_->Connect(dsn, opts); }

Error Reindexer::EnableStorage(const string& storagePath, bool skipPlaceholderCheck) {
	return impl_->EnableStorage(storagePath, skipPlaceholderCheck, ctx_);
}
Error Reindexer::AddNamespace(const NamespaceDef& nsDef) { return impl_->AddNamespace(nsDef, ctx_); }
Error Reindexer::OpenNamespace(string_view nsName, const StorageOpts& storage) { return impl_->OpenNamespace(nsName, storage, ctx_); }
Error Reindexer::DropNamespace(string_view nsName) { return impl_->DropNamespace(nsName, ctx_); }
Error Reindexer::CloseNamespace(string_view nsName) { return impl_->CloseNamespace(nsName, ctx_); }
Error Reindexer::TruncateNamespace(string_view nsName) { return impl_->TruncateNamespace(nsName, ctx_); }
Error Reindexer::RenameNamespace(string_view srcNsName, const std::string& dstNsName) {
	return impl_->RenameNamespace(srcNsName, dstNsName, ctx_);
}
Error Reindexer::Insert(string_view nsName, Item& item) { return impl_->Insert(nsName, item, ctx_); }
Error Reindexer::Update(string_view nsName, Item& item) { return impl_->Update(nsName, item, ctx_); }
Error Reindexer::Upsert(string_view nsName, Item& item) { return impl_->Upsert(nsName, item, ctx_); }
Error Reindexer::Delete(string_view nsName, Item& item) { return impl_->Delete(nsName, item, ctx_); }
Item Reindexer::NewItem(string_view nsName) { return impl_->NewItem(nsName, ctx_); }
Transaction Reindexer::NewTransaction(string_view nsName) { return impl_->NewTransaction(nsName, ctx_); }
Error Reindexer::CommitTransaction(Transaction& tr, QueryResults& result) { return impl_->CommitTransaction(tr, result, ctx_); }
Error Reindexer::RollBackTransaction(Transaction& tr) { return impl_->RollBackTransaction(tr); }
Error Reindexer::GetMeta(string_view nsName, const string& key, string& data) { return impl_->GetMeta(nsName, key, data, ctx_); }
Error Reindexer::PutMeta(string_view nsName, const string& key, string_view data) { return impl_->PutMeta(nsName, key, data, ctx_); }
Error Reindexer::EnumMeta(string_view nsName, vector<string>& keys) { return impl_->EnumMeta(nsName, keys, ctx_); }
Error Reindexer::Delete(const Query& q, QueryResults& result) { return impl_->Delete(q, result, ctx_); }
Error Reindexer::Select(string_view query, QueryResults& result) { return impl_->Select(query, result, ctx_); }
Error Reindexer::Select(const Query& q, QueryResults& result) { return impl_->Select(q, result, ctx_); }
Error Reindexer::Update(const Query& query, QueryResults& result) { return impl_->Update(query, result, ctx_); }
Error Reindexer::Commit(string_view nsName) { return impl_->Commit(nsName); }
Error Reindexer::AddIndex(string_view nsName, const IndexDef& idx) { return impl_->AddIndex(nsName, idx, ctx_); }
Error Reindexer::UpdateIndex(string_view nsName, const IndexDef& idx) { return impl_->UpdateIndex(nsName, idx, ctx_); }
Error Reindexer::DropIndex(string_view nsName, const IndexDef& index) { return impl_->DropIndex(nsName, index, ctx_); }
Error Reindexer::EnumNamespaces(vector<NamespaceDef>& defs, EnumNamespacesOpts opts) { return impl_->EnumNamespaces(defs, opts, ctx_); }
Error Reindexer::InitSystemNamespaces() { return impl_->InitSystemNamespaces(); }
Error Reindexer::SubscribeUpdates(IUpdatesObserver* observer, bool subscribe) { return impl_->SubscribeUpdates(observer, subscribe); }
Error Reindexer::GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string>& suggestions) {
	return impl_->GetSqlSuggestions(sqlQuery, pos, suggestions, ctx_);
}
Error Reindexer::Status() { return impl_->Status(); }

}  // namespace reindexer
