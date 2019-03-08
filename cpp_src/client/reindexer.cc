#include "client/reindexer.h"
#include "client/rpcclient.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

Reindexer::Reindexer(const ReindexerConfig& config) { impl_ = new RPCClient(config); }
Reindexer::~Reindexer() { delete impl_; }
Error Reindexer::Connect(const string& dsn) { return impl_->Connect(dsn); }
Error Reindexer::Stop() { return impl_->Stop(); }
Error Reindexer::AddNamespace(const NamespaceDef& nsDef) { return impl_->AddNamespace(nsDef); }
Error Reindexer::OpenNamespace(string_view nsName, const StorageOpts& storage) { return impl_->OpenNamespace(nsName, storage); }
Error Reindexer::DropNamespace(string_view nsName) { return impl_->DropNamespace(nsName); }
Error Reindexer::CloseNamespace(string_view nsName) { return impl_->CloseNamespace(nsName); }
Error Reindexer::Insert(string_view nsName, Item& item, Completion cmpl) { return impl_->Insert(nsName, item, cmpl); }
Error Reindexer::Update(string_view nsName, Item& item, Completion cmpl) { return impl_->Update(nsName, item, cmpl); }
Error Reindexer::Update(const Query& q, QueryResults& result) { return impl_->Update(q, result); }
Error Reindexer::Upsert(string_view nsName, Item& item, Completion cmpl) { return impl_->Upsert(nsName, item, cmpl); }
Error Reindexer::Delete(string_view nsName, Item& item, Completion cmpl) { return impl_->Delete(nsName, item, cmpl); }
Item Reindexer::NewItem(string_view nsName) { return impl_->NewItem(nsName); }
Error Reindexer::GetMeta(string_view nsName, const string& key, string& data) { return impl_->GetMeta(nsName, key, data); }
Error Reindexer::PutMeta(string_view nsName, const string& key, const string_view& data) { return impl_->PutMeta(nsName, key, data); }
Error Reindexer::EnumMeta(string_view nsName, vector<string>& keys) { return impl_->EnumMeta(nsName, keys); }
Error Reindexer::Delete(const Query& q, QueryResults& result) { return impl_->Delete(q, result); }
Error Reindexer::Select(string_view query, QueryResults& result, Completion cmpl) { return impl_->Select(query, result, cmpl); }
Error Reindexer::Select(const Query& q, QueryResults& result, Completion cmpl) { return impl_->Select(q, result, cmpl); }
Error Reindexer::Commit(string_view nsName) { return impl_->Commit(nsName); }
Error Reindexer::AddIndex(string_view nsName, const IndexDef& idx) { return impl_->AddIndex(nsName, idx); }
Error Reindexer::UpdateIndex(string_view nsName, const IndexDef& idx) { return impl_->UpdateIndex(nsName, idx); }
Error Reindexer::DropIndex(string_view nsName, const IndexDef& index) { return impl_->DropIndex(nsName, index); }
Error Reindexer::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll) { return impl_->EnumNamespaces(defs, bEnumAll); }
Error Reindexer::SubscribeUpdates(IUpdatesObserver* observer, bool subscribe) { return impl_->SubscribeUpdates(observer, subscribe); }
Error Reindexer::GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string>& suggests) {
	return impl_->GetSqlSuggestions(sqlQuery, pos, suggests);
}

}  // namespace client
}  // namespace reindexer
