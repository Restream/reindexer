#include "core/reindexer.h"
#include "core/reindexerimpl.h"

namespace reindexer {

Reindexer::Reindexer() { impl_ = new ReindexerImpl(); }
Reindexer::~Reindexer() { delete impl_; }
Error Reindexer::Connect(const string& dsn) { return impl_->Connect(dsn); }
Error Reindexer::EnableStorage(const string& storagePath, bool skipPlaceholderCheck) {
	return impl_->EnableStorage(storagePath, skipPlaceholderCheck);
}
Error Reindexer::AddNamespace(const NamespaceDef& nsDef) { return impl_->AddNamespace(nsDef); }
Error Reindexer::OpenNamespace(const string& name, const StorageOpts& storage, CacheMode cacheMode) {
	return impl_->OpenNamespace(name, storage, cacheMode);
}
Error Reindexer::DropNamespace(const string& _namespace) { return impl_->DropNamespace(_namespace); }
Error Reindexer::CloseNamespace(const string& _namespace) { return impl_->CloseNamespace(_namespace); }
Error Reindexer::Insert(const string& _namespace, Item& item, Completion cmpl) { return impl_->Insert(_namespace, item, cmpl); }
Error Reindexer::Update(const string& _namespace, Item& item, Completion cmpl) { return impl_->Update(_namespace, item, cmpl); }
Error Reindexer::Upsert(const string& _namespace, Item& item, Completion cmpl) { return impl_->Upsert(_namespace, item, cmpl); }
Error Reindexer::Delete(const string& _namespace, Item& item, Completion cmpl) { return impl_->Delete(_namespace, item, cmpl); }
Item Reindexer::NewItem(const string& _namespace) { return impl_->NewItem(_namespace); }
Error Reindexer::GetMeta(const string& _namespace, const string& key, string& data) { return impl_->GetMeta(_namespace, key, data); }
Error Reindexer::PutMeta(const string& _namespace, const string& key, const string_view& data) {
	return impl_->PutMeta(_namespace, key, data);
}
Error Reindexer::EnumMeta(const string& _namespace, vector<string>& keys) { return impl_->EnumMeta(_namespace, keys); }
Error Reindexer::Delete(const Query& q, QueryResults& result) { return impl_->Delete(q, result); }
Error Reindexer::Select(const string_view& query, QueryResults& result, Completion cmpl) { return impl_->Select(query, result, cmpl); }
Error Reindexer::Select(const Query& q, QueryResults& result, Completion cmpl) { return impl_->Select(q, result, cmpl); }
Error Reindexer::Commit(const string& _namespace) { return impl_->Commit(_namespace); }
Error Reindexer::AddIndex(const string& _namespace, const IndexDef& idx) { return impl_->AddIndex(_namespace, idx); }
Error Reindexer::UpdateIndex(const string& _namespace, const IndexDef& idx) { return impl_->UpdateIndex(_namespace, idx); }
Error Reindexer::DropIndex(const string& _namespace, const string& index) { return impl_->DropIndex(_namespace, index); }
Error Reindexer::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll) { return impl_->EnumNamespaces(defs, bEnumAll); }
Error Reindexer::InitSystemNamespaces() { return impl_->InitSystemNamespaces(); }
Error Reindexer::SubscribeUpdates(IUpdatesObserver* observer, bool subscribe) { return impl_->SubscribeUpdates(observer, subscribe); }

}  // namespace reindexer
