#include "client/reindexer.h"
#include "client/rpcclient.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

Reindexer::Reindexer(const ReindexerConfig& config) { impl_ = new RPCClient(config); }
Reindexer::~Reindexer() { delete impl_; }
Error Reindexer::Connect(const string& dsn) { return impl_->Connect(dsn); }
Error Reindexer::AddNamespace(const NamespaceDef& nsDef) { return impl_->AddNamespace(nsDef); }
Error Reindexer::OpenNamespace(const string& name, const StorageOpts& storage) { return impl_->OpenNamespace(name, storage); }
Error Reindexer::DropNamespace(const string& nsName) { return impl_->DropNamespace(nsName); }
Error Reindexer::CloseNamespace(const string& nsName) { return impl_->CloseNamespace(nsName); }
Error Reindexer::Insert(const string& nsName, Item& item) { return impl_->Insert(nsName, item); }
Error Reindexer::Update(const string& nsName, Item& item) { return impl_->Update(nsName, item); }
Error Reindexer::Upsert(const string& nsName, Item& item) { return impl_->Upsert(nsName, item); }
Error Reindexer::Delete(const string& nsName, Item& item) { return impl_->Delete(nsName, item); }
Item Reindexer::NewItem(const string& nsName) { return impl_->NewItem(nsName); }
Error Reindexer::GetMeta(const string& nsName, const string& key, string& data) { return impl_->GetMeta(nsName, key, data); }
Error Reindexer::PutMeta(const string& nsName, const string& key, const string_view& data) { return impl_->PutMeta(nsName, key, data); }
Error Reindexer::EnumMeta(const string& nsName, vector<string>& keys) { return impl_->EnumMeta(nsName, keys); }
Error Reindexer::Delete(const Query& q, QueryResults& result) { return impl_->Delete(q, result); }
Error Reindexer::Select(const string& query, QueryResults& result) { return impl_->Select(query, result); }
Error Reindexer::Select(const Query& q, QueryResults& result) { return impl_->Select(q, result); }
Error Reindexer::Commit(const string& nsName) { return impl_->Commit(nsName); }
Error Reindexer::ConfigureIndex(const string& nsName, const string& index, const string& config) {
	return impl_->ConfigureIndex(nsName, index, config);
}
Error Reindexer::AddIndex(const string& nsName, const IndexDef& idx) { return impl_->AddIndex(nsName, idx); }
Error Reindexer::UpdateIndex(const string& nsName, const IndexDef& idx) { return impl_->UpdateIndex(nsName, idx); }
Error Reindexer::DropIndex(const string& nsName, const string& index) { return impl_->DropIndex(nsName, index); }
Error Reindexer::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll) { return impl_->EnumNamespaces(defs, bEnumAll); }

}  // namespace client
}  // namespace reindexer
