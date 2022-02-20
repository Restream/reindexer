#include "synccororeindexer.h"
#include "client/cororeindexer.h"
#include "client/cororpcclient.h"
#include "client/itemimpl.h"
#include "synccororeindexerimpl.h"

namespace reindexer {
namespace client {

SyncCoroReindexer::SyncCoroReindexer(const CoroReindexerConfig& config, size_t connCount)
	: impl_(new SyncCoroReindexerImpl(config, connCount)), ctx_() {}
SyncCoroReindexer::~SyncCoroReindexer() = default;

Error SyncCoroReindexer::Connect(const string& dsn, const client::ConnectOpts& opts) { return impl_->Connect(dsn, opts); }
Error SyncCoroReindexer::Stop() { return impl_->Stop(); }
Error SyncCoroReindexer::AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts) {
	return impl_->AddNamespace(nsDef, ctx_, replOpts);
}
Error SyncCoroReindexer::OpenNamespace(std::string_view nsName, const StorageOpts& storage, const NsReplicationOpts& replOpts) {
	return impl_->OpenNamespace(nsName, ctx_, storage, replOpts);
}
Error SyncCoroReindexer::DropNamespace(std::string_view nsName) { return impl_->DropNamespace(nsName, ctx_); }
Error SyncCoroReindexer::CloseNamespace(std::string_view nsName) { return impl_->CloseNamespace(nsName, ctx_); }
Error SyncCoroReindexer::TruncateNamespace(std::string_view nsName) { return impl_->TruncateNamespace(nsName, ctx_); }
Error SyncCoroReindexer::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName) {
	return impl_->RenameNamespace(srcNsName, dstNsName, ctx_);
}
Error SyncCoroReindexer::Insert(std::string_view nsName, Item& item) { return impl_->Insert(nsName, item, ctx_); }
Error SyncCoroReindexer::Insert(std::string_view nsName, Item& item, SyncCoroQueryResults& result) {
	return impl_->Insert(nsName, item, result, ctx_);
}
Error SyncCoroReindexer::Update(std::string_view nsName, Item& item) { return impl_->Update(nsName, item, ctx_); }
Error SyncCoroReindexer::Update(std::string_view nsName, Item& item, SyncCoroQueryResults& result) {
	return impl_->Update(nsName, item, result, ctx_);
}
Error SyncCoroReindexer::Update(const Query& q, SyncCoroQueryResults& result) { return impl_->Update(q, result, ctx_); }
Error SyncCoroReindexer::Upsert(std::string_view nsName, Item& item) { return impl_->Upsert(nsName, item, ctx_); }
Error SyncCoroReindexer::Upsert(std::string_view nsName, Item& item, SyncCoroQueryResults& result) {
	return impl_->Upsert(nsName, item, result, ctx_);
}
Error SyncCoroReindexer::Delete(std::string_view nsName, Item& item) { return impl_->Delete(nsName, item, ctx_); }
Error SyncCoroReindexer::Delete(std::string_view nsName, Item& item, SyncCoroQueryResults& result) {
	return impl_->Delete(nsName, item, result, ctx_);
}
Item SyncCoroReindexer::NewItem(std::string_view nsName) { return impl_->NewItem(nsName, ctx_); }
Error SyncCoroReindexer::GetMeta(std::string_view nsName, const string& key, string& data) {
	return impl_->GetMeta(nsName, key, data, ctx_);
}
Error SyncCoroReindexer::GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data) {
	return impl_->GetMeta(nsName, key, data, ctx_);
}
Error SyncCoroReindexer::PutMeta(std::string_view nsName, const string& key, std::string_view data) {
	return impl_->PutMeta(nsName, key, data, ctx_);
}
Error SyncCoroReindexer::EnumMeta(std::string_view nsName, vector<string>& keys) { return impl_->EnumMeta(nsName, keys, ctx_); }
Error SyncCoroReindexer::Delete(const Query& q, SyncCoroQueryResults& result) { return impl_->Delete(q, result, ctx_); }
Error SyncCoroReindexer::Select(std::string_view query, SyncCoroQueryResults& result) { return impl_->Select(query, result, ctx_); }
Error SyncCoroReindexer::Select(const Query& q, SyncCoroQueryResults& result) { return impl_->Select(q, result, ctx_); }
Error SyncCoroReindexer::Commit(std::string_view nsName) { return impl_->Commit(nsName, ctx_); }
Error SyncCoroReindexer::AddIndex(std::string_view nsName, const IndexDef& idx) { return impl_->AddIndex(nsName, idx, ctx_); }
Error SyncCoroReindexer::UpdateIndex(std::string_view nsName, const IndexDef& idx) { return impl_->UpdateIndex(nsName, idx, ctx_); }
Error SyncCoroReindexer::DropIndex(std::string_view nsName, const IndexDef& index) { return impl_->DropIndex(nsName, index, ctx_); }
Error SyncCoroReindexer::SetSchema(std::string_view nsName, std::string_view schema) { return impl_->SetSchema(nsName, schema, ctx_); }
Error SyncCoroReindexer::GetSchema(std::string_view nsName, int format, std::string& schema) {
	return impl_->GetSchema(nsName, format, schema, ctx_);
}
Error SyncCoroReindexer::EnumNamespaces(vector<NamespaceDef>& defs, EnumNamespacesOpts opts) {
	return impl_->EnumNamespaces(defs, opts, ctx_);
}
Error SyncCoroReindexer::EnumDatabases(vector<string>& dbList) { return impl_->EnumDatabases(dbList, ctx_); }
Error SyncCoroReindexer::GetSqlSuggestions(const std::string_view sqlQuery, int pos, vector<string>& suggests) {
	return impl_->GetSqlSuggestions(sqlQuery, pos, suggests);
}
Error SyncCoroReindexer::Status(bool forceCheck) { return impl_->Status(forceCheck, ctx_); }

SyncCoroTransaction SyncCoroReindexer::NewTransaction(std::string_view nsName) {
	CoroTransaction tr = impl_->NewTransaction(nsName, ctx_);
	if (tr.Status().ok()) {
		return SyncCoroTransaction(impl_, std::move(tr));
	}
	return SyncCoroTransaction(tr.Status());
}

Error SyncCoroReindexer::CommitTransaction(SyncCoroTransaction& tr, SyncCoroQueryResults& result) {
	return impl_->CommitTransaction(tr, result, ctx_);
}
Error SyncCoroReindexer::RollBackTransaction(SyncCoroTransaction& tr) { return impl_->RollBackTransaction(tr, ctx_); }
Error SyncCoroReindexer::GetReplState(std::string_view nsName, ReplicationStateV2& state) {
	return impl_->GetReplState(nsName, state, ctx_);
}

}  // namespace client
}  // namespace reindexer
