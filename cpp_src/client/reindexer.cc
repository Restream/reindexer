#include "client/reindexer.h"
#include "client/cororeindexer.h"
#include "client/reindexerimpl.h"
#include "client/rpcclient.h"

namespace reindexer {
namespace client {

Reindexer::Reindexer(const ReindexerConfig& config, uint32_t connCount, uint32_t threads)
	: impl_(new ReindexerImpl(config, connCount, threads)), ctx_() {}
Reindexer::~Reindexer() = default;

Error Reindexer::Connect(const std::string& dsn, const client::ConnectOpts& opts) { return Connect(DSN(dsn), opts); }
Error Reindexer::Connect(const DSN& dsn, const client::ConnectOpts& opts) { return impl_->Connect(dsn, opts); }
void Reindexer::Stop() { impl_->Stop(); }
Error Reindexer::AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts) {
	return impl_->AddNamespace(nsDef, ctx_, replOpts);
}
Error Reindexer::OpenNamespace(std::string_view nsName, const StorageOpts& storage, const NsReplicationOpts& replOpts) {
	return impl_->OpenNamespace(nsName, ctx_, storage, replOpts);
}
Error Reindexer::DropNamespace(std::string_view nsName) { return impl_->DropNamespace(nsName, ctx_); }
Error Reindexer::CloseNamespace(std::string_view nsName) { return impl_->CloseNamespace(nsName, ctx_); }
Error Reindexer::TruncateNamespace(std::string_view nsName) { return impl_->TruncateNamespace(nsName, ctx_); }
Error Reindexer::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName) {
	return impl_->RenameNamespace(srcNsName, dstNsName, ctx_);
}
Error Reindexer::Insert(std::string_view nsName, Item& item) { return impl_->Insert(nsName, item, RPCDataFormat::CJSON, ctx_); }
Error Reindexer::Insert(std::string_view nsName, Item& item, QueryResults& result) { return impl_->Insert(nsName, item, result, ctx_); }
Error Reindexer::Update(std::string_view nsName, Item& item) { return impl_->Update(nsName, item, RPCDataFormat::CJSON, ctx_); }
Error Reindexer::Update(std::string_view nsName, Item& item, QueryResults& result) { return impl_->Update(nsName, item, result, ctx_); }
Error Reindexer::Update(const Query& q, QueryResults& result) { return impl_->Update(q, result, ctx_); }
Error Reindexer::Upsert(std::string_view nsName, Item& item) { return impl_->Upsert(nsName, item, RPCDataFormat::CJSON, ctx_); }
Error Reindexer::Upsert(std::string_view nsName, Item& item, QueryResults& result) { return impl_->Upsert(nsName, item, result, ctx_); }
Error Reindexer::Delete(std::string_view nsName, Item& item) { return impl_->Delete(nsName, item, RPCDataFormat::CJSON, ctx_); }
Error Reindexer::Delete(std::string_view nsName, Item& item, QueryResults& result) { return impl_->Delete(nsName, item, result, ctx_); }
Item Reindexer::NewItem(std::string_view nsName) { return impl_->NewItem(nsName, ctx_); }
Error Reindexer::GetMeta(std::string_view nsName, const std::string& key, std::string& data) {
	return impl_->GetMeta(nsName, key, data, ctx_);
}
Error Reindexer::GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data) {
	return impl_->GetMeta(nsName, key, data, ctx_);
}
Error Reindexer::PutMeta(std::string_view nsName, const std::string& key, std::string_view data) {
	return impl_->PutMeta(nsName, key, data, ctx_);
}
Error Reindexer::EnumMeta(std::string_view nsName, std::vector<std::string>& keys) { return impl_->EnumMeta(nsName, keys, ctx_); }
Error Reindexer::DeleteMeta(std::string_view nsName, const std::string& key) { return impl_->DeleteMeta(nsName, key, ctx_); }
Error Reindexer::Delete(const Query& q, QueryResults& result) { return impl_->Delete(q, result, ctx_); }
Error Reindexer::Select(std::string_view query, QueryResults& result) { return impl_->Select(query, result, ctx_); }
Error Reindexer::Select(const Query& q, QueryResults& result) { return impl_->Select(q, result, ctx_); }
Error Reindexer::AddIndex(std::string_view nsName, const IndexDef& idx) { return impl_->AddIndex(nsName, idx, ctx_); }
Error Reindexer::UpdateIndex(std::string_view nsName, const IndexDef& idx) { return impl_->UpdateIndex(nsName, idx, ctx_); }
Error Reindexer::DropIndex(std::string_view nsName, const IndexDef& index) { return impl_->DropIndex(nsName, index, ctx_); }
Error Reindexer::SetSchema(std::string_view nsName, std::string_view schema) { return impl_->SetSchema(nsName, schema, ctx_); }
Error Reindexer::GetSchema(std::string_view nsName, int format, std::string& schema) {
	return impl_->GetSchema(nsName, format, schema, ctx_);
}
Error Reindexer::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts) {
	return impl_->EnumNamespaces(defs, opts, ctx_);
}
Error Reindexer::EnumDatabases(std::vector<std::string>& dbList) { return impl_->EnumDatabases(dbList, ctx_); }
Error Reindexer::GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggests) {
	return impl_->GetSqlSuggestions(sqlQuery, pos, suggests);
}
Error Reindexer::Status(bool forceCheck) { return impl_->Status(forceCheck, ctx_); }

Transaction Reindexer::NewTransaction(std::string_view nsName) {
	CoroTransaction tr = impl_->NewTransaction(nsName, ctx_);
	if (tr.Status().ok()) {
		return Transaction(impl_, std::move(tr));
	}
	return Transaction(tr.Status());
}

Error Reindexer::CommitTransaction(Transaction& tr, QueryResults& result) { return impl_->CommitTransaction(tr, result, ctx_); }
Error Reindexer::RollBackTransaction(Transaction& tr) { return impl_->RollBackTransaction(tr, ctx_); }
Error Reindexer::GetReplState(std::string_view nsName, ReplicationStateV2& state) { return impl_->GetReplState(nsName, state, ctx_); }

Error Reindexer::SaveNewShardingConfig(std::string_view config, int64_t sourceId) noexcept {
	return impl_->SaveNewShardingConfig(config, sourceId, ctx_);
}

Error Reindexer::ResetShardingConfigCandidate(int64_t sourceId) noexcept { return impl_->ResetShardingConfigCandidate(sourceId, ctx_); }

Error Reindexer::ResetOldShardingConfig(int64_t sourceId) noexcept { return impl_->ResetOldShardingConfig(sourceId, ctx_); }

Error Reindexer::RollbackShardingConfigCandidate(int64_t sourceId) noexcept {
	return impl_->RollbackShardingConfigCandidate(sourceId, ctx_);
}

Error Reindexer::ApplyNewShardingConfig(int64_t sourceId) noexcept { return impl_->ApplyNewShardingConfig(sourceId, ctx_); }

}  // namespace client
}  // namespace reindexer
