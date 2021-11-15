#include "core/reindexer.h"
#include "core/clusterproxy.h"
#include "core/reindexerimpl.h"

namespace reindexer {

Reindexer::Reindexer(ReindexerConfig cfg) : impl_(new ClusterProxy(std::move(cfg))), owner_(true) {}

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
Error Reindexer::AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts) {
	return impl_->AddNamespace(nsDef, replOpts, ctx_);
}
Error Reindexer::OpenNamespace(std::string_view nsName, const StorageOpts& storage, const NsReplicationOpts& replOpts) {
	return impl_->OpenNamespace(nsName, storage, replOpts, ctx_);
}
Error Reindexer::DropNamespace(std::string_view nsName) { return impl_->DropNamespace(nsName, ctx_); }
Error Reindexer::CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const StorageOpts& opts, lsn_t version) {
	return impl_->CreateTemporaryNamespace(baseName, resultName, opts, version, ctx_);
}
Error Reindexer::CloseNamespace(std::string_view nsName) { return impl_->CloseNamespace(nsName, ctx_); }
Error Reindexer::TruncateNamespace(std::string_view nsName) { return impl_->TruncateNamespace(nsName, ctx_); }
Error Reindexer::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName) {
	return impl_->RenameNamespace(srcNsName, dstNsName, ctx_);
}
Error Reindexer::Insert(std::string_view nsName, Item& item) { return impl_->Insert(nsName, item, ctx_); }
Error Reindexer::Update(std::string_view nsName, Item& item) { return impl_->Update(nsName, item, ctx_); }
Error Reindexer::Upsert(std::string_view nsName, Item& item) { return impl_->Upsert(nsName, item, ctx_); }
Error Reindexer::Delete(std::string_view nsName, Item& item) { return impl_->Delete(nsName, item, ctx_); }
Error Reindexer::Insert(std::string_view nsName, Item& item, QueryResults& qr) { return impl_->Insert(nsName, item, qr, ctx_); }
Error Reindexer::Update(std::string_view nsName, Item& item, QueryResults& qr) { return impl_->Update(nsName, item, qr, ctx_); }
Error Reindexer::Upsert(std::string_view nsName, Item& item, QueryResults& qr) { return impl_->Upsert(nsName, item, qr, ctx_); }
Error Reindexer::Delete(std::string_view nsName, Item& item, QueryResults& qr) { return impl_->Delete(nsName, item, qr, ctx_); }
Item Reindexer::NewItem(std::string_view nsName) { return impl_->NewItem(nsName, ctx_); }
Transaction Reindexer::NewTransaction(std::string_view nsName) { return impl_->NewTransaction(nsName, ctx_); }
Error Reindexer::CommitTransaction(Transaction& tr, QueryResults& result) { return impl_->CommitTransaction(tr, result, ctx_); }
Error Reindexer::RollBackTransaction(Transaction& tr) { return impl_->RollBackTransaction(tr, ctx_); }
Error Reindexer::GetMeta(std::string_view nsName, const string& key, string& data) { return impl_->GetMeta(nsName, key, data, ctx_); }
Error Reindexer::PutMeta(std::string_view nsName, const string& key, std::string_view data) {
	return impl_->PutMeta(nsName, key, data, ctx_);
}
Error Reindexer::EnumMeta(std::string_view nsName, vector<string>& keys) { return impl_->EnumMeta(nsName, keys, ctx_); }
Error Reindexer::Delete(const Query& q, QueryResults& result) { return impl_->Delete(q, result, ctx_); }
Error Reindexer::Select(std::string_view query, QueryResults& result) { return impl_->Select(query, result, ctx_); }
Error Reindexer::Select(const Query& q, QueryResults& result) { return impl_->Select(q, result, ctx_); }
Error Reindexer::Update(const Query& query, QueryResults& result) { return impl_->Update(query, result, ctx_); }
Error Reindexer::Commit(std::string_view nsName) { return impl_->Commit(nsName, ctx_); }
Error Reindexer::AddIndex(std::string_view nsName, const IndexDef& idx) { return impl_->AddIndex(nsName, idx, ctx_); }
Error Reindexer::SetSchema(std::string_view nsName, std::string_view schema) { return impl_->SetSchema(nsName, schema, ctx_); }
Error Reindexer::GetSchema(std::string_view nsName, int format, std::string& schema) {
	return impl_->GetSchema(nsName, format, schema, ctx_);
}
Error Reindexer::UpdateIndex(std::string_view nsName, const IndexDef& idx) { return impl_->UpdateIndex(nsName, idx, ctx_); }
Error Reindexer::DropIndex(std::string_view nsName, const IndexDef& index) { return impl_->DropIndex(nsName, index, ctx_); }
Error Reindexer::EnumNamespaces(vector<NamespaceDef>& defs, EnumNamespacesOpts opts) { return impl_->EnumNamespaces(defs, opts, ctx_); }
Error Reindexer::InitSystemNamespaces() { return impl_->InitSystemNamespaces(); }
Error Reindexer::GetProtobufSchema(WrSerializer& ser, vector<string>& namespaces) { return impl_->GetProtobufSchema(ser, namespaces); }
Error Reindexer::GetReplState(std::string_view nsName, ReplicationStateV2& state) { return impl_->GetReplState(nsName, state, ctx_); }
Error Reindexer::SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus& status) {
	return impl_->SetClusterizationStatus(nsName, status, ctx_);
}
Error Reindexer::GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot) {
	return impl_->GetSnapshot(nsName, opts, snapshot, ctx_);
}
Error Reindexer::ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch) {
	return impl_->ApplySnapshotChunk(nsName, ch, ctx_);
}
Error Reindexer::SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response) {
	return impl_->SuggestLeader(suggestion, response);
}
Error Reindexer::LeadersPing(const cluster::NodeData& leader) { return impl_->LeadersPing(leader); }
Error Reindexer::GetRaftInfo(cluster::RaftInfo& info) { return impl_->GetRaftInfo(info, ctx_); }
Error Reindexer::GetSqlSuggestions(const std::string_view sqlQuery, int pos, vector<string>& suggestions) {
	return impl_->GetSqlSuggestions(sqlQuery, pos, suggestions, ctx_);
}

Error Reindexer::ClusterControlRequest(const ClusterControlRequestData& request) { return impl_->ClusterControlRequest(request); }

void Reindexer::ShutdownCluster() { impl_->ShutdownCluster(); }

Error Reindexer::Status() { return impl_->Status(); }

}  // namespace reindexer
