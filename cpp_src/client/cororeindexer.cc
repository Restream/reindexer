#include "client/cororeindexer.h"
#include "client/rpcclient.h"
#include "tools/catch_and_return.h"
#include "tools/cpucheck.h"

namespace reindexer {
namespace client {

CoroReindexer::CoroReindexer(const ReindexerConfig& config) : impl_(new RPCClient(config, nullptr)), owner_(true), ctx_() {
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

Error CoroReindexer::Connect(const std::string& dsn, net::ev::dynamic_loop& loop, const ConnectOpts& opts) noexcept {
	RETURN_RESULT_NOEXCEPT(Connect(DSN(dsn), loop, opts));
}
Error CoroReindexer::Connect(const DSN& dsn, net::ev::dynamic_loop& loop, const ConnectOpts& opts) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Connect(dsn, loop, opts));
}
void CoroReindexer::Stop() noexcept {
	try {
		impl_->Stop();
		// Do not except any exceptions here
	} catch (std::exception& e) {
		fprintf(stderr, "reindexer error: unexpected system error in CoroReindexer::Stop: %s\n", e.what());
	} catch (...) {
		fprintf(stderr, "reindexer error: unexpected system error in CoroReindexer::Stop: <no description available>\n");
	}
}
Error CoroReindexer::AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->AddNamespace(nsDef, ctx_, replOpts));
}
Error CoroReindexer::OpenNamespace(std::string_view nsName, const StorageOpts& storage, const NsReplicationOpts& replOpts) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->OpenNamespace(nsName, ctx_, storage, replOpts));
}
Error CoroReindexer::DropNamespace(std::string_view nsName) noexcept { RETURN_RESULT_NOEXCEPT(impl_->DropNamespace(nsName, ctx_)); }
Error CoroReindexer::CloseNamespace(std::string_view nsName) noexcept { RETURN_RESULT_NOEXCEPT(impl_->CloseNamespace(nsName, ctx_)); }
Error CoroReindexer::CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const StorageOpts& opts,
											  lsn_t version) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->CreateTemporaryNamespace(baseName, resultName, ctx_, opts, version));
}
Error CoroReindexer::TruncateNamespace(std::string_view nsName) noexcept { RETURN_RESULT_NOEXCEPT(impl_->TruncateNamespace(nsName, ctx_)); }
Error CoroReindexer::RenameNamespace(std::string_view srcNsName, std::string_view dstNsName) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->RenameNamespace(srcNsName, dstNsName, ctx_));
}
Error CoroReindexer::Insert(std::string_view nsName, Item& item) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Insert(nsName, item, RPCDataFormat::CJSON, ctx_));
}
Error CoroReindexer::Insert(std::string_view nsName, std::string_view cjson) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Insert(nsName, cjson, ctx_));
}
Error CoroReindexer::Update(std::string_view nsName, Item& item) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Update(nsName, item, RPCDataFormat::CJSON, ctx_));
}
Error CoroReindexer::Update(std::string_view nsName, std::string_view cjson) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Update(nsName, cjson, ctx_));
}
Error CoroReindexer::Update(const Query& q, CoroQueryResults& result) noexcept { RETURN_RESULT_NOEXCEPT(impl_->Update(q, result, ctx_)); }
Error CoroReindexer::Upsert(std::string_view nsName, Item& item) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Upsert(nsName, item, RPCDataFormat::CJSON, ctx_));
}
Error CoroReindexer::Upsert(std::string_view nsName, std::string_view cjson) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Upsert(nsName, cjson, ctx_));
}
Error CoroReindexer::Delete(std::string_view nsName, Item& item) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Delete(nsName, item, RPCDataFormat::CJSON, ctx_));
}
Item CoroReindexer::NewItem(std::string_view nsName) noexcept { return impl_->NewItem(nsName, *impl_, ctx_.execTimeout()); }
Error CoroReindexer::GetMeta(std::string_view nsName, const std::string& key, std::string& data) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->GetMeta(nsName, key, data, ctx_));
}
Error CoroReindexer::GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->GetMeta(nsName, key, data, ctx_));
}
Error CoroReindexer::PutMeta(std::string_view nsName, const std::string& key, std::string_view data) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->PutMeta(nsName, key, data, ctx_));
}
Error CoroReindexer::EnumMeta(std::string_view nsName, std::vector<std::string>& keys) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->EnumMeta(nsName, keys, ctx_));
}
Error CoroReindexer::DeleteMeta(std::string_view nsName, const std::string& key) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->DeleteMeta(nsName, key, ctx_));
}
Error CoroReindexer::Delete(const Query& q, CoroQueryResults& result) noexcept { RETURN_RESULT_NOEXCEPT(impl_->Delete(q, result, ctx_)); }
Error CoroReindexer::Delete(std::string_view nsName, std::string_view cjson) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Delete(nsName, cjson, ctx_));
}
Error CoroReindexer::ExecSQL(std::string_view query, CoroQueryResults& result) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->ExecSQL(query, result, ctx_));
}
Error CoroReindexer::Select(const Query& q, CoroQueryResults& result) noexcept { RETURN_RESULT_NOEXCEPT(impl_->Select(q, result, ctx_)); }
Error CoroReindexer::Version(std::string& version) noexcept { RETURN_RESULT_NOEXCEPT(impl_->Version(version, ctx_)); }
Error CoroReindexer::AddIndex(std::string_view nsName, const IndexDef& idx) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->AddIndex(nsName, idx, ctx_));
}
Error CoroReindexer::UpdateIndex(std::string_view nsName, const IndexDef& idx) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->UpdateIndex(nsName, idx, ctx_));
}
Error CoroReindexer::DropIndex(std::string_view nsName, const IndexDef& index) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->DropIndex(nsName, index, ctx_));
}
Error CoroReindexer::SetSchema(std::string_view nsName, std::string_view schema) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->SetSchema(nsName, schema, ctx_));
}
Error CoroReindexer::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->EnumNamespaces(defs, opts, ctx_));
}
Error CoroReindexer::EnumDatabases(std::vector<std::string>& dbList) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->EnumDatabases(dbList, ctx_));
}
Error CoroReindexer::GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggests) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->GetSqlSuggestions(sqlQuery, pos, suggests));
}
Error CoroReindexer::Status(bool forceCheck) noexcept { RETURN_RESULT_NOEXCEPT(impl_->Status(forceCheck, ctx_)); }
CoroTransaction CoroReindexer::NewTransaction(std::string_view nsName) noexcept { return impl_->NewTransaction(nsName, ctx_); }
Error CoroReindexer::CommitTransaction(CoroTransaction& tr, CoroQueryResults& result) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->CommitTransaction(tr, result, ctx_));
}
Error CoroReindexer::RollBackTransaction(CoroTransaction& tr) noexcept { RETURN_RESULT_NOEXCEPT(impl_->RollBackTransaction(tr, ctx_)); }
Error CoroReindexer::GetReplState(std::string_view nsName, ReplicationStateV2& state) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->GetReplState(nsName, state, ctx_));
}
Error CoroReindexer::SetClusterOperationStatus(std::string_view nsName, const ClusterOperationStatus& status) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->SetClusterOperationStatus(nsName, status, ctx_));
}
Error CoroReindexer::GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->GetSnapshot(nsName, opts, snapshot, ctx_));
}
Error CoroReindexer::ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->ApplySnapshotChunk(nsName, ch, ctx_));
}
Error CoroReindexer::SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->SetTagsMatcher(nsName, std::move(tm), ctx_));
}
Expected<int64_t> CoroReindexer::AddConnectionStateObserver(CoroReindexer::ConnectionStateHandlerT callback) noexcept {
	try {
		return impl_->AddConnectionStateObserver(std::move(callback));
	} catch (std::exception& e) {
		return Unexpected(std::move(e));
	} catch (...) {
		return Unexpected(Error(errSystem, "Unknown exception type in reindexer client"));
	}
}
Error CoroReindexer::RemoveConnectionStateObserver(int64_t id) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->RemoveConnectionStateObserver(id));
}

Error CoroReindexer::ShardingControlRequest(const sharding::ShardingControlRequestData& request,
											sharding::ShardingControlResponseData& response) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->ShardingControlRequest(request, response, ctx_));
}

}  // namespace client
}  // namespace reindexer
