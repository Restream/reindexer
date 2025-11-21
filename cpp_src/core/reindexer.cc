#include "core/reindexer.h"
#include "core/shardingproxy.h"
#include "estl/defines.h"
#include "reindexer_version.h"
#include "tools/catch_and_return.h"
#include "tools/cpucheck.h"

namespace reindexer {

using namespace std::string_view_literals;

static void printPkValue(const Item::FieldRef& f, WrSerializer& ser) {
	ser << f.Name() << " = "sv;
	f.operator Variant().Dump(ser);
}

static void printPkFields(const Item& item, WrSerializer& ser) {
	size_t jsonPathIdx = 0;
	const FieldsSet fields = item.PkFields();
	for (auto it = fields.begin(); it != fields.end(); ++it) {
		if (it != fields.begin()) {
			ser << " AND "sv;
		}
		int field = *it;
		if (field == IndexValueType::SetByJsonPath) {
			assertrx(jsonPathIdx < fields.getTagsPathsLength());
			printPkValue(item[fields.getJsonPath(jsonPathIdx++)], ser);
		} else {
			printPkValue(item[field], ser);
		}
	}
}

Reindexer::Reindexer(ReindexerConfig cfg) : impl_(new ShardingProxy(std::move(cfg))), owner_(true) {
	//
	reindexer::CheckRequiredSSESupport();
}

Reindexer::~Reindexer() {
	if (owner_) {
		delete impl_;
	}
}

Reindexer::Reindexer(const Reindexer& rdx) noexcept : impl_(rdx.impl_), owner_(false), ctx_(rdx.ctx_) {}
Reindexer::Reindexer(Reindexer&& rdx) noexcept : impl_(rdx.impl_), owner_(rdx.owner_), ctx_(std::move(rdx.ctx_)) { rdx.owner_ = false; }

bool Reindexer::NeedTraceActivity() const noexcept {
	// No connection check required here
	return impl_->NeedTraceActivity();
}

Error Reindexer::Connect(const std::string& dsn, ConnectOpts opts) noexcept {
	try {
		return impl_->Connect(dsn, opts);
	} catch (std::exception& e) {
		return e;
	}
}

Error Reindexer::AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts) noexcept {
	return callWithConnectCheck([&] {
		if (!validateUserNsName(nsDef.name)) {
			return Error(errParams, "Namespace name '{}' contains invalid character. Only alphas, digits,'_','-', are allowed", nsDef.name);
		}
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "CREATE NAMESPACE "sv << nsDef.name; });
		return impl_->AddNamespace(nsDef, replOpts, rdxCtx);
	});
}
Error Reindexer::OpenNamespace(std::string_view nsName, const StorageOpts& storage, const NsReplicationOpts& replOpts) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "OPEN NAMESPACE "sv << nsName; });
		return impl_->OpenNamespace(nsName, storage, replOpts, rdxCtx);
	});
}
Error Reindexer::DropNamespace(std::string_view nsName) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "DROP NAMESPACE "sv << nsName; });
		return impl_->DropNamespace(nsName, rdxCtx);
	});
}
Error Reindexer::CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const StorageOpts& opts,
										  lsn_t version) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx =
			impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "CREATE TEMPORARY NAMESPACE "sv << resultName << '*'; });
		return impl_->CreateTemporaryNamespace(baseName, resultName, opts, version, rdxCtx);
	});
}
Error Reindexer::CloseNamespace(std::string_view nsName) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "CLOSE NAMESPACE "sv << nsName; });
		return impl_->CloseNamespace(nsName, rdxCtx);
	});
}
Error Reindexer::TruncateNamespace(std::string_view nsName) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "TRUNCATE "sv << nsName; });
		return impl_->TruncateNamespace(nsName, rdxCtx);
	});
}
Error Reindexer::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "RENAME "sv << srcNsName << " TO "sv << dstNsName; });
		return impl_->RenameNamespace(srcNsName, dstNsName, rdxCtx);
	});
}
Error Reindexer::Insert(std::string_view nsName, Item& item) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "INSERT INTO "sv << nsName; });
		return impl_->Insert(nsName, item, rdxCtx);
	});
}
Error Reindexer::Update(std::string_view nsName, Item& item) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) {
			s << "UPDATE "sv << nsName << " WHERE "sv;
			printPkFields(item, s);
		});
		return impl_->Update(nsName, item, rdxCtx);
	});
}
Error Reindexer::Upsert(std::string_view nsName, Item& item) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) {
			s << "UPSERT INTO "sv << nsName << " WHERE "sv;
			printPkFields(item, s);
		});
		return impl_->Upsert(nsName, item, rdxCtx);
	});
}
Error Reindexer::Delete(std::string_view nsName, Item& item) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) {
			s << "DELETE FROM "sv << nsName << " WHERE "sv;
			printPkFields(item, s);
		});
		return impl_->Delete(nsName, item, rdxCtx);
	});
}
Error Reindexer::Insert(std::string_view nsName, Item& item, QueryResults& qr) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "INSERT INTO "sv << nsName; });
		return impl_->Insert(nsName, item, qr, rdxCtx);
	});
}
Error Reindexer::Update(std::string_view nsName, Item& item, QueryResults& qr) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) {
			s << "UPDATE "sv << nsName << " WHERE "sv;
			printPkFields(item, s);
		});
		return impl_->Update(nsName, item, qr, rdxCtx);
	});
}
Error Reindexer::Upsert(std::string_view nsName, Item& item, QueryResults& qr) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) {
			s << "UPSERT INTO "sv << nsName << " WHERE "sv;
			printPkFields(item, s);
		});
		return impl_->Upsert(nsName, item, qr, rdxCtx);
	});
}
Error Reindexer::Delete(std::string_view nsName, Item& item, QueryResults& qr) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) {
			s << "DELETE FROM "sv << nsName << " WHERE "sv;
			printPkFields(item, s);
		});
		return impl_->Delete(nsName, item, qr, rdxCtx);
	});
}
Item Reindexer::NewItem(std::string_view nsName) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "CREATE ITEM FOR "sv << nsName; });
		return impl_->NewItem(nsName, rdxCtx);
	});
}
Transaction Reindexer::NewTransaction(std::string_view nsName) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "START TRANSACTION "sv << nsName; });
		return impl_->NewTransaction(nsName, rdxCtx);
	});
}
Error Reindexer::CommitTransaction(Transaction& tr, QueryResults& result) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "COMMIT TRANSACTION "sv << tr.GetNsName(); });
		return impl_->CommitTransaction(tr, result, rdxCtx);
	});
}
Error Reindexer::RollBackTransaction(Transaction& tr) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "ROLLBACK TRANSACTION "sv << tr.GetNsName(); });
		return impl_->RollBackTransaction(tr, rdxCtx);
	});
}
Error Reindexer::GetMeta(std::string_view nsName, const std::string& key, std::string& data) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(
			ctx_, [&](WrSerializer& s) { s << "SELECT META FROM "sv << nsName << " WHERE KEY = '"sv << key << '\''; });
		return impl_->GetMeta(nsName, key, data, rdxCtx);
	});
}
Error Reindexer::GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(
			ctx_, [&](WrSerializer& s) { s << "SELECT SHARDED META FROM "sv << nsName << " WHERE KEY = '"sv << key << '\''; });
		return impl_->GetMeta(nsName, key, data, rdxCtx);
	});
}
Error Reindexer::PutMeta(std::string_view nsName, const std::string& key, std::string_view data) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(
			ctx_, [&](WrSerializer& s) { s << "UPDATE "sv << nsName << " SET META = '"sv << data << "' WHERE KEY = '"sv << key << '\''; });
		return impl_->PutMeta(nsName, key, data, rdxCtx);
	});
}
Error Reindexer::EnumMeta(std::string_view nsName, std::vector<std::string>& keys) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "SELECT META FROM "sv << nsName; });
		return impl_->EnumMeta(nsName, keys, rdxCtx);
	});
}
Error Reindexer::DeleteMeta(std::string_view nsName, const std::string& key) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(
			ctx_, [&](WrSerializer& s) { s << "DELETE META FROM "sv << nsName << " WHERE KEY = '"sv << key << '\''; });
		return impl_->DeleteMeta(nsName, key, rdxCtx);
	});
}
Error Reindexer::Delete(const Query& q, QueryResults& result) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { q.GetSQL(s); }, result);
		return impl_->Delete(q, result, rdxCtx);
	});
}
Error Reindexer::ExecSQL(std::string_view query, QueryResults& result, unsigned proxyFetchLimit) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << query; }, result);
		return impl_->ExecSQL(query, result, proxyFetchLimit, rdxCtx);
	});
}
Error Reindexer::Select(const Query& q, QueryResults& result, unsigned proxyFetchLimit) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { q.GetSQL(s); }, result);
		return impl_->Select(q, result, proxyFetchLimit, rdxCtx);
	});
}
Error Reindexer::Update(const Query& q, QueryResults& result) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { q.GetSQL(s); }, result);
		return impl_->Update(q, result, rdxCtx);
	});
}
Error Reindexer::AddIndex(std::string_view nsName, const IndexDef& idx) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx =
			impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "CREATE INDEX "sv << idx.Name() << " ON "sv << nsName; });
		return impl_->AddIndex(nsName, idx, rdxCtx);
	});
}
Error Reindexer::SetSchema(std::string_view nsName, std::string_view schema) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "SET SCHEMA ON "sv << nsName; });
		return impl_->SetSchema(nsName, schema, rdxCtx);
	});
}
Error Reindexer::GetSchema(std::string_view nsName, int format, std::string& schema) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "GET SCHEMA ON "sv << nsName; });
		return impl_->GetSchema(nsName, format, schema, rdxCtx);
	});
}
Error Reindexer::UpdateIndex(std::string_view nsName, const IndexDef& idx) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx =
			impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "UPDATE INDEX "sv << idx.Name() << " ON "sv << nsName; });
		return impl_->UpdateIndex(nsName, idx, rdxCtx);
	});
}
Error Reindexer::DropIndex(std::string_view nsName, const IndexDef& index) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx =
			impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "DROP INDEX "sv << index.Name() << " ON "sv << nsName; });
		return impl_->DropIndex(nsName, index, rdxCtx);
	});
}
Error Reindexer::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "SELECT NAMESPACES"sv; });
		return impl_->EnumNamespaces(defs, opts, rdxCtx);
	});
}
Error Reindexer::GetProtobufSchema(WrSerializer& ser, std::vector<std::string>& namespaces) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "CREATE PROTOBUF SCHEMAS"sv; });
		return impl_->GetProtobufSchema(ser, namespaces);
	});
}
Error Reindexer::GetReplState(std::string_view nsName, ReplicationStateV2& state) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "GET repl_state FROM "sv << nsName; });
		return impl_->GetReplState(nsName, state, rdxCtx);
	});
}
Error Reindexer::SetClusterOperationStatus(std::string_view nsName, const ClusterOperationStatus& status) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "SET cluster_status FOR "sv << nsName; });
		return impl_->SetClusterOperationStatus(nsName, status, rdxCtx);
	});
}
Error Reindexer::GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "GET SNAPSHOT FROM " << nsName; });
		return impl_->GetSnapshot(nsName, opts, snapshot, rdxCtx);
	});
}
Error Reindexer::ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "APPLY SNAPSHOT RECORD ON " << nsName; });
		return impl_->ApplySnapshotChunk(nsName, ch, rdxCtx);
	});
}
Error Reindexer::SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response) noexcept {
	return callWithConnectCheck([&] { return impl_->SuggestLeader(suggestion, response); });
}
Error Reindexer::LeadersPing(const cluster::NodeData& leader) noexcept {
	return callWithConnectCheck([&] { return impl_->LeadersPing(leader); });
}
Error Reindexer::GetRaftInfo(cluster::RaftInfo& info) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [](WrSerializer&) {});
		return impl_->GetRaftInfo(info, rdxCtx);
	});
}
Error Reindexer::GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "SQL SUGGESTIONS"; });
		return impl_->GetSqlSuggestions(sqlQuery, pos, suggestions, rdxCtx);
	});
}
Error Reindexer::ClusterControlRequest(const ClusterControlRequestData& request) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "CLUSTER CONTROL REQUEST"; });
		return impl_->ClusterControlRequest(request);
	});
}
Error Reindexer::SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "SET TAGSMATCHER " << nsName; });
		return impl_->SetTagsMatcher(nsName, std::move(tm), rdxCtx);
	});
}
Error Reindexer::ShutdownCluster() noexcept {
	// No connection check required
	try {
		impl_->ShutdownCluster();
	}
	CATCH_AND_RETURN
	return {};
}
Error Reindexer::Status() noexcept {
	// No connection check required
	RETURN_RESULT_NOEXCEPT(impl_->Status());
}
Error Reindexer::Version(std::string& version) const noexcept {
	// No connection check required
	try {
		version = REINDEX_VERSION;
	}
	CATCH_AND_RETURN
	return {};
}

Error Reindexer::DumpIndex(std::ostream& os, std::string_view nsName, std::string_view index) noexcept {
	return callWithConnectCheck([&] {
		const auto rdxCtx = impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "DUMP INDEX " << index << " ON " << nsName; });
		return impl_->DumpIndex(os, nsName, index, rdxCtx);
	});
}

Error Reindexer::SubscribeUpdates(IEventsObserver& observer, EventSubscriberConfig&& cfg) noexcept {
	return callWithConnectCheck([&] { return impl_->SubscribeUpdates(observer, std::move(cfg)); });
}
Error Reindexer::UnsubscribeUpdates(IEventsObserver& observer) noexcept {
	// No connection check required
	RETURN_RESULT_NOEXCEPT(impl_->UnsubscribeUpdates(observer));
}

Error Reindexer::ShardingControlRequest(const sharding::ShardingControlRequestData& request,
										sharding::ShardingControlResponseData& response) noexcept {
	return callWithConnectCheck([&] {
		return impl_->ShardingControlRequest(request, response,
											 impl_->CreateRdxContext(ctx_, [&](WrSerializer& s) { s << "SHARDING CONTROL REQUEST"; }));
	});
}

static const Error kUnknowExceptioError(errSystem, "Unknown exception in Reindexer");

template <typename FnT, typename T>
RX_ALWAYS_INLINE T Reindexer::callWithConnectCheck(FnT&& f) noexcept {
	try {
		if (!impl_->IsConnected()) [[unlikely]] {
			return T(Error(errNotValid, "Reindexer is not connected. Call Connect() first"));
		}
		return f();
	} catch (std::exception& e) {
		return T(Error(e));
	} catch (...) {
		return T(kUnknowExceptioError);
	}
}

}  // namespace reindexer
