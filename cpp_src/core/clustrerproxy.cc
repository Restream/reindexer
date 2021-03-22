#include "clustrerproxy.h"
#include "core/reindexerimpl.h"
#include "core/type_consts.h"

using namespace reindexer;

template <typename Fn, Fn fn, typename FnL, FnL fnl, typename... Args>
Error ClusterProxy::funRWCall(const InternalRdxContext &_ctx, Args... args) {
	Error err;
	unsigned short sId = impl_.configProvider_.GetReplicationConfig().serverId;
	do {
		if (_ctx.LSN().isEmpty()) {
			cluster::RaftInfo info;
			err = impl_.GetRaftInfo(false, info, _ctx);
			if (!err.ok()) continue;
			if (info.role == cluster::RaftInfo::Role::None) {
				err = (impl_.*fn)(std::forward<Args>(args)..., _ctx);
				continue;
			}

			if (info.role == cluster::RaftInfo::Role::Follower) {
				std::shared_ptr<client::SyncCoroReindexer> leader = getLeader(_ctx);
				if (leader) {
					client::SyncCoroReindexer l = leader->WithLSN(_ctx.LSN()).WithServerId(sId);
					err = (l.*fnl)(std::forward<Args>(args)...);
				} else {
					err = Error(errLogic, "Can't get leader");
					continue;
				}
			} else if (info.role == cluster::RaftInfo::Role::Leader) {
				err = (impl_.*fn)(std::forward<Args>(args)..., _ctx);
			}
		} else {
			err = (impl_.*fn)(std::forward<Args>(args)..., _ctx);
		}
	} while (err.code() == errWrongReplicationData || err.code() == errUpdateReplication);
	return err;
}

template <typename Fn, Fn fn, typename FnL, FnL fnl>
Error ClusterProxy::funRWCall(string_view nsName, Item &item, const InternalRdxContext &ctx) {
	Error err;
	unsigned short sId = impl_.configProvider_.GetReplicationConfig().serverId;
	do {
		if (ctx.LSN().isEmpty()) {
			cluster::RaftInfo info;
			err = impl_.GetRaftInfo(false, info, ctx);
			if (!err.ok()) continue;
			if (info.role == cluster::RaftInfo::Role::None) {
				err = (impl_.*fn)(nsName, item, ctx);
				continue;
			}

			if (info.role == cluster::RaftInfo::Role::Follower) {
				std::shared_ptr<client::SyncCoroReindexer> leader = getLeader(ctx);
				if (leader) {
					client::Item clientItem = leader->NewItem(nsName);
					if (clientItem.Status().ok()) {
						auto jsonData = item.GetJSON();
						Error err = clientItem.FromJSON(jsonData);
						client::SyncCoroReindexer l = leader->WithLSN(ctx.LSN()).WithServerId(sId);
						err = (l.*fnl)(nsName, clientItem);
					} else {
						err = clientItem.Status();
					}
				} else {
					err = Error(errLogic, "Can't get leader");
					continue;
				}
			} else if (info.role == cluster::RaftInfo::Role::Leader) {
				err = (impl_.*fn)(nsName, item, ctx);
			}
		} else {
			err = (impl_.*fn)(nsName, item, ctx);
		}
	} while (err.code() == errWrongReplicationData || err.code() == errUpdateReplication);
	return err;
}

#define CallRWFun(Fn) \
	funRWCall<decltype(&ReindexerImpl::Fn), &ReindexerImpl::Fn, decltype(&client::SyncCoroReindexer::Fn), &client::SyncCoroReindexer::Fn>

template <typename Fn, Fn fn, typename FnL, FnL fnl>
Error ClusterProxy::funRWCall(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	Error err;
	unsigned short sId = impl_.configProvider_.GetReplicationConfig().serverId;
	do {
		if (ctx.LSN().isEmpty()) {
			cluster::RaftInfo info;
			err = impl_.GetRaftInfo(false, info, ctx);
			if (!err.ok()) continue;
			if (info.role == cluster::RaftInfo::Role::None) {
				err = (impl_.*fn)(query, result, ctx);
				continue;
			}

			if (info.role == cluster::RaftInfo::Role::Follower) {
				std::shared_ptr<client::SyncCoroReindexer> leader = getLeader(ctx);
				if (leader) {
					client::SyncCoroReindexer l = leader->WithLSN(ctx.LSN()).WithServerId(sId);
					client::SyncCoroQueryResults clientResults(l);
					err = (l.*fnl)(query, clientResults);
				} else {
					err = Error(errLogic, "Can't get leader");
					continue;
				}
			} else if (info.role == cluster::RaftInfo::Role::Leader) {
				err = (impl_.*fn)(query, result, ctx);
			}
		} else {
			err = (impl_.*fn)(query, result, ctx);
		}
	} while (err.code() == errWrongReplicationData || err.code() == errUpdateReplication);
	return err;
}

std::shared_ptr<client::SyncCoroReindexer> ClusterProxy::getLeader(const InternalRdxContext &ctx) {
	cluster::RaftInfo info;
	impl_.GetRaftInfo(false, info, ctx);
	if (info.leaderId != leaderId_) {
		leaderId_ = info.leaderId;
		std::string leaderDsn;
		Error err = impl_.GetLeaderDsn(leaderDsn);
		if (!err.ok()) {
			return std::shared_ptr<client::SyncCoroReindexer>();
		}
		if (err.ok() && leaderDsn.empty()) {
			return std::shared_ptr<client::SyncCoroReindexer>();
		}
		auto leader = std::make_shared<client::SyncCoroReindexer>();
		leader->Connect(leaderDsn);
		leader_ = leader;
	}
	return leader_;
}

ClusterProxy::ClusterProxy(IClientsStats *clientsStats) : impl_(clientsStats), leaderId_(-1) {}

Error ClusterProxy::Connect(const string &dsn, ConnectOpts opts) {
	Error err = impl_.Connect(dsn, opts);
	if (!err.ok()) {
		return err;
	}
	return err;
}

Error ClusterProxy::OpenNamespace(string_view nsName, const StorageOpts &opts, const InternalRdxContext &ctx) {
	return CallRWFun(OpenNamespace)(ctx, nsName, opts);
}

Error ClusterProxy::AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx) { return CallRWFun(AddNamespace)(ctx, nsDef); }
Error ClusterProxy::CloseNamespace(string_view nsName, const InternalRdxContext &ctx) { return CallRWFun(CloseNamespace)(ctx, nsName); }

Error ClusterProxy::DropNamespace(string_view nsName, const InternalRdxContext &ctx) { return CallRWFun(DropNamespace)(ctx, nsName); }
Error ClusterProxy::TruncateNamespace(string_view nsName, const InternalRdxContext &ctx) {
	return CallRWFun(TruncateNamespace)(ctx, nsName);
}
Error ClusterProxy::RenameNamespace(string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx) {
	return CallRWFun(RenameNamespace)(ctx, srcNsName, dstNsName);
}
Error ClusterProxy::AddIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return CallRWFun(AddIndex)(ctx, nsName, index);
}

Error ClusterProxy::UpdateIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return CallRWFun(UpdateIndex)(ctx, nsName, index);
}
Error ClusterProxy::DropIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return CallRWFun(DropIndex)(ctx, nsName, index);
}
Error ClusterProxy::SetSchema(string_view nsName, string_view schema, const InternalRdxContext &ctx) {
	return CallRWFun(SetSchema)(ctx, nsName, schema);
}
Error ClusterProxy::GetSchema(string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx) {
	return impl_.GetSchema(nsName, format, schema, ctx);
}
Error ClusterProxy::EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx) {
	return impl_.EnumNamespaces(defs, opts, ctx);
}

Error ClusterProxy::Insert(string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return CallRWFun(Insert)(nsName, item, ctx);  //
}

Error ClusterProxy::Update(string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return funRWCall<Error (ReindexerImpl::*)(string_view, Item &, const InternalRdxContext &ctx),
					 static_cast<Error (ReindexerImpl::*)(string_view, Item &, const InternalRdxContext &ctx)>(&ReindexerImpl::Update),
					 Error (client::SyncCoroReindexer::*)(string_view, client::Item &),
					 static_cast<Error (client::SyncCoroReindexer::*)(string_view, client::Item &)>(&client::SyncCoroReindexer::Update)>(
		nsName, item, ctx);
}
Error ClusterProxy::Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	return funRWCall<Error (ReindexerImpl::*)(const Query &, QueryResults &, const InternalRdxContext &ctx),
					 static_cast<Error (ReindexerImpl::*)(const Query &, QueryResults &, const InternalRdxContext &ctx)>(
						 &ReindexerImpl::Update),
					 Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
					 static_cast<Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &)>(
						 &client::SyncCoroReindexer::Update)>(query, result, ctx);
}
Error ClusterProxy::Upsert(string_view nsName, Item &item, const InternalRdxContext &ctx) { return CallRWFun(Upsert)(nsName, item, ctx); }
Error ClusterProxy::Delete(string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return funRWCall<Error (ReindexerImpl::*)(string_view, Item &, const InternalRdxContext &ctx),
					 static_cast<Error (ReindexerImpl::*)(string_view, Item &, const InternalRdxContext &ctx)>(&ReindexerImpl::Delete),
					 Error (client::SyncCoroReindexer::*)(string_view, client::Item &),
					 static_cast<Error (client::SyncCoroReindexer::*)(string_view, client::Item &)>(&client::SyncCoroReindexer::Delete)>(
		nsName, item, ctx);
}
Error ClusterProxy::Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	return funRWCall<Error (ReindexerImpl::*)(const Query &, QueryResults &, const InternalRdxContext &ctx),
					 static_cast<Error (ReindexerImpl::*)(const Query &, QueryResults &, const InternalRdxContext &ctx)>(
						 &ReindexerImpl::Delete),
					 Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
					 static_cast<Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &)>(
						 &client::SyncCoroReindexer::Delete)>(query, result, ctx);

	/*	cluster::RaftInfo info;
		Error err = impl_.GetRaftInfo(info, ctx);
		if (!err.ok()) return err;
		if (info.role == cluster::RaftInfo::Role::None) {
			err = impl_.Delete(query, result, ctx);
			return err;
		}
		if (ctx.LSN().isEmpty()) {
			if (info.role == cluster::RaftInfo::Role::Follower) {
				std::shared_ptr<client::SyncCoroReindexer> leader = getLeader(ctx);
				client::SyncCoroReindexer l = leader->WithLSN(ctx.LSN());
				client::SyncCoroQueryResults clientResults(l);
				err = l.Delete(query, clientResults);
				//			Query tempQ(query._namespace);
				//			tempQ.Limit(0);
				//			impl_.Select(tempQ, result, ctx);
				// result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, FieldsSet(ns_->tagsMatcher_, q.selectFilter_), ns_->schema_);
				for (auto it = clientResults.begin(); it != clientResults.end(); ++it) {
					auto item = it.GetItem();

					auto itCJson = item.GetCJSON();
					//				PayloadValue pv(itCJson.size(), reinterpret_cast<const unsigned char *>(itCJson.data()));
					//				pv.SetLSN(lsn_t());
					//				result.Add(ItemRef(-1, pv, 0, 0, true));

					Item sItemStack = impl_.NewItem(query._namespace);
					Item *sItem = new Item(std::move(sItemStack));
					err = sItem->FromCJSON(itCJson);
					sItem->setID(item.GetID());
					if (err.ok()) {
						result.AddItem(*sItem, true, true);
					}
				}

			} else if (info.role == cluster::RaftInfo::Role::Leader) {
				err = impl_.Delete(query, result, ctx);
				return err;
			}
		} else {  // not empry LSN from replication
			err = impl_.Delete(query, result, ctx);
			return err;
		}
		return err;

	*/
}
Error ClusterProxy::Select(string_view query, QueryResults &result, const InternalRdxContext &ctx) {
	auto trim = [](string_view q) -> string_view {
		unsigned int i = 0;
		for (i = 0; i < q.size(); i++) {
			if (!isspace(q[i])) break;
		}
		return q.substr(i);
	};
	bool isSelect = false;
	auto qTrim = trim(query);
	if (checkIfStartsWith("explain", qTrim)) {
		qTrim = trim(qTrim);
		if (checkIfStartsWith("select", qTrim)) {
			isSelect = true;
		}
	} else if (checkIfStartsWith("select", qTrim)) {
		isSelect = true;
	}
	Error err;
	if (isSelect) {
		Error e = impl_.Select(query, result, ctx);
		return e;
	} else {
		do {
			if (ctx.LSN().isEmpty()) {
				cluster::RaftInfo info;
				err = impl_.GetRaftInfo(false, info, ctx);
				if (!err.ok()) return err;
				if (info.role == cluster::RaftInfo::Role::None) {
					err = impl_.Select(query, result, ctx);
					continue;
				}
				unsigned short sId = impl_.configProvider_.GetReplicationConfig().serverId;

				if (info.role == cluster::RaftInfo::Role::Follower) {
					std::shared_ptr<client::SyncCoroReindexer> leader = getLeader(ctx);
					if (leader) {
						client::SyncCoroReindexer l = leader->WithLSN(ctx.LSN()).WithServerId(sId);
						client::SyncCoroQueryResults res(l);
						err = l.Select(query, res);
					} else {
						err = Error(errLogic, "Can't get leader");
					}
				} else if (info.role == cluster::RaftInfo::Role::Leader) {
					err = impl_.Select(query, result, ctx);
				}
			} else {
				err = impl_.Select(query, result, ctx);
			}
		} while (err.code() == errWrongReplicationData || err.code() == errUpdateReplication);
	}
	return err;
}
Error ClusterProxy::Select(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	if (query.Type() == QuerySelect) {
		return impl_.Select(query, result, ctx);
	}
	assert(false);
	return errOK;
}

Error ClusterProxy::Commit(string_view nsName) { return impl_.Commit(nsName); }

Item ClusterProxy::NewItem(string_view nsName, const InternalRdxContext &ctx) { return impl_.NewItem(nsName, ctx); }

Transaction ClusterProxy::NewTransaction(string_view nsName, const InternalRdxContext &ctx) { return impl_.NewTransaction(nsName, ctx); }
Error ClusterProxy::CommitTransaction(Transaction &tr, QueryResults &result, const InternalRdxContext &ctx) {
	return impl_.CommitTransaction(tr, result, ctx);
}
Error ClusterProxy::RollBackTransaction(Transaction &tr) { return impl_.RollBackTransaction(tr); }

Error ClusterProxy::GetMeta(string_view nsName, const string &key, string &data, const InternalRdxContext &ctx) {
	return impl_.GetMeta(nsName, key, data, ctx);
}
Error ClusterProxy::PutMeta(string_view nsName, const string &key, string_view data, const InternalRdxContext &ctx) {
	return CallRWFun(PutMeta)(ctx, nsName, key, data);
}
Error ClusterProxy::EnumMeta(string_view nsName, vector<string> &keys, const InternalRdxContext &ctx) {
	return impl_.EnumMeta(nsName, keys, ctx);
}
Error ClusterProxy::GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string> &suggestions, const InternalRdxContext &ctx) {
	return impl_.GetSqlSuggestions(sqlQuery, pos, suggestions, ctx);
}
Error ClusterProxy::Status() { return impl_.Status(); }
Error ClusterProxy::GetProtobufSchema(WrSerializer &ser, vector<string> &namespaces) { return impl_.GetProtobufSchema(ser, namespaces); }
Error ClusterProxy::GetReplState(string_view nsName, ReplicationStateV2 &state, const InternalRdxContext &ctx) {
	return impl_.GetReplState(nsName, state, ctx);
}

bool ClusterProxy::NeedTraceActivity() { return impl_.NeedTraceActivity(); }
Error ClusterProxy::EnableStorage(const string &storagePath, bool skipPlaceholderCheck, const InternalRdxContext &ctx) {
	return impl_.EnableStorage(storagePath, skipPlaceholderCheck, ctx);
}
Error ClusterProxy::InitSystemNamespaces() { return impl_.InitSystemNamespaces(); }
Error ClusterProxy::SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts) {
	return impl_.SubscribeUpdates(observer, filters, opts);
}
Error ClusterProxy::ApplySnapshotChunk(string_view nsName, const SnapshotChunk &ch, const InternalRdxContext &ctx) {
	return impl_.ApplySnapshotChunk(nsName, ch, ctx);
}

Error ClusterProxy::SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response) {
	return impl_.SuggestLeader(suggestion, response);
}
Error ClusterProxy::LeadersPing(const cluster::NodeData &leader) { return impl_.LeadersPing(leader); }

Error ClusterProxy::GetRaftInfo(cluster::RaftInfo &info, const InternalRdxContext &ctx) { return impl_.GetRaftInfo(true, info, ctx); }
Error ClusterProxy::UnsubscribeUpdates(IUpdatesObserver *observer) { return impl_.UnsubscribeUpdates(observer); }

Error ClusterProxy::CreateTemporaryNamespace(string_view baseName, std::string &resultName, const StorageOpts &opts,
											 const InternalRdxContext &ctx) {
	return impl_.CreateTemporaryNamespace(baseName, resultName, opts, ctx);
}

Error ClusterProxy::GetSnapshot(string_view nsName, lsn_t from, Snapshot &snapshot, const InternalRdxContext &ctx) {
	return impl_.GetSnapshot(nsName, from, snapshot, ctx);
}
