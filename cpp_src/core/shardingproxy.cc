#include "shardingproxy.h"
#include "parallelexecutor.h"

namespace reindexer {

void ShardingProxy::ShutdownCluster() {
	impl_.ShutdownCluster();
	if (isShardingInitialized()) {
		shardingRouter_->Shutdown();
	}
}

Error ShardingProxy::Connect(const std::string &dsn, ConnectOpts opts) {
	try {
		Error err = impl_.Connect(dsn, opts);
		if (!err.ok()) {
			return err;
		}
		// Expecting for the first time Connect is being called under exlusive lock.
		// And all the subsequent calls will be perfomed under shared locks.
		if (!isShardingInitialized() && impl_.GetShardingConfig()) {
			shardingRouter_ = std::make_shared<sharding::LocatorService>(impl_, *(impl_.GetShardingConfig()));
			err = shardingRouter_->Start();
			if (err.ok()) {
				shardingInitialized_.store(true, std::memory_order_release);
			} else {
				shardingRouter_.reset();
			}
		}
		return err;
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::OpenNamespace(std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts,
								   const RdxContext &ctx) {
	try {
		auto localOpen = [this, &ctx](std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts) {
			return impl_.OpenNamespace(nsName, opts, replOpts, ctx);
		};

		if (isWithSharding(nsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(ctx, &client::Reindexer::OpenNamespace, localOpen, nsName, opts, replOpts);
		}
		return localOpen(nsName, opts, replOpts);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::AddNamespace(const NamespaceDef &nsDef, const NsReplicationOpts &replOpts, const RdxContext &ctx) {
	try {
		auto localAdd = [this, &ctx](const NamespaceDef &nsDef, const NsReplicationOpts &replOpts) {
			return impl_.AddNamespace(nsDef, replOpts, ctx);
		};
		if (isWithSharding(nsDef.name, ctx)) {
			Error status = shardingRouter_->AwaitShards(ctx);
			if (!status.ok()) return status;

			auto connections = shardingRouter_->GetShardsConnections(status);
			if (!status.ok()) return status;
			for (auto &connection : *connections) {
				if (connection) {
					status = connection->AddNamespace(nsDef, replOpts);
				} else {
					status = localAdd(nsDef, replOpts);
				}
				if (!status.ok()) return status;
			}
			return status;
		}
		return localAdd(nsDef, replOpts);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::CloseNamespace(std::string_view nsName, const RdxContext &ctx) {
	try {
		auto localClose = [this, &ctx](std::string_view nsName) { return impl_.CloseNamespace(nsName, ctx); };

		if (isWithSharding(nsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;
			return delegateToShardsByNs(ctx, &client::Reindexer::CloseNamespace, localClose, nsName);
		}
		return localClose(nsName);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::DropNamespace(std::string_view nsName, const RdxContext &ctx) {
	try {
		auto localDrop = [this, &ctx](std::string_view nsName) { return impl_.DropNamespace(nsName, ctx); };

		if (isWithSharding(nsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(ctx, &client::Reindexer::DropNamespace, localDrop, nsName);
		}
		return localDrop(nsName);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::TruncateNamespace(std::string_view nsName, const RdxContext &ctx) {
	try {
		auto localTruncate = [this, &ctx](std::string_view nsName) { return impl_.TruncateNamespace(nsName, ctx); };
		if (isWithSharding(nsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(ctx, &client::Reindexer::TruncateNamespace, localTruncate, nsName);
		}
		return localTruncate(nsName);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const RdxContext &ctx) {
	try {
		auto localRename = [this, &ctx](std::string_view srcNsName, const std::string &dstNsName) {
			return impl_.RenameNamespace(srcNsName, dstNsName, ctx);
		};
		if (isWithSharding(srcNsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(ctx, &client::Reindexer::RenameNamespace, localRename, srcNsName, dstNsName);
		}
		return localRename(srcNsName, dstNsName);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::AddIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx) {
	try {
		auto localAddIndex = [this, &ctx](std::string_view nsName, const IndexDef &index) { return impl_.AddIndex(nsName, index, ctx); };

		if (isWithSharding(nsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(ctx, &client::Reindexer::AddIndex, localAddIndex, nsName, index);
		}
		return localAddIndex(nsName, index);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::UpdateIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx) {
	try {
		auto localUpdateIndex = [this, &ctx](std::string_view nsName, const IndexDef &index) {
			return impl_.UpdateIndex(nsName, index, ctx);
		};

		if (isWithSharding(nsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(ctx, &client::Reindexer::UpdateIndex, localUpdateIndex, nsName, index);
		}
		return localUpdateIndex(nsName, index);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::DropIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx) {
	try {
		auto localDropIndex = [this, &ctx](std::string_view nsName, const IndexDef &index) { return impl_.DropIndex(nsName, index, ctx); };
		if (isWithSharding(nsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(ctx, &client::Reindexer::DropIndex, localDropIndex, nsName, index);
		}
		return localDropIndex(nsName, index);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::SetSchema(std::string_view nsName, std::string_view schema, const RdxContext &ctx) {
	try {
		auto localSetSchema = [this, &ctx](std::string_view nsName, std::string_view schema) {
			return impl_.SetSchema(nsName, schema, ctx);
		};
		if (isWithSharding(nsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;

			return delegateToShards(ctx, &client::Reindexer::SetSchema, localSetSchema, nsName, schema);
		}
		return localSetSchema(nsName, schema);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Insert(std::string_view nsName, Item &item, const RdxContext &ctx) {
	try {
		auto insertFn = [this, &ctx](std::string_view nsName, Item &item) { return impl_.Insert(nsName, item, ctx); };

		if (isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Insert>(ctx, nsName, item, insertFn);
		}
		return insertFn(nsName, item);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Insert(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx) {
	try {
		auto insertFn = [this, &ctx](std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return impl_.Insert(nsName, item, qr, ctx);
		};

		result.SetQuery(nullptr);
		if (isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Insert>(ctx, nsName, item, result, insertFn);
		}
		return insertFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Update(std::string_view nsName, Item &item, const RdxContext &ctx) {
	try {
		auto updateFn = [this, &ctx](std::string_view nsName, Item &item) { return impl_.Update(nsName, item, ctx); };

		if (isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Update>(ctx, nsName, item, updateFn);
		}
		return updateFn(nsName, item);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Update(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx) {
	try {
		auto updateFn = [this, &ctx](std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return impl_.Update(nsName, item, qr, ctx);
		};

		result.SetQuery(nullptr);
		if (isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Update>(ctx, nsName, item, result, updateFn);
		}
		return updateFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Update(const Query &query, QueryResults &result, const RdxContext &ctx) {
	try {
		auto updateFn = [this](const Query &q, LocalQueryResults &qr, const RdxContext &ctx) { return impl_.Update(q, qr, ctx); };

		result.SetQuery(&query);
		if (isWithSharding(query, ctx)) {
			return executeQueryOnShard(query, result, 0, ctx, std::move(updateFn));
		}
		addEmptyLocalQrWithShardID(query, result);
		return updateFn(query, result.ToLocalQr(false), ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Upsert(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx) {
	try {
		auto upsertFn = [this, &ctx](std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return impl_.Upsert(nsName, item, qr, ctx);
		};

		result.SetQuery(nullptr);
		if (isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Upsert>(ctx, nsName, item, result, upsertFn);
		}
		return upsertFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Upsert(std::string_view nsName, Item &item, const RdxContext &ctx) {
	try {
		auto upsertFn = [this, &ctx](std::string_view nsName, Item &item) { return impl_.Upsert(nsName, item, ctx); };

		if (isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Upsert>(ctx, nsName, item, upsertFn);
		}
		return upsertFn(nsName, item);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Delete(std::string_view nsName, Item &item, const RdxContext &ctx) {
	try {
		auto deleteFn = [this, &ctx](std::string_view nsName, Item &item) { return impl_.Delete(nsName, item, ctx); };

		if (isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Delete>(ctx, nsName, item, deleteFn);
		}
		return deleteFn(nsName, item);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Delete(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx) {
	try {
		auto deleteFn = [this, &ctx](std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return impl_.Delete(nsName, item, qr, ctx);
		};

		result.SetQuery(nullptr);
		if (isWithSharding(nsName, ctx)) {
			return modifyItemOnShard<&client::Reindexer::Delete>(ctx, nsName, item, result, deleteFn);
		}
		return deleteFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Delete(const Query &query, QueryResults &result, const RdxContext &ctx) {
	try {
		auto deleteFn = [this](const Query &q, LocalQueryResults &qr, const RdxContext &ctx) { return impl_.Delete(q, qr, ctx); };

		result.SetQuery(&query);
		if (isWithSharding(query, ctx)) {
			return executeQueryOnShard(query, result, 0, ctx, std::move(deleteFn));
		}
		addEmptyLocalQrWithShardID(query, result);
		return deleteFn(query, result.ToLocalQr(false), ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::Select(std::string_view sql, QueryResults &result, unsigned proxyFetchLimit, const RdxContext &ctx) {
	try {
		Query query;
		query.FromSQL(sql);
		switch (query.type_) {
			case QuerySelect: {
				return Select(query, result, proxyFetchLimit, ctx);
			}
			case QueryDelete: {
				return Delete(query, result, ctx);
			}
			case QueryUpdate: {
				return Update(query, result, ctx);
			}
			case QueryTruncate: {
				return TruncateNamespace(query.Namespace(), ctx);
			}
			default:
				return Error(errLogic, "Incorrect sql type %d", query.type_);
		}
	} catch (const Error &err) {
		return err;
	}
}

Error ShardingProxy::Select(const Query &query, QueryResults &result, unsigned proxyFetchLimit, const RdxContext &ctx) {
	try {
		if (query.Type() != QuerySelect) {
			return Error(errLogic, "'Select' call request type is not equal to 'QuerySelect'.");
		}

		result.SetQuery(&query);
		if (isWithSharding(query, ctx)) {
			return executeQueryOnShard(
				query, result, proxyFetchLimit, ctx,
				[this](const Query &q, LocalQueryResults &qr, const RdxContext &ctx) { return impl_.Select(q, qr, ctx); });
		}
		addEmptyLocalQrWithShardID(query, result);
		return impl_.Select(query, result.ToLocalQr(false), ctx);
	} catch (const Error &err) {
		return err;
	}
}

Error ShardingProxy::Commit(std::string_view nsName, const RdxContext &ctx) {
	try {
		auto localCommit = [this](std::string_view nsName) { return impl_.Commit(nsName); };
		if (isWithSharding(nsName, ctx)) {
			Error err = shardingRouter_->AwaitShards(ctx);
			if (!err.ok()) return err;
			return delegateToShardsByNs(ctx, &client::Reindexer::Commit, localCommit, nsName);
		}
		return localCommit(nsName);
	} catch (Error &e) {
		return e;
	}
}

Transaction ShardingProxy::NewTransaction(std::string_view nsName, const RdxContext &ctx) {
	try {
		if (isWithSharding(nsName, ctx)) {
			return Transaction(impl_.NewTransaction(nsName, ctx), shardingRouter_);
		}
		return impl_.NewTransaction(nsName, ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::CommitTransaction(Transaction &tr, QueryResults &result, const RdxContext &ctx) {
	if (!tr.Status().ok()) {
		return Error(tr.Status().code(), "Unable to commit tx with error status: '%s'", tr.Status().what());
	}

	try {
		result.SetQuery(nullptr);
		return impl_.CommitTransaction(tr, result, isWithSharding(tr.GetNsName(), ctx), ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::GetMeta(std::string_view nsName, const std::string &key, std::vector<ShardedMeta> &data, const RdxContext &ctx) {
	try {
		auto localGetMeta = [this, &ctx](std::string_view nsName, const std::string &key, std::vector<ShardedMeta> &data, int shardId) {
			data.emplace_back(shardId, std::string());
			return impl_.GetMeta(nsName, key, data.back().data, ctx);
		};

		if (isWithSharding(nsName, ctx)) {
			auto predicate = [](const ShardedMeta &) noexcept { return true; };
			Error err = collectFromShardsByNs(
				ctx,
				static_cast<Error (client::Reindexer::*)(std::string_view, const std::string &, std::vector<ShardedMeta> &)>(
					&client::Reindexer::GetMeta),
				localGetMeta, data, predicate, nsName, key);
			return err;
		}

		return localGetMeta(nsName, key, data, ctx.ShardId());
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::PutMeta(std::string_view nsName, const std::string &key, std::string_view data, const RdxContext &ctx) {
	try {
		auto localPutMeta = [this, &ctx](std::string_view nsName, const std::string &key, std::string_view data) {
			return impl_.PutMeta(nsName, key, data, ctx);
		};

		if (isWithSharding(nsName, ctx)) {
			return delegateToShards(ctx, &client::Reindexer::PutMeta, localPutMeta, nsName, key, data);
		}
		return localPutMeta(nsName, key, data);
	} catch (Error &e) {
		return e;
	}
}

Error ShardingProxy::EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const RdxContext &ctx) {
	try {
		auto localEnumMeta = [this, &ctx](std::string_view nsName, std::vector<std::string> &keys, [[maybe_unused]] int shardId) {
			return impl_.EnumMeta(nsName, keys, ctx);
		};

		if (isWithSharding(nsName, ctx)) {
			fast_hash_set<std::string> allKeys;
			Error err = collectFromShardsByNs(
				ctx, &client::Reindexer::EnumMeta, localEnumMeta, keys,
				[&allKeys](const std::string &key) { return allKeys.emplace(key).second; }, nsName);

			return err;
		}
		return localEnumMeta(nsName, keys, ShardingKeyType::NotSetShard /*not used*/);
	} catch (Error &e) {
		return e;
	}
}

bool ShardingProxy::isSharderQuery(const Query &q) const {
	if (isSharded(q.Namespace())) {
		return true;
	}
	for (const auto &jq : q.joinQueries_) {
		if (isSharded(jq.Namespace())) {
			return true;
		}
	}
	for (const auto &mq : q.mergeQueries_) {
		if (isSharded(mq.Namespace())) {
			return true;
		}
	}
	return false;
}

bool ShardingProxy::isSharded(std::string_view nsName) const noexcept { return shardingRouter_->IsSharded(nsName); }

void ShardingProxy::calculateNewLimitOfsset(size_t count, size_t totalCount, unsigned &limit, unsigned &offset) {
	if (limit != UINT_MAX) {
		if (count >= limit) {
			limit = 0;
		} else {
			limit -= count;
		}
	}
	if (totalCount >= offset) {
		offset = 0;
	} else {
		offset -= totalCount;
	}
}

void ShardingProxy::addEmptyLocalQrWithShardID(const Query &query, QueryResults &result) {
	LocalQueryResults lqr;
	int actualShardId = ShardingKeyType::NotSharded;
	if (isShardingInitialized() && isSharderQuery(query) && !query.local_) {
		actualShardId = shardingRouter_->ActualShardId();
	}
	result.AddQr(std::move(lqr), actualShardId);
}

reindexer::client::Item ShardingProxy::toClientItem(std::string_view ns, client::Reindexer *connection, reindexer::Item &item) {
	assertrx(connection);
	Error err;
	reindexer::client::Item clientItem = connection->NewItem(ns);
	if (clientItem.Status().ok()) {
		err = clientItem.FromJSON(item.GetJSON());
		if (err.ok() && clientItem.Status().ok()) {
			clientItem.SetPrecepts(item.impl_->GetPrecepts());
			if (item.impl_->tagsMatcher().isUpdated()) {
				// Add new names missing in JSON from tm
				clientItem.impl_->addTagNamesFrom(item.impl_->tagsMatcher());
			}
			return clientItem;
		}
	}
	if (!err.ok()) throw err;
	throw clientItem.Status();
}

template <typename ClientF, typename LocalF, typename T, typename Predicate, typename... Args>
Error ShardingProxy::collectFromShardsByNs(const RdxContext &rdxCtx, const ClientF &clientF, const LocalF &localF, std::vector<T> &result,
										   const Predicate &predicate, std::string_view nsName, Args &&...args) {
	try {
		Error status;
		auto connections = shardingRouter_->GetShardsConnections(nsName, -1, status);
		if (!status.ok()) return status;

		ParallelExecutor execNodes(shardingRouter_->ActualShardId());
		return execNodes.ExecCollect(rdxCtx, std::move(connections), clientF, localF, result, predicate, nsName,
									 std::forward<Args>(args)...);
	} catch (const Error &err) {
		return err;
	}
}

template <typename Func, typename FLocal, typename... Args>
Error ShardingProxy::delegateToShards(const RdxContext &rdxCtx, const Func &f, const FLocal &local, Args &&...args) {
	Error status;
	try {
		auto connections = shardingRouter_->GetShardsConnections(status);
		if (!status.ok()) return status;

		ParallelExecutor execNodes(shardingRouter_->ActualShardId());
		return execNodes.Exec(rdxCtx, std::move(connections), f, local, std::forward<Args>(args)...);
	} catch (const Error &err) {
		return err;
	}

	return status;
}

template <typename Func, typename FLocal, typename... Args>
Error ShardingProxy::delegateToShardsByNs(const RdxContext &rdxCtx, const Func &f, const FLocal &local, std::string_view nsName,
										  Args &&...args) {
	Error status;
	try {
		auto connections = shardingRouter_->GetShardsConnections(nsName, -1, status);
		if (!status.ok()) return status;

		ParallelExecutor execNodes(shardingRouter_->ActualShardId());
		return execNodes.Exec(rdxCtx, std::move(connections), f, local, nsName, std::forward<Args>(args)...);
	} catch (const Error &err) {
		return err;
	}
	return status;
}

template <ShardingProxy::ItemModifyFT fn, typename LocalFT>
Error ShardingProxy::modifyItemOnShard(const RdxContext &ctx, std::string_view nsName, Item &item, const LocalFT &localFn) {
	try {
		Error status;
		auto connection = shardingRouter_->GetShardConnectionWithId(nsName, item, status);
		if (!status.ok()) {
			return status;
		}
		if (connection) {
			client::Item clientItem = toClientItem(nsName, connection.get(), item);
			if (!clientItem.Status().ok()) return clientItem.Status();
			const auto timeout = ctx.GetRemainingTimeout();
			if (timeout.has_value() && timeout->count() <= 0) {
				return Error(errTimeout, "Item modify request timeout");
			}
			auto conn = timeout.has_value() ? connection->WithContext(ctx.GetCancelCtx()).WithTimeout(*timeout)
											: connection->WithContext(ctx.GetCancelCtx());
			const auto ward = ctx.BeforeShardingProxy();
			status = (conn.*fn)(nsName, clientItem);
			if (!status.ok()) {
				return status;
			}
			*item.impl_ = ItemImpl(clientItem.impl_->Type(), clientItem.impl_->tagsMatcher());
			Error err = item.impl_->FromJSON(clientItem.GetJSON());
			assertrx(err.ok());
			item.setID(clientItem.GetID());
			item.setShardID(connection.ShardId());
			item.setLSN(clientItem.GetLSN());
			return status;
		}
		status = localFn(nsName, item);
		if (!status.ok()) {
			return status;
		}
		item.setShardID(shardingRouter_->ActualShardId());
	} catch (const Error &err) {
		return err;
	}
	return Error();
}

template <ShardingProxy::ItemModifyQrFT fn, typename LocalFT>
Error ShardingProxy::modifyItemOnShard(const RdxContext &ctx, std::string_view nsName, Item &item, QueryResults &result,
									   const LocalFT &localFn) {
	Error status;
	auto connection = shardingRouter_->GetShardConnectionWithId(nsName, item, status);
	if (!status.ok()) {
		return status;
	}
	if (connection) {
		client::Item clientItem = toClientItem(nsName, connection.get(), item);
		client::QueryResults qrClient(result.Flags(), 0, false);
		if (!clientItem.Status().ok()) return clientItem.Status();

		const auto timeout = ctx.GetRemainingTimeout();
		if (timeout.has_value() && timeout->count() <= 0) {
			return Error(errTimeout, "Item modify request timeout");
		}
		auto conn = timeout.has_value() ? connection->WithContext(ctx.GetCancelCtx()).WithTimeout(*timeout)
										: connection->WithContext(ctx.GetCancelCtx());
		const auto ward = ctx.BeforeShardingProxy();
		status = (conn.*fn)(nsName, clientItem, qrClient);
		if (!status.ok()) {
			return status;
		}
		result.AddQr(std::move(qrClient), connection.ShardId());
		return status;
	}
	result.AddQr(LocalQueryResults(), shardingRouter_->ActualShardId());
	status = localFn(nsName, item, result.ToLocalQr(false));
	if (!status.ok()) {
		return status;
	}
	return status;
}

template <typename LocalFT>
Error ShardingProxy::executeQueryOnShard(const Query &query, QueryResults &result, unsigned proxyFetchLimit, const RdxContext &ctx,
										 LocalFT &&localAction) noexcept {
	Error status;
	try {
		auto connectionsPtr = shardingRouter_->GetShardsConnectionsWithId(query, status);
		if (!status.ok()) return status;

		assertrx(connectionsPtr);
		sharding::ConnectionsVector &connections = *connectionsPtr;

		if (query.Type() == QueryUpdate && connections.size() != 1) {
			return Error(errLogic, "Update request can be executed on one node only.");
		}

		if (query.Type() == QueryDelete && connections.size() != 1) {
			return Error(errLogic, "Delete request can be executed on one node only.");
		}

		const bool isDistributedQuery = connections.size() > 1;
		if (!isDistributedQuery) {
			assert(connections.size() == 1);

			if (connections[0]) {
				const auto timeout = ctx.GetRemainingTimeout();
				if (timeout.has_value() && timeout->count() <= 0) {
					return Error(errTimeout, "Sharded request timeout");
				}

				auto connection =
					timeout.has_value()
						? connections[0]->WithShardingParallelExecution(false).WithContext(ctx.GetCancelCtx()).WithTimeout(*timeout)
						: connections[0]->WithShardingParallelExecution(false).WithContext(ctx.GetCancelCtx());
				client::QueryResults qrClient(result.Flags(), proxyFetchLimit, true);
				status = executeQueryOnClient(connection, query, qrClient, [](size_t, size_t) {});
				if (status.ok()) {
					result.AddQr(std::move(qrClient), connections[0].ShardId(), true);
				}

			} else {
				const auto shCtx = ctx.WithShardId(shardingRouter_->ActualShardId(), false);
				LocalQueryResults lqr;

				status = localAction(query, lqr, shCtx);
				if (status.ok()) {
					result.AddQr(std::move(lqr), shardingRouter_->ActualShardId(), true);
				}
			}
			return status;
		} else if (query.count == UINT_MAX && query.start == 0 && query.sortingEntries_.empty()) {
			ParallelExecutor exec(shardingRouter_->ActualShardId());
			return exec.ExecSelect(query, result, connections, ctx, std::forward<LocalFT>(localAction));
		} else {
			unsigned limit = query.count;
			unsigned offset = query.start;

			Query distributedQuery(query);
			if (!distributedQuery.sortingEntries_.empty()) {
				const auto ns = impl_.GetNamespacePtr(distributedQuery.Namespace(), ctx)->getMainNs();
				result.SetOrdering(distributedQuery, *ns, ctx);
			}
			if (distributedQuery.count != UINT_MAX && !distributedQuery.sortingEntries_.empty()) {
				distributedQuery.Limit(distributedQuery.start + distributedQuery.count);
			}
			if (distributedQuery.start != 0) {
				if (distributedQuery.sortingEntries_.empty()) {
					distributedQuery.ReqTotal();
				} else {
					distributedQuery.Offset(0);
				}
			}

			size_t strictModeErrors = 0;
			for (size_t i = 0; i < connections.size(); ++i) {
				if (connections[i]) {
					const auto timeout = ctx.GetRemainingTimeout();
					if (timeout.has_value() && timeout->count() <= 0) {
						return Error(errTimeout, "Sharded request timeout");
					}
					auto connection =
						timeout.has_value()
							? connections[i]
								  ->WithShardingParallelExecution(connections.size() > 1)
								  .WithContext(ctx.GetCancelCtx())
								  .WithTimeout(*timeout)
							: connections[i]->WithShardingParallelExecution(connections.size() > 1).WithContext(ctx.GetCancelCtx());
					client::QueryResults qrClient(result.Flags(), proxyFetchLimit, false);

					if (distributedQuery.sortingEntries_.empty()) {
						distributedQuery.Limit(limit);
						distributedQuery.Offset(offset);
					}
					status = executeQueryOnClient(connection, distributedQuery, qrClient,
												  [&limit, &offset, this](size_t count, size_t totalCount) {
													  calculateNewLimitOfsset(count, totalCount, limit, offset);
												  });
					if (status.ok()) {
						result.AddQr(std::move(qrClient), connections[i].ShardId(), (i + 1) == connections.size());
					}
				} else {
					assertrx(i == 0);
					const auto shCtx = ctx.WithShardId(shardingRouter_->ActualShardId(), true);
					LocalQueryResults lqr;
					status = localAction(distributedQuery, lqr, shCtx);
					if (status.ok()) {
						if (distributedQuery.sortingEntries_.empty()) {
							calculateNewLimitOfsset(lqr.Count(), lqr.TotalCount(), limit, offset);
						}
						result.AddQr(std::move(lqr), shardingRouter_->ActualShardId(), (i + 1) == connections.size());
					}
				}
				if (!status.ok()) {
					if (status.code() != errStrictMode || ++strictModeErrors == connections.size()) {
						return status;
					}
				}
				if (distributedQuery.calcTotal == ModeNoTotal && limit == 0 && (i + 1) != connections.size()) {
					result.RebuildMergedData();
					break;
				}
			}
		}
	} catch (const Error &e) {
		return e;
	}
	return Error{};
}

template <typename CalucalteFT>
Error ShardingProxy::executeQueryOnClient(client::Reindexer &connection, const Query &q, client::QueryResults &qrClient,
										  const CalucalteFT &limitOffsetCalc) {
	Error status;
	switch (q.Type()) {
		case QuerySelect: {
			status = connection.Select(q, qrClient);
			if (q.sortingEntries_.empty()) {
				limitOffsetCalc(qrClient.Count(), qrClient.TotalCount());
			}
			break;
		}
		case QueryUpdate: {
			status = connection.Update(q, qrClient);
			break;
		}
		case QueryDelete: {
			status = connection.Delete(q, qrClient);
			break;
		}
		default:
			std::abort();
	}
	return status;
}

}  // namespace reindexer
