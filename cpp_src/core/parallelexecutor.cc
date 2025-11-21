#include "parallelexecutor.h"
#include <vector>

namespace reindexer {

Error ParallelExecutor::createIntegralError(std::vector<std::pair<Error, int>>& errors, size_t clientCount) {
	if (errors.empty()) {
		return {};
	}
	std::string descr;
	bool eqErr = true;
	const Error& firstErr = errors[0].first;
	unsigned errStrictModeCounter = 0;
	for (const auto& e : errors) {
		if (!(e.first == firstErr)) {
			eqErr = false;
		}
		if (e.first.code() == errStrictMode) {
			++errStrictModeCounter;
		}
		descr += fmt::format("[ shard:{} err:{} ]", e.second, e.first.what());
	}
	if (errStrictModeCounter == errors.size() && errors.size() < clientCount) {
		return {};
	}
	if (eqErr) {
		return errors[0].first;
	}

	return Error(errors[0].first.code(), descr);
}

Error ParallelExecutor::ExecSelect(const Query& query, QueryResults& result, const sharding::ConnectionsVector& connections,
								   const RdxContext& ctx,
								   std::function<Error(const Query&, LocalQueryResults&, const RdxContext&)>&& localAction) {
	condition_variable cv;
	mutex mtx;

	size_t clientCompl = 0;

	std::vector<std::pair<Error, int>> clientErrors;
	clientErrors.reserve(connections.size());
	bool isLocalCall = false;
	std::deque<ConnectionData<client::QueryResults>> clientResults;

	auto ward = ctx.BeforeShardingProxy();

	size_t clientCount = countClientConnection(connections);
	for (auto itr = connections.rbegin(); itr != connections.rend(); ++itr) {
		if (auto& connection = *itr; connection) {
			const int shardId = connection.ShardId();
			auto& clientData = clientResults.emplace_back(connection.ShardId());
			clientData.results = client::QueryResults{result.Flags()};
			clientData.connection =
				connection->WithShardingParallelExecution(connections.size() > 1)
					.WithCompletion([clientCount, &clientCompl, &clientErrors, shardId, &mtx, &cv, this](const Error& err) {
						completionFunction(clientCount, clientCompl, clientErrors, shardId, mtx, cv, err);
					})
					.WithContext(ctx.GetCancelCtx());

			Error err = clientData.connection.Select(query, clientData.results);
			if (!err.ok()) {
				lock_guard lck(mtx);
				clientErrors.emplace_back(std::move(err), shardId);
			}
		} else {
			const auto shCtx = ctx.WithShardId(localShardId_, true);
			LocalQueryResults lqr;
			Error status = localAction(query, lqr, shCtx);
			isLocalCall = true;
			if (status.ok()) {
				result.AddQr(std::move(lqr), localShardId_, false);
			} else {
				lock_guard lck(mtx);
				clientErrors.emplace_back(std::move(status), localShardId_);
			}
		}
	}
	if (clientCount) {
		unique_lock lck(mtx);
		cv.wait(lck, [&clientCompl, clientCount] { return clientCompl == clientCount; });
		Error status = createIntegralError(clientErrors, isLocalCall ? clientCount + 1 : clientCount);
		if (!status.ok()) {
			return status;
		}
		const auto shardingVersion = result.GetShardingConfigVersion();
		for (size_t i = 0; i < clientResults.size(); ++i) {
			bool hasError = false;
			auto& clientData = clientResults[i];
			for (auto& ep : clientErrors) {
				if (ep.second == clientData.shardId) {
					hasError = true;
					break;
				}
			}
			if (!hasError) [[likely]] {
				if (clientData.results.GetShardingConfigVersion() != shardingVersion) [[unlikely]] {
					return Error(errLogic,
								 "Distributed parallel query: local and remote sharding versions (source IDs) are different: {} vs {}",
								 shardingVersion, clientData.results.GetShardingConfigVersion());
				}
				result.AddQr(std::move(clientData.results), clientData.shardId, (i + 1) == clientResults.size());
			}
		}
		return status;
	}
	return createIntegralError(clientErrors, isLocalCall ? clientCount + 1 : clientCount);
}

void ParallelExecutor::completionFunction(size_t clientCount, size_t& clientCompl, std::vector<std::pair<Error, int>>& clientErrors,
										  int shardId, mutex& mtx, condition_variable& cv, const Error& err) {
	lock_guard lck(mtx);
	clientCompl++;
	if (!err.ok()) {
		clientErrors.emplace_back(err, shardId);
	}
	if (clientCompl == clientCount) {
		cv.notify_one();
	}
}

size_t ParallelExecutor::countClientConnection(const sharding::ConnectionsVector& connections) {
	size_t count = 0;
	for (auto itr = connections.begin(); itr != connections.end(); ++itr) {
		if (!itr->IsOnThisShard()) {
			count++;
		}
	}
	return count;
}

}  // namespace reindexer
