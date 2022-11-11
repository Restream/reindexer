#include "parallelexecutor.h"
#include <vector>

namespace reindexer {

Error ParallelExecutor::createIntegralError(h_vector<std::pair<Error, int>, 8> &errors, size_t clientCount) {
	if (errors.empty()) {
		return errOK;
	}
	std::string descr;
	bool eqErr = true;
	const Error &firstErr = errors[0].first;
	unsigned errStrictModeCounter = 0;
	for (const auto &e : errors) {
		if (!(e.first == firstErr)) {
			eqErr = false;
		}
		if (e.first.code() == errStrictMode) {
			errStrictModeCounter++;
		}
		descr += fmt::format("[ shard:{} err:{} ]", e.second, e.first.what());
	}
	if (errStrictModeCounter == errors.size() && errors.size() < clientCount) {
		return errOK;
	}
	if (eqErr) {
		return errors[0].first;
	}

	return Error(errors[0].first.code(), descr);
}

Error ParallelExecutor::ExecSelect(const Query &query, QueryResults &result, const sharding::ConnectionsVector &connections,
								   const InternalRdxContext &ctx, const RdxContext &rdxCtx,
								   const std::function<Error(const Query &, LocalQueryResults &, const RdxContext &)> &&localAction) {
	InternalRdxContext shardingCtx(ctx);
	shardingCtx.SetShardingParallelExecution(true);
	std::condition_variable cv;
	std::mutex mtx;

	size_t clientCompl = 0;
	std::string errStringCompl;

	h_vector<std::pair<Error, int>, 8> clientErrors;
	bool isLocalCall = false;
	std::deque<ConnectionData<client::QueryResults>> clientResults;

	auto ward = rdxCtx.BeforeShardingProxy();

	size_t clientCount = countClientConnection(connections);

	for (auto itr = connections.rbegin(); itr != connections.rend(); ++itr) {
		auto &connection = *itr;
		int shardId = connection.ShardId();
		if (connection) {
			clientResults.emplace_back(connection.ShardId());
			clientResults.back().results = client::QueryResults{result.Flags()};
			clientResults.back().connection =
				connection->WithShardingParallelExecution(connections.size() > 1)
					.WithCompletion([clientCount, &clientCompl, &clientErrors, shardId, &mtx, &cv, this](const Error &err) {
						completionFunction(clientCount, clientCompl, clientErrors, shardId, mtx, cv, err);
					})
					.WithContext(shardingCtx.DeadlineCtx());

			Error err = clientResults.back().connection.Select(query, clientResults.back().results);
			if (!err.ok()) {
				std::lock_guard lck(mtx);
				clientErrors.emplace_back(std::move(err), shardId);
			}

		} else {
			shardingCtx.WithShardId(localShardId_, connections.size() > 1);
			LocalQueryResults lqr;
			Error status = localAction(query, lqr, rdxCtx);
			isLocalCall = true;
			if (status.ok()) {
				result.AddQr(std::move(lqr), localShardId_, false);
			} else {
				std::lock_guard lck(mtx);
				clientErrors.emplace_back(std::move(status), shardId);
			}
		}
	}
	if (clientCount) {
		std::unique_lock lck(mtx);
		cv.wait(lck, [&clientCompl, clientCount] { return clientCompl == clientCount; });
		Error status = createIntegralError(clientErrors, isLocalCall ? clientCount + 1 : clientCount);
		if (!status.ok()) {
			return status;
		}
		for (size_t i = 0; i < clientResults.size(); ++i) {
			result.AddQr(std::move(clientResults[i].results), clientResults[i].shardId, (i + 1) == clientResults.size());
		}
		return status;
	}
	return createIntegralError(clientErrors, isLocalCall ? clientCount + 1 : clientCount);
}

void ParallelExecutor::completionFunction(size_t clientCount, size_t &clientCompl, h_vector<std::pair<Error, int>, 8> &clientErrors,
										  int shardId, std::mutex &mtx, std::condition_variable &cv, const Error &err) {
	std::lock_guard lck(mtx);
	clientCompl++;
	if (!err.ok()) {
		clientErrors.push_back(std::make_pair(err, shardId));
	}
	if (clientCompl == clientCount) {
		cv.notify_one();
	}
};

size_t ParallelExecutor::countClientConnection(const sharding::ConnectionsVector &connections) {
	size_t count = 0;
	for (auto itr = connections.begin(); itr != connections.end(); ++itr) {
		if (!itr->IsOnThisShard()) count++;
	}
	return count;
}

}  // namespace reindexer
