#pragma once

#include "cluster/sharding/sharding.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

class ParallelExecutor {
	struct ConnectionDataBase {
		ConnectionDataBase() = default;
		ConnectionDataBase(int _shardId) : shardId(_shardId) {}
		client::SyncCoroReindexer connection;
		const int shardId = -1;
	};

	template <typename R>
	struct ConnectionData : public ConnectionDataBase {
		ConnectionData() = default;
		ConnectionData(int _shardId) : ConnectionDataBase(_shardId) {}
		R results;
	};

public:
	ParallelExecutor(int localShardId) : localShardId_(localShardId) {}

	template <typename Func, typename FLocal, typename... Args>
	Error Exec(const RdxContext &rdxCtx, sharding::ConnectionsPtr connections, Func f, const FLocal &&local, Args &&...args) {
		std::condition_variable cv;
		std::mutex mtx;

		size_t clientCompl = 0;

		h_vector<std::pair<Error, int>, 8> clientErrors;

		int isLocalCall = 0;
		h_vector<ConnectionDataBase, 8> results;

		auto ward = rdxCtx.BeforeShardingProxy();

		size_t clientCount = countClientConnection(*connections.get());

		for (auto itr = connections->rbegin(); itr != connections->rend(); ++itr) {
			auto connection = *itr;
			int shardId = itr->ShardId();
			if (connection) {
				results.emplace_back(shardId);
				results.back().connection =
					connection->WithCompletion([clientCount, &clientCompl, &clientErrors, shardId, &mtx, &cv, this](const Error &err) {
						completionFunction(clientCount, clientCompl, clientErrors, shardId, mtx, cv, err);
					});

				Error err = std::invoke(f, results.back().connection, std::forward<Args>(args)...);

				if (!err.ok()) {
					std::lock_guard lck(mtx);
					clientErrors.emplace_back(std::move(err), shardId);
				}
			} else {
				Error err = local(std::forward<Args>(args)...);
				isLocalCall = 1;
				if (!err.ok()) {
					std::lock_guard lck(mtx);
					clientErrors.emplace_back(std::move(err), shardId);
				}
			}
		}
		if (clientCount) {
			std::unique_lock lck(mtx);
			cv.wait(lck, [&clientCompl, clientCount] { return clientCompl == clientCount; });
		}
		return createIntegralError(clientErrors, isLocalCall);
	}
	template <typename Func, typename FLocal, typename T, typename P, typename... Args>
	Error ExecCollect(const RdxContext &rdxCtx, sharding::ConnectionsPtr connections, Func f, const FLocal &&local, std::vector<T> &result,
					  P &&predicated, std::string_view nsName, Args &&...args) {
		std::condition_variable cv;
		std::mutex mtx;

		h_vector<std::pair<Error, int>, 8> clientErrors;
		size_t clientCompl = 0;

		std::string errString;

		std::deque<ConnectionData<std::vector<T>>> results;
		int isLocalCall = 0;

		auto ward = rdxCtx.BeforeShardingProxy();
		size_t clientCount = countClientConnection(*connections.get());
		for (auto itr = connections->rbegin(); itr != connections->rend(); ++itr) {
			auto connection = *itr;
			int shardId = itr->ShardId();

			if (connection) {
				results.push_back(ConnectionData<std::vector<T>>{});
				results.back().connection =
					connection->WithCompletion([clientCount, &clientCompl, &clientErrors, shardId, &mtx, &cv, this](const Error &err) {
						completionFunction(clientCount, clientCompl, clientErrors, shardId, mtx, cv, err);
					});
				Error err = std::invoke(f, results.back().connection, nsName, std::forward<Args>(args)..., results.back().results);
				if (!err.ok()) {
					std::lock_guard lck(mtx);
					clientErrors.emplace_back(std::move(err), shardId);
				}
			} else {
				results.push_back(ConnectionData<std::vector<T>>{});
				Error err = local(nsName, std::forward<Args>(args)..., results.back().results, localShardId_);
				isLocalCall = 1;
				if (!err.ok()) {
					std::lock_guard lck(mtx);
					clientErrors.emplace_back(std::move(err), shardId);
				}
			}
		}
		if (clientCount) {
			std::unique_lock lck(mtx);
			cv.wait(lck, [&clientCompl, clientCount] { return clientCompl == clientCount; });
			Error status = createIntegralError(clientErrors, clientCount + isLocalCall);
			if (!status.ok()) {
				return status;
			}
			for (const auto &nodeResult : results) {
				for (auto &&t : nodeResult.results) {
					if (predicated(t)) {
						result.emplace_back(std::move(t));
					}
				}
			}
			return status;
		}
		return createIntegralError(clientErrors, isLocalCall);
	}

	Error ExecSelect(const Query &query, QueryResults &result, const sharding::ConnectionsVector &connections,
					 const InternalRdxContext &ctx, const RdxContext &rdxCtx,
					 const std::function<Error(const Query &, LocalQueryResults &, const RdxContext &)> &&localAction);

private:
	Error createIntegralError(h_vector<std::pair<Error, int>, 8> &errors, size_t clientCount);
	void completionFunction(size_t clientCount, size_t &clientCompl, h_vector<std::pair<Error, int>, 8> &clientErrors, int shardId,
							std::mutex &mtx, std::condition_variable &cv, const Error &err);

	size_t countClientConnection(const sharding::ConnectionsVector &connections);

	const int localShardId_;
};

}  // namespace reindexer
