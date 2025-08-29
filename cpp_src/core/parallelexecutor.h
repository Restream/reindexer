#pragma once

#include <deque>
#include "cluster/sharding/sharding.h"
#include "core/queryresults/queryresults.h"
#include "estl/lock.h"

namespace reindexer {

class [[nodiscard]] ParallelExecutor {
	struct [[nodiscard]] ConnectionDataBase {
		ConnectionDataBase() = default;
		ConnectionDataBase(int _shardId) : shardId(_shardId) {}
		client::Reindexer connection;
		const int shardId = -1;
	};

	template <typename R>
	struct [[nodiscard]] ConnectionData : public ConnectionDataBase {
		ConnectionData() = default;
		ConnectionData(int _shardId) : ConnectionDataBase(_shardId) {}
		R results;
	};

public:
	ParallelExecutor(int localShardId) : localShardId_(localShardId) {}

	template <typename Func, typename FLocal, typename... Args>
	Error Exec(const RdxContext& rdxCtx, sharding::ConnectionsPtr&& connections, const Func& f, const FLocal& local, Args&&... args) {
		condition_variable cv;
		mutex mtx;

		size_t clientCompl = 0;
		std::vector<std::pair<Error, int>> clientErrors;
		clientErrors.reserve(connections->size());
		int isLocalCall = 0;
		std::vector<ConnectionDataBase> results;
		results.reserve(connections->size());

		auto ward = rdxCtx.BeforeShardingProxy();

		size_t clientCount = countClientConnection(*connections.get());
		for (auto itr = connections->rbegin(); itr != connections->rend(); ++itr) {
			if (auto connection = *itr; connection) {
				const int shardId = itr->ShardId();
				auto& clientData = results.emplace_back(shardId);
				clientData.connection =
					connection
						->WithCompletion([clientCount, &clientCompl, &clientErrors, shardId, &mtx, &cv, this](const Error& err) {
							completionFunction(clientCount, clientCompl, clientErrors, shardId, mtx, cv, err);
						})
						.WithShardId(shardId, true);

				auto& conn = clientData.connection;
				auto invokeWrap = [&f, &conn](auto&&... args) { return std::invoke(f, conn, std::forward<decltype(args)>(args)...); };

				Error err;
				// check whether it is necessary to pass the ShardId to the function
				if constexpr (std::is_invocable_v<Func, client::Reindexer&, Args..., int>) {
					err = invokeWrap(std::forward<Args>(args)..., shardId);
				} else {
					err = invokeWrap(std::forward<Args>(args)...);
				}

				if (!err.ok()) {
					lock_guard lck(mtx);
					clientErrors.emplace_back(std::move(err), shardId);
				}
			} else {
				Error err = local(std::forward<Args>(args)...);
				isLocalCall = 1;
				if (!err.ok()) {
					lock_guard lck(mtx);
					clientErrors.emplace_back(std::move(err), localShardId_);
				}
			}
		}
		if (clientCount) {
			unique_lock lck(mtx);
			cv.wait(lck, [&clientCompl, clientCount] { return clientCompl == clientCount; });
		}
		return createIntegralError(clientErrors, isLocalCall);
	}
	template <typename ClientF, typename LocalF, typename T, typename Predicate, typename... Args>
	Error ExecCollect(const RdxContext& rdxCtx, sharding::ConnectionsPtr&& connections, const ClientF& clientF, const LocalF& local,
					  std::vector<T>& result, const Predicate& predicated, std::string_view nsName, Args&&... args) {
		condition_variable cv;
		mutex mtx;
		std::vector<std::pair<Error, int>> clientErrors;
		clientErrors.reserve(connections->size());
		size_t clientCompl = 0;
		std::deque<ConnectionData<std::vector<T>>> results;
		int isLocalCall = 0;
		auto ward = rdxCtx.BeforeShardingProxy();
		size_t clientCount = countClientConnection(*connections.get());
		for (auto itr = connections->rbegin(); itr != connections->rend(); ++itr) {
			if (auto connection = *itr; connection) {
				const int shardId = itr->ShardId();
				auto& clientData = results.emplace_back();
				clientData.connection =
					connection->WithCompletion([clientCount, &clientCompl, &clientErrors, shardId, &mtx, &cv, this](const Error& err) {
						completionFunction(clientCount, clientCompl, clientErrors, shardId, mtx, cv, err);
					});
				Error err = std::invoke(clientF, clientData.connection, nsName, std::forward<Args>(args)..., clientData.results);
				if (!err.ok()) {
					lock_guard lck(mtx);
					clientErrors.emplace_back(std::move(err), shardId);
				}
			} else {
				auto& localData = results.emplace_back();
				Error err = local(nsName, std::forward<Args>(args)..., localData.results, localShardId_);
				isLocalCall = 1;
				if (!err.ok()) {
					lock_guard lck(mtx);
					clientErrors.emplace_back(std::move(err), localShardId_);
				}
			}
		}
		if (clientCount) {
			unique_lock lck(mtx);
			cv.wait(lck, [&clientCompl, clientCount] { return clientCompl == clientCount; });
			Error status = createIntegralError(clientErrors, clientCount + isLocalCall);
			if (!status.ok()) {
				return status;
			}
			for (const auto& nodeResult : results) {
				for (auto&& t : nodeResult.results) {
					if (predicated(t)) {
						result.emplace_back(std::move(t));
					}
				}
			}
			return status;
		}
		return createIntegralError(clientErrors, isLocalCall);
	}

	Error ExecSelect(const Query& query, QueryResults& result, const sharding::ConnectionsVector& connections, const RdxContext& ctx,
					 std::function<Error(const Query&, LocalQueryResults&, const RdxContext&)>&& localAction);

private:
	Error createIntegralError(std::vector<std::pair<Error, int>>& errors, size_t clientCount);
	void completionFunction(size_t clientCount, size_t& clientCompl, std::vector<std::pair<Error, int>>& clientErrors, int shardId,
							mutex& mtx, condition_variable& cv, const Error& err);

	size_t countClientConnection(const sharding::ConnectionsVector& connections);

	const int localShardId_;
};

}  // namespace reindexer
