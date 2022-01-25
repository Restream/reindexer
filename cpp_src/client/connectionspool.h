#pragma once

#include "client/cororpcclient.h"

namespace reindexer {
namespace client {

template <typename CmdT>
class Connection {
public:
	static constexpr auto kConnectionChSize = 100;
	Connection(const CoroReindexerConfig &cfg, Namespaces::PtrT nss) : rx(cfg, std::move(nss)), cmdCh_(kConnectionChSize) {}

	template <typename U>
	void PushCmd(U &&obj) {
		++requests_;
		cmdCh_.push(std::forward<U>(obj));
	}
	std::pair<CmdT *, bool> PopCmd() { return cmdCh_.pop(); }
	bool IsChOpened() const noexcept { return cmdCh_.opened(); }
	void CloseCh() { cmdCh_.close(); }
	void OnRequestDone() noexcept {
		assert(requests_);
		--requests_;
	}
	size_t Requests() const noexcept { return requests_; }

	CoroRPCClient rx;

private:
	coroutine::channel<CmdT *> cmdCh_;
	size_t requests_ = 0;
};

template <typename CmdT>
class ConnectionsPool {
public:
	ConnectionsPool(size_t connCount, const CoroReindexerConfig &cfg) {
		assert(connCount);
		for (size_t i = 0; i < connCount; ++i) {
			clients_.emplace_back(cfg, sharedNamespaces_);
		}
	}

	Connection<CmdT> &GetConn() noexcept {
		size_t idx = 0;
		size_t minReq = std::numeric_limits<size_t>::max();
		for (size_t i = 0; i < clients_.size(); ++i) {
			const auto req = clients_[i].Requests();
			if (minReq > req) {
				minReq = req;
				idx = i;
			}
		}
		return clients_[idx];
	}
	typename std::deque<Connection<CmdT>>::iterator begin() noexcept { return clients_.begin(); }
	typename std::deque<Connection<CmdT>>::const_iterator end() const noexcept { return clients_.cend(); }

private:
	std::deque<Connection<CmdT>> clients_;
	Namespaces::PtrT sharedNamespaces_ = make_intrusive<Namespaces::IntrusiveT>();
	size_t idx_ = 0;
};

}  // namespace client
}  // namespace reindexer
