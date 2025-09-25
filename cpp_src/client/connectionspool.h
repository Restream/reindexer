#pragma once

#include "client/rpcclient.h"
#include "estl/shared_mutex.h"

namespace reindexer {
namespace client {

struct [[nodiscard]] ConnectionsPoolData {
	ConnectionsPoolData(size_t connCount, const ReindexerConfig& cfg, INamespaces::PtrT sharedNss) {
		assert(connCount);
		assert(sharedNss);
		sharedNamespaces = std::move(sharedNss);
		for (size_t i = 0; i < connCount; ++i) {
			clients.emplace_back(cfg, sharedNamespaces);
		}
	}

	std::deque<RPCClient> clients;
	INamespaces::PtrT sharedNamespaces;
};

template <typename CmdT>
class [[nodiscard]] Connection {
public:
	static constexpr auto kConnectionChSize = 100;
	Connection(RPCClient& _rx) : rx(_rx), cmdCh_(kConnectionChSize) {}

	template <typename U>
	void PushCmd(U&& obj) {
		++requests_;
		cmdCh_.push(std::forward<U>(obj));
	}
	std::pair<CmdT, bool> PopCmd() noexcept { return cmdCh_.pop(); }
	bool IsChOpened() const noexcept { return cmdCh_.opened(); }
	void CloseCh() noexcept { cmdCh_.close(); }
	void OnRequestDone() noexcept {
		assert(requests_);
		--requests_;
	}
	size_t Requests() const noexcept { return requests_; }

	RPCClient& rx;

private:
	coroutine::channel<CmdT> cmdCh_;
	size_t requests_ = 0;
};

template <typename CmdT>
class [[nodiscard]] ConnectionsPool {
public:
	ConnectionsPool(ConnectionsPoolData& data) noexcept : data_(data) {
		for (auto& c : data_.clients) {
			connections_.emplace_back(c);
		}
	}

	Connection<CmdT>& GetConn() noexcept {
		size_t idx = 0;
		size_t minReq = std::numeric_limits<size_t>::max();
		for (size_t i = 0; i < data_.clients.size(); ++i) {
			const auto req = connections_[i].Requests();
			if (minReq > req) {
				minReq = req;
				idx = i;
			}
		}
		return connections_[idx];
	}
	Connection<CmdT>& GetConn(size_t idx) noexcept { return connections_[idx]; }
	typename std::deque<Connection<CmdT>>::iterator begin() noexcept { return connections_.begin(); }
	typename std::deque<Connection<CmdT>>::const_iterator end() const noexcept { return connections_.cend(); }
	size_t Size() const noexcept { return data_.clients.size(); }

private:
	ConnectionsPoolData& data_;
	std::deque<Connection<CmdT>> connections_;
	size_t idx_ = 0;
};

}  // namespace client
}  // namespace reindexer
