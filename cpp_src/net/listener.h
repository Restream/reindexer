#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include "estl/mutex.h"
#include "iserverconnection.h"
#include "net/ev/ev.h"
#include "socket.h"
#include "vendor/sparse-map/sparse_set.h"

namespace reindexer {
namespace net {

class [[nodiscard]] IListener {
public:
	virtual ~IListener() = default;
	/// Bind listener to specified host:port
	/// @param addr - tcp host:port for bind or file path for the unix domain socket
	/// @param type - socket's type: tcp or unix
	virtual void Bind(std::string addr, socket_domain type) = 0;
	/// Stop synchronously stops listener
	virtual void Stop() noexcept = 0;
};

struct [[nodiscard]] ConnPtrEqual {
	using is_transparent = void;

	bool operator()(const IServerConnection* lhs, const std::unique_ptr<IServerConnection>& rhs) const noexcept { return lhs == rhs.get(); }
	bool operator()(const std::unique_ptr<IServerConnection>& lhs, const IServerConnection* rhs) const noexcept { return lhs.get() == rhs; }
	bool operator()(const std::unique_ptr<IServerConnection>& lhs, const std::unique_ptr<IServerConnection>& rhs) const noexcept {
		return lhs == rhs;
	}
};

struct [[nodiscard]] ConnPtrHash {
	using transparent_key_equal = ConnPtrEqual;

	size_t operator()(const IServerConnection* ptr) const noexcept { return std::hash<uintptr_t>()(uintptr_t(ptr)); }
	size_t operator()(const std::unique_ptr<IServerConnection>& ptr) const noexcept { return std::hash<uintptr_t>()(uintptr_t(ptr.get())); }
};

enum class [[nodiscard]] ListenerType {
	/// In shared mode each listener (except the first one) running in own detached thread and
	/// able to accept/handle connections by himself. Connections may migrate between listeners only
	/// after rebalance() call.
	/// This mode is more effective for short-term connections, but all the connections will be handled by shared thread pool.
	Shared,
	/// In mixed mode only the first listener is able to accept connections.
	/// When connection is accepted, this listener await the first client's message to check, if client has requested dedicated thread.
	/// If dedicated thread was requested new connection will be moved to new thread, otherwise
	/// new connection will be moved to one of the shared listeners.
	/// This mode is optimal for long-term connections and required to break request cycles in synchronous replications, but
	/// is not effective for short-term connections, because first client's message can not be handled in acceptor thread.
	Mixed
};

/// Network listener implementation
template <ListenerType LT>
class [[nodiscard]] Listener final : public IListener {
public:
	/// Constructs new listener object.
	/// @param loop - ev::loop of caller's thread, listener's socket will be bound to that loop.
	/// @param connFactory - Connection factory, will create objects with IServerConnection interface implementation.
	/// @param maxListeners - Maximum number of threads, which listener will utilize. std::thread::hardware_concurrency() by default
	Listener(ev::dynamic_loop& loop, ConnectionFactory&& connFactory, openssl::SslCtxPtr SslCtx, int maxListeners = 0);
	~Listener() override;
	Listener(const Listener&) = delete;
	Listener(Listener&&) = delete;
	/// Bind listener to specified host:port
	/// @param addr - tcp host:port for bind or file path for the unix domain socket
	/// @param type - socket's type: tcp or unix
	void Bind(std::string addr, socket_domain type) override;
	/// Stop synchronously stops listener
	void Stop() noexcept override;

private:
	void reserve_stack() noexcept;
	void io_accept(ev::io& watcher, int revents);
	void timeout_cb(ev::periodic& watcher, int);
	void async_cb(ev::async& watcher);
	void rebalance();
	void rebalance_from_acceptor();
	// Locks shared_->mtx_. Should not be called under external lock.
	void rebalance_conn(IServerConnection*, IServerConnection::BalancingType type);
	void run_dedicated_thread(std::unique_ptr<IServerConnection> conn) noexcept;
	// May lock shared_->mtx_. Should not be called under external lock.
	void startup_shared_thread();

	struct [[nodiscard]] Shared {
		class [[nodiscard]] Worker {
		public:
			Worker(std::shared_ptr<Shared> shared, std::unique_ptr<IServerConnection> conn);
			~Worker();
			Worker(Worker&& other) = delete;

			void Run();
			void SendTerminateAsync() noexcept { async_.send(); }

		private:
			ev::dynamic_loop loop_;
			std::unique_ptr<IServerConnection> conn_;
			ev::async async_;
			std::shared_ptr<Shared> shared_;
		};

		Shared(ConnectionFactory&& connFactory, int maxListeners, openssl::SslCtxPtr SslCtx);
		~Shared();
		openssl::SslCtxPtr sslCtx_;
		lst_socket sock_;
		const int maxListeners_;
		std::atomic<int> sharedListenersCount_{0};
		std::atomic<int> connsCountOnSharedListeners_{0};
		std::vector<Listener*> listeners_;
		mutex mtx_;
		ConnectionFactory connFactory_;
		std::atomic<bool> terminating_;
		std::string addr_;
		std::vector<std::unique_ptr<IServerConnection>> idle_;
		steady_clock_w::time_point ts_;
		std::vector<Worker*> dedicatedWorkers_;
	};
	class [[nodiscard]] ListeningThreadData {
	public:
		ListeningThreadData(std::shared_ptr<Shared> shared) : listener_(loop_, shared), shared_(std::move(shared)) { assertrx(shared_); }

		void Loop() {
			if constexpr (LT == ListenerType::Shared) {
				listener_.io_.start(shared_->sock_.fd(), ev::READ);
			}
			while (!shared_->terminating_) {
				loop_.run();
			}
		}
		Shared& GetShared() noexcept { return *shared_; }

	private:
		ev::dynamic_loop loop_;
		Listener listener_;
		std::shared_ptr<Shared> shared_;
	};
	// Locks shared_->mtx_. Should not be called under external lock.
	Listener(ev::dynamic_loop& loop, std::shared_ptr<Shared> shared);
	static void clone(std::unique_ptr<ListeningThreadData> d) noexcept;

	ev::io io_;
	ev::periodic timer_;
	ev::dynamic_loop& loop_;
	ev::async async_;
	std::shared_ptr<Shared> shared_;
	std::vector<std::unique_ptr<IServerConnection>> connections_;
	tsl::sparse_set<std::unique_ptr<IServerConnection>, ConnPtrHash, ConnPtrHash::transparent_key_equal> accepted_;
	uint64_t id_{0};
	bool isMainListener_{false};
};

/// Network listener implementation
class [[nodiscard]] ForkedListener final : public IListener {
public:
	/// Constructs new listener object.
	/// @param loop - ev::loop of caller's thread, listener's socket will be bound to that loop.
	/// @param connFactory - Connection factory, will create objects with IServerConnection interface implementation.
	ForkedListener(ev::dynamic_loop& loop, ConnectionFactory&& connFactory, openssl::SslCtxPtr SslCtx);
	~ForkedListener() override;
	/// Bind listener to specified host:port
	/// @param addr - tcp host:port for bind or file path for the unix domain socket
	/// @param type - socket's type: tcp or unix
	void Bind(std::string addr, socket_domain type) override;
	/// Stop synchronously stops listener
	void Stop() noexcept override;

protected:
	void io_accept(ev::io& watcher, int revents);
	void async_cb(ev::async& watcher);

	struct [[nodiscard]] Worker {
		Worker(std::unique_ptr<IServerConnection>&& conn, ev::async& async) : conn(std::move(conn)), async(&async) {}
		Worker(Worker&& other) noexcept : conn(std::move(other.conn)), async(other.async) {}
		Worker& operator=(Worker&& other) noexcept {
			if (&other != this) {
				conn = std::move(other.conn);
				async = other.async;
			}
			return *this;
		}

		std::unique_ptr<IServerConnection> conn;
		ev::async* async;
	};

	openssl::SslCtxPtr sslCtx_;
	lst_socket sock_;
	mutex mtx_;
	ConnectionFactory connFactory_;
	std::atomic<bool> terminating_{false};
	std::string addr_;

	ev::io io_;
	ev::dynamic_loop& loop_;
	ev::async async_;
	std::vector<Worker> workers_;
	std::atomic<int> runningThreadsCount_ = {0};
};

}  // namespace net
}  // namespace reindexer
