#pragma once

#include <net/ev/ev.h>
#include <functional>

namespace reindexer {
namespace net {

class socket;

/// Server side network connection interface for Listener.
class [[nodiscard]] IServerConnection {
public:
	enum class [[nodiscard]] BalancingType { NotSet, None, Shared, Dedicated };

	virtual ~IServerConnection() = default;
	/// Check if connection is finished.
	/// @return true if finished, false is still alive.
	virtual bool IsFinished() const noexcept = 0;

	/// Returns requested balancing type (if already set)
	/// @return balancing type, requested by this conn
	virtual BalancingType GetBalancingType() const noexcept = 0;

	/// Set callback, which move this connection into separate thread
	virtual void SetRebalanceCallback(std::function<void(IServerConnection*, BalancingType)> cb) = 0;

	/// @return true if this connection has pending data
	virtual bool HasPendingData() const noexcept = 0;
	/// Force pending data handling
	virtual void HandlePendingData() = 0;

	/// Restart connection
	/// @param s - socket of the accepted connection.
	/// @return true - if successfully restarted, false - if connection can't be restarted.
	virtual bool Restart(socket&& s) = 0;
	/// Attach connection to another listener loop. Must be called from thread of loop
	/// @param loop - another loop to bind
	virtual void Attach(ev::dynamic_loop& loop) = 0;
	/// Detach connection from listener loop. Must  be called from thread of current loop
	virtual void Detach() = 0;
};

/// Functor factory type for creating new connection. Listener will call this factory after accept of client connection.
/// @param loop - Current loop of Listener's thread.
/// @param s - Socket of the accepted connection.
/// @param allowCustomBalancing - true, if caller supports custom balancing hints
typedef std::function<IServerConnection*(ev::dynamic_loop& loop, socket&& s, bool allowCustomBalancing)> ConnectionFactory;

}  // namespace net
}  // namespace reindexer
