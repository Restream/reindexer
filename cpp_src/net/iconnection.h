#pragma once

#include <net/ev/ev.h>
#include <functional>

namespace reindexer {
namespace net {

/// Server side network connection interface for Listener.
class IConnection {
public:
	virtual ~IConnection() = default;
	/// Check if connection is finished.
	/// @return true if finished, false is still alive.
	virtual bool IsFinished() = 0;

	/// Restart connection
	/// @param fd - file descriptor of accepted connection.
	/// @return true - if successfuly restarted, false - if connection can't be restarted.
	virtual bool Restart(int fd) = 0;
	/// Reatrach connection to another listener loop
	/// @param loop - another loop to bind
	virtual void Reatach(ev::dynamic_loop &loop) = 0;
};

/// Functor factory type for creating new connection. Listener will call this factory after accept of client connection.
/// @param loop - Current loop of Listener's thread.
/// @param fd file  - Descriptor of accepted connection.
typedef std::function<IConnection *(ev::dynamic_loop &loop, int fd)> ConnectionFactory;

}  // namespace net
}  // namespace reindexer
