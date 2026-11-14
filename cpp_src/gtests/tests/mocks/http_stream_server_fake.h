#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>
#include "estl/condition_variable.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "net/ev/ev.h"
#include "net/http/router.h"
#include "net/listener.h"

namespace reindexer_tests {

/// Byte 'i' of every test body is StreamPattern()[i % StreamPattern().size()].
/// The size of the pattern is not a power of two, so a lost or repeated part of the body breaks the match.
std::span<const char> StreamPattern();

/// Exception, which the factory and the stream handler throw.
/// HttpStatus has code 403, Error has code errNotFound (404 on the regular routes)
enum class [[nodiscard]] ExceptionKind { StdException, HttpStatus, Error };

/// Behavior of the stream handler for the requests with one name
struct [[nodiscard]] StreamBehavior {
	/// Preflight() returns 413 for a larger body
	static constexpr uint64_t kMaxBodySize = 10ull * 1024 * 1024 * 1024;
	std::optional<reindexer::net::http::HttpStatusCode> preflightStatus = std::nullopt;
	bool factoryThrows = false;
	bool preflightThrows = false;
	/// OnChunk() returns 406, when the handler has received more bytes than this value
	uint64_t rejectAfter = std::numeric_limits<uint64_t>::max();
	/// OnChunk() throws, when the handler has received more bytes than this value
	uint64_t throwAfter = std::numeric_limits<uint64_t>::max();
	std::optional<reindexer::net::http::HttpStatusCode> finishStatus = std::nullopt;
	bool finishThrows = false;
	/// The body is written into '<dir>/<name>.tmp', Finish() renames it into '<name>.bin', Abort() removes it
	bool writeToFile = false;
	/// Exception for factoryThrows, preflightThrows, throwAfter and finishThrows
	ExceptionKind exceptionKind = ExceptionKind::StdException;
	/// OnChunk() waits this time before it handles the first part of the body
	std::chrono::milliseconds firstChunkDelay{0};
	/// Preflight() writes a response and returns StatusOK, which the contract does not allow
	bool preflightWritesResponse = false;
};

/// What the stream handler of one request has received
struct [[nodiscard]] StreamRecord {
	/// "preflight", "finish", "abort:<reason>", "destroy"
	std::vector<std::string> events;
	uint64_t handlerId = 0;
	uint64_t received = 0;
	size_t chunks = 0;
	bool emptyChunk = false;
	/// The body differs from StreamPattern()
	bool patternMismatch = false;
	/// Copies of the request head, which the handler makes in Finish()
	std::string uri;
	std::string header;
	std::vector<std::string> urlParams;
};

/// HTTP server in a separate thread with streamed routes PUT '/stream/:name' (default idle timeout)
/// and PUT '/short/:name' (kShortIdleTimeout). Both routes use the same handler.
/// A middleware answers 401 without the header 'X-Token: secret'.
/// The response callback of the router throws for the requests with the name 'response_throws'.
/// The regular route GET '/big' answers with kBigResponseSize bytes.
/// Preflight() answers 403 without the header 'X-Role: owner', like a role check before the body.
class [[nodiscard]] HTTPStreamServerFake {
public:
	static constexpr std::chrono::milliseconds kShortIdleTimeout{500};
	static constexpr size_t kBigResponseSize = 64 * 1024 * 1024;

	explicit HTTPStreamServerFake(std::string fileDir) : fileDir_(std::move(fileDir)) {}
	HTTPStreamServerFake(const HTTPStreamServerFake&) = delete;
	HTTPStreamServerFake& operator=(const HTTPStreamServerFake&) = delete;
	~HTTPStreamServerFake() { Stop(); }

	/// @return empty string or the error of the listener start
	std::string Start(uint16_t port);
	void Stop();

	void SetBehavior(const std::string& name, const StreamBehavior& behavior);
	/// Waits until the handler of the request is destroyed and returns its record
	StreamRecord WaitDestroyed(const std::string& name);
	int FactoryCalls() const noexcept { return factoryCalls_; }
	const std::string& FileDir() const noexcept { return fileDir_; }

	std::unique_ptr<reindexer::net::http::IStreamHandler> CreateHandler(const reindexer::net::http::Request& req);
	int CheckToken(reindexer::net::http::Context& ctx);
	int BigResponse(reindexer::net::http::Context& ctx);

private:
	class StreamHandler;

	void onResponse(reindexer::net::http::Context& ctx);

	template <typename F>
	void update(const std::string& name, F&& f) {
		{
			reindexer::lock_guard lck(mtx_);
			f(records_[name]);
		}
		cond_.notify_all();
	}

	const std::string fileDir_;
	reindexer::net::http::Router router_;
	std::unique_ptr<reindexer::net::IListener> listener_;
	reindexer::net::ev::dynamic_loop loop_;
	reindexer::net::ev::async stop_;
	std::thread thread_;
	std::atomic<bool> ready_{false};
	std::atomic<bool> terminate_{false};
	std::atomic<int> factoryCalls_{0};
	std::atomic<uint64_t> nextHandlerId_{1};
	reindexer::mutex mtx_;
	reindexer::condition_variable cond_;
	std::map<std::string, StreamBehavior> behaviors_;
	std::map<std::string, StreamRecord> records_;
};

}  // namespace reindexer_tests
