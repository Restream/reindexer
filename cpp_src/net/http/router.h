#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include "estl/h_vector.h"
#include "net/stat.h"
#include "tools/errors.h"
#include "tools/ssize_t.h"
#include "tools/stringstools.h"

namespace reindexer {
class chunk;
namespace net::http {

// Thanks to windows.h include
#ifdef DELETE
#undef DELETE
#endif

enum [[nodiscard]] HttpStatusCode {
	StatusContinue = 100,
	StatusOK = 200,
	StatusCreated = 201,
	StatusAccepted = 202,
	StatusNoContent = 204,
	StatusMovedPermanently = 301,
	StatusMovedTemporarily = 302,
	StatusNotModified = 304,
	StatusBadRequest = 400,
	StatusUnauthorized = 401,
	StatusForbidden = 403,
	StatusNotFound = 404,
	StatusMethodNotAllowed = 405,
	StatusNotAcceptable = 406,
	StatusRequestTimeout = 408,
	StatusLengthRequired = 411,
	StatusRequestEntityTooLarge = 413,
	StatusTooManyRequests = 429,
	StatusInternalServerError = 500,
	StatusNotImplemented = 501,
	StatusBadGateway = 502,
	StatusServiceUnavailable = 503,
	StatusGatewayTimeout = 504,
};

enum [[nodiscard]] HttpMethod : int {
	kMethodUnknown = -1,
	kMethodGET = 0,
	kMethodPOST,
	kMethodOPTIONS,
	kMethodHEAD,
	kMethodPUT,
	kMethodDELETE,
	kMethodPATCH,
	kMaxMethod,
};

typedef std::string_view UrlParam;

struct [[nodiscard]] HttpStatus : public std::exception {
	HttpStatus() noexcept : code_(StatusOK) {}
	HttpStatus(HttpStatusCode httpcode, std::string httpwhat) : code_(httpcode), what_(std::move(httpwhat)) {}
	explicit HttpStatus(const Error& err) : code_(errCodeToHttpStatus(err.code())), what_(err.whatStr()) {}

	const char* what() const noexcept override { return what_.c_str(); }
	const std::string& whatStr() const noexcept { return what_; }
	HttpStatusCode code() const noexcept { return code_; }
	static HttpStatusCode errCodeToHttpStatus(int errCode);

private:
	const HttpStatusCode code_;
	const std::string what_;
};

struct [[nodiscard]] Header {
	std::string_view name;
	std::string_view val;
};

struct [[nodiscard]] Param {
	Param() = default;
	Param(std::string_view n, std::string_view v) noexcept : name(n), val(v) {}

	const std::string_view name;
	const std::string_view val;
};

class [[nodiscard]] Headers final : public h_vector<Header, 16> {
public:
	using h_vector::h_vector;
	std::string_view Get(std::string_view name) const noexcept {
		auto it = std::find_if(begin(), end(), [=](const Header& hdr) { return iequals(name, hdr.name); });
		return it != end() ? it->val : std::string_view();
	}
};

class [[nodiscard]] Params final : public h_vector<Param, 8> {
public:
	using h_vector::h_vector;
	std::string_view Get(std::string_view name, std::string_view defVal = std::string_view()) const {
		auto it = std::find_if(begin(), end(), [=](const Param& param) { return name == param.name; });
		return it != end() ? it->val : defVal;
	}
};

struct [[nodiscard]] Request {
	std::string clientAddr;
	std::string_view uri;
	std::string_view path;
	std::string_view method;

	Headers headers;
	Params params;
	h_vector<UrlParam, 4> urlParams;

	size_t size{0};
};

class [[nodiscard]] Writer {
public:
	enum class [[nodiscard]] WriteMode { Default = 0, PreChunkedBody = 1 };
	virtual void Write(chunk&& ch, WriteMode mode = WriteMode::Default) = 0;
	virtual void Write(std::string_view data) = 0;
	virtual chunk GetChunk() = 0;

	virtual void SetHeader(const Header& hdr) = 0;
	virtual void SetRespCode(int code) = 0;
	virtual void SetContentLength(size_t len) = 0;

	virtual int RespCode() = 0;
	virtual ssize_t Written() = 0;
	virtual ~Writer() = default;
};

class [[nodiscard]] Reader {
public:
	virtual ssize_t Read(void* buf, size_t size) = 0;
	virtual std::string Read(size_t size = INT_MAX) = 0;
	virtual ssize_t Pending() const = 0;
	virtual ~Reader() = default;
};

struct [[nodiscard]] ClientData {
	virtual ~ClientData() = default;
};

static constexpr std::string_view kGzSuffix(".gz");

struct [[nodiscard]] Context {
	int JSON(int code, std::string_view slice) const;
	int JSON(int code, chunk&& chunk) const;
	int CSV(int code, chunk&& chunk) const;
	void MSGPACK(int code, std::string_view slice) const;
	int MSGPACK(int code, chunk&& chunk) const;
	void Protobuf(int code, std::string_view slice) const;
	int Protobuf(int code, chunk&& chunk) const;
	int String(int code, std::string_view slice) const;
	int String(int code, chunk&& chunk) const;
	int File(int code, std::string_view path, std::string_view data, bool isGzip, bool withCache) const;
	int Redirect(std::string_view url) const;

	Request* request{nullptr};
	Writer* writer{nullptr};
	Reader* body{nullptr};
	std::unique_ptr<ClientData> clientData;

	Stat stat;
};

/// Reason of the IStreamHandler::Abort() call
enum class [[nodiscard]] StreamAbortReason {
	ConnectionClosed,  ///< Connection was closed before the whole body was received
	ChunkRejected,	   ///< OnChunk() returned an error status
	HandlerException,  ///< OnChunk() or Finish() threw an exception, or Preflight() wrote a response
	Timeout,		   ///< No part of the body came during the idle timeout of the route
};

/// Handler of one request with a streamed body.
/// The route factory creates a separate handler for each request. The connection owns the handler until the request ends.
/// After a successful Preflight() the handler gets the parts of the body in OnChunk(). When the whole body is received,
/// Finish() is called. A return from Finish() ends the request without Abort(). If the request ends earlier or Finish() throws,
/// Abort() is called once. Nothing is called after a refusal from Preflight(), after a return from Finish() or after Abort().
/// The calls are never concurrent. They may come from different threads, because a connection may move between listener threads.
/// A method returns StatusOK or an error code of 400 or more. Other codes give 500: the response always has a body, and 200 is
/// the only success code, like on the regular routes.
/// An exception from the factory or from a method gives the same status as on the regular routes: the code of a thrown HttpStatus
/// or Error, 500 for other exceptions.
class [[nodiscard]] IStreamHandler {
public:
	virtual ~IStreamHandler() = default;

	/// Checks the request before the body is read. Pending 'Expect: 100-continue' is answered after this call.
	/// The handler may acquire resources for the whole upload here.
	/// @param ctx - request context after the middlewares. The context, its request and the request head bytes stay valid
	/// until the handler is destroyed. The handler must not write a response with ctx.
	/// @param contentLength - body size from the 'Content-Length' header
	/// @return StatusOK to receive the body. Any other status is sent to the client instead of reading the body.
	/// Before such a return the handler must release everything it acquired: Abort() is not called after a refusal.
	virtual HttpStatus Preflight(Context& ctx, uint64_t contentLength) = 0;
	/// Receives the next part of the body. The next part is not read from the socket until this call returns.
	/// @param chunk - non-empty part of the body. The memory is valid only during the call.
	/// @return StatusOK to continue. Any other status leads to Abort(), the status is sent to the client
	/// and the connection is closed without reading the rest of the body. An exception has the same effect.
	virtual HttpStatus OnChunk(std::span<const char> chunk) = 0;
	/// Called once, after exactly 'contentLength' bytes were passed to OnChunk().
	/// @return final status of the request. Abort() is not called after a return from this method, whatever the status is.
	/// If the method throws, Abort() is called and the client gets the status of the exception.
	virtual HttpStatus Finish() = 0;
	/// Called once if the request ends after a successful Preflight() without a return from Finish().
	/// Must release all the request resources, e.g. remove a partially written file.
	virtual void Abort(StreamAbortReason reason) noexcept = 0;
};

class ServerConnection;

/// Http router implementation.
class [[nodiscard]] Router final {
	friend class ServerConnection;

public:
	/// Default idle timeout of a streamed request body
	static constexpr std::chrono::seconds kDefaultStreamIdleTimeout{60};

	/// Add handler for http POST method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context&)>
	void POST(const char* path, K* object) {
		addRoute<K, func>(kMethodPOST, path, object);
	}
	/// Add handler for http GET method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context&)>
	void GET(const char* path, K* object) {
		addRoute<K, func>(kMethodGET, path, object);
	}
	/// Add handler for http OPTIONS method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context&)>
	void OPTIONS(const char* path, K* object) {
		addRoute<K, func>(kMethodOPTIONS, path, object);
	}
	template <class K, int (K::*func)(Context&)>
	/// Add handler for http DELETE method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	void DELETE(const char* path, K* object) {
		addRoute<K, func>(kMethodDELETE, path, object);
	}
	/// Add handler for http PUT method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context&)>
	void PUT(const char* path, K* object) {
		addRoute<K, func>(kMethodPUT, path, object);
	}
	/// Add handler for http PUT method with a streamed request body.
	/// The body is not buffered: the factory creates a separate handler for each request, and the handler receives the body
	/// in chunks. The request must have exactly one valid 'Content-Length' header and no 'Transfer-Encoding' header.
	/// The request size limit of the connection applies to the request head only: the handler checks the body size in Preflight().
	/// Streamed routes are matched before the routes of the same method, which were added with PUT().
	/// @param path - URI pattern
	/// @param object - factory class object
	/// @param idleTimeout - if no part of the body comes during this time, Abort() is called, the client gets 408 and the connection
	/// is closed. Time in the handler is not counted. Zero disables the timeout
	/// @tparam func - factory. It is called after the middlewares and may be called from different threads at the same time
	template <class K, std::unique_ptr<IStreamHandler> (K::*func)(const Request&)>
	void PUTStream(const char* path, K* object, std::chrono::milliseconds idleTimeout = kDefaultStreamIdleTimeout) {
		addStreamRoute<K, func>(kMethodPUT, path, object, idleTimeout);
	}
	/// Add handler for http PATCH method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context&)>
	void PATCH(const char* path, K* object) {
		addRoute<K, func>(kMethodPATCH, path, object);
	}
	/// Add handler for http HEAD method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context&)>
	void HEAD(const char* path, K* object) {
		addRoute<K, func>(kMethodHEAD, path, object);
	}
	/// Add middleware
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context&)>
	void Middleware(K* object) {
		middlewares_.emplace_back(func_wrapper<K, func>, object);
	}
	/// Add logger for requests
	/// @param object - logger class object
	/// @tparam func - logger
	template <class K, void (K::*func)(Context&)>
	void Logger(K* object) {
		logger_ = [=](Context& ctx) { (static_cast<K*>(object)->*func)(ctx); };
	}

	/// Add default handler for not found URI's
	/// @tparam func - handler
	template <class K, int (K::*func)(Context&)>
	void NotFound(K* object) {
		notFoundHandler_ = Handler{func_wrapper<K, func>, object};
	}

	/// Set response sent notifier
	/// @param object class object
	/// @param func function, to be called on response sent
	template <class K>
	void OnResponse(K* object, void (K::*func)(Context& ctx)) {
		onResponse_ = [=](Context& ctx) { (static_cast<K*>(object)->*func)(ctx); };
	}

protected:
	struct [[nodiscard]] StreamRoute {
		using FactoryT = std::unique_ptr<IStreamHandler> (*)(void* obj, const Request& req);
		StreamRoute(std::string&& p, FactoryT f, void* o, std::chrono::milliseconds t) noexcept
			: path(std::move(p)), factory(f), object(o), idleTimeout(t) {}

		std::unique_ptr<IStreamHandler> CreateHandler(const Request& req) const { return factory(object, req); }

		const std::string path;
		const FactoryT factory;
		void* const object;
		const std::chrono::milliseconds idleTimeout;
	};

	int handle(Context& ctx);
	void log(Context& ctx) {
		if (logger_) {
			logger_(ctx);
		}
	}
	int runMiddlewares(Context& ctx);
	/// @return streamed route for the request or nullptr, if the request is handled by the regular routes
	const StreamRoute* findStreamRoute(std::string_view method, std::string_view path) const;
	/// Matches the URL path with the route pattern and fills urlParams with the values of the pattern parameters.
	/// On mismatch urlParams keeps the parameters, which were found before the mismatch.
	static bool matchRoute(std::string_view route, std::string_view url, h_vector<UrlParam, 4>& urlParams);

	template <class K, int (K::*func)(Context&)>
	void addRoute(HttpMethod method, const char* path, K* object) {
		routes_[method].emplace_back(path, Handler{func_wrapper<K, func>, object});
	}
	template <class K, std::unique_ptr<IStreamHandler> (K::*func)(const Request&)>
	void addStreamRoute(HttpMethod method, const char* path, K* object, std::chrono::milliseconds idleTimeout) {
		streamRoutes_[method].emplace_back(path, factory_wrapper<K, func>, object, idleTimeout);
		hasStreamRoutes_ = true;
	}

	template <class K, int (K::*func)(Context& ctx)>
	static int func_wrapper(void* obj, Context& ctx) {
		return (static_cast<K*>(obj)->*func)(ctx);
	}
	template <class K, std::unique_ptr<IStreamHandler> (K::*func)(const Request&)>
	static std::unique_ptr<IStreamHandler> factory_wrapper(void* obj, const Request& req) {
		return (static_cast<K*>(obj)->*func)(req);
	}

	struct [[nodiscard]] Handler {
		using FuncT = std::function<int(void* obj, Context& ctx)>;
		Handler(FuncT&& f, void* o) noexcept : func(std::move(f)), object(o) {}

		FuncT func;
		void* object{nullptr};
	};

	struct [[nodiscard]] Route {
		Route(std::string&& p, Handler&& _h) : path(std::move(p)), h(std::move(_h)) {}

		const std::string path;
		const Handler h;
	};

	std::vector<Route> routes_[kMaxMethod];
	std::vector<StreamRoute> streamRoutes_[kMaxMethod];
	bool hasStreamRoutes_ = false;
	std::vector<Handler> middlewares_;

	Handler notFoundHandler_{{}, nullptr};
	std::function<void(Context& ctx)> logger_;
	std::function<void(Context& ctx)> onResponse_;
};
}  // namespace net::http
}  // namespace reindexer
