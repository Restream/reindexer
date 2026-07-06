#pragma once

#include <algorithm>
#include <functional>
#include <memory>
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

struct [[nodiscard]] HttpStatus {
	HttpStatus() noexcept : code(StatusOK) {}
	HttpStatus(HttpStatusCode httpcode, std::string httpwhat) : code(httpcode), what(std::move(httpwhat)) {}
	explicit HttpStatus(const Error& err) : code(errCodeToHttpStatus(err.code())), what(err.whatStr()) {}

	const HttpStatusCode code;
	const std::string what;

	static HttpStatusCode errCodeToHttpStatus(int errCode);
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
	std::string_view Get(std::string_view name) noexcept {
		auto it = std::find_if(begin(), end(), [=](const Header& hdr) { return iequals(name, hdr.name); });
		return it != end() ? it->val : std::string_view();
	}
};

class [[nodiscard]] Params final : public h_vector<Param, 8> {
public:
	using h_vector::h_vector;
	std::string_view Get(std::string_view name, std::string_view defVal = std::string_view()) {
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

class ServerConnection;

/// Http router implementation.
class [[nodiscard]] Router final {
	friend class ServerConnection;

public:
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
	int handle(Context& ctx);
	void log(Context& ctx) {
		if (logger_) {
			logger_(ctx);
		}
	}

	template <class K, int (K::*func)(Context&)>
	void addRoute(HttpMethod method, const char* path, K* object) {
		routes_[method].emplace_back(path, Handler{func_wrapper<K, func>, object});
	}

	template <class K, int (K::*func)(Context& ctx)>
	static int func_wrapper(void* obj, Context& ctx) {
		return (static_cast<K*>(obj)->*func)(ctx);
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
	std::vector<Handler> middlewares_;

	Handler notFoundHandler_{{}, nullptr};
	std::function<void(Context& ctx)> logger_;
	std::function<void(Context& ctx)> onResponse_;
};
}  // namespace net::http
}  // namespace reindexer
