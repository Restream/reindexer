#pragma once

#include <algorithm>
#include <climits>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include "estl/h_vector.h"
#include "estl/string_view.h"
#include "net/stat.h"
#include "tools/ssize_t.h"

namespace reindexer {
namespace net {
namespace http {

// Thank's to windows.h include
#ifdef DELETE
#undef DELETE
#endif

using std::string;

enum {
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

enum HttpMethod {
	kMethodGET,
	kMethodPOST,
	kMethodOPTIONS,
	kMethodHEAD,
	kMethodPUT,
	kMethodDELETE,
	kMaxMethod,
};

typedef string_view UrlParam;

struct Header {
	string_view name;
	string_view val;
};

struct Param {
	string_view name;
	string_view val;
};

class Headers : public h_vector<Header, 16> {
public:
	using h_vector::h_vector;
	string_view Get(const string_view &name) {
		auto it = std::find_if(begin(), end(), [=](const Header &hdr) { return name == hdr.name; });
		return it != end() ? it->val : string_view();
	}
};
class Params : public h_vector<Param, 8> {
public:
	using h_vector::h_vector;
	string_view Get(const string_view &name) {
		auto it = std::find_if(begin(), end(), [=](const Param &param) { return name == param.name; });
		return it != end() ? it->val : string_view();
	}
};

struct Request {
	string_view uri;
	string_view path;
	string_view method;

	Headers headers;
	Params params;
	h_vector<UrlParam, 4> urlParams;
};

class Writer {
public:
	virtual ssize_t Write(const void *buf, size_t size) = 0;

	size_t Write(char ch) { return Write(&ch, 1); }
	ssize_t Write(const string_view &buf) { return Write(buf.data(), buf.size()); }

	virtual bool SetHeader(const Header &hdr) = 0;
	virtual bool SetRespCode(int code) = 0;
	virtual bool SetContentLength(size_t len) = 0;
	virtual bool SetConnectionClose() = 0;

	virtual int RespCode() = 0;
	virtual int Written() = 0;
	virtual ~Writer() = default;
};

class Reader {
public:
	virtual ssize_t Read(void *buf, size_t size) = 0;
	virtual std::string Read(size_t size = INT_MAX) = 0;
	virtual ssize_t Pending() const = 0;
	virtual ~Reader() = default;
};

class ClientData {
public:
	typedef std::shared_ptr<ClientData> Ptr;
	virtual ~ClientData() = default;
};

struct Context {
	int JSON(int code, const string_view &slice);
	int String(int code, const string_view &slice);
	int File(int code, const char *path);
	int Printf(int code, const char *contentType, const char *fmt, ...);
	int Redirect(const char *url);

	Request *request;
	Writer *writer;
	Reader *body;
	ClientData::Ptr clientData;

	Stat stat;
};

class ServerConnection;

/// Http router implementation.
class Router {
	friend class ServerConnection;

public:
	/// Add handler for http POST method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context &)>
	void POST(const char *path, K *object) {
		addRoute<K, func>(kMethodPOST, path, object);
	}
	/// Add handler for http GET method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context &)>
	void GET(const char *path, K *object) {
		addRoute<K, func>(kMethodGET, path, object);
	}
	/// Add handler for http OPTIONS method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context &)>
	void OPTIONS(const char *path, K *object) {
		addRoute<K, func>(kMethodOPTIONS, path, object);
	}
	template <class K, int (K::*func)(Context &)>
	/// Add handler for http DELETE method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	void DELETE(const char *path, K *object) {
		addRoute<K, func>(kMethodDELETE, path, object);
	}
	/// Add handler for http PUT method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context &)>
	void PUT(const char *path, K *object) {
		addRoute<K, func>(kMethodPUT, path, object);
	}
	/// Add handler for http HEAD method.
	/// @param path - URI pattern
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context &)>
	void HEAD(const char *path, K *object) {
		addRoute<K, func>(kMethodHEAD, path, object);
	}
	/// Add middleware
	/// @param object - handler class object
	/// @tparam func - handler
	template <class K, int (K::*func)(Context &)>
	void Middleware(K *object) {
		Handler h{func_wrapper<K, func>, object};
		middlewares_.push_back(h);
	}
	/// Add logger for requests
	/// @param object - logger class object
	/// @tparam func - logger
	template <class K, void (K::*func)(Context &)>
	void Logger(K *object) {
		logger_ = [=](Context &ctx) { (static_cast<K *>(object)->*func)(ctx); };
	}

	/// Add default handler for not found URI's
	/// @tparam func - handler
	template <class K, int (K::*func)(Context &)>
	void NotFound(K *object) {
		notFoundHandler_ = Handler{func_wrapper<K, func>, object};
	}

protected:
	int handle(Context &ctx);
	void log(Context &ctx) {
		if (logger_) logger_(ctx);
	}

	template <class K, int (K::*func)(Context &)>
	void addRoute(HttpMethod method, const char *path, K *object) {
		Handler h{func_wrapper<K, func>, object};
		Route r(path, h);
		routes_[method].push_back(r);
	}

	template <class K, int (K::*func)(Context &ctx)>
	static int func_wrapper(void *obj, Context &ctx) {
		return (static_cast<K *>(obj)->*func)(ctx);
	}

	struct Handler {
		std::function<int(void *obj, Context &ctx)> func_;
		void *object_;
	};

	struct Route {
		Route(string path, Handler h) : path_(path), h_(h) {}

		string path_;
		Handler h_;
	};

	std::vector<Route> routes_[kMaxMethod];
	std::vector<Handler> middlewares_;

	Handler notFoundHandler_;
	std::function<void(Context &ctx)> logger_;
};
}  // namespace http
}  // namespace net
}  // namespace reindexer
