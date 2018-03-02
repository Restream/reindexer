#pragma once

#include <climits>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include "estl/h_vector.h"
#include "tools/slice.h"

namespace reindexer {
namespace net {
namespace http {

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

typedef const char *UrlParam;

struct Header {
	const char *name;
	const char *val;
};

struct Param {
	const char *name;
	const char *val;
};

struct Request {
	const char *uri;
	const char *method;
	char path[1024];

	h_vector<Header, 16> headers;
	h_vector<Param, 8> params;
	h_vector<UrlParam, 8> urlParams;

	const char *body;
	size_t bodyLen;
};

class Writer {
public:
	virtual ssize_t Write(const void *buf, size_t size) = 0;
	template <int N>
	size_t Write(const char (&str)[N]) {
		return Write(str, N - 1);
	}

	virtual bool SetHeader(const Header &hdr) = 0;
	virtual bool SetRespCode(int code) = 0;
	virtual bool SetContentLength(size_t len) = 0;
	virtual bool SetConnectionClose() = 0;

	virtual int RespCode() = 0;
	virtual int Written() = 0;
	virtual ~Writer(){};
};

class Reader {
public:
	virtual ssize_t Read(void *buf, size_t size) = 0;
	virtual std::string Read(size_t size = INT_MAX) = 0;
	virtual ssize_t Pending() const = 0;
	virtual ~Reader(){};
};

class ClientData {
public:
	typedef std::shared_ptr<ClientData> Ptr;
	virtual ~ClientData() = default;
};

struct Context {
	int JSON(int code, const reindexer::Slice &slice);
	int String(int code, const reindexer::Slice &slice);
	int File(int code, const char *path);
	int Printf(int code, const char *contentType, const char *fmt, ...);
	int Redirect(const char *url);

	Request *request;
	Writer *writer;

	Reader *body;
	ClientData::Ptr clientData;
};

class Connection;

/// Http router implementation.
class Router {
	friend class Connection;

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

	/// Enable calcultion of routes performance and allocations statistics
	void enableStats() { enableStats_ = true; }
	/// Print to stdout routes performance and allocations statistics
	void printStats();

protected:
	int handle(Context &ctx);

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

	struct Stats {
		void add(int t, int a) {
			count++;
			if (max < t) max = t;
			if (!min || min > t) min = t;
			total += t;
			avg = total / count;
			allocs += a;
		}
		int count = 0, avg = 0, min = 0, max = 0, total = 0, allocs = 0;
	};

	struct Route {
		Route(string path, Handler h) : path_(path), h_(h) {}

		string path_;
		Handler h_;
		Stats stat_;
	};

	std::vector<Route> routes_[kMaxMethod];
	std::vector<Handler> middlewares_;
	Stats writeStats_;

	bool enableStats_ = false;
	std::mutex lockStats_;

	Handler notFoundHandler_;
	std::function<void(Context &ctx)> logger_;
};
}  // namespace http
}  // namespace net
}  // namespace reindexer
