#pragma once

#include <climits>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include "estl/h_vector.h"
#include "tools/slice.h"

namespace reindexer_server {

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

using reindexer::h_vector;

struct Header {
	const char *name;
	const char *val;
};

struct Param {
	const char *name;
	const char *val;
};

struct Request {
	const char *path;
	const char *method;
	const char *pathParams;
	h_vector<Header, 16> headers;
	h_vector<Param, 8> params;

	const char *body;
	size_t bodyLen;
};

class Writer {
public:
	virtual ssize_t Write(const void *buf, size_t size) = 0;
	virtual bool SetHeader(const Header &hdr) = 0;
	virtual bool SetRespCode(int code) = 0;
	virtual bool SetContentLength(size_t len) = 0;
	virtual bool SetConnectionClose() = 0;
	virtual ~Writer(){};
};

class Reader {
public:
	virtual ssize_t Read(void *buf, size_t size) = 0;
	virtual std::string Read(size_t size = INT_MAX) = 0;
	virtual ssize_t Pending() const = 0;
	virtual ~Reader(){};
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
};

class Router {
	friend class Connection;

public:
	template <class K, int (K::*func)(Context &)>
	void POST(const char *path, K *object) {
		addRoute<K, func>(kMethodPOST, path, object);
	}
	template <class K, int (K::*func)(Context &)>
	void GET(const char *path, K *object) {
		addRoute<K, func>(kMethodGET, path, object);
	}
	template <class K, int (K::*func)(Context &)>
	void OPTIONS(const char *path, K *object) {
		addRoute<K, func>(kMethodOPTIONS, path, object);
	}
	template <class K, int (K::*func)(Context &)>
	void DELETE(const char *path, K *object) {
		addRoute<K, func>(kMethodDELETE, path, object);
	}
	template <class K, int (K::*func)(Context &)>
	void PUT(const char *path, K *object) {
		addRoute<K, func>(kMethodPUT, path, object);
	}
	template <class K, int (K::*func)(Context &)>
	void HEAD(const char *path, K *object) {
		addRoute<K, func>(kMethodHEAD, path, object);
	}

	void enableStats() { enableStats_ = true; }
	void printStats();

protected:
	int handle(Context &ctx);

	template <class K, int (K::*func)(Context &)>
	void addRoute(HttpMethod method, const char *path, K *object) {
		Handler h{func_wrapper<K, func>, object};
		Route r{path, h, {}};
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
		std::string path_;
		Handler h_;
		Stats stat_;
	};

	std::vector<Route> routes_[kMaxMethod];
	Stats writeStats_;

	bool enableStats_ = false;
	std::mutex lockStats_;
};
}  // namespace http
}  // namespace reindexer_server
