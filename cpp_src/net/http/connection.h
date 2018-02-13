#pragma once

#include <string.h>
#include "estl/cbuf.h"
#include "estl/h_vector.h"
#include "net/ev/ev.h"
#include "net/iconnection.h"
#include "picohttpparser/picohttpparser.h"
#include "router.h"

namespace reindexer {
namespace net {
namespace http {

using reindexer::cbuf;
using reindexer::h_vector;

const ssize_t kHttpReadbufSize = 0x8000;
const ssize_t kHttpWriteBufSize = 0x8000;
const ssize_t kHttpMaxHeaders = 128;

class Connection : public IConnection {
public:
	Connection(int fd, ev::dynamic_loop &loop, Router &router);
	~Connection();

	static ConnectionFactory NewFactory(Router &router) {
		return [&router](ev::dynamic_loop &loop, int fd) { return new Connection(fd, loop, router); };
	};

	bool IsFinished() override final { return fd_ < 0; }
	bool Restart(int fd) override final;
	void Reatach(ev::dynamic_loop &loop) override final;

protected:
	// Generic callback
	void callback(ev::io &watcher, int revents);
	void write_cb();
	void read_cb();

	class BodyReader : public Reader {
	public:
		BodyReader(Connection *conn) : conn_(conn) {}
		ssize_t Read(void *buf, size_t size) override final;
		std::string Read(size_t size = INT_MAX) override final;
		ssize_t Pending() const override final;

	protected:
		Connection *conn_;
	};
	class ResponseWriter : public Writer {
	public:
		ResponseWriter(Connection *conn) : conn_(conn) {}
		virtual bool SetHeader(const Header &hdr) override final;
		virtual bool SetRespCode(int code) override final;
		virtual bool SetContentLength(size_t len) override final;
		virtual bool SetConnectionClose() override final;
		ssize_t Write(const void *buf, size_t size) override final;
		template <int N>
		ssize_t Write(const char (&str)[N]) {
			return Write(str, N - 1);
		}

	protected:
		bool isChunkedResponse() { return contentLength_ == -1; }

		int code_ = StatusOK;
		h_vector<char, 0x200> headers_;
		bool respSend_ = false;
		ssize_t contentLength_ = -1;
		Connection *conn_;
	};

	void handleRequest(Request &req);
	void badRequest(int code, const char *msg);
	void parseRequest();

	void parseParams(char *p);
	void writeHttpResponse(int code);

	ev::io io_;
	int fd_;
	int curEvents_ = 0;
	cbuf<char> wrBuf_, rdBuf_;
	Router &router_;
	Request request_;
	ssize_t bodyLeft_ = 0;
	bool formData_ = false;
	bool closeConn_ = false;
	bool enableHttp11_ = false;
	bool expectContinue_ = false;
	phr_chunked_decoder chunked_decoder_;
};
}  // namespace http
}  // namespace net
}  // namespace reindexer
