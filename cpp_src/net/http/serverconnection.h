#pragma once

#include <string.h>
#include "estl/h_vector.h"
#include "net/connection.h"
#include "net/iserverconnection.h"
#include "picohttpparser/picohttpparser.h"
#include "router.h"

namespace reindexer {
namespace net {
namespace http {

using reindexer::cbuf;
using reindexer::h_vector;

const ssize_t kHttpMaxHeaders = 128;
const ssize_t kHttpMaxBodySize = 2 * 1024 * 1024LL;
class ServerConnection : public IServerConnection, public ConnectionST {
public:
	ServerConnection(int fd, ev::dynamic_loop &loop, Router &router);

	static ConnectionFactory NewFactory(Router &router) {
		return [&router](ev::dynamic_loop &loop, int fd) { return new ServerConnection(fd, loop, router); };
	};

	bool IsFinished() override final { return !sock_.valid(); }
	bool Restart(int fd) override final;
	void Detach() override final;
	void Attach(ev::dynamic_loop &loop) override final;

protected:
	class BodyReader : public Reader {
	public:
		BodyReader(ServerConnection *conn) : conn_(conn) {}
		ssize_t Read(void *buf, size_t size) override final;
		std::string Read(size_t size = INT_MAX) override final;
		ssize_t Pending() const override final;

	protected:
		ServerConnection *conn_;
	};
	class ResponseWriter : public Writer {
	public:
		ResponseWriter(ServerConnection *conn) : conn_(conn) {}
		virtual bool SetHeader(const Header &hdr) override final;
		virtual bool SetRespCode(int code) override final;
		virtual bool SetContentLength(size_t len) override final;
		virtual bool SetConnectionClose() override final;
		ssize_t Write(const void *buf, size_t size) override final;
		template <int N>
		ssize_t Write(const char (&str)[N]) {
			return Write(str, N - 1);
		}
		bool IsRespSent() { return respSend_; }
		virtual int RespCode() override final { return code_; };
		virtual int Written() override final { return written_; };

	protected:
		bool isChunkedResponse() { return contentLength_ == -1; }

		int code_ = StatusOK;
		h_vector<char, 0x200> headers_;
		bool respSend_ = false;
		ssize_t contentLength_ = -1, written_ = 0;
		ServerConnection *conn_;
	};

	void handleRequest(Request &req);
	void badRequest(int code, const char *msg);
	void onRead() override;
	void onClose() override;

	void parseParams(const string_view &str);
	void writeHttpResponse(int code);

	Router &router_;
	Request request_;
	ssize_t bodyLeft_ = 0;
	bool formData_ = false;
	bool enableHttp11_ = false;
	bool expectContinue_ = false;
	phr_chunked_decoder chunked_decoder_;
	// cbuf<char> tmpBuf_;
};
}  // namespace http
}  // namespace net
}  // namespace reindexer
