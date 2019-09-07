#pragma once

#include <string.h>
#include "net/connection.h"
#include "net/iserverconnection.h"
#include "picohttpparser/picohttpparser.h"
#include "router.h"
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace http {

const ssize_t kHttpMaxHeaders = 128;
const ssize_t kHttpMaxBodySize = 2 * 1024 * 1024LL;
class ServerConnection : public IServerConnection, public ConnectionST {
public:
	ServerConnection(int fd, ev::dynamic_loop &loop, Router &router);

	static ConnectionFactory NewFactory(Router &router) {
		return [&router](ev::dynamic_loop &loop, int fd) { return new ServerConnection(fd, loop, router); };
	}

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
		ResponseWriter(ServerConnection *conn) : headers_(conn->wrBuf_.get_chunk()), conn_(conn) {}
		virtual bool SetHeader(const Header &hdr) override final;
		virtual bool SetRespCode(int code) override final;
		virtual bool SetContentLength(size_t len) override final;
		virtual bool SetConnectionClose() override final;
		ssize_t Write(chunk &&chunk) override final;
		ssize_t Write(string_view data) override final;
		virtual chunk GetChunk() override final;

		bool IsRespSent() { return respSend_; }
		virtual int RespCode() override final { return code_; }
		virtual ssize_t Written() override final { return written_; }

	protected:
		bool isChunkedResponse() { return contentLength_ == -1; }

		int code_ = StatusOK;

		WrSerializer headers_;
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
	void setJsonStatus(Context &ctx, bool success, int responseCode, const string &status);

	Router &router_;
	Request request_;
	ssize_t bodyLeft_ = 0;
	bool formData_ = false;
	bool enableHttp11_ = false;
	bool expectContinue_ = false;
	phr_chunked_decoder chunked_decoder_{0, 0, 0, 0};
	// cbuf<char> tmpBuf_;
};
}  // namespace http
}  // namespace net
}  // namespace reindexer
