#pragma once

#include "net/connection.h"
#include "net/iserverconnection.h"
#include "picohttpparser/picohttpparser.h"
#include "router.h"
#include "tools/serializer.h"

namespace reindexer::net::http {

const ssize_t kHttpMaxHeaders = 128;
class [[nodiscard]] ServerConnection final : public IServerConnection, public ConnectionST {
public:
	ServerConnection(socket&& s, ev::dynamic_loop& loop, Router& router, size_t maxRequestSize);

	static ConnectionFactory NewFactory(Router& router, size_t maxRequestSize) {
		return [&router, maxRequestSize](ev::dynamic_loop& loop, socket&& s, bool allowCustomBalancing) {
			(void)allowCustomBalancing;
			return new ServerConnection(std::move(s), loop, router, maxRequestSize);
		};
	}

	bool IsFinished() const noexcept override final { return !sock_.valid(); }
	BalancingType GetBalancingType() const noexcept override final { return BalancingType::None; }
	void SetRebalanceCallback(std::function<void(IServerConnection*, BalancingType)> cb) override final { (void)cb; }
	bool HasPendingData() const noexcept override final { return false; }
	void HandlePendingData() override final {}
	bool Restart(socket&& s) override final;
	void Detach() override final;
	void Attach(ev::dynamic_loop& loop) override final;

private:
	class [[nodiscard]] BodyReader final : public Reader {
	public:
		explicit BodyReader(ServerConnection* conn) : conn_(conn) {}
		ssize_t Read(void* buf, size_t size) override final;
		std::string Read(size_t size = std::numeric_limits<size_t>::max()) override final;
		ssize_t Pending() const override final;

	protected:
		ServerConnection* conn_{nullptr};
	};
	class [[nodiscard]] ResponseWriter : public Writer {
	public:
		explicit ResponseWriter(ServerConnection* conn) : headers_(conn->wrBuf_.get_chunk()), conn_(conn) {}
		virtual void SetHeader(const Header& hdr) noexcept override final;
		virtual void SetRespCode(int code) noexcept override final;
		virtual void SetContentLength(size_t len) noexcept override final;
		void Write(chunk&& chunk, Writer::WriteMode mode = WriteMode::Default) override final;
		void Write(std::string_view data) override final;
		virtual chunk GetChunk() override final;

		bool IsRespSent() const noexcept { return respSend_; }
		virtual int RespCode() noexcept override final { return code_; }
		virtual ssize_t Written() noexcept override final { return written_; }

	private:
		bool isChunkedResponse() const noexcept { return contentLength_ == -1; }

		int code_ = StatusOK;

		WrSerializer headers_;
		bool respSend_ = false;
		ssize_t contentLength_ = -1, written_ = 0;
		ServerConnection* conn_{nullptr};
	};

	void handleRequest(Request& req);
	void badRequest(int code, const char* msg);
	ReadResT onRead() override;
	void onClose() override;
	void handleException(Context& ctx, const Error& err);

	void parseParams(std::string_view str);
	void writeHttpResponse(int code);
	void setMsgpackStatus(Context& ctx, bool success, int responseCode, const std::string& status);
	void setProtobufStatus(Context& ctx, bool success, int responseCode, const std::string& status);
	void setJsonStatus(Context& ctx, bool success, int responseCode, const std::string& status);
	void setStatus(Context& ctx, bool success, int responseCode, const std::string& status);

	Router& router_;
	Request request_;
	ssize_t bodyLeft_ = 0;
	bool formData_ = false;
	bool enableHttp11_ = false;
	bool expectContinue_ = false;
	phr_chunked_decoder chunked_decoder_{0, 0, 0, 0};
	const size_t maxRequestSize_ = 0;
};
}  // namespace reindexer::net::http
