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

	[[nodiscard]] bool IsFinished() const noexcept override final { return !sock_.valid(); }
	[[nodiscard]] BalancingType GetBalancingType() const noexcept override final { return BalancingType::None; }
	void SetRebalanceCallback(std::function<void(IServerConnection*, BalancingType)> cb) override final { (void)cb; }
	[[nodiscard]] bool HasPendingData() const noexcept override final { return false; }
	void HandlePendingData() override final {}
	[[nodiscard]] bool Restart(socket&& s) override final;
	void Detach() override final;
	void Attach(ev::dynamic_loop& loop) override final;

private:
	class [[nodiscard]] BodyReader final : public Reader {
	public:
		explicit BodyReader(ServerConnection* conn) : conn_(conn) {}
		[[nodiscard]] ssize_t Read(void* buf, size_t size) override final;
		[[nodiscard]] std::string Read(size_t size = std::numeric_limits<size_t>::max()) override final;
		[[nodiscard]] ssize_t Pending() const override final;

	protected:
		ServerConnection* conn_{nullptr};
	};
	class ResponseWriter : public Writer {
	public:
		explicit ResponseWriter(ServerConnection* conn) : headers_(conn->wrBuf_.get_chunk()), conn_(conn) {}
		virtual bool SetHeader(const Header& hdr) noexcept override final;
		virtual bool SetRespCode(int code) noexcept override final;
		virtual bool SetContentLength(size_t len) noexcept override final;
		ssize_t Write(chunk&& chunk, Writer::WriteMode mode = WriteMode::Default) override final;
		ssize_t Write(std::string_view data) override final;
		virtual chunk GetChunk() override final;

		[[nodiscard]] bool IsRespSent() const noexcept { return respSend_; }
		[[nodiscard]] virtual int RespCode() noexcept override final { return code_; }
		[[nodiscard]] virtual ssize_t Written() noexcept override final { return written_; }

	private:
		[[nodiscard]] bool isChunkedResponse() const noexcept { return contentLength_ == -1; }

		int code_ = StatusOK;

		WrSerializer headers_;
		bool respSend_ = false;
		ssize_t contentLength_ = -1, written_ = 0;
		ServerConnection* conn_{nullptr};
	};

	void handleRequest(Request& req);
	void badRequest(int code, const char* msg);
	[[nodiscard]] ReadResT onRead() override;
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
