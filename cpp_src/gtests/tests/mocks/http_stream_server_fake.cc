#include "http_stream_server_fake.h"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <tuple>
#include "net/http/serverconnection.h"
#include "tools/fsops.h"

namespace reindexer_tests {

namespace http = reindexer::net::http;
using namespace std::string_view_literals;

namespace {

constexpr size_t kMaxRequestSize = 8 * 1024 * 1024;

[[noreturn]] void throwException(ExceptionKind kind, std::string_view what) {
	switch (kind) {
		case ExceptionKind::HttpStatus:
			throw http::HttpStatus(http::StatusForbidden, std::string(what));
		case ExceptionKind::Error:
			throw reindexer::Error(errNotFound, what);
		case ExceptionKind::StdException:
			break;
	}
	throw std::runtime_error(std::string(what));
}

// Checks that the chunk continues StreamPattern() from the offset
bool matchesPattern(std::span<const char> chunk, uint64_t offset) noexcept {
	const auto pattern = StreamPattern();
	size_t pos = size_t(offset % pattern.size());
	while (!chunk.empty()) {
		const size_t len = std::min(chunk.size(), pattern.size() - pos);
		if (std::memcmp(chunk.data(), pattern.data() + pos, len) != 0) {
			return false;
		}
		chunk = chunk.subspan(len);
		pos = 0;
	}
	return true;
}

}  // namespace

std::span<const char> StreamPattern() {
	static const std::string pattern = [] {
		std::string res(1024 * 1024 + 13, '\0');
		for (size_t i = 0; i < res.size(); ++i) {
			res[i] = char(uint32_t(i * 2654435761u) >> 24);
		}
		return res;
	}();
	return {pattern.data(), pattern.size()};
}

class [[nodiscard]] HTTPStreamServerFake::StreamHandler final : public http::IStreamHandler {
public:
	StreamHandler(HTTPStreamServerFake& server, std::string name, const StreamBehavior& behavior, uint64_t id)
		: server_(server), name_(std::move(name)), behavior_(behavior), id_(id) {}
	StreamHandler(const StreamHandler&) = delete;
	StreamHandler& operator=(const StreamHandler&) = delete;
	~StreamHandler() override {
		removeFile();
		addEvent("destroy"sv);
	}

	http::HttpStatus Preflight(http::Context& ctx, uint64_t contentLength) override {
		request_ = ctx.request;
		server_.update(name_, [this](StreamRecord& record) {
			record.events.emplace_back("preflight");
			record.handlerId = id_;
		});
		if (behavior_.preflightThrows) {
			throwException(behavior_.exceptionKind, "Preflight failed"sv);
		}
		if (request_->headers.Get("X-Role"sv) != "owner"sv) {
			return http::HttpStatus(http::StatusForbidden, "Role 'owner' is required");
		}
		if (contentLength > behavior_.kMaxBodySize) {
			return http::HttpStatus(http::StatusRequestEntityTooLarge, "Body is too large");
		}
		if (behavior_.preflightStatus) {
			return http::HttpStatus(*behavior_.preflightStatus, "Refused by Preflight");
		}
		if (behavior_.writeToFile) {
			tmpPath_ = reindexer::fs::JoinPath(server_.FileDir(), name_ + ".tmp");
			file_ = std::fopen(tmpPath_.c_str(), "wb");
			if (!file_) {
				tmpPath_.clear();
				return http::HttpStatus(http::StatusInternalServerError, "Unable to create the file");
			}
		}
		if (behavior_.preflightWritesResponse) {
			std::ignore = ctx.String(http::StatusOK, "Written by Preflight"sv);
		}
		return http::HttpStatus();
	}

	http::HttpStatus OnChunk(std::span<const char> chunk) override {
		if (!received_ && behavior_.firstChunkDelay.count()) {
			std::this_thread::sleep_for(behavior_.firstChunkDelay);
		}
		const bool matches = matchesPattern(chunk, received_);
		received_ += chunk.size();
		server_.update(name_, [&](StreamRecord& record) {
			++record.chunks;
			record.received = received_;
			record.emptyChunk = record.emptyChunk || chunk.empty();
			record.patternMismatch = record.patternMismatch || !matches;
		});
		if (received_ > behavior_.throwAfter) {
			throwException(behavior_.exceptionKind, "OnChunk failed"sv);
		}
		if (received_ > behavior_.rejectAfter) {
			return http::HttpStatus(http::StatusNotAcceptable, "Rejected by OnChunk");
		}
		if (file_ && std::fwrite(chunk.data(), 1, chunk.size(), file_) != chunk.size()) {
			return http::HttpStatus(http::StatusInternalServerError, "Unable to write the file");
		}
		return http::HttpStatus();
	}

	http::HttpStatus Finish() override {
		// The request head is read after the whole body was received
		server_.update(name_, [this](StreamRecord& record) {
			record.events.emplace_back("finish");
			record.uri = std::string(request_->uri);
			record.header = std::string(request_->headers.Get("X-Test"sv));
			for (const auto& param : request_->urlParams) {
				record.urlParams.emplace_back(param);
			}
		});
		if (behavior_.finishThrows) {
			throwException(behavior_.exceptionKind, "Finish failed"sv);
		}
		if (behavior_.finishStatus) {
			removeFile();
			return http::HttpStatus(*behavior_.finishStatus, "Status from Finish");
		}
		if (file_) {
			const bool closed = (std::fclose(file_) == 0);
			file_ = nullptr;
			if (!closed || reindexer::fs::Rename(tmpPath_, reindexer::fs::JoinPath(server_.FileDir(), name_ + ".bin")) != 0) {
				removeFile();
				return http::HttpStatus(http::StatusInternalServerError, "Unable to store the file");
			}
			tmpPath_.clear();
		}
		return http::HttpStatus();
	}

	void Abort(http::StreamAbortReason reason) noexcept override {
		removeFile();
		switch (reason) {
			case http::StreamAbortReason::ConnectionClosed:
				addEvent("abort:connection_closed"sv);
				break;
			case http::StreamAbortReason::ChunkRejected:
				addEvent("abort:chunk_rejected"sv);
				break;
			case http::StreamAbortReason::HandlerException:
				addEvent("abort:handler_exception"sv);
				break;
			case http::StreamAbortReason::Timeout:
				addEvent("abort:timeout"sv);
				break;
		}
	}

private:
	void removeFile() noexcept {
		if (file_) {
			std::ignore = std::fclose(file_);
			file_ = nullptr;
		}
		if (!tmpPath_.empty()) {
			std::ignore = std::remove(tmpPath_.c_str());
			tmpPath_.clear();
		}
	}
	void addEvent(std::string_view event) noexcept {
		try {
			server_.update(name_, [event](StreamRecord& record) { record.events.emplace_back(event); });
		} catch (...) {
			fprintf(stderr, "Unable to record the event '%s' of the stream '%s'\n", std::string(event).c_str(), name_.c_str());
		}
	}

	HTTPStreamServerFake& server_;
	const std::string name_;
	const StreamBehavior behavior_;
	const uint64_t id_;
	const http::Request* request_{nullptr};
	uint64_t received_{0};
	FILE* file_{nullptr};
	std::string tmpPath_;
};

std::string HTTPStreamServerFake::Start(uint16_t port) {
	router_.PUTStream<HTTPStreamServerFake, &HTTPStreamServerFake::CreateHandler>("/stream/:name", this);
	router_.PUTStream<HTTPStreamServerFake, &HTTPStreamServerFake::CreateHandler>("/short/:name", this, kShortIdleTimeout);
	router_.GET<HTTPStreamServerFake, &HTTPStreamServerFake::BigResponse>("/big", this);
	router_.Middleware<HTTPStreamServerFake, &HTTPStreamServerFake::CheckToken>(this);
	router_.OnResponse(this, &HTTPStreamServerFake::onResponse);
	std::string error;
	thread_ = std::thread([this, port, &error] {
		stop_.set(loop_);
		stop_.set([](reindexer::net::ev::async& sig) { sig.loop.break_loop(); });
		stop_.start();
		listener_ = std::make_unique<reindexer::net::Listener<reindexer::net::ListenerType::Shared>>(
			loop_, http::ServerConnection::NewFactory(router_, kMaxRequestSize), nullptr);
		try {
			listener_->Bind("127.0.0.1:" + std::to_string(port), reindexer::net::socket_domain::tcp);
		} catch (const std::exception& e) {
			error = e.what();
		}
		const bool bound = error.empty();
		// 'error' belongs to the caller and is not used after this point
		ready_ = true;
		while (bound && !terminate_) {
			loop_.run();
		}
		stop_.stop();
		listener_->Stop();
		listener_.reset();
	});
	while (!ready_) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return error;
}

void HTTPStreamServerFake::Stop() {
	if (thread_.joinable()) {
		terminate_ = true;
		stop_.send();
		thread_.join();
	}
}

void HTTPStreamServerFake::SetBehavior(const std::string& name, const StreamBehavior& behavior) {
	reindexer::lock_guard lck(mtx_);
	behaviors_[name] = behavior;
}

StreamRecord HTTPStreamServerFake::WaitDestroyed(const std::string& name) {
	reindexer::unique_lock lck(mtx_);
	std::ignore = cond_.wait_for(lck, std::chrono::seconds(30), [&] {
		const auto& events = records_[name].events;
		return !events.empty() && events.back() == "destroy";
	});
	return records_[name];
}

std::unique_ptr<http::IStreamHandler> HTTPStreamServerFake::CreateHandler(const http::Request& req) {
	++factoryCalls_;
	std::string name(req.urlParams.empty() ? std::string_view() : req.urlParams[0]);
	StreamBehavior behavior;
	{
		reindexer::lock_guard lck(mtx_);
		if (auto it = behaviors_.find(name); it != behaviors_.end()) {
			behavior = it->second;
		}
	}
	if (behavior.factoryThrows) {
		throwException(behavior.exceptionKind, "Factory failed"sv);
	}
	return std::make_unique<StreamHandler>(*this, std::move(name), behavior, nextHandlerId_++);
}

void HTTPStreamServerFake::onResponse(http::Context& ctx) {
	// There is no request in the context of an answer to a protocol error
	if (ctx.request && ctx.request->path.ends_with("/response_throws"sv)) {
		throw std::runtime_error("Response callback failed");
	}
}

int HTTPStreamServerFake::CheckToken(http::Context& ctx) {
	if (ctx.request->headers.Get("X-Token"sv) == "secret"sv) {
		return 0;
	}
	std::ignore = ctx.String(http::StatusUnauthorized, "Token is required"sv);
	return -1;
}

int HTTPStreamServerFake::BigResponse(http::Context& ctx) {
	const std::string body(kBigResponseSize, 'x');
	return ctx.String(http::StatusOK, std::string_view(body));
}

}  // namespace reindexer_tests
