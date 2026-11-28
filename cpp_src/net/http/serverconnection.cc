
#include "serverconnection.h"
#include <algorithm>
#include <charconv>
#include <limits>
#include <unordered_map>
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "itoa/itoa.h"
#include "server/outputparameters.h"
#include "time/fast_time.h"
#include "tools/assertrx.h"
#include "tools/clock.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer::net::http {

using namespace std::string_view_literals;

static const std::string_view kStrEOL = "\r\n"sv;
extern std::unordered_map<int, std::string_view> kHTTPCodes;

// Streamed body bytes, after which the connection returns to the event loop, even if the socket has more data
constexpr uint64_t kStreamReadBudget = 1024 * 1024;

namespace {

std::string_view trimOWS(std::string_view str) noexcept {
	const auto isOWS = [](char c) noexcept { return c == ' ' || c == '\t'; };
	while (!str.empty() && isOWS(str.front())) {
		str.remove_prefix(1);
	}
	while (!str.empty() && isOWS(str.back())) {
		str.remove_suffix(1);
	}
	return str;
}

// Parses the 'Content-Length' value of a regular request: decimal digits with optional spaces or tabs around them.
// The regular path keeps the body size in int arithmetic, so a larger value is refused as too large
HttpStatus parseRegularContentLength(std::string_view value, ssize_t& length) {
	value = trimOWS(value);
	uint64_t result = 0;
	const auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), result);
	if (value.empty() || end != value.data() + value.size() || (ec != std::errc() && ec != std::errc::result_out_of_range)) {
		return HttpStatus(StatusBadRequest, "Invalid 'Content-Length' header");
	}
	if (ec == std::errc::result_out_of_range || result > uint64_t(std::numeric_limits<int>::max())) {
		return HttpStatus(StatusRequestEntityTooLarge, "");
	}
	length = ssize_t(result);
	return HttpStatus();
}

// Checks the body framing of a request for a streamed route. The rules are stricter than for the other routes:
// ambiguous framing is refused before the body is read, because the server can not find the end of such a body.
// Obsolete line folding is refused for the same reason.
HttpStatus checkStreamFraming(const Headers& headers, uint64_t& contentLength) {
	std::string_view lengthValue, encodingValue;
	unsigned lengthCount = 0, encodingCount = 0;
	for (const auto& hdr : headers) {
		if (hdr.name.empty()) [[unlikely]] {
			return HttpStatus(StatusBadRequest, "Obsolete line folding of the headers is not supported");
		}
		if (iequals(hdr.name, "content-length"sv)) {
			++lengthCount;
			lengthValue = trimOWS(hdr.val);
		} else if (iequals(hdr.name, "transfer-encoding"sv)) {
			++encodingCount;
			encodingValue = trimOWS(hdr.val);
		}
	}
	if (encodingCount) [[unlikely]] {
		if (lengthCount) {
			return HttpStatus(StatusBadRequest, "'Content-Length' and 'Transfer-Encoding' headers can not be used together");
		}
		if (encodingCount == 1 && iequals(encodingValue, "chunked"sv)) {
			// The same answer as for the other routes
			return HttpStatus(StatusInternalServerError, "Sorry, chunked encoded body not implemented");
		}
		return HttpStatus(StatusBadRequest, "Unsupported 'Transfer-Encoding' header");
	}
	if (!lengthCount) [[unlikely]] {
		return HttpStatus(StatusLengthRequired, "'Content-Length' header is required");
	}
	if (lengthCount > 1) [[unlikely]] {
		return HttpStatus(StatusBadRequest, "Multiple 'Content-Length' headers");
	}
	const auto isDigit = [](char c) noexcept { return c >= '0' && c <= '9'; };
	if (lengthValue.empty() || !std::all_of(lengthValue.begin(), lengthValue.end(), isDigit)) [[unlikely]] {
		return HttpStatus(StatusBadRequest, "Invalid 'Content-Length' header");
	}
	contentLength = 0;
	for (const char c : lengthValue) {
		const auto digit = uint64_t(c - '0');
		if (contentLength > (std::numeric_limits<uint64_t>::max() - digit) / 10) [[unlikely]] {
			return HttpStatus(StatusRequestEntityTooLarge, "'Content-Length' value is too large");
		}
		contentLength = contentLength * 10 + digit;
	}
	return HttpStatus();
}

struct [[nodiscard]] HandlerResult {
	HttpStatus status;
	bool exception = false;
};

// An exception is never a success, even if the thrown status has a success code
HandlerResult exceptionResult(const HttpStatus& status) {
	if (status.code() < StatusBadRequest) {
		return {.status = HttpStatus(StatusInternalServerError, status.whatStr()), .exception = true};
	}
	return {.status = status, .exception = true};
}

// The status envelope always has a body, and only 200 means success, like on the regular routes.
// So a code below 400 other than 200 (e.g. 204, which can not have a body) is replaced with 500
HttpStatus responseStatus(const HttpStatus& status) {
	if (status.code() == StatusOK || status.code() >= StatusBadRequest) [[likely]] {
		return status;
	}
	return HttpStatus(StatusInternalServerError, "Unsupported status " + std::to_string(int(status.code())) + " from the stream handler");
}

// Calls a method of the stream handler. An exception gives the same status as in handleRequest()
template <typename F>
HandlerResult callStreamHandler(F&& f) {
	try {
		return {.status = f()};
	} catch (const HttpStatus& status) {
		return exceptionResult(status);
	} catch (const Error& err) {
		return exceptionResult(HttpStatus(err));
	} catch (const std::exception& e) {
		return exceptionResult(HttpStatus(Error(errLogic, e.what())));
	} catch (...) {
		return exceptionResult(HttpStatus(Error(errLogic, "Unknown exception")));
	}
}

}  // namespace

struct [[nodiscard]] ServerConnection::StreamState {
	enum class [[nodiscard]] Phase { Head, Body, Done };

	StreamState(ServerConnection* conn, std::string&& h) : head(std::move(h)), writer(conn), reader(conn) {
		ctx.request = &request;
		ctx.writer = &writer;
		ctx.body = &reader;
	}
	StreamState(const StreamState&) = delete;
	StreamState& operator=(const StreamState&) = delete;
	~StreamState() { Abort(StreamAbortReason::ConnectionClosed); }

	// Calls handler's Abort() if the handler expects it
	void Abort(StreamAbortReason reason) noexcept {
		if (phase == Phase::Body) {
			phase = Phase::Done;
			handler->Abort(reason);
		}
	}

	// The handler is declared after the request data, so it is destroyed first and may refer to the request until then
	const std::string head;
	Request request;
	ResponseWriter writer;
	BodyReader reader;
	Context ctx;
	std::unique_ptr<IStreamHandler> handler;
	uint64_t bodySize = 0;
	uint64_t bodyLeft = 0;
	Phase phase = Phase::Head;
	std::chrono::milliseconds idleTimeout{0};
	// The moment, when the connection started to wait for the next part of the body
	steady_clock_w::time_point lastRead;
};

ServerConnection::ServerConnection(socket&& s, ev::dynamic_loop& loop, Router& router, size_t maxRequestSize)
	: ConnectionST(std::move(s), loop, false, (maxRequestSize && maxRequestSize < kConnReadbufSize) ? maxRequestSize : kConnReadbufSize),
	  router_(router),
	  maxRequestSize_(maxRequestSize) {
	streamTimer_.set<ServerConnection, &ServerConnection::onStreamTimer>(this);
	streamTimer_.set(loop);
	callback(io_, ev::READ);
}

ServerConnection::~ServerConnection() = default;

bool ServerConnection::Restart(socket&& s) {
	restart(std::move(s));
	streamTimer_.stop();
	stream_.reset();
	streamReadBytes_ = 0;
	bodyLeft_ = 0;
	formData_ = false;
	enableHttp11_ = false;
	expectContinue_ = false;
	callback(io_, ev::READ);
	return true;
}

void ServerConnection::Attach(ev::dynamic_loop& loop) {
	if (!attached_) {
		attach(loop);
		streamTimer_.set(loop);
		// The connection may move to another thread in the middle of a streamed body
		restartStreamTimer();
	}
}
void ServerConnection::Detach() {
	if (attached_) {
		streamTimer_.stop();
		streamTimer_.reset();
		detach();
	}
}

void ServerConnection::onClose() {
	streamTimer_.stop();
	// Calls Abort() of the stream handler, if the body was not received completely
	stream_.reset();
}

void ServerConnection::handleException(Context& ctx, const Error& status) {
	auto writer = dynamic_cast<ResponseWriter*>(ctx.writer);
	if (writer && !writer->IsRespSent()) {
		HttpStatus httpStatus(status);
		setStatus(ctx, false, httpStatus.code(), httpStatus.whatStr());
	}
}

void ServerConnection::setMsgpackStatus(Context& ctx, bool success, int responseCode, const std::string& status) {
	WrSerializer ser;
	MsgPackBuilder builder(ser, ObjType::TypeObject, 3);
	builder.Put("success", success);
	builder.Put("response_code", responseCode);
	builder.Put("description", status);
	builder.End();
	ctx.MSGPACK(responseCode, ser.Slice());
}

void ServerConnection::setProtobufStatus(Context& ctx, bool success, int responseCode, const std::string& status) {
	WrSerializer ser;
	ProtobufBuilder builder(ser);
	builder.Put(kProtoErrorResultsFields.at(kParamSuccess), success);
	builder.Put(kProtoErrorResultsFields.at(kParamResponseCode), responseCode);
	builder.Put(kProtoErrorResultsFields.at(kParamDescription), status);
	builder.End();
	ctx.Protobuf(responseCode, ser.Slice());
}

void ServerConnection::setJsonStatus(Context& ctx, bool success, int responseCode, const std::string& status) {
	WrSerializer ser;
	JsonBuilder builder(ser);
	builder.Put("success", success);
	builder.Put("response_code", responseCode);
	builder.Put("description", status);
	builder.End();
	std::ignore = ctx.JSON(responseCode, ser.Slice());
}

void ServerConnection::setStatus(Context& ctx, bool success, int responseCode, const std::string& status) {
	const std::string_view format = ctx.request->params.Get("format"sv);
	if (format == kMsgPackFmt) {
		return setMsgpackStatus(ctx, success, responseCode, status);
	}
	if (format == kProtobufFmt) {
		return setProtobufStatus(ctx, success, responseCode, status);
	}
	return setJsonStatus(ctx, success, responseCode, status);
}

void ServerConnection::handleRequest(Request& req) {
	ResponseWriter writer(this);
	BodyReader reader(this);
	Context ctx;
	ctx.request = &req;
	ctx.writer = &writer;
	ctx.body = &reader;
	ctx.stat.sizeStat.reqSizeBytes = req.size;

	try {
		std::ignore = router_.handle(ctx);
	} catch (const HttpStatus& status) {
		if (!writer.IsRespSent()) {
			setStatus(ctx, false, status.code(), status.whatStr());
		}
	} catch (const Error& status) {
		handleException(ctx, status);
	} catch (const std::exception& e) {
		handleException(ctx, Error(errLogic, e.what()));
	} catch (...) {
		handleException(ctx, Error(errLogic, "Unknown exception"));
	}
	finishResponse(ctx);
}

void ServerConnection::finishResponse(Context& ctx) {
	router_.log(ctx);

	ctx.writer->Write(std::string_view());
	ctx.stat.sizeStat.respSizeBytes = ctx.writer->Written();
	if (router_.onResponse_) {
		router_.onResponse_(ctx);
	}
}

void ServerConnection::badRequest(int code, const char* msg) {
	ResponseWriter writer(this);
	HandlerStat stat;
	Context ctx;
	ctx.request = nullptr;
	ctx.writer = &writer;
	ctx.body = nullptr;
	ctx.clientData = nullptr;
	ctx.stat.allocStat = stat;

	closeConn_ = true;
	auto dummy = ctx.String(code, msg);
	(void)dummy;
	ctx.stat.sizeStat.respSizeBytes = ctx.writer->Written();
	if (router_.onResponse_) {
		router_.onResponse_(ctx);
	}
}

void ServerConnection::parseParams(std::string_view str, Params& params) {
	const char* p = str.data();

	const char *name = nullptr, *value = nullptr;
	size_t namelen = 0;
	for (size_t l = 0; l < str.length(); l++) {
		if (!name) {
			name = p;
		}
		switch (*p) {
			case '&':
				if (!value) {
					namelen = p - name;
				}
				if (name) {
					params.emplace_back(std::string_view(name, namelen), std::string_view(value, value ? p - value : 0));
					name = value = nullptr;
					namelen = 0;
				}
				break;
			case '=':
				if (namelen == 0) {
					namelen = p - name;
					value = p + 1;
				}
				break;
			default:
				break;
		}
		p++;
	}
	if (name) {
		if (!value) {
			namelen = p - name;
		}
		params.emplace_back(std::string_view(name, namelen), std::string_view(value, value ? p - value : 0));
	}
}

void ServerConnection::writeHttpResponse(int code) {
	WrSerializer ser(wrBuf_.get_chunk());

	ser << "HTTP/1.1 "sv << code << ' ';

	auto it = kHTTPCodes.find(code);
	if (it != kHTTPCodes.end()) {
		ser << it->second;
	}
	ser << kStrEOL;
	wrBuf_.write(ser.DetachChunk());
}

ServerConnection::ReadResT ServerConnection::onRead() {
	size_t method_len = 0, path_len = 0, num_headers = kHttpMaxHeaders;
	const char *method, *uri;
	int minor_version = 0;
	struct phr_header headers[kHttpMaxHeaders];

	try {
		while (rdBuf_.size()) {
			if (stream_) {
				if (!readStreamBody()) {
					return ReadResT::Default;
				}
				continue;
			}
			if (!bodyLeft_) {
				auto chunk = rdBuf_.tail();

				num_headers = kHttpMaxHeaders;
				minor_version = 0;
				int res = phr_parse_request(chunk.data(), chunk.size(), &method, &method_len, &uri, &path_len, &minor_version, headers,
											&num_headers, 0);
				assertrx(res <= int(chunk.size()));

				if (res == -2) {
					if (rdBuf_.size() > chunk.size()) {
						rdBuf_.unroll();
						continue;
					}
					if (rdBuf_.size() == rdBuf_.capacity()) {
						if (!maxRequestSize_ || rdBuf_.capacity() < maxRequestSize_) {
							auto newCapacity = rdBuf_.capacity() * 2;
							if (maxRequestSize_ && newCapacity > maxRequestSize_) {
								newCapacity = maxRequestSize_;
							}
							rdBuf_.reserve(newCapacity);
						} else {
							badRequest(StatusRequestEntityTooLarge, "");
							return ReadResT::Default;
						}
					}
					return ReadResT::Default;
				} else if (res < 0) {
					badRequest(StatusBadRequest, "");
					return ReadResT::Default;
				}

				enableHttp11_ = (minor_version >= 1);
				request_.clientAddr = clientAddr_;
				request_.method = std::string_view(method, method_len);
				request_.uri = std::string_view(uri, path_len);
				request_.headers.clear();
				request_.params.clear();
				request_.size = size_t(res);

				auto p = request_.uri.find('?');
				if (p != std::string_view::npos) {
					parseParams(request_.uri.substr(p + 1), request_.params);
				}
				request_.path = request_.uri.substr(0, p);

				if (const auto* streamRoute = router_.findStreamRoute(request_.method, request_.path)) [[unlikely]] {
					if (closeConn_ || !startStream(*streamRoute, std::string_view(chunk.data(), res))) {
						return ReadResT::Default;
					}
					continue;
				}

				formData_ = false;
				expectContinue_ = false;
				for (int i = 0; i < int(num_headers); i++) {
					Header hdr{std::string_view(headers[i].name, headers[i].name_len),
							   std::string_view(headers[i].value, headers[i].value_len)};

					if (iequals(hdr.name, "content-length"sv)) {
						if (const auto status = parseRegularContentLength(hdr.val, bodyLeft_); status.code() != StatusOK) [[unlikely]] {
							badRequest(status.code(), status.what());
							return ReadResT::Default;
						}
					} else if (iequals(hdr.name, "transfer-encoding"sv) && iequals(hdr.val, "chunked"sv)) {
						bodyLeft_ = -1;
						memset(&chunked_decoder_, 0, sizeof(chunked_decoder_));
						badRequest(http::StatusInternalServerError, "Sorry, chunked encoded body not implemented");
						return ReadResT::Default;
					} else if (iequals(hdr.name, "content-type"sv) && iequals(hdr.val, "application/x-www-form-urlencoded"sv)) {
						formData_ = true;
					} else if (iequals(hdr.name, "connection"sv) && iequals(hdr.val, "close"sv)) {
						enableHttp11_ = false;
					} else if (iequals(hdr.name, "expect"sv) && iequals(hdr.val, "100-continue"sv)) {
						expectContinue_ = true;
					}
					request_.headers.push_back(hdr);
				}
				const bool requireRdRealloc = bodyLeft_ > 0 && unsigned(bodyLeft_ + res) > rdBuf_.capacity();
				if (requireRdRealloc) {
					if (!maxRequestSize_ || size_t(bodyLeft_) < maxRequestSize_) {
						// slow path: body is to big - need realloc
						// save current buffer.
						rdBuf_.reserve(bodyLeft_ + res + 0x1000);
						bodyLeft_ = 0;
						continue;
					} else {
						badRequest(StatusRequestEntityTooLarge, "");
						return ReadResT::Default;
					}
				} else {
					std::ignore = rdBuf_.erase(res);
				}
				if (expectContinue_) {
					if (bodyLeft_ < int(rdBuf_.capacity() - res)) {
						writeHttpResponse(StatusContinue);
						wrBuf_.write(kStrEOL);
					} else {
						badRequest(StatusRequestEntityTooLarge, "");
						return ReadResT::Default;
					}
				}
				if (!bodyLeft_) {
					handleRequest(request_);
					shrinkRdBufIfNeeded();
				}
			} else if (int(rdBuf_.size()) >= bodyLeft_) {
				// TODO: support chunked request body

				if (formData_) {
					auto chunk = rdBuf_.tail();
					if (chunk.size() < size_t(bodyLeft_)) {
						rdBuf_.unroll();
						chunk = rdBuf_.tail();
					}
					assertrx(chunk.size() >= size_t(bodyLeft_));
					parseParams(std::string_view(chunk.data(), bodyLeft_), request_.params);
				}

				request_.size += size_t(bodyLeft_);
				handleRequest(request_);
				std::ignore = rdBuf_.erase(bodyLeft_);
				bodyLeft_ = 0;
				shrinkRdBufIfNeeded();
			} else {
				break;
			}
		}
		if (!rdBuf_.size() && !bodyLeft_) {
			rdBuf_.clear();
			shrinkRdBufIfNeeded();
		}
		if (stream_ && streamReadBytes_ >= kStreamReadBudget && !sock_.ssl) [[unlikely]] {
			// Return to the event loop to serve the other connections during a long upload.
			// TLS connections do not do it: OpenSSL may keep decrypted bytes, which do not cause a new read event
			streamReadBytes_ = 0;
			return ReadResT::Yield;
		}
	} catch (std::exception& e) {
		fprintf(stderr, "reindexer error: dropping HTTP-connection. Reason: %s\n", e.what());
		closeConn_ = true;
	}
	return ReadResT::Default;
}

bool ServerConnection::startStream(const Router::StreamRoute& route, std::string_view head) {
	// The request is parsed again from a copy of its head, which lives until the end of the request:
	// the read buffer is reused for the body while the handler works
	stream_ = std::make_unique<StreamState>(this, std::string(head));
	std::ignore = rdBuf_.erase(head.size());
	streamReadBytes_ = 0;
	auto& st = *stream_;
	st.idleTimeout = route.idleTimeout;

	const char *method = nullptr, *uri = nullptr;
	size_t methodLen = 0, uriLen = 0, headersCount = kHttpMaxHeaders;
	int minorVersion = 0;
	phr_header headers[kHttpMaxHeaders];
	const int res =
		phr_parse_request(st.head.data(), st.head.size(), &method, &methodLen, &uri, &uriLen, &minorVersion, headers, &headersCount, 0);
	if (res != int(st.head.size())) [[unlikely]] {
		throw Error(errLogic, "Unable to parse the copy of HTTP request head: {}", res);
	}

	auto& req = st.request;
	req.clientAddr = clientAddr_;
	req.method = std::string_view(method, methodLen);
	req.uri = std::string_view(uri, uriLen);
	req.size = st.head.size();
	const auto p = req.uri.find('?');
	if (p != std::string_view::npos) {
		parseParams(req.uri.substr(p + 1), req.params);
	}
	req.path = req.uri.substr(0, p);
	if (!Router::matchRoute(route.path, req.path, req.urlParams)) [[unlikely]] {
		throw Error(errLogic, "Copy of HTTP request head does not match the route '{}'", route.path);
	}

	bool expectContinue = false;
	enableHttp11_ = (minorVersion >= 1);
	for (size_t i = 0; i < headersCount; ++i) {
		Header hdr{std::string_view(headers[i].name, headers[i].name_len), std::string_view(headers[i].value, headers[i].value_len)};
		if (iequals(hdr.name, "connection"sv) && iequals(trimOWS(hdr.val), "close"sv)) {
			enableHttp11_ = false;
		} else if (iequals(hdr.name, "expect"sv) && iequals(trimOWS(hdr.val), "100-continue"sv)) {
			expectContinue = (minorVersion >= 1);
		}
		req.headers.push_back(hdr);
	}

	uint64_t contentLength = 0;
	if (const auto framing = checkStreamFraming(req.headers, contentLength); framing.code() != StatusOK) [[unlikely]] {
		stream_.reset();
		badRequest(framing.code(), framing.what());
		return false;
	}

	// The body can not be skipped safely, so any answer before the body closes the connection
	closeConn_ = true;
	auto& ctx = st.ctx;
	const auto mw = callStreamHandler(
		[&] { return router_.runMiddlewares(ctx) == 0 ? HttpStatus() : HttpStatus(StatusInternalServerError, "Request was refused"); });
	if (mw.status.code() != StatusOK) [[unlikely]] {
		endStreamRequest(mw.status);
		return false;
	}

	const auto created = callStreamHandler([&] {
		st.handler = route.CreateHandler(req);
		return st.handler ? HttpStatus() : HttpStatus(StatusInternalServerError, "Stream handler was not created");
	});
	if (created.status.code() != StatusOK) [[unlikely]] {
		endStreamRequest(created.status);
		return false;
	}

	const auto preflight = callStreamHandler([&] { return st.handler->Preflight(ctx, contentLength); });
	if (preflight.status.code() != StatusOK) [[unlikely]] {
		// Abort() is not called after a refusal or an exception from Preflight()
		endStreamRequest(preflight.status);
		return false;
	}
	st.phase = StreamState::Phase::Body;
	if (st.writer.IsRespSent()) [[unlikely]] {
		// Preflight() has written a response instead of returning a status
		abortStream(StreamAbortReason::HandlerException, HttpStatus(StatusInternalServerError, "Unexpected response from the handler"));
		return false;
	}

	closeConn_ = false;
	st.bodySize = contentLength;
	st.bodyLeft = contentLength;
	if (!contentLength) {
		return finishStream();
	}
	st.lastRead = steady_clock_w::now_coarse();
	restartStreamTimer();
	if (expectContinue) {
		writeHttpResponse(StatusContinue);
		wrBuf_.write(kStrEOL);
	}
	return true;
}

bool ServerConnection::readStreamBody() {
	auto& st = *stream_;
	while (st.bodyLeft && rdBuf_.size()) {
		const auto data = rdBuf_.tail();
		const size_t size = size_t(std::min(uint64_t(data.size()), st.bodyLeft));
		const auto res = callStreamHandler([&] { return st.handler->OnChunk(std::span<const char>(data.data(), size)); });
		std::ignore = rdBuf_.erase(size);
		st.bodyLeft -= size;
		streamReadBytes_ += size;
		if (res.exception) [[unlikely]] {
			abortStream(StreamAbortReason::HandlerException, res.status);
			return false;
		}
		if (res.status.code() != StatusOK) [[unlikely]] {
			abortStream(StreamAbortReason::ChunkRejected, res.status);
			return false;
		}
		// Time in the handler is not counted as idle time
		st.lastRead = steady_clock_w::now_coarse();
	}
	return st.bodyLeft ? true : finishStream();
}

void ServerConnection::restartStreamTimer() {
	if (!stream_ || stream_->phase != StreamState::Phase::Body || stream_->idleTimeout <= std::chrono::milliseconds::zero()) {
		return;
	}
	// The timer is set once for the whole timeout and not on every read: the next deadline is checked when it fires
	const auto left = stream_->lastRead + stream_->idleTimeout - steady_clock_w::now_coarse();
	streamTimer_.start(std::max(std::chrono::duration<double>(left).count(), 0.0));
}

void ServerConnection::onStreamTimer(ev::timer& /*watcher*/, int /*time*/) {
	// The event loop does not catch exceptions from timer callbacks, so they are handled here, like in onRead()
	try {
		if (!stream_ || stream_->phase != StreamState::Phase::Body) {
			return;
		}
		auto& st = *stream_;
		const auto now = steady_clock_w::now_coarse();
		if (sock_.has_pending_data()) {
			// The client sends the body, the data is read on the next read event
			st.lastRead = now;
		}
		if (now - st.lastRead < st.idleTimeout) {
			restartStreamTimer();
			return;
		}
		abortStream(StreamAbortReason::Timeout,
					HttpStatus(StatusRequestTimeout, "No part of the request body came during the idle timeout"));
	} catch (const std::exception& e) {
		fprintf(stderr, "reindexer error: dropping HTTP-connection. Reason: %s\n", e.what());
		closeConn_ = true;
	} catch (...) {
		fprintf(stderr, "reindexer error: dropping HTTP-connection. Reason: unknown exception\n");
		closeConn_ = true;
	}
	// Sends the response and closes the connection
	callback(io_, ev::WRITE);
}

bool ServerConnection::finishStream() {
	auto& st = *stream_;
	const auto res = callStreamHandler([&] { return st.handler->Finish(); });
	if (res.exception) [[unlikely]] {
		st.Abort(StreamAbortReason::HandlerException);
	} else {
		st.phase = StreamState::Phase::Done;
	}
	endStreamRequest(res.status);
	return !closeConn_;
}

void ServerConnection::abortStream(StreamAbortReason reason, const HttpStatus& status) {
	// The rest of the body is not read, so the connection can not be reused
	closeConn_ = true;
	stream_->Abort(reason);
	endStreamRequest(status);
}

void ServerConnection::endStreamRequest(const HttpStatus& status) {
	auto& st = *stream_;
	auto& ctx = st.ctx;
	if (!st.writer.IsRespSent()) {
		const auto response = responseStatus(status);
		setStatus(ctx, response.code() == StatusOK, response.code(), response.whatStr());
	}
	ctx.stat.sizeStat.reqSizeBytes = st.request.size + size_t(st.bodySize - st.bodyLeft);
	finishResponse(ctx);
	streamTimer_.stop();
	stream_.reset();
}

void ServerConnection::ResponseWriter::SetHeader(const Header& hdr) {
	if (respSend_) {
		return;
	}
	headers_ << hdr.name << ": "sv << hdr.val << kStrEOL;
}
void ServerConnection::ResponseWriter::SetRespCode(int code) noexcept {
	if (respSend_) {
		return;
	}
	code_ = code;
}

void ServerConnection::ResponseWriter::SetContentLength(size_t length) noexcept {
	if (respSend_) {
		return;
	}
	contentLength_ = length;
}

void ServerConnection::ResponseWriter::Write(chunk&& chunk, Writer::WriteMode mode) {
	char szBuf[64], dtBuf[128];
	if (!respSend_) {
		conn_->writeHttpResponse(code_);

		if (conn_->closeConn_) {
			// The connection is closed after this response, e.g. after a refusal before the body
			SetHeader(Header{"Connection"sv, "close"sv});
		} else if (conn_->enableHttp11_) {
			SetHeader(Header{"Connection"sv, "keep-alive"sv});
		}
		if (!isChunkedResponse()) {
			size_t l = u64toa(contentLength_, szBuf) - szBuf;
			SetHeader(Header{"Content-Length"sv, {szBuf, l}});
		} else {
			SetHeader(Header{"Transfer-Encoding"sv, "chunked"sv});
		}

		std::tm tm;
		std::time_t t = system_clock_w::to_time_t(system_clock_w::now_coarse());
		fast_gmtime_r(&t, &tm);				   // gmtime_r(&t, &tm);
		size_t l = fast_strftime(dtBuf, &tm);  // strftime(tmpBuf, sizeof(tmpBuf), "%a %c", &tm);
		SetHeader(Header{"Date"sv, {dtBuf, l}});
		SetHeader(Header{"Server"sv, "reindex"sv});

		headers_ << kStrEOL;
		conn_->wrBuf_.write(headers_.DetachChunk());
		respSend_ = true;
	}

	size_t len = chunk.len();
	if (isChunkedResponse() && mode == WriteMode::Default) {
		size_t l = u32toax(len, szBuf) - szBuf;
		conn_->wrBuf_.write({szBuf, l});
		conn_->wrBuf_.write(kStrEOL);
	}

	conn_->wrBuf_.write(std::move(chunk));

	written_ += len;
	if (isChunkedResponse() && mode == WriteMode::Default) {
		conn_->wrBuf_.write(kStrEOL);
	}
	if (!len && !conn_->enableHttp11_) {
		conn_->closeConn_ = true;
	}
}

void ServerConnection::ResponseWriter::Write(std::string_view data) {
	WrSerializer ser(conn_->wrBuf_.get_chunk());
	ser << data;
	Write(ser.DetachChunk());
}
chunk ServerConnection::ResponseWriter::GetChunk() { return conn_->wrBuf_.get_chunk(); }

ssize_t ServerConnection::BodyReader::Read(void* buf, size_t size) {
	size_t readSize = 0;
	if (conn_->bodyLeft_ > 0) {
		readSize = conn_->rdBuf_.read(reinterpret_cast<char*>(buf), std::min(ssize_t(size), conn_->bodyLeft_));
		conn_->bodyLeft_ -= readSize;
	}
	return readSize;
}

std::string ServerConnection::BodyReader::Read(size_t size) {
	std::string ret;
	if (conn_->bodyLeft_ > 0) {
		size = std::min(ssize_t(size), conn_->bodyLeft_);
		ret.resize(size);
		size_t readSize = conn_->rdBuf_.read(&ret[0], size);
		conn_->bodyLeft_ -= readSize;
	}
	return ret;
}

ssize_t ServerConnection::BodyReader::Pending() const { return conn_->bodyLeft_; }

}  // namespace reindexer::net::http
