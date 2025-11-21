
#include "serverconnection.h"
#include <unordered_map>
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "itoa/itoa.h"
#include "server/outputparameters.h"
#include "time/fast_time.h"
#include "tools/assertrx.h"
#include "tools/errors.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer::net::http {

using namespace std::string_view_literals;

static const std::string_view kStrEOL = "\r\n"sv;
extern std::unordered_map<int, std::string_view> kHTTPCodes;

ServerConnection::ServerConnection(socket&& s, ev::dynamic_loop& loop, Router& router, size_t maxRequestSize)
	: ConnectionST(std::move(s), loop, false, maxRequestSize < kConnReadbufSize ? maxRequestSize : kConnReadbufSize),
	  router_(router),
	  maxRequestSize_(maxRequestSize) {
	callback(io_, ev::READ);
}

bool ServerConnection::Restart(socket&& s) {
	restart(std::move(s));
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
	}
}
void ServerConnection::Detach() {
	if (attached_) {
		detach();
	}
}

void ServerConnection::onClose() {}

void ServerConnection::handleException(Context& ctx, const Error& status) {
	auto writer = dynamic_cast<ResponseWriter*>(ctx.writer);
	if (writer && !writer->IsRespSent()) {
		HttpStatus httpStatus(status);
		setStatus(ctx, false, httpStatus.code, httpStatus.what);
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
	ProtobufBuilder builder(&ser);
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
			setStatus(ctx, false, status.code, status.what);
		}
	} catch (const Error& status) {
		handleException(ctx, status);
	} catch (const std::exception& e) {
		handleException(ctx, Error(errLogic, e.what()));
	} catch (...) {
		handleException(ctx, Error(errLogic, "Unknown exception"));
	}
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

void ServerConnection::parseParams(std::string_view str) {
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
					request_.params.emplace_back(std::string_view(name, namelen), std::string_view(value, value ? p - value : 0));
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
		request_.params.emplace_back(std::string_view(name, namelen), std::string_view(value, value ? p - value : 0));
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
					parseParams(request_.uri.substr(p + 1));
				}
				request_.path = request_.uri.substr(0, p);

				formData_ = false;
				for (int i = 0; i < int(num_headers); i++) {
					Header hdr{std::string_view(headers[i].name, headers[i].name_len),
							   std::string_view(headers[i].value, headers[i].value_len)};

					if (iequals(hdr.name, "content-length"sv)) {
						bodyLeft_ = stoi(hdr.val);
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
					parseParams(std::string_view(chunk.data(), bodyLeft_));
				}

				request_.size += size_t(bodyLeft_);
				handleRequest(request_);
				std::ignore = rdBuf_.erase(bodyLeft_);
				bodyLeft_ = 0;
			} else {
				break;
			}
		}
		if (!rdBuf_.size() && !bodyLeft_) {
			rdBuf_.clear();
		}
	} catch (std::exception& e) {
		fprintf(stderr, "reindexer error: dropping HTTP-connection. Reason: %s\n", e.what());
		closeConn_ = true;
	}
	return ReadResT::Default;
}

void ServerConnection::ResponseWriter::SetHeader(const Header& hdr) noexcept {
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

		if (conn_->enableHttp11_ && !conn_->closeConn_) {
			SetHeader(Header{"ServerConnection"sv, "keep-alive"sv});
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
