
#include "serverconnection.h"
#include <ctime>
#include <unordered_map>
#include "core/cjson/jsonbuilder.h"
#include "itoa/itoa.h"
#include "time/fast_time.h"
#include "tools/errors.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
namespace reindexer {
namespace net {
namespace http {

static const string_view kStrEOL = "\r\n"_sv;
extern std::unordered_map<int, string_view> kHTTPCodes;

ServerConnection::ServerConnection(int fd, ev::dynamic_loop &loop, Router &router) : ConnectionST(fd, loop), router_(router) {
	callback(io_, ev::READ);
}

bool ServerConnection::Restart(int fd) {
	restart(fd);
	bodyLeft_ = 0;
	formData_ = false;
	enableHttp11_ = false;
	expectContinue_ = false;
	callback(io_, ev::READ);
	return true;
}

void ServerConnection::Attach(ev::dynamic_loop &loop) {
	if (!attached_) attach(loop);
}
void ServerConnection::Detach() {
	if (attached_) detach();
}

void ServerConnection::onClose() {}

void ServerConnection::setJsonStatus(Context &ctx, bool success, int responseCode, const string &status) {
	WrSerializer ser;
	JsonBuilder builder(ser);
	builder.Put("success", success);
	builder.Put("response_code", responseCode);
	builder.Put("description", status);
	builder.End();
	ctx.JSON(responseCode, ser.Slice());
}

void ServerConnection::handleRequest(Request &req) {
	ResponseWriter writer(this);
	BodyReader reader(this);
	Context ctx;
	ctx.request = &req;
	ctx.writer = &writer;
	ctx.body = &reader;
	ctx.stat.sizeStat.reqSizeBytes = req.size;

	try {
		router_.handle(ctx);
	} catch (const HttpStatus &status) {
		if (!writer.IsRespSent()) {
			setJsonStatus(ctx, false, status.code, status.what);
		}
	} catch (const Error &status) {
		if (!writer.IsRespSent()) {
			setJsonStatus(ctx, false, StatusInternalServerError, status.what());
		}
	}
	router_.log(ctx);

	ctx.writer->Write(string_view());
	ctx.stat.sizeStat.respSizeBytes = ctx.writer->Written();
	if (router_.onResponse_) {
		router_.onResponse_(ctx);
	}
}

void ServerConnection::badRequest(int code, const char *msg) {
	ResponseWriter writer(this);
	HandlerStat stat;
	Context ctx;
	ctx.request = nullptr;
	ctx.writer = &writer;
	ctx.body = nullptr;
	ctx.clientData = nullptr;
	ctx.stat.allocStat = stat;

	closeConn_ = true;
	ctx.String(code, msg);
	ctx.stat.sizeStat.respSizeBytes = ctx.writer->Written();
	if (router_.onResponse_) {
		router_.onResponse_(ctx);
	}
}

void ServerConnection::parseParams(const string_view &str) {
	const char *p = str.data();

	const char *name = nullptr, *value = nullptr;
	size_t namelen = 0;
	for (size_t l = 0; l < str.length(); l++) {
		if (!name) name = p;
		switch (*p) {
			case '&':
				if (!value) namelen = p - name;
				if (name) {
					request_.params.push_back(Param{string_view(name, namelen), string_view(value, value ? p - value : 0)});
					name = value = nullptr;
				}
				break;
			case '=':
				namelen = p - name;
				value = p + 1;
				break;
			default:
				break;
		}
		p++;
	}
	if (name) {
		if (!value) namelen = p - name;
		request_.params.push_back(Param{string_view(name, namelen), string_view(value, value ? p - value : 0)});
	}
}

void ServerConnection::writeHttpResponse(int code) {
	WrSerializer ser(wrBuf_.get_chunk());

	ser << "HTTP/1.1 "_sv << code << ' ';

	auto it = kHTTPCodes.find(code);
	if (it != kHTTPCodes.end()) ser << it->second;
	ser << kStrEOL;
	wrBuf_.write(ser.DetachChunk());
}

void ServerConnection::onRead() {
	size_t method_len = 0, path_len = 0, num_headers = kHttpMaxHeaders;
	const char *method, *uri;
	int minor_version = 0;
	struct phr_header headers[kHttpMaxHeaders];

	while (rdBuf_.size()) {
		if (!bodyLeft_) {
			auto chunk = rdBuf_.tail();

			num_headers = kHttpMaxHeaders;
			minor_version = 0;
			int res = phr_parse_request(chunk.data(), chunk.size(), &method, &method_len, &uri, &path_len, &minor_version, headers,
										&num_headers, 0);
			assert(res <= int(chunk.size()));

			if (res == -2) {
				if (rdBuf_.size() > chunk.size()) {
					rdBuf_.unroll();
					continue;
				}
				return;
			} else if (res < 0) {
				badRequest(StatusBadRequest, "");
				return;
			}

			enableHttp11_ = (minor_version >= 1);
			request_.clientAddr = clientAddr_;
			request_.method = string_view(method, method_len);
			request_.uri = string_view(uri, path_len);
			request_.headers.clear();
			request_.params.clear();
			request_.size = size_t(res);

			auto p = request_.uri.find('?');
			if (p != string_view::npos) {
				parseParams(request_.uri.substr(p + 1));
			}
			request_.path = request_.uri.substr(0, p);

			formData_ = false;
			for (int i = 0; i < int(num_headers); i++) {
				Header hdr{string_view(headers[i].name, headers[i].name_len), string_view(headers[i].value, headers[i].value_len)};

				if (iequals(hdr.name, "content-length"_sv)) {
					bodyLeft_ = stoi(hdr.val);
				} else if (iequals(hdr.name, "transfer-encoding"_sv) && iequals(hdr.val, "chunked"_sv)) {
					bodyLeft_ = -1;
					memset(&chunked_decoder_, 0, sizeof(chunked_decoder_));
					badRequest(http::StatusInternalServerError, "Sorry, chunked encoded body not implemented");
					return;
				} else if (iequals(hdr.name, "content-type"_sv) && iequals(hdr.val, "application/x-www-form-urlencoded"_sv)) {
					formData_ = true;
				} else if (iequals(hdr.name, "connection"_sv) && iequals(hdr.val, "close"_sv)) {
					enableHttp11_ = false;
				} else if (iequals(hdr.name, "expect"_sv) && iequals(hdr.val, "100-continue"_sv)) {
					expectContinue_ = true;
				}
				request_.headers.push_back(hdr);
			}
			if (bodyLeft_ > 0 && unsigned(bodyLeft_ + res) > rdBuf_.capacity() && bodyLeft_ < kHttpMaxBodySize) {
				// slow path: body is to big - need realloc
				// save current buffer.
				rdBuf_.reserve(bodyLeft_ + res + 0x1000);
				bodyLeft_ = 0;
				continue;
			} else {
				rdBuf_.erase(res);
			}
			if (expectContinue_) {
				if (bodyLeft_ < int(rdBuf_.capacity() - res)) {
					writeHttpResponse(StatusContinue);
					wrBuf_.write(kStrEOL);
				} else {
					badRequest(StatusRequestEntityTooLarge, "");
					return;
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
				assert(chunk.size() >= size_t(bodyLeft_));
				parseParams(string_view(chunk.data(), bodyLeft_));
			}

			request_.size += size_t(bodyLeft_);
			handleRequest(request_);
			rdBuf_.erase(bodyLeft_);
			bodyLeft_ = 0;
		} else {
			break;
		}
	}
	if (!rdBuf_.size() && !bodyLeft_) rdBuf_.clear();
}

bool ServerConnection::ResponseWriter::SetHeader(const Header &hdr) {
	if (respSend_) return false;
	headers_ << hdr.name << ": "_sv << hdr.val << kStrEOL;
	return true;
}
bool ServerConnection::ResponseWriter::SetRespCode(int code) {
	if (respSend_) return false;
	code_ = code;
	return true;
}

bool ServerConnection::ResponseWriter::SetContentLength(size_t length) {
	if (respSend_) return false;
	contentLength_ = length;
	return true;
}

ssize_t ServerConnection::ResponseWriter::Write(chunk &&chunk) {
	char szBuf[64], dtBuf[128];
	if (!respSend_) {
		conn_->writeHttpResponse(code_);

		if (conn_->enableHttp11_ && !conn_->closeConn_) {
			SetHeader(Header{"ServerConnection"_sv, "keep-alive"_sv});
		}
		if (!isChunkedResponse()) {
			size_t l = u64toa(contentLength_, szBuf) - szBuf;
			SetHeader(Header{"Content-Length"_sv, {szBuf, l}});
		} else {
			SetHeader(Header{"Transfer-Encoding"_sv, "chunked"_sv});
		}

		std::tm tm;
		std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		fast_gmtime_r(&t, &tm);				   // gmtime_r(&t, &tm);
		size_t l = fast_strftime(dtBuf, &tm);  // strftime(tmpBuf, sizeof(tmpBuf), "%a %c", &tm);
		SetHeader(Header{"Date"_sv, {dtBuf, l}});
		SetHeader(Header{"Server"_sv, "reindex"_sv});

		headers_ << kStrEOL;
		conn_->wrBuf_.write(headers_.DetachChunk());
		respSend_ = true;
	}

	size_t len = chunk.len_;
	if (isChunkedResponse()) {
		size_t l = u32toax(len, szBuf) - szBuf;
		conn_->wrBuf_.write({szBuf, l});
		conn_->wrBuf_.write(kStrEOL);
	}

	conn_->wrBuf_.write(std::move(chunk));

	written_ += len;
	if (isChunkedResponse()) {
		conn_->wrBuf_.write(kStrEOL);
	}
	if (!len && !conn_->enableHttp11_) {
		conn_->closeConn_ = true;
	}
	return len;
}
ssize_t ServerConnection::ResponseWriter::Write(string_view data) {
	WrSerializer ser(conn_->wrBuf_.get_chunk());
	ser << data;
	return Write(ser.DetachChunk());
}
chunk ServerConnection::ResponseWriter::GetChunk() { return conn_->wrBuf_.get_chunk(); }

bool ServerConnection::ResponseWriter::SetConnectionClose() {
	conn_->closeConn_ = true;
	return true;
}

ssize_t ServerConnection::BodyReader::Read(void *buf, size_t size) {
	size_t readed = conn_->rdBuf_.read(reinterpret_cast<char *>(buf), std::min(ssize_t(size), conn_->bodyLeft_));
	conn_->bodyLeft_ -= readed;
	return readed;
}

std::string ServerConnection::BodyReader::Read(size_t size) {
	std::string ret;
	size = std::min(ssize_t(size), conn_->bodyLeft_);
	ret.resize(size);
	size_t readed = conn_->rdBuf_.read(&ret[0], size);
	conn_->bodyLeft_ -= readed;
	return ret;
}

ssize_t ServerConnection::BodyReader::Pending() const { return conn_->bodyLeft_; }

}  // namespace http
}  // namespace net
}  // namespace reindexer
