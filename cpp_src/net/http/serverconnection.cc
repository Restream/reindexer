
#include "serverconnection.h"
#include <ctime>
#include <unordered_map>
#include "itoa/itoa.h"
#include "time/fast_time.h"
#include "tools/errors.h"
#include "tools/stringstools.h"
namespace reindexer {
namespace net {
namespace http {

static const char kStrEOL[] = "\r\n";
extern std::unordered_map<int, const char *> kHTTPCodes;

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

void ServerConnection::handleRequest(Request &req) {
	ResponseWriter writer(this);
	BodyReader reader(this);
	Stat stat;
	Context ctx;
	ctx.request = &req;
	ctx.writer = &writer;
	ctx.body = &reader;
	ctx.stat = stat;

	try {
		router_.handle(ctx);
	} catch (const HttpStatus &status) {
		if (!writer.IsRespSent()) {
			ctx.String(status.code, status.what);
		}
	} catch (const Error &status) {
		if (!writer.IsRespSent()) {
			ctx.String(StatusInternalServerError, status.what());
		}
	}
	router_.log(ctx);

	ctx.writer->Write(0, 0);
}

void ServerConnection::badRequest(int code, const char *msg) {
	ResponseWriter writer(this);
	Stat stat;
	Context ctx;
	ctx.request = nullptr;
	ctx.writer = &writer;
	ctx.body = nullptr;
	ctx.clientData = nullptr;
	ctx.stat = stat;

	closeConn_ = true;
	ctx.String(code, msg);
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
	char tmpBuf[512];
	char *d = tmpBuf;
	d = strappend(d, "HTTP/1.1 ");
	d = i32toa(code, d);

	*d++ = ' ';
	auto it = kHTTPCodes.find(code);
	if (it != kHTTPCodes.end()) d = strappend(d, it->second);
	d = strappend(d, kStrEOL);

	wrBuf_.write(tmpBuf, d - tmpBuf);
}

void ServerConnection::onRead() {
	size_t method_len = 0, path_len = 0, num_headers = kHttpMaxHeaders;
	const char *method, *uri;
	int minor_version = 0;
	struct phr_header headers[kHttpMaxHeaders];

	while (rdBuf_.size()) {
		if (!bodyLeft_) {
			auto it = rdBuf_.tail();

			num_headers = kHttpMaxHeaders;
			minor_version = 0;
			int res = phr_parse_request(it.data, it.len, &method, &method_len, &uri, &path_len, &minor_version, headers, &num_headers, 0);
			assert(res <= int(it.len));

			if (res == -2) {
				if (rdBuf_.size() > it.len) {
					rdBuf_.unroll();
					continue;
				}
				return;
			} else if (res < 0) {
				badRequest(StatusBadRequest, "");
				return;
			}

			enableHttp11_ = (minor_version >= 1);
			request_.method = string_view(method, method_len);
			request_.uri = string_view(uri, path_len);
			request_.headers.clear();
			request_.params.clear();

			auto p = request_.uri.find('?');
			if (p != string_view::npos) {
				parseParams(request_.uri.substr(p + 1));
			}
			request_.path = request_.uri.substr(0, p);

			formData_ = false;
			for (int i = 0; i < int(num_headers); i++) {
				Header hdr{string_view(headers[i].name, headers[i].name_len), string_view(headers[i].value, headers[i].value_len)};

				if (iequals(hdr.name, "content-length"_sv)) {
					bodyLeft_ = atoi(hdr.val.data());
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
					wrBuf_.write(kStrEOL, sizeof(kStrEOL) - 1);
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
				auto it = rdBuf_.tail();
				if (it.len < size_t(bodyLeft_)) {
					rdBuf_.unroll();
					it = rdBuf_.tail();
				}
				assert(it.len >= size_t(bodyLeft_));
				parseParams(string_view(it.data, bodyLeft_));
			}

			handleRequest(request_);
			rdBuf_.erase(bodyLeft_);
			bodyLeft_ = 0;
		} else
			break;
	}
}

bool ServerConnection::ResponseWriter::SetHeader(const Header &hdr) {
	if (respSend_) return false;
	size_t pos = headers_.size();

	headers_.reserve(headers_.size() + hdr.name.size() + hdr.val.size() + 5);

	char *d = &headers_[pos];
	d = strappend(d, hdr.name);
	d = strappend(d, ": ");
	d = strappend(d, hdr.val);
	d = strappend(d, kStrEOL);
	*d = 0;
	headers_.resize(d - headers_.begin());

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

ssize_t ServerConnection::ResponseWriter::Write(const void *buf, size_t size) {
	char tmpBuf[256];
	if (!respSend_) {
		conn_->writeHttpResponse(code_);

		if (conn_->enableHttp11_ && !conn_->closeConn_) {
			SetHeader(Header{"ServerConnection", "keep-alive"});
		}
		if (!isChunkedResponse()) {
			*u32toa(contentLength_, tmpBuf) = 0;
			SetHeader(Header{"Content-Length", tmpBuf});
		} else {
			SetHeader(Header{"Transfer-Encoding", "chunked"});
		}

		std::tm tm;
		std::time_t t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		fast_gmtime_r(&t, &tm);		 // gmtime_r(&t, &tm);
		fast_strftime(tmpBuf, &tm);  // strftime(tmpBuf, sizeof(tmpBuf), "%a %c", &tm);
		SetHeader(Header{"Date", tmpBuf});
		SetHeader(Header{"Server", "reindex"});

		conn_->wrBuf_.write(headers_.data(), headers_.size());
		conn_->wrBuf_.write(kStrEOL, sizeof(kStrEOL) - 1);
		respSend_ = true;
	}

	if (isChunkedResponse()) {
		int n = u32toax(size, tmpBuf) - tmpBuf;
		conn_->wrBuf_.write(tmpBuf, n);
		conn_->wrBuf_.write(kStrEOL, sizeof(kStrEOL) - 1);
	}

	conn_->wrBuf_.write(reinterpret_cast<const char *>(buf), size);
	written_ += size;
	if (isChunkedResponse()) {
		conn_->wrBuf_.write(kStrEOL, sizeof(kStrEOL) - 1);
	}
	if (!size && !conn_->enableHttp11_) {
		conn_->closeConn_ = true;
	}
	return size;
}
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
