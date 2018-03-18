
#include "connection.h"
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <time.h>
#include <unistd.h>
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

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

Connection::Connection(int fd, ev::dynamic_loop &loop, Router &router)
	: fd_(fd), curEvents_(0), wrBuf_(kHttpWriteBufSize), rdBuf_(kHttpReadbufSize), router_(router) {
	int flag = 1;
	setsockopt(fd, SOL_TCP, TCP_NODELAY, &flag, sizeof(flag));

	io_.set<Connection, &Connection::callback>(this);
	io_.set(loop);
	callback(io_, ev::READ);
}

Connection::~Connection() {
	if (fd_ >= 0) {
		close(fd_);
		io_.stop();
	}
}

bool Connection::Restart(int fd) {
	assert(fd_ < 0);
	fd_ = fd;
	int flag = 1;
	setsockopt(fd, SOL_TCP, TCP_NODELAY, &flag, sizeof(flag));

	wrBuf_.clear();
	rdBuf_.clear();
	curEvents_ = 0;
	bodyLeft_ = 0;
	formData_ = false;
	closeConn_ = false;
	enableHttp11_ = false;
	expectContinue_ = false;
	callback(io_, ev::READ);
	return true;
}

void Connection::Reatach(ev::dynamic_loop &loop) {
	io_.stop();
	io_.set<Connection, &Connection::callback>(this);
	io_.set(loop);
	io_.start(fd_, curEvents_);
}

// Generic callback
void Connection::callback(ev::io & /*watcher*/, int revents) {
	if (ev::ERROR & revents) return;

	if (revents & ev::READ) {
		read_cb();
		revents |= ev::WRITE;
	}
	if (revents & ev::WRITE) {
		write_cb();
		if (!wrBuf_.size()) wrBuf_.clear();
	}

	int nevents = ev::READ | (wrBuf_.size() ? ev::WRITE : 0);

	if (curEvents_ != nevents && fd_ >= 0) {
		(curEvents_) ? io_.set(nevents) : io_.start(fd_, nevents);
		curEvents_ = nevents;
	}
}

// Socket is writable
void Connection::write_cb() {
	while (wrBuf_.size()) {
		auto it = wrBuf_.tail();
		ssize_t written = ::send(fd_, it.data, it.len, 0);

		if (written < 0 && errno == EINTR) continue;

		if (written < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
				io_.loop.break_loop();
				io_.stop();
				close(fd_);
				fd_ = -1;
			}
			return;
		}

		wrBuf_.erase(written);
		if (written < ssize_t(it.len)) return;
	};
	if (closeConn_) {
		io_.loop.break_loop();
		io_.stop();
		close(fd_);
		fd_ = -1;
	}
}

// Receive message from client socket
void Connection::read_cb() {
	for (;;) {
		auto it = rdBuf_.head();
		ssize_t nread = ::recv(fd_, it.data, it.len, 0);

		if (nread < 0 && errno == EINTR) continue;

		if ((nread < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) || nread == 0) {
			io_.loop.break_loop();
			io_.stop();
			close(fd_);
			fd_ = -1;
		} else if (nread > 0) {
			rdBuf_.advance_head(nread);
			if (!closeConn_) parseRequest();
		}
		if (nread < ssize_t(it.len) || !rdBuf_.available()) return;
	}
}

void Connection::handleRequest(Request &req) {
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
	} catch (const Error &err) {
		if (!writer.IsRespSent()) {
			ctx.String(http::StatusInternalServerError, err.what());
		}
	}

	ctx.writer->Write(0, 0);
}

void Connection::badRequest(int code, const char *msg) {
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

void Connection::parseParams(char *p) {
	char *name = nullptr;
	char *value = nullptr;
	while (*p) {
		if (!name) name = p;
		switch (*p) {
			case '&':
				*p++ = 0;
				if (name) {
					urldecode2(name, name);
					if (value) urldecode2(value, value);
					request_.params.push_back(Param{name, value});
					name = value = nullptr;
				}
				break;
			case '=':
				*p++ = 0;
				value = p;
				break;
			default:
				p++;
				break;
		}
	}
	if (name) {
		urldecode2(name, name);
		if (value) urldecode2(value, value);
		request_.params.push_back(Param{name, value});
		name = nullptr;
	}
}

void Connection::writeHttpResponse(int code) {
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

void Connection::parseRequest() {
	size_t method_len = 0, path_len = 0, num_headers = kHttpMaxHeaders;
	int minor_version = 0;
	struct phr_header headers[kHttpMaxHeaders];

	while (rdBuf_.size()) {
		if (!bodyLeft_) {
			auto it = rdBuf_.tail();

			num_headers = kHttpMaxHeaders;
			minor_version = 0;
			int res = phr_parse_request(it.data, it.len, &request_.method, &method_len, &request_.uri, &path_len, &minor_version, headers,
										&num_headers, 0);
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
			const_cast<char *>(request_.method)[method_len] = 0;
			const_cast<char *>(request_.uri)[path_len] = 0;
			strncpy(request_.path, request_.uri, sizeof(request_.path));
			request_.path[sizeof(request_.path) - 1] = 0;
			request_.headers.clear();
			request_.params.clear();

			char *p = strchr(const_cast<char *>(request_.path), '?');
			if (p) {
				*p = 0;
				parseParams(p + 1);
			}

			formData_ = false;
			char *tmpHdrVal = reinterpret_cast<char *>(alloca(res)), *d;
			for (int i = 0; i < int(num_headers); i++) {
				Header hdr{headers[i].name, headers[i].value};
				const_cast<char *>(hdr.name)[headers[i].name_len] = 0;
				const_cast<char *>(hdr.val)[headers[i].value_len] = 0;
				for (char *p = const_cast<char *>(hdr.name); *p; ++p) *p = tolower(*p);
				d = tmpHdrVal;
				for (const char *p = hdr.val; *p; ++p, ++d) *d = tolower(*p);
				*d++ = 0;

				if (!strcmp(hdr.name, "content-length")) {
					bodyLeft_ = atoi(hdr.val);
				} else if (!strcmp(hdr.name, "transfer-encoding") && !strcmp(tmpHdrVal, "chunked")) {
					bodyLeft_ = -1;
					memset(&chunked_decoder_, 0, sizeof(chunked_decoder_));
				} else if (!strcmp(hdr.name, "content-type") && !strcmp(tmpHdrVal, "application/x-www-form-urlencoded")) {
					formData_ = true;
				} else if (!strcmp(hdr.name, "connection") && !strcmp(tmpHdrVal, "close")) {
					enableHttp11_ = false;
				} else if (!strcmp(hdr.name, "expect") && !strcmp(tmpHdrVal, "100-continue")) {
					expectContinue_ = true;
				}
				request_.headers.push_back(hdr);
			}

			rdBuf_.erase(res);
			if (expectContinue_) {
				if (bodyLeft_ < int(rdBuf_.available())) {
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
			// auto it = rdBuf_.tail();
			// size_t bufSz = it.len;
			// if (bodyLeft_ < 0) {
			// 	int ret = phr_decode_chunked(&chunked_decoder_, it.data, &bufSz);
			// 	if (ret == -2) {
			// 		continue;
			// 	}
			// }

			if (formData_) {
				auto it = rdBuf_.tail();
				if (it.len < size_t(bodyLeft_)) {
					rdBuf_.unroll();
					it = rdBuf_.tail();
				}
				if (it.len < size_t(bodyLeft_)) {
					badRequest(StatusInternalServerError, "error parsing form-urlencoded");
					return;
				}
				char tmp = it.data[bodyLeft_];
				it.data[bodyLeft_] = 0;
				parseParams(it.data);
				it.data[bodyLeft_] = tmp;
				rdBuf_.erase(bodyLeft_);
				bodyLeft_ = 0;
			}

			handleRequest(request_);
			rdBuf_.erase(bodyLeft_);
			bodyLeft_ = 0;
		} else
			break;
	}
}

bool Connection::ResponseWriter::SetHeader(const Header &hdr) {
	if (respSend_) return false;
	size_t pos = headers_.size();

	headers_.reserve(headers_.size() + strlen(hdr.name) + strlen(hdr.val) + 5);

	char *d = &headers_[pos];
	d = strappend(d, hdr.name);
	d = strappend(d, ": ");
	d = strappend(d, hdr.val);
	d = strappend(d, kStrEOL);
	*d = 0;
	headers_.resize(d - headers_.begin());

	return true;
}
bool Connection::ResponseWriter::SetRespCode(int code) {
	if (respSend_) return false;
	code_ = code;
	return true;
}

bool Connection::ResponseWriter::SetContentLength(size_t length) {
	if (respSend_) return false;
	contentLength_ = length;
	return true;
}

ssize_t Connection::ResponseWriter::Write(const void *buf, size_t size) {
	char tmpBuf[256];
	if (!respSend_) {
		conn_->writeHttpResponse(code_);

		if (conn_->enableHttp11_ && !conn_->closeConn_) {
			SetHeader(Header{"Connection", "keep-alive"});
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
bool Connection::ResponseWriter::SetConnectionClose() {
	conn_->closeConn_ = true;
	return true;
}

ssize_t Connection::BodyReader::Read(void *buf, size_t size) {
	size_t readed = conn_->rdBuf_.read(reinterpret_cast<char *>(buf), std::min(ssize_t(size), conn_->bodyLeft_));
	conn_->bodyLeft_ -= readed;
	return readed;
}

std::string Connection::BodyReader::Read(size_t size) {
	std::string ret;
	size = std::min(ssize_t(size), conn_->bodyLeft_);
	ret.resize(size);
	size_t readed = conn_->rdBuf_.read(&ret[0], size);
	conn_->bodyLeft_ -= readed;
	return ret;
}

ssize_t Connection::BodyReader::Pending() const { return conn_->bodyLeft_; }

}  // namespace http
}  // namespace net
}  // namespace reindexer
