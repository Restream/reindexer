#include "router.h"
#include <cstdarg>
#include <unordered_map>
#include "debug/allocdebug.h"
#include "tools/fsops.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace net {
namespace http {

std::unordered_map<int, const char *> kHTTPCodes = {

	{StatusContinue, "Continue"},
	{StatusOK, "OK"},
	{StatusCreated, "Created"},
	{StatusAccepted, "Accepted"},
	{StatusNoContent, "No Content"},
	{StatusMovedPermanently, "MovedPermanently"},
	{StatusMovedTemporarily, "Moved Temporarily"},
	{StatusNotModified, "Not Modified"},
	{StatusBadRequest, "Bad Request"},
	{StatusUnauthorized, "Unauthorized"},
	{StatusForbidden, "Forbidden"},
	{StatusNotFound, "Not found"},
	{StatusMethodNotAllowed, "Method Not Allowed"},
	{StatusNotAcceptable, "Not Acceptable"},
	{StatusRequestTimeout, "Request Timeout"},
	{StatusLengthRequired, "Length Required"},
	{StatusRequestEntityTooLarge, "Request Entity Too Large"},
	{StatusTooManyRequests, "Too Many Requests"},
	{StatusInternalServerError, "Internal Server Error"},
	{StatusNotImplemented, "Not Implemented"},
	{StatusBadGateway, "Bad Gateway"},
	{StatusServiceUnavailable, "Service Unavailable"},
	{StatusGatewayTimeout, "Gateway Timeout"},
};

HttpStatusCode HttpStatus::errCodeToHttpStatus(int errCode) {
	switch (errCode) {
		case errOK:
			return StatusOK;
		case errParams:
			return StatusBadRequest;
		case errForbidden:
			return StatusForbidden;
		default:
			return StatusInternalServerError;
	}
}

int Context::JSON(int code, const string_view &slice) {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	writer->Write(slice);
	return 0;
}

int Context::String(int code, const string_view &slice) {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type", "text/plain; charset=utf-8"});
	writer->Write(slice);
	return 0;
}

int Context::Redirect(const char *url) {
	writer->SetHeader(http::Header{"Location", url});
	return String(http::StatusMovedPermanently, "");
}

int Context::Printf(int code, const char *contentType, const char *fmt, ...) {
	char buf[0x5000];
	va_list args;
	va_start(args, fmt);
	int size = vsnprintf(buf, sizeof(buf), fmt, args);
	va_end(args);
	writer->SetContentLength(size);
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type", contentType});
	writer->Write(buf, size);
	return 0;
}

static const char *lookupContentType(const char *path) {
	const char *p = strrchr(path, '.');
	if (!p) {
		return "application/octet-stream";
	}
	p++;
	if (!strncmp(p, "html", 4)) return "text/html; charset=utf-8";
	if (!strncmp(p, "json", 4)) return "application/json; charset=utf-8";
	if (!strncmp(p, "yml", 3)) return "application/yml; charset=utf-8";
	if (!strncmp(p, "css", 3)) return "text/css; charset=utf-8";
	if (!strncmp(p, "js", 2)) return "application/javascript; charset=utf-8";
	if (!strncmp(p, "woff", 2)) return "font/woff";
	if (!strncmp(p, "woff2", 2)) return "font/woff2";

	return "application/octet-stream";
}

int Context::File(int code, const char *path, const string_view& data) {
	std::string content;


	if (data.length() == 0) {
		if (fs::ReadFile(path, content) < 0) {
			return String(http::StatusNotFound, "File not found");
		}
	} else {
		content.assign(data.data(), data.length());
	}

	writer->SetContentLength(content.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type", lookupContentType(path)});
	writer->Write(content.data(), content.size());
	return 0;
}

const char *mathodNames[] = {"GET", "POST", "OPTIONS", "HEAD", "PUT", "DELETE", nullptr};

HttpMethod lookupMethod(const string_view &method) {
	for (const char **p = mathodNames; *p; p++)
		if (method == *p) return HttpMethod(p - mathodNames);
	return HttpMethod(-1);
}

int Router::handle(Context &ctx) {
	auto method = lookupMethod(ctx.request->method);
	if (method < 0) {
		return ctx.String(StatusBadRequest, "Invalid method");
	}
	int res = 0;

	for (auto &r : routes_[method]) {
		string_view url = ctx.request->path;
		string_view route = r.path_;
		ctx.request->urlParams.clear();

		for (;;) {
			auto patternPos = route.find(':');
			auto asteriskPos = route.find('*');
			if (patternPos == string_view::npos || asteriskPos != string_view::npos) {
				if (url.substr(0, asteriskPos) != route.substr(0, asteriskPos)) break;

				for (auto &mw : middlewares_) {
					res = mw.func_(mw.object_, ctx);
					if (res != 0) {
						return res;
					}
				}
				res = r.h_.func_(r.h_.object_, ctx);
				return res;
			}

			if (url.substr(0, patternPos) != route.substr(0, patternPos)) break;

			url = url.substr(patternPos);
			route = route.substr(patternPos);

			auto nextUrlPos = url.find('/');
			auto nextRoutePos = route.find('/');

			ctx.request->urlParams.push_back(url.substr(0, nextUrlPos));

			url = url.substr(nextUrlPos == string_view::npos ? nextUrlPos : nextUrlPos + 1);
			route = route.substr(nextRoutePos == string_view::npos ? nextRoutePos : nextRoutePos + 1);
		}
	}
	res = notFoundHandler_.object_ != nullptr ? notFoundHandler_.func_(notFoundHandler_.object_, ctx)
											  : ctx.String(StatusNotFound, "Not found");
	return res;
}
}  // namespace http
}  // namespace net
}  // namespace reindexer
