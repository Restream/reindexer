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

int Context::JSON(int code, const reindexer::Slice &slice) {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	writer->Write(slice.data(), slice.size());
	return 0;
}

int Context::String(int code, const reindexer::Slice &slice) {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type", "text/plain; charset=utf-8"});
	writer->Write(slice.data(), slice.size());
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

int Context::File(int code, const char *path) {
	std::string content;

	if (reindexer::ReadFile(path, content) < 0) {
		return String(http::StatusNotFound, "File not found");
	}

	writer->SetContentLength(content.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type", lookupContentType(path)});
	writer->Write(content.data(), content.size());
	return 0;
}

const char *mathodNames[] = {"GET", "POST", "OPTIONS", "HEAD", "PUT", "DELETE", nullptr};

HttpMethod lookupMethod(const char *method) {
	for (const char **p = mathodNames; *p; p++)
		if (!strcmp(method, *p)) return HttpMethod(p - mathodNames);
	return HttpMethod(-1);
}

int Router::handle(Context &ctx) {
	auto method = lookupMethod(ctx.request->method);
	if (method < 0) {
		return ctx.String(StatusBadRequest, "Invalid method");
	}

	string url;
	auto &v = routes_[method];
	for (auto &r : v) {
		url.assign(ctx.request->path);
		ctx.request->urlParams.clear();

		char *urlPtr = &url[0];
		const char *routePtr = r.path_.c_str();

		size_t urlLen = strlen(urlPtr);
		size_t routeLen = r.path_.length();

		char *endUrlPtr = urlPtr + urlLen;
		const char *endRoutePtr = routePtr + routeLen;

		for (;;) {
			const char *patternPtr = strchr(routePtr, ':');
			const char *asteriskPtr = strchr(routePtr, '*');
			if (!patternPtr || asteriskPtr) {
				if ((asteriskPtr && strncmp(urlPtr, routePtr, asteriskPtr - routePtr)) || (!asteriskPtr && strcmp(urlPtr, routePtr))) break;

				int res = 0;
				for (auto &mw : middlewares_) {
					auto ret = mw.func_(mw.object_, ctx);
					if (ret != 0) {
						return ret;
					}
				}

				res = r.h_.func_(r.h_.object_, ctx);

				if (logger_) {
					logger_(ctx);
				}

				return res;
			}

			if (strncmp(urlPtr, routePtr, patternPtr - routePtr)) break;

			urlPtr += patternPtr - routePtr;
			routePtr += patternPtr - routePtr;

			char *paramEndPtr = strchr(urlPtr, '/');
			routePtr = strchr(routePtr, '/');

			if (paramEndPtr == nullptr) paramEndPtr = endUrlPtr;
			if (routePtr == nullptr) routePtr = endRoutePtr;

			*paramEndPtr = '\0';
			ctx.request->urlParams.push_back(urlPtr);

			// skip \ overwritten by \0
			urlPtr = paramEndPtr != endUrlPtr ? ++paramEndPtr : paramEndPtr;
			if (routePtr != endRoutePtr) routePtr++;
		}
	}
	return notFoundHandler_.object_ != nullptr ? notFoundHandler_.func_(notFoundHandler_.object_, ctx)
											   : ctx.String(StatusNotFound, "Not found");
}
}  // namespace http
}  // namespace net
}  // namespace reindexer
