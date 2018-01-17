#include "router.h"
#include <cstdarg>
#include <unordered_map>
#include "debug/allocdebug.h"
#include "tools/fsops.h"

namespace reindexer_server {

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
	if (!strncmp(p, "js", 2)) return "text/javascript; charset=utf-8";

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
		if (!strcmp(method, *p)) return (HttpMethod)(p - mathodNames);
	return (HttpMethod)-1;
}

int Router::handle(Context &ctx) {
	auto method = lookupMethod(ctx.request->method);
	if (method < 0) {
		return ctx.String(StatusBadRequest, "Invalid method");
	}

	size_t l = strlen(ctx.request->path);
	auto &v = routes_[method];
	for (auto &r : v) {
		if (r.path_.length() > l) continue;
		if (!strncmp(r.path_.c_str(), ctx.request->path, r.path_.length())) {
			ctx.request->pathParams = ctx.request->path + r.path_.length();
			int res = 0;
			if (enableStats_) {
				auto tm0 = std::chrono::high_resolution_clock::now();
				int a0 = get_alloc_cnt_total();
				res = r.h_.func_(r.h_.object_, ctx);
				auto tm1 = std::chrono::high_resolution_clock::now();
				int a1 = get_alloc_cnt_total();
				std::lock_guard<std::mutex> lock(lockStats_);
				r.stat_.add(std::chrono::duration_cast<std::chrono::microseconds>(tm1 - tm0).count(), a1 - a0);
			} else {
				res = r.h_.func_(r.h_.object_, ctx);
			}

			return res;
		}
	}
	return ctx.String(StatusNotFound, "Not found");
}

void Router::printStats() {
	if (!enableStats_) {
		return;
	}

	printf("Method URI                      Min     Max     Avg    Total    Count    Allocs\n");
	printf("------ ---                      ---     ---     ---    -----    -----    ------\n");

	std::lock_guard<std::mutex> lock(lockStats_);
	for (int m = 0; m < kMaxMethod; m++) {
		for (auto &r : routes_[m]) {
			printf("%-6s %-20s %5dus %5dus %5dus %6dms %8d %8d\n", mathodNames[m], r.path_.c_str(), r.stat_.min, r.stat_.max, r.stat_.avg,
				   r.stat_.total / 1000, r.stat_.count, r.stat_.allocs);
		}
	}
	printf("%-6s %-20s %5dus %5dus %5dus %6dms %8d %8d\n", "-", "WRITE", writeStats_.min, writeStats_.max, writeStats_.avg,
		   writeStats_.total / 1000, writeStats_.count, writeStats_.allocs);
}

}  // namespace http
}  // namespace reindexer_server
