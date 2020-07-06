#include "router.h"
#include <cstdarg>
#include <unordered_map>
#include "debug/allocdebug.h"
#include "estl/chunk_buf.h"
#include "tools/fsops.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace net {
namespace http {

std::unordered_map<int, string_view> kHTTPCodes = {

	{StatusContinue, "Continue"_sv},
	{StatusOK, "OK"_sv},
	{StatusCreated, "Created"_sv},
	{StatusAccepted, "Accepted"_sv},
	{StatusNoContent, "No Content"_sv},
	{StatusMovedPermanently, "MovedPermanently"_sv},
	{StatusMovedTemporarily, "Moved Temporarily"_sv},
	{StatusNotModified, "Not Modified"_sv},
	{StatusBadRequest, "Bad Request"_sv},
	{StatusUnauthorized, "Unauthorized"_sv},
	{StatusForbidden, "Forbidden"_sv},
	{StatusNotFound, "Not found"_sv},
	{StatusMethodNotAllowed, "Method Not Allowed"_sv},
	{StatusNotAcceptable, "Not Acceptable"_sv},
	{StatusRequestTimeout, "Request Timeout"_sv},
	{StatusLengthRequired, "Length Required"_sv},
	{StatusRequestEntityTooLarge, "Request Entity Too Large"_sv},
	{StatusTooManyRequests, "Too Many Requests"_sv},
	{StatusInternalServerError, "Internal Server Error"_sv},
	{StatusNotImplemented, "Not Implemented"_sv},
	{StatusBadGateway, "Bad Gateway"_sv},
	{StatusServiceUnavailable, "Service Unavailable"_sv},
	{StatusGatewayTimeout, "Gateway Timeout"_sv},
};

HttpStatusCode HttpStatus::errCodeToHttpStatus(int errCode) {
	switch (errCode) {
		case errOK:
			return StatusOK;
		case errNotFound:
			return StatusNotFound;
		case errParams:
			return StatusBadRequest;
		case errForbidden:
			return StatusForbidden;
		default:
			return StatusInternalServerError;
	}
}

int Context::JSON(int code, string_view slice) {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"_sv, "application/json; charset=utf-8"_sv});
	writer->Write(slice);
	return 0;
}

int Context::JSON(int code, chunk &&chunk) {
	writer->SetContentLength(chunk.len_);
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"_sv, "application/json; charset=utf-8"_sv});
	writer->Write(std::move(chunk));
	return 0;
}

int Context::MSGPACK(int code, chunk &&chunk) {
	writer->SetContentLength(chunk.len_);
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"_sv, "application/x-msgpack; charset=utf-8"_sv});
	writer->Write(std::move(chunk));
	return 0;
}

int Context::String(int code, string_view slice) {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"_sv, "text/plain; charset=utf-8"_sv});
	writer->Write(slice);
	return 0;
}

int Context::String(int code, chunk &&chunk) {
	writer->SetContentLength(chunk.len_);
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"_sv, "text/plain; charset=utf-8"_sv});
	writer->Write(std::move(chunk));
	return 0;
}

int Context::Redirect(string_view url) {
	writer->SetHeader(http::Header{"Location"_sv, url});
	return String(http::StatusMovedPermanently, "");
}

static string_view lookupContentType(string_view path) {
	auto p = path.find_last_of(".");
	if (p == string_view::npos) {
		return "application/octet-stream"_sv;
	}
	p++;
	if (path.substr(p, 4) == "html"_sv) return "text/html; charset=utf-8"_sv;
	if (path.substr(p, 4) == "json"_sv) return "application/json; charset=utf-8"_sv;
	if (path.substr(p, 3) == "yml"_sv) return "application/yml; charset=utf-8"_sv;
	if (path.substr(p, 3) == "css"_sv) return "text/css; charset=utf-8"_sv;
	if (path.substr(p, 2) == "js"_sv) return "application/javascript; charset=utf-8"_sv;
	if (path.substr(p, 4) == "woff"_sv) return "font/woff"_sv;
	if (path.substr(p, 5) == "woff2"_sv) return "font/woff2"_sv;

	return "application/octet-stream"_sv;
}

int Context::File(int code, string_view path, string_view data) {
	std::string content;

	if (data.length() == 0) {
		if (fs::ReadFile(string(path), content) < 0) {
			return String(http::StatusNotFound, "File not found");
		}
	} else {
		content.assign(data.data(), data.length());
	}

	writer->SetContentLength(content.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"_sv, lookupContentType(path)});
	writer->Write(content);
	return 0;
}

std::vector<string_view> methodNames = {"GET"_sv, "POST"_sv, "OPTIONS"_sv, "HEAD"_sv, "PUT"_sv, "DELETE"_sv};

HttpMethod lookupMethod(string_view method) {
	for (auto &cm : methodNames)
		if (method == cm) return HttpMethod(&cm - &methodNames[0]);
	return HttpMethod(-1);
}

int Router::handle(Context &ctx) {
	auto method = lookupMethod(ctx.request->method);
	if (method < 0) {
		return ctx.String(StatusBadRequest, "Invalid method"_sv);
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
											  : ctx.String(StatusNotFound, "Not found"_sv);
	return res;
}
}  // namespace http
}  // namespace net
}  // namespace reindexer
