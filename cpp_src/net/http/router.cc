#include "router.h"

#include <unordered_map>
#include "estl/chunk.h"
#include "tools/fsops.h"

namespace reindexer::net::http {

using namespace std::string_view_literals;

std::unordered_map<int, std::string_view> kHTTPCodes = {

	{StatusContinue, "Continue"sv},
	{StatusOK, "OK"sv},
	{StatusCreated, "Created"sv},
	{StatusAccepted, "Accepted"sv},
	{StatusNoContent, "No Content"sv},
	{StatusMovedPermanently, "MovedPermanently"sv},
	{StatusMovedTemporarily, "Moved Temporarily"sv},
	{StatusNotModified, "Not Modified"sv},
	{StatusBadRequest, "Bad Request"sv},
	{StatusUnauthorized, "Unauthorized"sv},
	{StatusForbidden, "Forbidden"sv},
	{StatusNotFound, "Not found"sv},
	{StatusMethodNotAllowed, "Method Not Allowed"sv},
	{StatusNotAcceptable, "Not Acceptable"sv},
	{StatusRequestTimeout, "Request Timeout"sv},
	{StatusLengthRequired, "Length Required"sv},
	{StatusRequestEntityTooLarge, "Request Entity Too Large"sv},
	{StatusTooManyRequests, "Too Many Requests"sv},
	{StatusInternalServerError, "Internal Server Error"sv},
	{StatusNotImplemented, "Not Implemented"sv},
	{StatusBadGateway, "Bad Gateway"sv},
	{StatusServiceUnavailable, "Service Unavailable"sv},
	{StatusGatewayTimeout, "Gateway Timeout"sv},
};

HttpStatusCode HttpStatus::errCodeToHttpStatus(int errCode) {
	switch (errCode) {
		case errOK:
			return StatusOK;
		case errNotFound:
			return StatusNotFound;
		case errNotValid:
		case errStrictMode:
		case errParams:
		case errParseSQL:
		case errParseDSL:
		case errQueryExec:
			return StatusBadRequest;
		case errForbidden:
			return StatusForbidden;
		case errTimeout:
			return StatusRequestTimeout;
		default:
			return StatusInternalServerError;
	}
}

int Context::JSON(int code, std::string_view slice) const {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, "application/json; charset=utf-8"sv});
	writer->Write(slice);
	return 0;
}

int Context::JSON(int code, chunk&& chunk) const {
	writer->SetContentLength(chunk.len());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, "application/json; charset=utf-8"sv});
	writer->Write(std::move(chunk));
	return 0;
}

int Context::CSV(int code, chunk&& chunk) const {
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, "application/csv; charset=utf-8"sv});
	if (auto filename = request->headers.Get("Save-CSV-To"sv); !filename.empty()) {
		writer->SetHeader(http::Header{"Content-Disposition"sv, fmt::format("attachment; filename=\"{}\"", filename)});
	}
	writer->Write(std::move(chunk), Writer::WriteMode::PreChunkedBody);
	return 0;
}

int Context::MSGPACK(int code, chunk&& chunk) const {
	writer->SetContentLength(chunk.len());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, "application/x-msgpack; charset=utf-8"sv});
	writer->Write(std::move(chunk));
	return 0;
}

void Context::MSGPACK(int code, std::string_view slice) const {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, "application/x-msgpack; charset=utf-8"sv});
	writer->Write(slice);
}

int Context::Protobuf(int code, chunk&& chunk) const {
	writer->SetContentLength(chunk.len());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, "application/protobuf; charset=utf-8"sv});
	writer->Write(std::move(chunk));
	return 0;
}

void Context::Protobuf(int code, std::string_view slice) const {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, "application/protobuf; charset=utf-8"sv});
	writer->Write(slice);
}

int Context::String(int code, std::string_view slice) const {
	writer->SetContentLength(slice.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, "text/plain; charset=utf-8"sv});
	writer->Write(slice);
	return 0;
}

int Context::String(int code, chunk&& chunk) const {
	writer->SetContentLength(chunk.len());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, "text/plain; charset=utf-8"sv});
	writer->Write(std::move(chunk));
	return 0;
}

int Context::Redirect(std::string_view url) const {
	writer->SetHeader(http::Header{"Location"sv, url});
	return String(http::StatusMovedPermanently, "");
}

static std::string_view lookupContentType(std::string_view path) {
	auto p = path.find_last_of('.');
	if (p == std::string_view::npos) {
		return "application/octet-stream"sv;
	}
	p++;
	if (path.substr(p, 4) == "html"sv) {
		return "text/html; charset=utf-8"sv;
	}
	if (path.substr(p, 4) == "json"sv) {
		return "application/json; charset=utf-8"sv;
	}
	if (path.substr(p, 3) == "yml"sv) {
		return "application/yml; charset=utf-8"sv;
	}
	if (path.substr(p, 3) == "css"sv) {
		return "text/css; charset=utf-8"sv;
	}
	if (path.substr(p, 2) == "js"sv) {
		return "application/javascript; charset=utf-8"sv;
	}
	if (path.substr(p, 4) == "woff"sv) {
		return "font/woff"sv;
	}
	if (path.substr(p, 5) == "woff2"sv) {
		return "font/woff2"sv;
	}

	return "application/octet-stream"sv;
}

int Context::File(int code, std::string_view path, std::string_view data, bool isGzip, bool withCache) const {
	std::string content;
	if (data.empty()) {
		if (fs::ReadFile(isGzip ? std::string(path).append(kGzSuffix) : std::string(path), content) < 0) {
			return String(http::StatusNotFound, "File not found");
		}
	} else {
		content.assign(data.data(), data.length());
	}

	writer->SetContentLength(content.size());
	writer->SetRespCode(code);
	writer->SetHeader(http::Header{"Content-Type"sv, lookupContentType(path)});
	if (withCache) {
		writer->SetHeader(http::Header{"Cache-Control"sv, "public, max-age=31536000"sv});
	}
	if (isGzip) {
		writer->SetHeader(http::Header{"Content-Encoding"sv, "gzip"sv});
	}
	writer->Write(content);
	return 0;
}

std::vector<std::string_view> methodNames = {"GET"sv, "POST"sv, "OPTIONS"sv, "HEAD"sv, "PUT"sv, "DELETE"sv, "PATCH"sv};

HttpMethod lookupMethod(std::string_view method) {
	for (auto& cm : methodNames) {
		if (method == cm) {
			return HttpMethod(&cm - &methodNames[0]);
		}
	}
	return HttpMethod(kMethodUnknown);
}

int Router::handle(Context& ctx) {
	auto method = lookupMethod(ctx.request->method);
	if (method < 0) {
		return ctx.String(StatusBadRequest, "Invalid method"sv);
	}
	int res = 0;

	for (auto& r : routes_[method]) {
		if (matchRoute(r.path, ctx.request->path, ctx.request->urlParams)) {
			res = runMiddlewares(ctx);
			if (res != 0) {
				return res;
			}
			return r.h.func(r.h.object, ctx);
		}
	}
	res = notFoundHandler_.object != nullptr ? notFoundHandler_.func(notFoundHandler_.object, ctx)
											 : ctx.String(StatusNotFound, "Not found"sv);
	return res;
}

int Router::runMiddlewares(Context& ctx) {
	for (auto& mw : middlewares_) {
		const int res = mw.func(mw.object, ctx);
		if (res != 0) {
			return res;
		}
	}
	return 0;
}

const Router::StreamRoute* Router::findStreamRoute(std::string_view method, std::string_view path) const {
	if (!hasStreamRoutes_) {
		return nullptr;
	}
	const auto methodIdx = lookupMethod(method);
	if (methodIdx < 0) [[unlikely]] {
		return nullptr;
	}
	h_vector<UrlParam, 4> urlParams;
	for (auto& r : streamRoutes_[methodIdx]) {
		if (matchRoute(r.path, path, urlParams)) {
			return &r;
		}
	}
	return nullptr;
}

bool Router::matchRoute(std::string_view route, std::string_view url, h_vector<UrlParam, 4>& urlParams) {
	urlParams.clear();

	for (;;) {
		auto patternPos = route.find(':');
		auto asteriskPos = route.find('*');
		if (patternPos == std::string_view::npos || asteriskPos != std::string_view::npos) {
			return url.substr(0, asteriskPos) == route.substr(0, asteriskPos);
		}

		if (url.substr(0, patternPos) != route.substr(0, patternPos)) {
			return false;
		}

		url = url.substr(patternPos);
		route = route.substr(patternPos);

		auto nextUrlPos = url.find('/');
		auto nextRoutePos = route.find('/');

		urlParams.push_back(url.substr(0, nextUrlPos));

		url = url.substr(nextUrlPos == std::string_view::npos ? url.size() : nextUrlPos + 1);
		route = route.substr(nextRoutePos == std::string_view::npos ? route.size() : nextRoutePos + 1);
	}
}
}  // namespace reindexer::net::http
