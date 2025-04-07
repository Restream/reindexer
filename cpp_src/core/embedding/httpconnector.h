#pragma once

#include <memory>
#include <string>

namespace httplib {
class Client;
}  // namespace httplib

namespace reindexer {

class ConnectorPool;

class HttpConnector final {
public:
	struct Response {
		bool ok{false};
		std::string content;
	};

	HttpConnector() = delete;
	HttpConnector(HttpConnector&&) noexcept = delete;
	HttpConnector(const HttpConnector&) noexcept = delete;
	HttpConnector& operator=(const HttpConnector&) noexcept = delete;
	HttpConnector& operator=(HttpConnector&&) noexcept = delete;
	~HttpConnector();

	bool Connect(size_t connect_timeout_ms, size_t read_timeout_ms, size_t write_timeout_ms);
	bool Connected() const;
	void Disconnect();

	Response Send(const std::string& path, std::string_view json);

private:
	friend ConnectorPool;
	explicit HttpConnector(const std::string& url);

	const std::string url_;
	std::unique_ptr<httplib::Client> client_;
};

}  // namespace reindexer
