#pragma once

#include <memory>
#include <string>

namespace httplib {
class Client;
}  // namespace httplib

namespace reindexer {

class chunk;
class ConnectorPool;

class [[nodiscard]] HttpConnector final {
public:
	HttpConnector() = delete;
	HttpConnector(HttpConnector&&) noexcept = delete;
	HttpConnector(const HttpConnector&) noexcept = delete;
	HttpConnector& operator=(const HttpConnector&) noexcept = delete;
	HttpConnector& operator=(HttpConnector&&) noexcept = delete;
	~HttpConnector();

	bool Connect(size_t connect_timeout_ms, size_t read_timeout_ms, size_t write_timeout_ms);
	bool Connected() const;
	void Disconnect();

	struct [[nodiscard]] Response {
		bool ok{false};
		std::string content;
	};
	Response Send(const std::string& path, chunk&& json);

private:
	friend ConnectorPool;
	explicit HttpConnector(const std::string& url);

	const std::string url_;
	std::unique_ptr<httplib::Client> client_;
};

}  // namespace reindexer
