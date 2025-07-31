#include "core/embedding/httpconnector.h"

#include "estl/chunk.h"
// #define CPPHTTPLIB_OPENSSL_SUPPORT // ToDo
#include "vendor/cpp-httplib/httplib.h"

namespace reindexer {

namespace {
const std::string kContentType{"application/json"};
}

HttpConnector::HttpConnector(const std::string& url) : url_{url} {}

HttpConnector::~HttpConnector() {
	try {
		Disconnect();
		// NOLINTBEGIN(bugprone-empty-catch)
	} catch (...) {
	}
	// NOLINTEND(bugprone-empty-catch)
}

bool HttpConnector::Connect(size_t connect_timeout_ms, size_t read_timeout_ms, size_t write_timeout_ms) {
	if (client_ && client_->is_valid()) {
		return true;
	}

	client_ = std::make_unique<httplib::Client>(url_);
	client_->set_connection_timeout(0, connect_timeout_ms * 1'000);
	client_->set_read_timeout(0, read_timeout_ms * 1'000);
	client_->set_write_timeout(0, write_timeout_ms * 1'000);
	client_->set_keep_alive(true);
	return client_->is_valid();
}

HttpConnector::Response HttpConnector::Send(const std::string& path, chunk&& json) {
	if (!client_) {
		return {false, "Connect before"};
	}

	auto res = client_->Post(path, {}, reinterpret_cast<char*>(json.data()), json.size(), kContentType, nullptr);

	Response response;
	if (res) {
		response.ok = (res->status == httplib::StatusCode::OK_200);
		response.content = response.ok ? res->body : res->reason;
	} else {
		response.ok = false;
		response.content = "Unexpected problem with client, error: " + to_string(res.error());
	}
	return response;
}

bool HttpConnector::Connected() const { return (client_) ? client_->is_valid() : false; }

void HttpConnector::Disconnect() {
	if (client_) {
		client_->stop();
		client_.reset();
	}
}

}  // namespace reindexer
