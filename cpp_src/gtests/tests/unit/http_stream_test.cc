// The test client uses POSIX sockets, so the tests are not built on Windows
#ifndef _WIN32

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "http_stream_server_fake.h"
#include "net/connection.h"
#include "tools/fsops.h"

namespace reindexer_tests {

namespace {

namespace http = reindexer::net::http;
using namespace std::string_view_literals;

using HeaderList = std::vector<std::pair<std::string, std::string>>;
using EventList = std::vector<std::string>;

constexpr uint64_t kMiB = 1024 * 1024;
constexpr uint64_t kGiB = 1024 * kMiB;

// Exceptions of the stream handler give the same statuses as on the regular routes
struct [[nodiscard]] ExceptionCase {
	ExceptionKind kind;
	std::string_view name;
	int code;
};
constexpr ExceptionCase kExceptionCases[] = {
	{ExceptionKind::StdException, "std", http::StatusInternalServerError},
	{ExceptionKind::HttpStatus, "status", http::StatusForbidden},
	{ExceptionKind::Error, "error", http::StatusNotFound},
};

// Blocking HTTP client, which sends raw bytes: the test controls the framing and the moment of every write
class [[nodiscard]] RawHttpClient {
public:
	struct [[nodiscard]] Response {
		// False if the connection was closed before the end of the response
		bool complete = false;
		int code = 0;
		std::string body;
	};

	explicit RawHttpClient(uint16_t port) {
		fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
		if (fd_ < 0) {
			return;
		}
		sockaddr_in addr{};
		addr.sin_family = AF_INET;
		addr.sin_port = htons(port);
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		if (::connect(fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
			Close();
			return;
		}
		timeval timeout{.tv_sec = 30, .tv_usec = 0};
		std::ignore = ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
		std::ignore = ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
		const int noDelay = 1;
		std::ignore = ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &noDelay, sizeof(noDelay));
	}
	RawHttpClient(const RawHttpClient&) = delete;
	RawHttpClient& operator=(const RawHttpClient&) = delete;
	~RawHttpClient() { Close(); }

	bool Connected() const noexcept { return fd_ >= 0; }
	void Close() noexcept {
		if (fd_ >= 0) {
			std::ignore = ::close(fd_);
			fd_ = -1;
		}
	}
	void ShutdownWrite() noexcept { std::ignore = ::shutdown(fd_, SHUT_WR); }
	// @return false if the server has closed the connection
	bool Send(std::string_view data) {
		while (!data.empty()) {
			const auto sent = ::send(fd_, data.data(), data.size(), 0);
			if (sent < 0 && errno == EINTR) {
				continue;
			}
			if (sent <= 0) {
				return false;
			}
			data.remove_prefix(size_t(sent));
		}
		return true;
	}
	// Sends the bytes [begin, end) of the test body
	bool SendBody(uint64_t begin, uint64_t end) {
		const auto pattern = StreamPattern();
		while (begin < end) {
			const size_t pos = size_t(begin % pattern.size());
			const size_t len = size_t(std::min<uint64_t>(end - begin, pattern.size() - pos));
			if (!Send(std::string_view(pattern.data() + pos, len))) {
				return false;
			}
			begin += len;
		}
		return true;
	}
	Response ReadResponse() {
		size_t headEnd = std::string::npos;
		while ((headEnd = buf_.find("\r\n\r\n")) == std::string::npos) {
			if (!recvMore()) {
				return {};
			}
		}
		std::string head = buf_.substr(0, headEnd);
		buf_.erase(0, headEnd + 4);
		Response res;
		res.code = int(std::strtol(head.c_str() + "HTTP/1.1 "sv.size(), nullptr, 10));
		for (auto& c : head) {
			c = char(std::tolower(uint8_t(c)));
		}
		size_t length = 0;
		if (const auto pos = head.find("\r\ncontent-length:"); pos != std::string::npos) {
			length = std::strtoull(head.c_str() + pos + "\r\ncontent-length:"sv.size(), nullptr, 10);
		}
		while (buf_.size() < length) {
			if (!recvMore()) {
				return {};
			}
		}
		res.body = buf_.substr(0, length);
		buf_.erase(0, length);
		res.complete = true;
		return res;
	}
	// Waits until data from the server can be read, without reading it
	// @return false if no data comes during the timeout
	bool WaitReadable(std::chrono::milliseconds timeout) const {
		pollfd pfd{.fd = fd_, .events = POLLIN, .revents = 0};
		for (;;) {
			const int ready = ::poll(&pfd, 1, int(timeout.count()));
			if (ready < 0 && errno == EINTR) {
				continue;
			}
			return ready > 0;
		}
	}
	// @return true if the server closes the connection without sending anything else
	bool IsClosedByServer() {
		if (!buf_.empty()) {
			return false;
		}
		char c = 0;
		const auto received = ::recv(fd_, &c, 1, 0);
		return received == 0 || (received < 0 && errno == ECONNRESET);
	}

private:
	bool recvMore() {
		char data[64 * 1024];
		for (;;) {
			const auto received = ::recv(fd_, data, sizeof(data), 0);
			if (received < 0 && errno == EINTR) {
				continue;
			}
			if (received <= 0) {
				return false;
			}
			buf_.append(data, size_t(received));
			return true;
		}
	}

	int fd_ = -1;
	std::string buf_;
};

#ifdef __linux__
// Peak growth of the resident memory of the process after the creation of the sampler
class [[nodiscard]] RssSampler {
public:
	RssSampler()
		: base_(currentRss()), max_(base_), thread_([this] {
			  while (!stop_) {
				  max_ = std::max(max_.load(), currentRss());
				  std::this_thread::sleep_for(std::chrono::milliseconds(2));
			  }
		  }) {}
	~RssSampler() { stopSampling(); }

	uint64_t PeakGrowth() {
		stopSampling();
		return max_ > base_ ? max_ - base_ : 0;
	}

private:
	static uint64_t currentRss() {
		std::ifstream status("/proc/self/status");
		std::string line;
		while (std::getline(status, line)) {
			if (line.starts_with("VmRSS:")) {
				return std::strtoull(line.c_str() + "VmRSS:"sv.size(), nullptr, 10) * 1024;
			}
		}
		return 0;
	}
	void stopSampling() {
		stop_ = true;
		if (thread_.joinable()) {
			thread_.join();
		}
	}

	const uint64_t base_;
	std::atomic<uint64_t> max_;
	std::atomic<bool> stop_{false};
	std::thread thread_;
};
#endif	// __linux__

// CPU time of the process during the given wall time
double CpuSecondsDuring(std::chrono::milliseconds wallTime) {
	const auto start = std::clock();
	std::this_thread::sleep_for(wallTime);
	return double(std::clock() - start) / CLOCKS_PER_SEC;
}

}  // namespace

class [[nodiscard]] HttpStreamApi : public ::testing::Test {
protected:
	static constexpr uint16_t kPort = 21480;

	struct [[nodiscard]] RequestHead {
		std::string_view method = "PUT";
		std::string target{};
		HeaderList headers{};
		// Headers for the token middleware and the role check of Preflight()
		bool auth = true;
		std::string_view version = "HTTP/1.1";
	};

	void SetUp() override {
		// The server and the client run in the same process: writes to a closed socket must not kill it
		std::ignore = std::signal(SIGPIPE, SIG_IGN);
		std::ignore = reindexer::fs::RmDirAll(kFileDir);
		ASSERT_EQ(reindexer::fs::MkDirAll(kFileDir), 0) << kFileDir;
		const auto err = server_.Start(kPort);
		ASSERT_TRUE(err.empty()) << err;
	}
	void TearDown() override {
		server_.Stop();
		std::ignore = reindexer::fs::RmDirAll(kFileDir);
	}

	static std::string MakeHead(const RequestHead& h) {
		std::string head = std::string(h.method) + ' ' + h.target + ' ' + std::string(h.version) + "\r\nHost: localhost\r\n";
		if (h.auth) {
			head += "X-Token: secret\r\nX-Role: owner\r\n";
		}
		for (const auto& [name, value] : h.headers) {
			head.append(name).append(": ").append(value).append("\r\n");
		}
		return head + "\r\n";
	}
	static HeaderList ContentLength(uint64_t size, HeaderList headers = {}) {
		headers.emplace_back("Content-Length", std::to_string(size));
		return headers;
	}
	static std::string Body(uint64_t size) {
		std::string body;
		const auto pattern = StreamPattern();
		while (body.size() < size) {
			body.append(pattern.data(), size_t(std::min<uint64_t>(size - body.size(), pattern.size())));
		}
		return body;
	}
	std::vector<std::string> Files() const {
		std::vector<std::string> res;
		for (const auto& entry : std::filesystem::directory_iterator(kFileDir)) {
			res.emplace_back(entry.path().filename().string());
		}
		return res;
	}
	// Uploads the whole body and checks that the handler has received it
	void Upload(RawHttpClient& client, const std::string& name, uint64_t size) {
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/" + name, .headers = ContentLength(size)})));
		ASSERT_TRUE(client.SendBody(0, size));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		ASSERT_EQ(res.code, http::StatusOK) << res.body;
		const auto record = server_.WaitDestroyed(name);
		EXPECT_EQ(record.events, (EventList{"preflight", "finish", "destroy"}));
		EXPECT_EQ(record.received, size);
		EXPECT_FALSE(record.patternMismatch);
	}

	const std::string kFileDir = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/HttpStreamApi");
	HTTPStreamServerFake server_{kFileDir};
};

// The handler receives exactly the declared body in order and without empty parts, while the read buffer is reused many times.
// The request head stays valid after the last part of the body. The next pipelined request is handled separately.
TEST_F(HttpStreamApi, BodyIsDeliveredInChunksAndHeadStaysValid) {
	const uint64_t size = 3 * kMiB + 7;
	const std::string target = "/stream/first?param=" + std::string(300, 'p');
	// The head is larger than the initial read buffer of the connection
	const HeaderList headers = ContentLength(size, {{"X-Test", "value"}, {"X-Big", std::string(40 * 1024, 'h')}});

	RawHttpClient client(kPort);
	ASSERT_TRUE(client.Connected());
	// The head, the body and the next request in one write
	ASSERT_TRUE(client.Send(MakeHead({.target = target, .headers = headers}) + Body(size) +
							MakeHead({.target = "/stream/second", .headers = ContentLength(5)}) + Body(5)));
	for (int i = 0; i < 2; ++i) {
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusOK) << res.body;
	}

	const auto record = server_.WaitDestroyed("first");
	EXPECT_EQ(record.events, (EventList{"preflight", "finish", "destroy"}));
	EXPECT_EQ(record.received, size);
	EXPECT_FALSE(record.patternMismatch);
	EXPECT_GT(record.chunks, 1u);
	EXPECT_FALSE(record.emptyChunk);
	EXPECT_EQ(record.uri, target);
	EXPECT_EQ(record.header, "value");
	EXPECT_EQ(record.urlParams, (std::vector<std::string>{"first"}));
	const auto second = server_.WaitDestroyed("second");
	EXPECT_EQ(second.events, (EventList{"preflight", "finish", "destroy"}));
	EXPECT_EQ(second.received, 5u);
	{
		SCOPED_TRACE("Empty body: Finish() right after Preflight()");
		Upload(client, "empty", 0);
	}
	{
		SCOPED_TRACE("HTTP/1.0: the connection is closed after the response");
		RawHttpClient client10(kPort);
		ASSERT_TRUE(client10.Send(MakeHead({.target = "/stream/http10", .headers = ContentLength(10), .version = "HTTP/1.0"}) + Body(10)));
		const auto res = client10.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusOK) << res.body;
		EXPECT_TRUE(client10.IsClosedByServer());
	}
}

// A body much larger than the request size limit (8 MiB) goes to disk, and the memory of the process does not grow with it
TEST_F(HttpStreamApi, LargeBodyIsWrittenToDisk) {
	const uint64_t size = 256 * kMiB;
	server_.SetBehavior("large", {.writeToFile = true});
	std::ignore = StreamPattern();
	RawHttpClient client(kPort);
	ASSERT_TRUE(client.Connected());
#ifdef __linux__
	RssSampler rss;
#endif
	Upload(client, "large", size);
#ifdef __linux__
	EXPECT_LT(rss.PeakGrowth(), 64 * kMiB);
#endif
	EXPECT_EQ(std::filesystem::file_size(reindexer::fs::JoinPath(kFileDir, "large.bin")), size);
	EXPECT_EQ(Files(), (std::vector<std::string>{"large.bin"}));
}

// 'Content-Length' is a 64-bit value: a body above 2 GiB is received completely, and the connection stays usable
TEST_F(HttpStreamApi, BodyLargerThan2GiB) {
	RawHttpClient client(kPort);
	ASSERT_TRUE(client.Connected());
	Upload(client, "huge", 2 * kGiB + kMiB + 3);
	Upload(client, "next", 10);
	{
		SCOPED_TRACE("'Content-Length' above 4 GiB keeps the high bits");
		server_.SetBehavior("above4gib", {.rejectAfter = kMiB});
		RawHttpClient client4(kPort);
		ASSERT_TRUE(
			client4.Send(MakeHead({.target = "/stream/above4gib", .headers = ContentLength(4 * kGiB + 7, {{"Expect", "100-continue"}})})));
		const auto interim = client4.ReadResponse();
		ASSERT_TRUE(interim.complete);
		EXPECT_EQ(interim.code, http::StatusContinue);
		// With a cut length of 7 bytes the request would end with 200 long before the refusal
		ASSERT_TRUE(client4.SendBody(0, kMiB + 1));
		const auto res = client4.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusNotAcceptable) << res.body;
		const auto record = server_.WaitDestroyed("above4gib");
		EXPECT_EQ(record.events, (EventList{"preflight", "abort:chunk_rejected", "destroy"}));
		EXPECT_EQ(record.received, kMiB + 1);
	}
}

// After a successful Preflight() the handler gets exactly one terminal call: a return from Finish() or Abort()
TEST_F(HttpStreamApi, ExactlyOneTerminalCall) {
	const uint64_t size = 8 * kMiB;
	{
		SCOPED_TRACE("OnChunk() refusal: Abort(), the status of OnChunk(), close without parsing the rest of the body");
		server_.SetBehavior("rejected", {.rejectAfter = kMiB, .writeToFile = true});
		RawHttpClient client(kPort);
		// The server reads every sent byte before the refusal, so it closes the connection without a reset
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/rejected", .headers = ContentLength(size)})));
		ASSERT_TRUE(client.SendBody(0, kMiB + 1));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusNotAcceptable) << res.body;
		const int factoryCalls = server_.FactoryCalls();
		std::ignore = client.Send(Body(64 * 1024) + MakeHead({.target = "/stream/after", .headers = ContentLength(5)}) + Body(5));
		EXPECT_TRUE(client.IsClosedByServer());
		const auto record = server_.WaitDestroyed("rejected");
		EXPECT_EQ(record.events, (EventList{"preflight", "abort:chunk_rejected", "destroy"}));
		EXPECT_EQ(server_.FactoryCalls(), factoryCalls);
		EXPECT_EQ(Files(), std::vector<std::string>{});
	}
	for (const auto& exception : kExceptionCases) {
		SCOPED_TRACE("Exception from OnChunk(): Abort(), the status of the exception and close: " + std::string(exception.name));
		const std::string name = "chunk_throw_" + std::string(exception.name);
		server_.SetBehavior(name, {.throwAfter = kMiB, .exceptionKind = exception.kind});
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/" + name, .headers = ContentLength(size)})));
		ASSERT_TRUE(client.SendBody(0, kMiB + 1));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, exception.code) << res.body;
		EXPECT_TRUE(client.IsClosedByServer());
		const auto record = server_.WaitDestroyed(name);
		EXPECT_EQ(record.events, (EventList{"preflight", "abort:handler_exception", "destroy"}));
	}
	{
		SCOPED_TRACE("The client closes the connection in the middle of the body: Abort(), no files");
		server_.SetBehavior("dropped", {.writeToFile = true});
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/dropped", .headers = ContentLength(size)})));
		ASSERT_TRUE(client.SendBody(0, 2 * kMiB + 3));
		client.Close();
		const auto record = server_.WaitDestroyed("dropped");
		EXPECT_EQ(record.events, (EventList{"preflight", "abort:connection_closed", "destroy"}));
		EXPECT_FALSE(record.patternMismatch);
		EXPECT_EQ(Files(), std::vector<std::string>{});
	}
	{
		SCOPED_TRACE("The client stops sending in the middle of the body: Abort() and close");
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/short", .headers = ContentLength(size)})));
		ASSERT_TRUE(client.SendBody(0, kMiB));
		client.ShutdownWrite();
		EXPECT_TRUE(client.IsClosedByServer());
		const auto record = server_.WaitDestroyed("short");
		EXPECT_EQ(record.events, (EventList{"preflight", "abort:connection_closed", "destroy"}));
	}
	for (const auto& exception : kExceptionCases) {
		SCOPED_TRACE("Exception from Finish(): Abort() and the status of the exception, the connection stays usable: " +
					 std::string(exception.name));
		const std::string name = "finish_throw_" + std::string(exception.name);
		server_.SetBehavior(name, {.finishThrows = true, .exceptionKind = exception.kind});
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/" + name, .headers = ContentLength(kMiB)})));
		ASSERT_TRUE(client.SendBody(0, kMiB));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, exception.code) << res.body;
		const auto record = server_.WaitDestroyed(name);
		EXPECT_EQ(record.events, (EventList{"preflight", "finish", "abort:handler_exception", "destroy"}));
		Upload(client, "after_" + name, 10);
	}
	{
		SCOPED_TRACE("Error status from Finish(): no Abort(), the connection stays usable");
		server_.SetBehavior("finish_status", {.finishStatus = http::StatusNotAcceptable});
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/finish_status", .headers = ContentLength(kMiB)})));
		ASSERT_TRUE(client.SendBody(0, kMiB));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusNotAcceptable) << res.body;
		const auto record = server_.WaitDestroyed("finish_status");
		EXPECT_EQ(record.events, (EventList{"preflight", "finish", "destroy"}));
		Upload(client, "after_finish_status", 10);
	}
	{
		SCOPED_TRACE("Finish() returns 204, which can not have a body: 500, the connection stays usable");
		server_.SetBehavior("finish_no_content", {.finishStatus = http::StatusNoContent});
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/finish_no_content", .headers = ContentLength(kMiB)})));
		ASSERT_TRUE(client.SendBody(0, kMiB));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusInternalServerError) << res.body;
		const auto record = server_.WaitDestroyed("finish_no_content");
		EXPECT_EQ(record.events, (EventList{"preflight", "finish", "destroy"}));
		Upload(client, "after_finish_no_content", 10);
	}
}

// Framing errors, middleware refusals and Preflight() refusals come before the body is read, and the connection is closed
TEST_F(HttpStreamApi, RefusalsBeforeBody) {
	struct [[nodiscard]] FramingCase {
		HeaderList headers;
		int code;
	};
	const FramingCase framingCases[] = {
		{{{"Content-Length", "5"}, {"Content-Length", "5"}}, http::StatusBadRequest},
		{{{"Content-Length", "5"}, {"content-length", "6"}}, http::StatusBadRequest},
		{{{"Content-Length", "5, 5"}}, http::StatusBadRequest},
		{{{"Content-Length", "+5"}}, http::StatusBadRequest},
		{{{"Content-Length", "-5"}}, http::StatusBadRequest},
		{{{"Content-Length", "abc"}}, http::StatusBadRequest},
		{{{"Content-Length", ""}}, http::StatusBadRequest},
		{{{"Content-Length", "0x10"}}, http::StatusBadRequest},
		{{{"Content-Length", "1 2"}}, http::StatusBadRequest},
		{{{"Content-Length", "5"}, {"Transfer-Encoding", "chunked"}}, http::StatusBadRequest},
		{{{"Transfer-Encoding", "chunked"}, {"Content-Length", "5"}}, http::StatusBadRequest},
		{{{"Content-Length", "5"}, {"Transfer-Encoding", "identity"}}, http::StatusBadRequest},
		{{{"Transfer-Encoding", "gzip"}}, http::StatusBadRequest},
		{{{"Transfer-Encoding", "gzip, chunked"}}, http::StatusBadRequest},
		{{{"Transfer-Encoding", "chunked"}, {"Transfer-Encoding", "chunked"}}, http::StatusBadRequest},
		{{{"Transfer-Encoding", "chunked"}}, http::StatusInternalServerError},
		{{}, http::StatusLengthRequired},
		{{{"Content-Length", "18446744073709551616"}}, http::StatusRequestEntityTooLarge},
		{{{"Content-Length", "99999999999999999999999999"}}, http::StatusRequestEntityTooLarge},
	};
	for (const auto& framing : framingCases) {
		SCOPED_TRACE(framing.headers.empty() ? std::string("no framing headers") : framing.headers.front().second);
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/framing", .headers = framing.headers}) + "12345" +
								MakeHead({.target = "/stream/after", .headers = ContentLength(5)}) + Body(5)));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, framing.code) << res.body;
		if (framing.code == http::StatusInternalServerError) {
			EXPECT_EQ(res.body, "Sorry, chunked encoded body not implemented");
		}
		EXPECT_TRUE(client.IsClosedByServer());
	}
	{
		SCOPED_TRACE("Obsolete line folding of the headers");
		RawHttpClient client(kPort);
		ASSERT_TRUE(
			client.Send("PUT /stream/folding HTTP/1.1\r\nHost: localhost\r\nX-Token: secret\r\nContent-Length: 5\r\n , 6\r\n\r\n12345"));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusBadRequest) << res.body;
		EXPECT_TRUE(client.IsClosedByServer());
	}
	{
		SCOPED_TRACE("Chunked body on a route without streaming: the old answer");
		RawHttpClient client(kPort);
		ASSERT_TRUE(
			client.Send(MakeHead({.target = "/regular", .headers = {{"Transfer-Encoding", "chunked"}}}) + "5\r\n12345\r\n0\r\n\r\n"));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusInternalServerError);
		EXPECT_EQ(res.body, "Sorry, chunked encoded body not implemented");
	}
	{
		SCOPED_TRACE("A middleware refusal comes before the handler is created");
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/no_token", .headers = ContentLength(5), .auth = false}) + "12345"));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusUnauthorized) << res.body;
		EXPECT_TRUE(client.IsClosedByServer());
	}
	EXPECT_EQ(server_.FactoryCalls(), 0);
	{
		SCOPED_TRACE("Spaces around 'Content-Length' and leading zeros are accepted");
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/spaces", .headers = {{"Content-Length", " \t005 \t"}}}) + Body(5)));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusOK) << res.body;
		EXPECT_EQ(server_.WaitDestroyed("spaces").received, 5u);
	}
	{
		SCOPED_TRACE("The role check refuses before the body: the client has sent only a part of it, no files");
		server_.SetBehavior("no_role", {.writeToFile = true});
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send("PUT /stream/no_role HTTP/1.1\r\nHost: localhost\r\nX-Token: secret\r\nContent-Length: 67108864\r\n\r\n" +
								Body(16 * 1024)));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusForbidden) << res.body;
		EXPECT_TRUE(client.IsClosedByServer());
		EXPECT_EQ(server_.WaitDestroyed("no_role").events, (EventList{"preflight", "destroy"}));
		EXPECT_EQ(Files(), std::vector<std::string>{});
	}
	{
		SCOPED_TRACE("'Expect: 100-continue': the role refusal comes instead of '100 Continue'");
		RawHttpClient client(kPort);
		ASSERT_TRUE(
			client.Send("PUT /stream/no_role_expect HTTP/1.1\r\nHost: localhost\r\nX-Token: secret\r\nContent-Length: 67108864\r\nExpect: "
						"100-continue\r\n\r\n"));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusForbidden) << res.body;
		EXPECT_TRUE(client.IsClosedByServer());
	}
	struct [[nodiscard]] PreflightCase {
		std::string name;
		StreamBehavior behavior;
		uint64_t size;
		int code;
		EventList events;
	};
	const PreflightCase preflightCases[] = {
		{"over_limit", {}, 10 * kGiB + 1, http::StatusRequestEntityTooLarge, {"preflight", "destroy"}},
		{"refused", {.preflightStatus = http::StatusNotAcceptable}, kMiB, http::StatusNotAcceptable, {"preflight", "destroy"}},
		{"refused_no_content", {.preflightStatus = http::StatusNoContent}, kMiB, http::StatusInternalServerError, {"preflight", "destroy"}},
		{"preflight_throw", {.preflightThrows = true}, kMiB, http::StatusInternalServerError, {"preflight", "destroy"}},
		{"preflight_throw_status",
		 {.preflightThrows = true, .exceptionKind = ExceptionKind::HttpStatus},
		 kMiB,
		 http::StatusForbidden,
		 {"preflight", "destroy"}},
		{"preflight_throw_error",
		 {.preflightThrows = true, .exceptionKind = ExceptionKind::Error},
		 kMiB,
		 http::StatusNotFound,
		 {"preflight", "destroy"}},
		{"factory_throw", {.factoryThrows = true}, kMiB, http::StatusInternalServerError, {}},
		{"factory_throw_status", {.factoryThrows = true, .exceptionKind = ExceptionKind::HttpStatus}, kMiB, http::StatusForbidden, {}},
	};
	for (const auto& preflight : preflightCases) {
		SCOPED_TRACE(preflight.name + ": the refusal comes instead of '100 Continue', Abort() is not called");
		server_.SetBehavior(preflight.name, preflight.behavior);
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(
			MakeHead({.target = "/stream/" + preflight.name, .headers = ContentLength(preflight.size, {{"Expect", "100-continue"}})})));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, preflight.code) << res.body;
		EXPECT_TRUE(client.IsClosedByServer());
		if (!preflight.events.empty()) {
			EXPECT_EQ(server_.WaitDestroyed(preflight.name).events, preflight.events);
		}
	}
	{
		SCOPED_TRACE("A successful Preflight() is answered with '100 Continue'");
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/continued", .headers = ContentLength(kMiB, {{"Expect", "100-continue"}})})));
		const auto interim = client.ReadResponse();
		ASSERT_TRUE(interim.complete);
		EXPECT_EQ(interim.code, http::StatusContinue);
		ASSERT_TRUE(client.SendBody(0, kMiB));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusOK) << res.body;
		const auto record = server_.WaitDestroyed("continued");
		EXPECT_EQ(record.events, (EventList{"preflight", "finish", "destroy"}));
		EXPECT_EQ(record.received, kMiB);
		EXPECT_FALSE(record.patternMismatch);
	}
}

// Concurrent uploads use separate handlers: their data and terminal calls do not mix
TEST_F(HttpStreamApi, ConcurrentUploadsAreIsolated) {
	const std::vector<std::string> names{"a", "b", "c"};
	const std::vector<uint64_t> sizes{6 * kMiB + 5, 3 * kMiB + 1, 4 * kMiB};
	constexpr uint64_t kPart = 256 * 1024;
	std::vector<std::unique_ptr<RawHttpClient>> clients;
	for (size_t i = 0; i < names.size(); ++i) {
		clients.emplace_back(std::make_unique<RawHttpClient>(kPort));
		ASSERT_TRUE(clients.back()->Send(MakeHead({.target = "/stream/" + names[i], .headers = ContentLength(sizes[i])})));
	}
	// The parts of the bodies go in turn, the third upload stops in the middle
	for (uint64_t offset = 0; offset < sizes[0]; offset += kPart) {
		for (size_t i = 0; i < 2; ++i) {
			if (offset < sizes[i]) {
				ASSERT_TRUE(clients[i]->SendBody(offset, std::min(offset + kPart, sizes[i])));
			}
		}
		if (offset < sizes[2] / 2) {
			ASSERT_TRUE(clients[2]->SendBody(offset, offset + kPart));
		} else if (clients[2]->Connected()) {
			clients[2]->Close();
		}
	}
	std::vector<uint64_t> ids;
	for (size_t i = 0; i < 2; ++i) {
		const auto res = clients[i]->ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusOK) << res.body;
		const auto record = server_.WaitDestroyed(names[i]);
		EXPECT_EQ(record.events, (EventList{"preflight", "finish", "destroy"}));
		EXPECT_EQ(record.received, sizes[i]);
		EXPECT_FALSE(record.patternMismatch);
		ids.emplace_back(record.handlerId);
	}
	const auto aborted = server_.WaitDestroyed(names[2]);
	EXPECT_EQ(aborted.events, (EventList{"preflight", "abort:connection_closed", "destroy"}));
	EXPECT_FALSE(aborted.patternMismatch);
	ids.emplace_back(aborted.handlerId);
	std::sort(ids.begin(), ids.end());
	EXPECT_TRUE(std::adjacent_find(ids.begin(), ids.end()) == ids.end()) << "Handlers are shared between the requests";
}

// The request ends with 408, when no part of the body comes during the idle timeout of the route.
// The whole upload may take longer than the timeout, and time in the handler is not counted.
// An exception from a callback of the router at the end of such a request closes the connection and does not stop the server
TEST_F(HttpStreamApi, IdleTimeoutOfBody) {
	constexpr auto kTimeout = HTTPStreamServerFake::kShortIdleTimeout;
	{
		SCOPED_TRACE("The client stops sending: 408, Abort() and close");
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/short/stalled", .headers = ContentLength(8 * kMiB)})));
		ASSERT_TRUE(client.SendBody(0, 64 * 1024));
		const auto start = std::chrono::steady_clock::now();
		const auto res = client.ReadResponse();
		const auto waited = std::chrono::steady_clock::now() - start;
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusRequestTimeout) << res.body;
		// The coarse clock of the server has a granularity of a few milliseconds
		EXPECT_GE(waited, kTimeout - std::chrono::milliseconds(50));
		EXPECT_TRUE(client.IsClosedByServer());
		const auto record = server_.WaitDestroyed("stalled");
		EXPECT_EQ(record.events, (EventList{"preflight", "abort:timeout", "destroy"}));
		EXPECT_EQ(record.received, 64 * 1024u);
	}
	{
		SCOPED_TRACE("Pauses shorter than the timeout: the request succeeds, though it takes longer than the timeout");
		constexpr uint64_t kPart = 16 * 1024, kParts = 6;
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/short/slow_client", .headers = ContentLength(kPart * kParts)})));
		for (uint64_t i = 0; i < kParts; ++i) {
			std::this_thread::sleep_for(kTimeout / 3);
			ASSERT_TRUE(client.SendBody(i * kPart, (i + 1) * kPart));
		}
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusOK) << res.body;
		const auto record = server_.WaitDestroyed("slow_client");
		EXPECT_EQ(record.events, (EventList{"preflight", "finish", "destroy"}));
		EXPECT_EQ(record.received, kPart * kParts);
	}
	{
		SCOPED_TRACE("The handler is slower than the timeout: its time is not idle time");
		server_.SetBehavior("slow_handler", {.firstChunkDelay = 2 * kTimeout});
		// The first part is small, so the handler gets it in one call together with the head
		constexpr uint64_t kPart = 1024;
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/short/slow_handler", .headers = ContentLength(2 * kPart)}) + Body(kPart)));
		// The second part comes soon after the handler returns from the first one
		std::this_thread::sleep_for(2 * kTimeout + kTimeout / 5);
		ASSERT_TRUE(client.SendBody(kPart, 2 * kPart));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusOK) << res.body;
		const auto record = server_.WaitDestroyed("slow_handler");
		EXPECT_EQ(record.events, (EventList{"preflight", "finish", "destroy"}));
		EXPECT_FALSE(record.patternMismatch);
	}
	{
		SCOPED_TRACE("The response callback throws, when the timer ends the request: the connection is closed, the server works");
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(MakeHead({.target = "/short/response_throws", .headers = ContentLength(8 * kMiB)})));
		ASSERT_TRUE(client.SendBody(0, 64 * 1024));
		// The callback is called after the response is written, so the response is still sent
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, http::StatusRequestTimeout) << res.body;
		EXPECT_TRUE(client.IsClosedByServer());
		const auto record = server_.WaitDestroyed("response_throws");
		EXPECT_EQ(record.events, (EventList{"preflight", "abort:timeout", "destroy"}));
		RawHttpClient next(kPort);
		Upload(next, "after_response_throws", 10);
	}
}

// A refused upload closes the connection. The client does not read the large response to the previous pipelined request
// and keeps sending the body: the closing connection must wait for the socket to become writable without spinning
TEST_F(HttpStreamApi, UnreadResponseWithRefusedBodyDoesNotSpin) {
	RawHttpClient client(kPort);
	ASSERT_TRUE(client.Connected());
	ASSERT_TRUE(client.Send(MakeHead({.method = "GET", .target = "/big"}) +
							MakeHead({.target = "/stream/spin", .headers = ContentLength(kMiB), .auth = false})));
	// The server starts to write only after it has handled both requests. A fixed pause is not enough: under sanitizers
	// the server can still build the large response during the measured time
	ASSERT_TRUE(client.WaitReadable(std::chrono::seconds(10)));
	ASSERT_TRUE(client.Send(Body(64 * 1024)));
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	EXPECT_LT(CpuSecondsDuring(std::chrono::seconds(1)), 0.5);
}

// The client sends the end of the body and closes its side of the connection at once. The last read fills the read buffer,
// so the next read in the same callback sees the end of the stream: the response must still be sent
TEST_F(HttpStreamApi, ResponseIsSentAfterHalfCloseWhenLastReadFillsBuffer) {
	constexpr uint64_t kFirstPart = 1024;
	const uint64_t size = kFirstPart + uint64_t(reindexer::net::kConnReadbufSize);
	server_.SetBehavior("half_close", {.firstChunkDelay = std::chrono::milliseconds(300)});
	RawHttpClient client(kPort);
	ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/half_close", .headers = ContentLength(size)}) + Body(kFirstPart)));
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	ASSERT_TRUE(client.SendBody(kFirstPart, size));
	client.ShutdownWrite();
	const auto res = client.ReadResponse();
	const auto record = server_.WaitDestroyed("half_close");
	EXPECT_EQ(record.events, (EventList{"preflight", "finish", "destroy"}));
	EXPECT_EQ(record.received, size);
	ASSERT_TRUE(res.complete);
	EXPECT_EQ(res.code, http::StatusOK) << res.body;
}

// Preflight() breaks the contract and writes a response itself: the handler gets Abort() as for a handler error,
// not as for a closed connection
TEST_F(HttpStreamApi, ResponseFromPreflightIsNotReportedAsClosedConnection) {
	server_.SetBehavior("preflight_writes", {.preflightWritesResponse = true});
	RawHttpClient client(kPort);
	ASSERT_TRUE(client.Send(MakeHead({.target = "/stream/preflight_writes", .headers = ContentLength(10)})));
	const auto res = client.ReadResponse();
	ASSERT_TRUE(res.complete);
	const auto record = server_.WaitDestroyed("preflight_writes");
	EXPECT_EQ(record.events, (EventList{"preflight", "abort:handler_exception", "destroy"}));
}

// A negative 'Content-Length' on a regular route is refused before the body, and the server keeps working
TEST_F(HttpStreamApi, NegativeContentLengthWithFormBody) {
	RawHttpClient client(kPort);
	ASSERT_TRUE(client.Connected());
	ASSERT_TRUE(client.Send(MakeHead({.method = "POST",
									  .target = "/regular",
									  .headers = {{"Content-Type", "application/x-www-form-urlencoded"}, {"Content-Length", "-5"}},
									  .auth = false}) +
							"a=1"));
	const auto res = client.ReadResponse();
	ASSERT_TRUE(res.complete);
	EXPECT_EQ(res.code, http::StatusBadRequest) << res.body;
	EXPECT_TRUE(client.IsClosedByServer());
	RawHttpClient next(kPort);
	Upload(next, "after_negative_length", 10);
}

TEST_F(HttpStreamApi, NegativeContentLengthWithPlainBody) {
	RawHttpClient client(kPort);
	ASSERT_TRUE(client.Connected());
	ASSERT_TRUE(
		client.Send(MakeHead({.method = "POST", .target = "/regular", .headers = {{"Content-Length", "-5"}}, .auth = false}) + "a=1"));
	const auto res = client.ReadResponse();
	ASSERT_TRUE(res.complete);
	EXPECT_EQ(res.code, http::StatusBadRequest) << res.body;
	EXPECT_TRUE(client.IsClosedByServer());
	RawHttpClient next(kPort);
	Upload(next, "after_negative_length", 10);
}

// Junk and too large values of 'Content-Length' on a regular route get an answer instead of a dropped connection
TEST_F(HttpStreamApi, InvalidContentLengthOnRegularRoute) {
	struct [[nodiscard]] LengthCase {
		std::string value;
		int code;
	};
	const LengthCase cases[] = {
		{"abc", http::StatusBadRequest},
		{"5x", http::StatusBadRequest},
		{"+5", http::StatusBadRequest},
		{"2147483648", http::StatusRequestEntityTooLarge},
		{"99999999999999999999999", http::StatusRequestEntityTooLarge},
	};
	for (const auto& lengthCase : cases) {
		SCOPED_TRACE(lengthCase.value);
		RawHttpClient client(kPort);
		ASSERT_TRUE(client.Send(
			MakeHead({.method = "POST", .target = "/regular", .headers = {{"Content-Length", lengthCase.value}}, .auth = false}) + "a=1"));
		const auto res = client.ReadResponse();
		ASSERT_TRUE(res.complete);
		EXPECT_EQ(res.code, lengthCase.code) << res.body;
		EXPECT_TRUE(client.IsClosedByServer());
	}
}

}  // namespace reindexer_tests

#endif	// _WIN32
