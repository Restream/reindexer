#pragma once

#include <string.h>
#include <mutex>
#include "connectinstatscollector.h"
#include "coroutine/coroutine.h"
#include "estl/cbuf.h"
#include "estl/chunk_buf.h"
#include "estl/mutex.h"
#include "net/socket.h"
#include "tools/ssize_t.h"

namespace reindexer {
namespace net {

using reindexer::cbuf;

constexpr int k_sock_closed_err = -1;
constexpr int k_connect_timeout_err = -2;

class manual_connection {
public:
	using async_cb_t = std::function<void(int err, size_t cnt, span<char> buf)>;

	enum class conn_state { init, connecting, connected };

	manual_connection(int fd, size_t rd_buf_size, bool enable_stat);
	virtual ~manual_connection();

	void set_connect_timeout(std::chrono::milliseconds timeout) noexcept { connect_timeout_ = timeout; }
	void close_conn(int err);
	void attach(ev::dynamic_loop &loop) noexcept;
	void detach() noexcept;
	void restart(int fd);

	template <typename buf_t>
	void async_read(buf_t &data, size_t cnt, async_cb_t cb) {
		async_read_impl(data, cnt, std::move(cb));
	}
	template <typename buf_t>
	size_t async_read(buf_t &data, size_t cnt, int &err) noexcept {
		auto co_id = coroutine::current();
		auto l = [&err, co_id](int _err, size_t /*cnt*/, span<char> /*buf*/) {
			err = _err;
			coroutine::resume(co_id);
		};
		return async_read_impl<buf_t, decltype(l), suspend_switch_policy>(data, cnt, std::move(l));
	}
	template <typename buf_t>
	void async_write(buf_t &data, async_cb_t cb, bool send_now = true) {
		async_write_impl(data, std::move(cb), send_now);
	}
	template <typename buf_t>
	size_t async_write(buf_t &data, int &err, bool send_now = true) noexcept {
		auto co_id = coroutine::current();
		auto l = [&err, co_id](int _err, size_t /*cnt*/, span<char> /*buf*/) {
			err = _err;
			coroutine::resume(co_id);
		};
		return async_write_impl<buf_t, decltype(l), suspend_switch_policy>(data, std::move(l), send_now);
	}
	int async_connect(std::string_view addr) noexcept;
	conn_state state() const noexcept { return state_; }
	int socket_last_error() const noexcept { return sock_.last_error(); }

private:
	class transfer_data {
	public:
		void set_expected(size_t expected) noexcept {
			expected_size_ = expected;
			transfered_size_ = 0;
		}
		void append_transfered(size_t transfered) noexcept { transfered_size_ += transfered; }
		size_t expected_size() const noexcept { return expected_size_; }
		size_t transfered_size() const noexcept { return transfered_size_; }

	private:
		size_t expected_size_ = 0;
		size_t transfered_size_ = 0;
	};

	struct async_data {
		bool empty() const noexcept { return cb == nullptr; }
		void set_cb(span<char> _buf, async_cb_t _cb) noexcept {
			assertrx(!cb);
			cb = std::move(_cb);
			buf = _buf;
		}
		void reset() noexcept {
			cb = nullptr;
			buf = span<char>();
		}

		async_cb_t cb = nullptr;
		transfer_data transfer;
		span<char> buf;
	};

	struct empty_switch_policy {
		void operator()(async_data & /*data*/) {}
	};
	struct suspend_switch_policy {
		void operator()(async_data &data) {
			while (!data.empty()) {
				coroutine::suspend();
			}
		}
	};

	template <typename buf_t, typename cb_t, typename switch_policy_t = empty_switch_policy>
	size_t async_read_impl(buf_t &data, size_t cnt, cb_t cb) {
		assertrx(r_data_.empty());
		assertrx(data.size() >= cnt);
		auto &transfer = r_data_.transfer;
		transfer.set_expected(cnt);
		int int_err = 0;
		auto data_span = span<char>(data.data(), cnt);
		if (state_ != conn_state::connecting) {
			auto nread = read(data_span, transfer, int_err);
			if (!nread) {
				return 0;
			}
		}

		if ((!int_err && transfer.transfered_size() < transfer.expected_size()) || sock_.would_block(int_err)) {
			r_data_.set_cb(data_span, std::move(cb));
			add_io_events(ev::READ);
			switch_policy_t swtch;
			swtch(r_data_);
		} else {
			cb(int_err, transfer.transfered_size(), span<char>(data.data(), data.size()));
		}
		return transfer.transfered_size();
	}

	template <typename buf_t, typename cb_t, typename switch_policy_t = empty_switch_policy>
	size_t async_write_impl(buf_t &data, cb_t cb, bool send_now) {
		assertrx(w_data_.empty());
		auto &transfer = w_data_.transfer;
		transfer.set_expected(data.size());
		int int_err = 0;
		if (data.size()) {
			auto data_span = span<char>(data.data(), data.size());
			if (send_now && state_ != conn_state::connecting) {
				write(data_span, transfer, int_err);
			}
			if (!send_now || (!int_err && transfer.transfered_size() < transfer.expected_size()) || sock_.would_block(int_err)) {
				w_data_.set_cb(data_span, std::move(cb));
				add_io_events(ev::WRITE);
				switch_policy_t swtch;
				swtch(w_data_);
			} else {
				cb(int_err, transfer.transfered_size(), data);
			}
		} else {
			cb(int_err, transfer.transfered_size(), data);
		}
		return transfer.transfered_size();
	}

	void on_async_op_done(async_data &data, int err) {
		if (!data.empty()) {
			auto cb = std::move(data.cb);
			auto buf = data.buf;
			auto transfered = data.transfer.transfered_size();
			data.reset();
			cb(err, transfered, buf);
		}
	}
	ssize_t write(span<char>, transfer_data &transfer, int &err_ref);
	ssize_t read(span<char>, transfer_data &transfer, int &err_ref);
	void read_to_buf(int &err_ref);
	void add_io_events(int events) noexcept;
	void set_io_events(int events) noexcept;
	void io_callback(ev::io &watcher, int revents);
	void connect_timer_cb(ev::timer &watcher, int);
	void write_cb();
	int read_cb();
	bool read_from_buf(span<char> rd_buf, transfer_data &transfer, bool read_full) noexcept;

	ev::io io_;
	socket sock_;
	ev::timer connect_timer_;
	conn_state state_ = conn_state::init;
	bool attached_ = false;
	int cur_events_ = 0;
	uint64_t conn_id_ = 0;

	async_data r_data_;
	async_data w_data_;
	cbuf<char> buffered_data_;
	std::chrono::milliseconds connect_timeout_ = std::chrono::seconds(10);

	std::unique_ptr<connection_stats_collector> stats_;
};

}  // namespace net
}  // namespace reindexer
