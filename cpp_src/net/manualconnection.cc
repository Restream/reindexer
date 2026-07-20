#include "manualconnection.h"
#include <errno.h>
#include "tools/catch_and_return.h"

namespace reindexer {
namespace net {

manual_connection::manual_connection(size_t rd_buf_size, bool enable_stat)
	: buffered_data_(rd_buf_size), stats_(enable_stat ? new connection_stats_collector : nullptr) {}

void manual_connection::attach(ev::dynamic_loop& loop) noexcept {
	assertrx(!attached_);
	io_.set<manual_connection, &manual_connection::io_callback>(this);
	io_.set(loop);
	connect_timer_.set<manual_connection, &manual_connection::connect_timer_cb>(this);
	connect_timer_.set(loop);
	if (stats_) {
		stats_->attach(loop);
	}
	if (cur_events_) {
		io_.start(sock_.fd(), cur_events_);
	}
	attached_ = true;
}

void manual_connection::detach() noexcept {
	assertrx(attached_);
	io_.stop();
	io_.reset();
	connect_timer_.stop();
	connect_timer_.reset();
	if (stats_) {
		stats_->detach();
	}
	attached_ = false;
}

void manual_connection::close_conn(int err) {
	state_ = conn_state::init;
	connect_timer_.stop();
	if (sock_.valid()) {
		io_.stop();
		if (sock_.close() != 0) [[unlikely]] {
			perror("sock_.close() error");
		}
	}
	cur_events_ = 0;
	const bool hadRData = !r_data_.empty();
	const bool hadWData = !w_data_.empty();
	if (hadRData) {
		std::ignore = read_from_buf(r_data_.buf, r_data_.transfer, false);
		buffered_data_.clear();
		on_async_op_done(r_data_, err);
	} else {
		buffered_data_.clear();
	}

	if (hadWData) {
		on_async_op_done(w_data_, err);
	}
	if (stats_) {
		stats_->stop();
	}
}

void manual_connection::restart(socket&& s) {
	assertrx(!sock_.valid());
	sock_ = std::move(s);
	if (stats_) {
		stats_->restart();
	}
}

Error manual_connection::with_tls(bool enable) {
	try {
		if (enable) {
			if (!sslCtx_) {
				sslCtx_ = openssl::create_client_context();
			}
			sock_.ssl = openssl::create_ssl(sslCtx_);
		} else {
			sslCtx_ = nullptr;
			sock_.ssl = nullptr;
		}
	}
	CATCH_AND_RETURN
	return {};
}

int manual_connection::async_connect(std::string_view addr, socket_domain type) noexcept {
	connect_timer_.stop();
	if (state_ == conn_state::connected || state_ == conn_state::connecting) {
		close_conn(k_sock_closed_err);
	}
	assertrx(w_data_.empty());
	++conn_id_;
	int ret = sock_.connect(addr, type);
	if (ret == 0) {
		state_ = conn_state::connected;
		return 0;
	} else if (!sock_.valid() || !sock_.would_block(sock_.last_error())) {
		state_ = conn_state::init;
		return -1;
	}
	state_ = conn_state::connecting;
	if (connect_timeout_.count() > 0) {
		connect_timer_.start(double(connect_timeout_.count()) / 1000);
	}
	set_io_events(ev::WRITE);
	return 0;
}

ssize_t manual_connection::write(std::span<char> wr_buf, transfer_data& transfer, int& err_ref) {
	err_ref = 0;
	ssize_t written = -1;
	auto cur_buf = wr_buf.subspan(transfer.transfered_size());
	do {
		written = sock_.send(cur_buf);
		int err = sock_.last_error();

		if (written < 0) {
			if (err == EINTR) {
				continue;
			} else {
				err_ref = err;
				if (socket::would_block(err)) {
					return 0;
				}
				close_conn(err);
				return -1;
			}
		}
	} while (written < 0);

	transfer.append_transfered(written);

	assertrx(wr_buf.size() >= transfer.transfered_size());
	auto remaining = wr_buf.size() - transfer.transfered_size();
	if (stats_) {
		stats_->update_write_stats(written, remaining);
	}

	if (remaining == 0) {
		on_async_op_done(w_data_, 0);
	}
	return written;
}

ssize_t manual_connection::read(std::span<char> rd_buf, transfer_data& transfer, int& err_ref) {
	bool need_read = !transfer.expected_size();
	ssize_t nread = 0;
	ssize_t read_this_time = 0;
	err_ref = 0;
	auto remain_to_transfer = transfer.expected_size() - transfer.transfered_size();
	if (read_from_buf(rd_buf, transfer, true)) {
		on_async_op_done(r_data_, 0);
		return remain_to_transfer;
	}
	buffered_data_.reserve(transfer.expected_size() + 0x800);
	while (transfer.transfered_size() < transfer.expected_size() || need_read) {
		auto it = buffered_data_.head();
		nread = sock_.recv(it);
		int err = sock_.last_error();

		if (nread < 0 && err == EINTR) {
			continue;
		}

		if ((nread < 0 && !socket::would_block(err)) || nread == 0) {
			if (nread == 0) {
				err = k_sock_closed_err;
			}
			err_ref = err;
			close_conn(err);
			return -1;
		} else if (nread > 0) {
			need_read = false;
			read_this_time += nread;
			buffered_data_.advance_head(nread);
			if (stats_) {
				stats_->update_read_stats(nread);
			}
			if (read_from_buf(rd_buf, transfer, true)) {
				on_async_op_done(r_data_, 0);
				return remain_to_transfer;
			}
		} else {
			err_ref = err;
			return nread;
		}
	}
	on_async_op_done(r_data_, 0);
	return read_this_time;
}

void manual_connection::read_to_buf(int& err_ref) {
	auto it = buffered_data_.head();
	ssize_t nread = sock_.recv(it);
	int err = sock_.last_error();

	if (nread < 0 && err == EINTR) {
		return;
	}

	if ((nread < 0 && !socket::would_block(err)) || nread == 0) {
		if (nread == 0) {
			err = k_sock_closed_err;
		}
		err_ref = err;
		close_conn(err);
	} else if (nread > 0) {
		buffered_data_.advance_head(nread);
		if (stats_) {
			stats_->update_read_stats(nread);
		}
	} else {
		err_ref = err;
	}
}

void manual_connection::add_io_events(int events) noexcept {
	const int curEvents = cur_events_;
	cur_events_ |= events;
	if (curEvents != cur_events_) {
		if (curEvents == 0) {
			io_.start(sock_.fd(), cur_events_);
		} else {
			io_.set(cur_events_);
		}
	}
}

void manual_connection::set_io_events(int events) noexcept {
	if (events != cur_events_) {
		if (cur_events_ == 0) {
			io_.start(sock_.fd(), events);
		} else {
			io_.set(events);
		}
		cur_events_ = events;
	}
}

void manual_connection::io_callback(ev::io&, int revents) {
	if (ev::ERROR & revents) {
		return;
	}

	if (state_ == conn_state::connecting && sock_.ssl) {
		if (int(openssl::SSL_get_fd(*sock_.ssl)) == -1) {
			openssl::SSL_set_fd(*sock_.ssl, sock_.fd());
		}

		if (int ssl_events = openssl::ssl_handshake<&openssl::SSL_connect>(sock_.ssl); ssl_events < 0) {
			close_conn(ssl_events == -SSL_ERROR_SYSCALL ? k_sock_closed_err : k_connect_ssl_err);
			return;
		} else if (ssl_events > 0) {
			set_io_events(ssl_events);
			return;
		}
	}

	const auto conn_id = conn_id_;
	if (revents & ev::READ) {
		int err = read_cb();
		if (!err) {
			revents |= ev::WRITE;
		}
	}

	const bool hadWData = w_data_.buf.size();

	if (revents & ev::WRITE && conn_id == conn_id_) {
		write_cb();
	}

	if (sock_.valid()) {
		int nevents = (r_data_.buf.size() || buffered_data_.available()) ? ev::READ : 0;
		if (hadWData || w_data_.buf.size()) {
			nevents |= ev::WRITE;
		}
		set_io_events(nevents);
	}
}

void manual_connection::connect_timer_cb(ev::timer&, int) { close_conn(k_connect_timeout_err); }

void manual_connection::write_cb() {
	if (state_ == conn_state::connecting && sock_.valid()) {
		connect_timer_.stop();
		state_ = conn_state::connected;
	}
	if (w_data_.buf.size()) {
		[[maybe_unused]] int err = 0;
		std::ignore = write(w_data_.buf, w_data_.transfer, err);
	}
}

int manual_connection::read_cb() {
	int err = 0;
	if (r_data_.buf.size()) {
		std::ignore = read(r_data_.buf, r_data_.transfer, err);
	} else {
		read_to_buf(err);
	}
	return err;
}

bool manual_connection::read_from_buf(std::span<char> rd_buf, transfer_data& transfer, bool read_full) noexcept {
	auto cur_buf = rd_buf.subspan(transfer.transfered_size());
	const bool will_read_full = read_full && buffered_data_.size() >= cur_buf.size();
	const bool will_read_any = !read_full && buffered_data_.size();
	if (will_read_full || will_read_any) {
		auto bytes_to_copy = cur_buf.size();
		if (will_read_any && bytes_to_copy > buffered_data_.size()) {
			bytes_to_copy = buffered_data_.size();
		}
		auto it = buffered_data_.tail();
		if (it.size() < bytes_to_copy) {
			buffered_data_.unroll();
			it = buffered_data_.tail();
		}
		memcpy(cur_buf.data(), it.data(), bytes_to_copy);
		std::ignore = buffered_data_.erase(bytes_to_copy);
		transfer.append_transfered(bytes_to_copy);
		return true;
	}
	return false;
}

}  // namespace net
}  // namespace reindexer
