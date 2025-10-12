#pragma once

#include "channel.h"

namespace reindexer {
namespace coroutine {

/// @class Tokens pool based on channel
template <typename T>
class [[nodiscard]] tokens_pool {
public:
	using OnTokenReturnF = std::function<void(const T&)>;

	/// @class Token guard owns token and returns it back to pool in desctructor
	class [[nodiscard]] token {
	public:
		token() = default;
		token(const token&) = delete;
		token(token&& o) : pool_(o.pool_), t_(std::move(o.t_)), onReturn_(std::move(o.onReturn_)) { o.pool_ = nullptr; }
		token& operator=(const token&) = delete;
		token& operator=(token&& o) {
			t_ = std::move(o.t_);
			pool_ = o.pool_;
			o.pool_ = nullptr;
			onReturn_ = std::move(o.onReturn_);
			return *this;
		}
		/// Return token to it's pool
		void to_pool() noexcept {
			if (is_valid()) {
				const T& val = value();
				pool_->return_token(std::move(t_));
				pool_ = nullptr;
				if (onReturn_) {
					onReturn_(val);
				}
			}
		}
		/// Get token's value. If token was already returned to pool, exception will be thrown
		const T& value() const {
			if (is_valid()) {
				return t_;
			}
			throw std::logic_error("Token was already returned to pool");
		}
		bool is_valid() const noexcept { return pool_; }
		~token() { to_pool(); }

	private:
		token(T t, tokens_pool* pool, OnTokenReturnF onReturn) : pool_(pool), t_(std::move(t)), onReturn_(std::move(onReturn)) {}

		friend class tokens_pool;
		tokens_pool* pool_ = nullptr;
		T t_ = T();
		OnTokenReturnF onReturn_;
	};

	tokens_pool(size_t count, OnTokenReturnF onTokenReturn = nullptr) : ch_(count), onTokenReturn_(std::move(onTokenReturn)) {
		for (size_t i = 0; i < count; ++i) {
			ch_.push(T());
		}
	}
	tokens_pool(std::vector<T> tokens, OnTokenReturnF onTokenReturn = nullptr)
		: ch_(tokens.size()), onTokenReturn_(std::move(onTokenReturn)) {
		for (auto&& token : tokens) {
			ch_.push(std::move(token));
		}
	}
	~tokens_pool() { assert(unused()); }
	/// Get token from pool. If pool is empty - await tokens from another coroutines
	token await_token() {
		assert(ch_.opened());
		auto res = ch_.pop();
		assert(res.second);
		return token(std::move(res.first), this, onTokenReturn_);
	}
	/// Returns true if none of the tokens are currently taken from pool
	bool unused() const noexcept { return ch_.full(); }

private:
	/// Return token to pool
	template <typename U>
	void return_token(U&& token) {
		assert(ch_.opened());
		assert(!ch_.full());
		ch_.push(std::forward<U>(token));
	}

	channel<T> ch_;
	OnTokenReturnF onTokenReturn_;
};

}  // namespace coroutine
}  // namespace reindexer
