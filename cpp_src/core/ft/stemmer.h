#pragma once

#include <string>
#include "estl/lock.h"
#include "estl/mutex.h"
#include "libstemmer/include/libstemmer.h"

namespace reindexer {
class [[nodiscard]] stemmer {
public:
	stemmer(const char* lang = "en") { stemmer_ = sb_stemmer_new(lang, "UTF_8"); }
	stemmer(const stemmer&) = delete;
	stemmer(stemmer&& other) noexcept {
		stemmer_ = other.stemmer_;
		other.stemmer_ = nullptr;
	}
	stemmer& operator=(stemmer&& other) noexcept {
		if (this != &other) {
			stemmer_ = other.stemmer_;
			other.stemmer_ = nullptr;
		}
		return *this;
	}
	~stemmer() { sb_stemmer_delete(stemmer_); }

	void stem(const std::string& src, std::string& dst) {
		lock_guard lock(lock_);

		auto res = sb_stemmer_stem(stemmer_, reinterpret_cast<const sb_symbol*>(src.data()), src.length());
		dst.assign(reinterpret_cast<const char*>(res));
	}

	sb_stemmer* stemmer_ = nullptr;
	mutex lock_;
};

}  // namespace reindexer
