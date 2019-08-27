#pragma once

#include <mutex>
#include <string>
#include "libstemmer/include/libstemmer.h"

namespace reindexer {
using std::mutex;
class stemmer {
public:
	stemmer(const char *lang = "en") { stemmer_ = sb_stemmer_new(lang, "UTF_8"); }
	stemmer(const stemmer &) = delete;
	stemmer(stemmer &&other) noexcept {
		stemmer_ = other.stemmer_;
		other.stemmer_ = nullptr;
	}
	~stemmer() { sb_stemmer_delete(stemmer_); }

	void stem(const std::string &src, std::string &dst) {
		std::lock_guard<mutex> lock(lock_);

		auto res = sb_stemmer_stem(stemmer_, reinterpret_cast<const sb_symbol *>(src.data()), src.length());
		dst.assign(reinterpret_cast<const char *>(res));
	}

	sb_stemmer *stemmer_ = nullptr;
	std::mutex lock_;
};

}  // namespace reindexer
