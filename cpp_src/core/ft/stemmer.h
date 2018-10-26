#pragma once

#include <mutex>
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

	void stem(char *dst, size_t dst_len, const char *src, size_t src_len) {
		std::lock_guard<mutex> lock(lock_);

		auto res = sb_stemmer_stem(stemmer_, reinterpret_cast<const sb_symbol *>(src), src_len);
		strncpy(dst, reinterpret_cast<const char *>(res), dst_len);
	}

	sb_stemmer *stemmer_ = nullptr;
	std::mutex lock_;
};

}  // namespace reindexer
