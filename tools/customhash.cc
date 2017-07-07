#include "customhash.h"

namespace search_engine {

/**we use this becouse default c++ hash function use size_t
size_t on our platform is 8 bytes - but it is very big hash for us
we really need only 4 bytes for hash
Thats why we use standard hash function from libstdc++ for size_t = 4 bytes (changed size_t to uint32_t)
Hash algoritm - MurmurHashUnaligned2
https://gcc.gnu.org/viewcvs/gcc/tags/gcc_6_1_0_release/libstdc%2B%2B-v3/libsupc%2B%2B/hash_bytes.cc?view=log
https://gcc.gnu.org/viewcvs/gcc/tags/gcc_6_1_0_release/libstdc%2B%2B-v3/libsupc%2B%2B/hash_bytes.cc?view=co&revision=235474&content-type=text%2Fplain
**/

static const uint32_t seed = 3339675911UL;

inline uint32_t unaligned_load(const char* p) {
	uint32_t result;
	__builtin_memcpy(&result, p, sizeof(result));
	return result;
}

// Implementation of Murmur hash for 32-bit size_t.
uint32_t _Hash_bytes(const void* ptr, uint32_t len) {
	const uint32_t m = 0x5bd1e995;
	uint32_t hash = seed ^ len;
	const char* buf = static_cast<const char*>(ptr);

	// Mix 4 bytes at a time into the hash.
	while (len >= 4) {
		uint32_t k = unaligned_load(buf);
		k *= m;
		k ^= k >> 24;
		k *= m;
		hash *= m;
		hash ^= k;
		buf += 4;
		len -= 4;
	}

	// Handle the last few bytes of the input array.
	switch (len) {
		case 3:
			hash ^= static_cast<unsigned char>(buf[2]) << 16;

		case 2:
			hash ^= static_cast<unsigned char>(buf[1]) << 8;

		case 1:
			hash ^= static_cast<unsigned char>(buf[0]);
			hash *= m;
	};

	// Do a few final mixes of the hash.
	hash ^= hash >> 13;

	hash *= m;

	hash ^= hash >> 15;

	return hash;
}

uint32_t Hash(const wstring& s) noexcept { return _Hash_bytes(s.data(), s.length() * sizeof(wchar_t)); }
uint32_t HashTreGram(const wchar_t* ptr) noexcept { return _Hash_bytes(ptr, 3 * sizeof(wchar_t)); }
}  // namespace search_engine
