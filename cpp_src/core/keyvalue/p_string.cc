#include "p_string.h"

#include <ostream>

namespace reindexer {
void p_string::Dump(std::ostream& os) const {
	os << "{p: " << std::hex << v << std::dec;
	if (v) {
		const auto l = length();
		os << ", length: " << l << ", type: " << type() << ", [" << std::hex;
		const char* d = data();
		for (size_t i = 0; i < l; ++i) {
			if (i != 0) {
				os << ' ';
			}
			os << static_cast<unsigned>(d[i]);
		}
		os << std::dec << ']';
	}
	os << '}';
}

}  // namespace reindexer
