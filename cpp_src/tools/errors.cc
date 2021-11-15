#include "errors.h"
#include <stdio.h>

namespace reindexer {

Error::Error(int code, std::string_view what) {
	if (code != errOK) {
		ptr_ = make_intrusive<intrusive_atomic_rc_wrapper<payload>>(code, std::string(what));
	}
}

Error::Error(int code) {
	if (code != errOK) {
		ptr_ = make_intrusive<intrusive_atomic_rc_wrapper<payload>>(code, std::string());
	}
}

const std::string& Error::what() const noexcept {
	static std::string noerr = "";
	return ptr_ ? ptr_->what_ : noerr;
}

int Error::code() const noexcept { return ptr_ ? ptr_->code_ : errOK; }

}  // namespace reindexer
