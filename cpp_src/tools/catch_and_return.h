#pragma once

#define CATCH_AND_RETURN                                 \
	catch (Error & err) {                                \
		return std::move(err);                           \
	}                                                    \
	catch (const std::exception& err) {                  \
		return Error{err};                               \
	}                                                    \
	catch (...) {                                        \
		return Error{errAssert, "Unexpected exception"}; \
	}

#define RETURN_RESULT_NOEXCEPT(expr) \
	try {                            \
		return expr;                 \
	}                                \
	CATCH_AND_RETURN
