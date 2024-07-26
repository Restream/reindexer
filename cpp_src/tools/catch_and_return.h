#pragma once

#define CATCH_STD_AND_RETURN                             \
	catch (const std::exception& err) {                  \
		return Error{err};                               \
	}                                                    \
	catch (...) {                                        \
		return Error{errAssert, "Unexpected exception"}; \
	}

#define CATCH_AND_RETURN       \
	catch (Error & err) {      \
		return std::move(err); \
	}                          \
	CATCH_STD_AND_RETURN

#define RETURN_RESULT_NOEXCEPT(...) \
	try {                           \
		return __VA_ARGS__;         \
	}                               \
	CATCH_AND_RETURN
