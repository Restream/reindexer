#pragma once

#define CATCH_AND_RETURN                                 \
	catch (std::exception & err) {                       \
		return Error{std::move(err)};                    \
	}                                                    \
	catch (...) {                                        \
		return Error{errAssert, "Unexpected exception"}; \
	}

#define RETURN_RESULT_NOEXCEPT(...) \
	try {                           \
		return __VA_ARGS__;         \
	}                               \
	CATCH_AND_RETURN
