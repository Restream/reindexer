/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/impl/FaissException.h>
#include <sstream>

#ifdef __GNUG__
#include <cxxabi.h>
#endif

namespace faiss {

FaissException::FaissException(const std::string& m) : msg(m) {}

FaissException::FaissException(
        const std::string& m,
        const char* funcName,
        const char* file,
        int line) {
    int size = snprintf(
            nullptr,
            0,
            "Error in %s at %s:%d: %s",
            funcName,
            file,
            line,
            m.c_str());
    msg.resize(size + 1);
    snprintf(
            &msg[0],
            msg.size(),
            "Error in %s at %s:%d: %s",
            funcName,
            file,
            line,
            m.c_str());
}

const char* FaissException::what() const noexcept {
    return msg.c_str();
}

void handleExceptions(
        std::vector<std::pair<int, std::exception_ptr>>& exceptions) {
    if (exceptions.size() == 1) {
        // throw the single received exception directly
        std::rethrow_exception(exceptions.front().second);

    } else if (exceptions.size() > 1) {
        // multiple exceptions; aggregate them and return a single exception
        std::stringstream ss;

        for (auto& p : exceptions) {
            try {
                std::rethrow_exception(p.second);
            } catch (std::exception& ex) {
                if (ex.what()) {
                    // exception message available
                    ss << "Exception thrown from index " << p.first << ": "
                       << ex.what() << "\n";
                } else {
                    // No message available
                    ss << "Unknown exception thrown from index " << p.first
                       << "\n";
                }
            } catch (...) {
                ss << "Unknown exception thrown from index " << p.first << "\n";
            }
        }

        throw FaissException(ss.str());
    }
}

// From
// https://stackoverflow.com/questions/281818/unmangling-the-result-of-stdtype-infoname

std::string demangle_cpp_symbol(const char* name) {
#ifdef __GNUG__
    int status = -1;
    const char* res = abi::__cxa_demangle(name, nullptr, nullptr, &status);
    std::string sres;
    if (status == 0) {
        sres = res;
    }
    free(const_cast<void*>(static_cast<const void*>(res)));
    return sres;
#else
    // don't know how to do this on other platforms
    return std::string(name);
#endif
}

} // namespace faiss
