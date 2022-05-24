#pragma once

#include <string_view>
#include "estl/intrusive_ptr.h"

namespace reindexer {
namespace client {

class Namespace;

class INamespaces {
public:
	using IntrusiveT = intrusive_atomic_rc_wrapper<INamespaces>;
	using PtrT = intrusive_ptr<IntrusiveT>;

	virtual void Add(const std::string &name) = 0;
	virtual void Erase(std::string_view name) = 0;
	virtual Namespace *Get(std::string_view name) = 0;
	virtual ~INamespaces() = default;
};

}  // namespace client
}  // namespace reindexer
