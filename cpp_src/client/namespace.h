#pragma once

#include <mutex>
#include "client/item.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"
#include "estl/shared_mutex.h"

namespace reindexer {
namespace client {

class Namespace {
public:
	typedef std::shared_ptr<Namespace> Ptr;

	Namespace(std::string name);
	Item NewItem();

	// protected:
	std::string name_;
	PayloadType payloadType_;
	TagsMatcher tagsMatcher_;

	shared_timed_mutex lck_;
};

}  // namespace client
}  // namespace reindexer
