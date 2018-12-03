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

	Namespace(const string &name);
	Item NewItem();

	// protected:
	string name_;
	PayloadType payloadType_;
	TagsMatcher tagsMatcher_;

	shared_timed_mutex lck_;
};

}  // namespace client
}  // namespace reindexer
