#pragma once

#include <mutex>
#include "client/item.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"

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

	std::mutex lck_;
};

using NSArray = h_vector<Namespace::Ptr, 1>;

}  // namespace client
}  // namespace reindexer