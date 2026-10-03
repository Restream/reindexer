#include "tagsmatcher.h"

namespace reindexer {

static const shared_cow_ptr<TagsMatcherImpl> kEmptyTmImpl(make_intrusive<intrusive_atomic_rc_wrapper<TagsMatcherImpl>>());

TagsMatcher::TagsMatcher() noexcept : impl_(kEmptyTmImpl), wasUpdated_(false) {}

}  // namespace reindexer
