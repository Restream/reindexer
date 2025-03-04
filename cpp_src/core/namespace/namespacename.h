#pragma once

#include "estl/h_vector.h"
#include "estl/intrusive_ptr.h"
#include "tools/stringstools.h"

namespace reindexer {

namespace namespace_name_impl {
class NamespaceNameImpl {
public:
	using HasherT = nocase_hash_str;

	explicit NamespaceNameImpl(std::string_view name) {
		size_t i = 0;
		data_.resize(name.size());
		for (auto it = data_.begin(), itEnd = data_.end(); it != itEnd; ++it, ++i) {
			(*it) = tolower(name[i]);
		}
		hash_ = HasherT()(std::string_view(data_.data(), data_.size()));
		if (std::memcmp(name.data(), data_.data(), name.size()) != 0) {
			originalNameStart_ = data_.size();
			data_.insert(data_.end(), name.begin(), name.end());
		} else {
			originalNameStart_ = 0;
		}
	}

	size_t Hash() const noexcept { return hash_; }
	bool Empty() const noexcept { return data_.empty(); }
	size_t Size() const noexcept { return data_.size(); }
	std::string_view OriginalName() const noexcept {
		return std::string_view(data_.data() + originalNameStart_, data_.size() - originalNameStart_);
	}
	operator std::string_view() const noexcept { return std::string_view(data_.data(), data_.size() - originalNameStart_); }

private:
	using VecT = h_vector<char, 44>;

	size_t hash_;
	VecT::size_type originalNameStart_;
	h_vector<char, 48> data_;
};
}  // namespace namespace_name_impl

class NamespaceName {
	using ValueT = intrusive_atomic_rc_wrapper<namespace_name_impl::NamespaceNameImpl>;

public:
	NamespaceName() = default;
	explicit NamespaceName(std::string_view name) : impl_(make_intrusive<ValueT>(name)) {}

	size_t hash() const noexcept { return impl_ ? impl_->Hash() : 0; }
	bool empty() const noexcept { return !impl_ || impl_->Empty(); }
	size_t size() const noexcept { return impl_ ? impl_->Size() : 0; }

	std::string_view OriginalName() const noexcept { return impl_ ? impl_->OriginalName() : std::string_view(); }
	operator std::string_view() const noexcept { return impl_ ? std::string_view(*impl_) : std::string_view(); }

private:
	intrusive_ptr<ValueT> impl_;
};

inline bool operator==(const NamespaceName& lhs, const NamespaceName& rhs) noexcept {
	return std::string_view(lhs) == std::string_view(rhs);
}

struct NamespaceNameEqual {
	using is_transparent = void;

	bool operator()(const NamespaceName& lhs, const NamespaceName& rhs) const noexcept { return lhs == rhs; }
	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return iequals(lhs, rhs); }
	bool operator()(const NamespaceName& lhs, std::string_view rhs) const noexcept { return iequals(lhs, rhs); }
	bool operator()(std::string_view lhs, const NamespaceName& rhs) const noexcept { return iequals(lhs, rhs); }
};

struct NamespaceNameLess {
	using is_transparent = void;

	bool operator()(const NamespaceName& lhs, const NamespaceName& rhs) const noexcept {
		return std::string_view(lhs) < std::string_view(rhs);
	}
	bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return iless(lhs, rhs); }
	bool operator()(const NamespaceName& lhs, std::string_view rhs) const noexcept { return iless(lhs, rhs); }
	bool operator()(std::string_view lhs, const NamespaceName& rhs) const noexcept { return iless(lhs, rhs); }
};

struct NamespaceNameHash {
	using is_transparent = void;

	size_t operator()(std::string_view hs) const noexcept { return namespace_name_impl::NamespaceNameImpl::HasherT()(hs); }
	size_t operator()(const NamespaceName& hs) const noexcept { return hs.hash(); }
};

}  // namespace reindexer
