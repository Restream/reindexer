#include "defaultvaluecoder.h"

namespace reindexer {

DefaultValueCoder::DefaultValueCoder(std::string_view ns, const PayloadFieldType &fld, std::vector<TagsPath> &&tps, int16_t fieldIdx)
	: ns_(ns),
	  field_(fld.Name()),
	  tags_(std::move(tps)),
	  fieldIdx_(fieldIdx),
	  type_(fld.Type().ToTagType()),
	  array_(fld.IsArray()),
	  basePath_(&tags_.front()) {}

bool DefaultValueCoder::Match(int field) noexcept {
	// non-nested field present in tuple
	if ((field == fieldIdx_) && ready()) {
		state_ = State::found;
	}
	return false;  // returned result is always same
}

bool DefaultValueCoder::Match(TagType tt, const TagsPath &tp) {
	static const bool result = false;  // returned result is always same

	// nothing to look for (start tuple global object)
	if (tp.empty()) {
		state_ = State::wait;
		inArray_ = false;
		arrField_ = 0;
		return result;
	}

	// found\recorded earlier
	if ((state_ == State::found) || ((state_ == State::write) && !inArray_)) {
		return result;
	}

	// check if active array has been processed
	const bool arrayTag = (tt == TAG_ARRAY);
	if (inArray_) {
		inArray_ = ((tt == TAG_OBJECT) || arrayTag) ? (tp.back() == arrField_) : (tp[tp.size() - 2] == arrField_);	// -2 pre-last item
		// recorded earlier - stop it
		if (!inArray_ && (state_ == State::write)) {
			return result;
		}
	}

	// try match nested field
	if (tt == TAG_OBJECT) {
		assertrx(state_ != State::found);
		match(tp);
		return result;
	}

	// may be end element of adjacent nested field
	if (arrayTag) {
		inArray_ = (tp.front() == basePath_->front());
		arrField_ = tp.back();
	}

	// not nested
	if (copyPos_ == 0) {
		return result;
	}

	// detect array insertion into array (not supported)
	if (arrayTag && array_) {
		state_ = State::found;	// do nothing
	} else if ((tp.front() == basePath_->front()) && (tp.size() > basePath_->size())) {
		++nestingLevel_;
	}

	return result;
}

void DefaultValueCoder::Serialize(WrSerializer &wrser) {
	if (blocked()) {
		return;	 // skip processing
	}

	// skip nested levels
	if ((basePath_->size() > 1) || (nestingLevel_ > 1)) {
		assertrx(nestingLevel_ > 0);
		--nestingLevel_;

		// new field - move to valid level
		if (nestingLevel_ > copyPos_) {
			return;
		}
	}

	write(wrser);
	Reset();
	state_ = State::write;
}

bool DefaultValueCoder::Reset() noexcept {
	nestingLevel_ = 1;
	copyPos_ = 0;
	// NOTE: return true when updating tuple
	return (state_ == State::write);
}

void DefaultValueCoder::match(const TagsPath &tp) {
	++nestingLevel_;

	for (auto &path : tags_) {
		if (path.front() != tp.front()) {
			continue;
		}

		copyPos_ = 1;
		auto pathSize = path.size();
		auto sz = std::min(pathSize, tp.size());
		for (size_t idx = 1; idx < sz; ++idx) {
			if (path[idx] != tp[idx]) {
				break;
			}
			++copyPos_;

			// we are trying to add field with non-nested paths, but an intersection was found in additional nested paths.
			// Stop, throw an error
			if (tags_.front().size() == 1) {
				throw Error(errLogic, "Cannot add field with name '%s' to namespace '%s'. One of nested json paths is already in use",
							field_, ns_);
			}
		}
		state_ = State::match;
		basePath_ = &path;
		break;
	}
}

void DefaultValueCoder::write(WrSerializer &wrser) const {
	int32_t nestedObjects = 0;
	for (size_t idx = copyPos_, sz = basePath_->size(); idx < sz; ++idx) {
		auto tagName = (*basePath_)[idx];
		// real index field in last tag
		const bool finalTag = (idx == (sz - 1));
		if (finalTag) {
			if (array_) {
				wrser.PutCTag(ctag{TAG_ARRAY, tagName, fieldIdx_});
				wrser.PutVarUint(0);
			} else {
				wrser.PutCTag(ctag{type_, tagName, fieldIdx_});
			}
			break;
		}

		// start nested object
		wrser.PutCTag(ctag{TAG_OBJECT, tagName});
		++nestedObjects;
	}

	// add end tags to all objects
	while (nestedObjects-- > 0) {
		wrser.PutCTag(kCTagEnd);
	}
}

}  // namespace reindexer
