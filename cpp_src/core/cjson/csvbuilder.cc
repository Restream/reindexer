#include "csvbuilder.h"

namespace reindexer::builders {
CsvBuilder::CsvBuilder(ObjType type, const CsvBuilder& parent)
	: ser_(parent.ser_),
	  tm_(parent.tm_),
	  type_(type),
	  level_(parent.level_ + 1),
	  startSerLen_(ser_->Len()),
	  ordering_(parent.ordering_),
	  buf_(parent.buf_),
	  positions_([this]() -> std::vector<std::pair<int, int>> {
		  if (level_ == 0 && ordering_) {
			  return {ordering_->size(), std::pair{-1, -1}};
		  }
		  return {};
	  }()) {
	if (level_ < 1) {
		return;
	} else if (level_ == 1) {
		(*ser_) << '"';
	}

	switch (type_) {
		case ObjType::TypeArray:
			(*ser_) << '[';
			break;
		case ObjType::TypeObject:
			(*ser_) << '{';
			break;
		case ObjType::TypeObjectArray:
		case ObjType::TypePlain:
		default:
			break;
	}
}

CsvBuilder::CsvBuilder(WrSerializer& ser, CsvOrdering& ordering)
	: ser_(&ser),
	  level_(-1),
	  ordering_(!ordering.ordering_.empty() ? &ordering.ordering_ : nullptr),
	  buf_(ordering_ ? &ordering.buf_ : nullptr) {}

CsvBuilder::~CsvBuilder() noexcept(false) {
	if (std::uncaught_exceptions() == 0) {
		End();
	}
}

std::string_view CsvBuilder::getNameByTag(TagName tagName) { return tagName.IsEmpty() ? std::string_view{} : tm_->tag2name(tagName); }

void CsvBuilder::End() {
	if (!positions_.empty()) {
		postProcessing();
	}
	if (level_ > 0) {
		switch (type_) {
			case ObjType::TypeArray:
				(*ser_) << ']';
				break;
			case ObjType::TypeObject:
				(*ser_) << '}';
				break;
			case ObjType::TypeObjectArray:
			case ObjType::TypePlain:
			default:
				break;
		}
	}

	if (level_ == 1) {
		(*ser_) << '"';
	}

	type_ = ObjType::TypePlain;
}

CsvBuilder CsvBuilder::Object(std::string_view name, int /*size*/) {
	putName(name);
	return CsvBuilder(ObjType::TypeObject, *this);
}

CsvBuilder CsvBuilder::Array(std::string_view name, int /*size*/) {
	putName(name);
	return CsvBuilder(ObjType::TypeArray, *this);
}

void CsvBuilder::putName(std::string_view name) {
	if (level_ == 0 && ordering_ && !ordering_->empty()) {
		tmProcessing(name);
	}

	if (count_++) {
		(*ser_) << ',';
	}

	if (level_ < 1) {
		return;
	}

	if (name.data()) {
		(*ser_) << '"';
		(*ser_).PrintJsonString(name, WrSerializer::PrintJsonStringMode::QuotedQuote);
		(*ser_) << '"';
		(*ser_) << ':';
	}
}

void CsvBuilder::tmProcessing(std::string_view name) {
	const TagName tag = tm_->name2tag(name);

	auto prevFinishPos = ser_->Len();
	if (!tag.IsEmpty()) {
		auto it = std::find_if(ordering_->begin(), ordering_->end(), [&tag](const auto& t) { return t == tag; });

		if (it != ordering_->end()) {
			if (curTagPos_ > -1) {
				positions_[curTagPos_].second = prevFinishPos;
			}
			curTagPos_ = std::distance(ordering_->begin(), it);
			positions_[curTagPos_].first = prevFinishPos + (count_ > 0 ? 1 : 0);
		} else {
			throw Error(errParams, "Tag {} from tagsmatcher was not passed with the schema", name);
		}
	} else {
		if (name.substr(0, 7) != "joined_") {
			throw Error(errParams, "The \"joined_*\"-like tag for joined namespaced is expected, but received {}", name);
		}

		if (curTagPos_ > -1) {
			positions_[curTagPos_].second = prevFinishPos;
		}
		if (count_) {
			(*ser_) << ',';
		}

		(*ser_) << "\"{";
		type_ = ObjType::TypeObject;
		count_ = 0;
		level_++;
	}
}

void CsvBuilder::postProcessing() {
	if (!buf_) {
		throw Error(errParams, "Buffer not initialized");
	}

	buf_->Reset();

	if (positions_[curTagPos_].second == -1) {
		positions_[curTagPos_].second = ser_->Len();
	}

	auto joinedData = std::string_view(ser_->Slice().data() + positions_[curTagPos_].second, ser_->Len() - positions_[curTagPos_].second);

	bool needDelim = false;
	for (auto& [begin, end] : positions_) {
		if (needDelim) {
			*buf_ << ',';
		} else {
			needDelim = true;
		}
		*buf_ << std::string_view{ser_->Slice().data() + begin, static_cast<size_t>(end - begin)};
	}

	*buf_ << joinedData;
	ser_->Reset(startSerLen_);
	*ser_ << buf_->Slice();
}

void CsvBuilder::Put(std::string_view name, std::string_view arg, int /*offset*/) {
	putName(name);

	std::string_view optQuote = level_ > 0 ? "\"" : "";
	(*ser_) << optQuote;
	(*ser_).PrintJsonString(arg, WrSerializer::PrintJsonStringMode::QuotedQuote);

	(*ser_) << optQuote;
}

void CsvBuilder::Put(std::string_view name, Uuid arg, int /*offset*/) {
	putName(name);
	ser_->PrintJsonUuid(arg);
}

void CsvBuilder::Raw(std::string_view name, std::string_view arg) {
	putName(name);
	(*ser_) << arg;
}

void CsvBuilder::Null(std::string_view name) {
	putName(name);
	(*ser_) << "null";
}

void CsvBuilder::Put(std::string_view name, const Variant& kv, int offset) {
	kv.Type().EvaluateOneOf(
		[&](KeyValueType::Int) { Put(name, int(kv), offset); }, [&](KeyValueType::Int64) { Put(name, int64_t(kv), offset); },
		[&](KeyValueType::Double) { Put(name, double(kv), offset); }, [&](KeyValueType::Float) { Put(name, float(kv), offset); },
		[&](KeyValueType::String) { Put(name, std::string_view(kv), offset); }, [&](KeyValueType::Null) { Null(name); },
		[&](KeyValueType::Bool) { Put(name, bool(kv), offset); },
		[&](KeyValueType::Tuple) {
			auto arrNode = Array(name);
			for (auto& val : kv.getCompositeValues()) {
				arrNode.Put({nullptr, 0}, val);
			}
		},
		[&](KeyValueType::Uuid) { Put(name, Uuid{kv}, offset); },
		[](concepts::OneOf<KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::FloatVector> auto) { assertrx_throw(false); });
}
}  // namespace reindexer::builders
