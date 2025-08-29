#include "baseencoder.h"
#include <cstdlib>
#include "cjsonbuilder.h"
#include "cjsontools.h"
#include "core/keyvalue/p_string.h"
#include "core/queryresults/fields_filter.h"
#include "csvbuilder.h"
#include "jsonbuilder.h"
#include "msgpackbuilder.h"
#include "protobufbuilder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

template <typename Builder>
BaseEncoder<Builder>::BaseEncoder(const TagsMatcher* tagsMatcher, const FieldsFilter* filter)
	: tagsMatcher_(tagsMatcher), filter_(filter) {}

template <typename Builder>
void BaseEncoder<Builder>::Encode(std::string_view tuple, Builder& builder, const h_vector<IAdditionalDatasource<Builder>*, 2>& dss) {
	Serializer rdser(tuple);
	builder.SetTagsMatcher(tagsMatcher_);
	if constexpr (kWithTagsPathTracking) {
		builder.SetTagsPath(&curTagsPath_);
	}

	[[maybe_unused]] const ctag begTag = rdser.GetCTag();
	assertrx(begTag.Type() == TAG_OBJECT);
	Builder objNode = builder.Object();
	while (encode(nullptr, rdser, objNode, true));
	for (auto ds : dss) {
		if (ds) {
			if (const auto joinsDs = ds->GetJoinsDatasource()) {
				for (size_t i = 0; i < joinsDs->GetJoinedRowsCount(); ++i) {
					encodeJoinedItems(objNode, joinsDs, i);
				}
			}
			ds->PutAdditionalFields(objNode);
		}
	}
}

template <typename Builder>
void BaseEncoder<Builder>::Encode(ConstPayload& pl, Builder& builder, const h_vector<IAdditionalDatasource<Builder>*, 2>& dss) {
	Serializer rdser(getPlTuple(pl));
	if (rdser.Eof()) {
		return;
	}

	objectScalarIndexes_.reset();
	std::fill(fieldsoutcnt_.begin(), fieldsoutcnt_.end(), 0);
	builder.SetTagsMatcher(tagsMatcher_);
	if constexpr (kWithTagsPathTracking) {
		builder.SetTagsPath(&curTagsPath_);
	}
	[[maybe_unused]] const ctag begTag = rdser.GetCTag();
	assertrx(begTag.Type() == TAG_OBJECT);
	Builder objNode = builder.Object();
	while (encode(&pl, rdser, objNode, true));
	for (auto ds : dss) {
		if (ds) {
			if (const auto joinsDs = ds->GetJoinsDatasource()) {
				for (size_t i = 0, cnt = joinsDs->GetJoinedRowsCount(); i < cnt; ++i) {
					encodeJoinedItems(objNode, joinsDs, i);
				}
			}
			ds->PutAdditionalFields(objNode);
		}
	}
}

template <typename Builder>
const TagsLengths& BaseEncoder<Builder>::GetTagsMeasures(ConstPayload& pl, IEncoderDatasourceWithJoins* ds) {
	tagsLengths_.clear();
	Serializer rdser(getPlTuple(pl));
	if (!rdser.Eof()) {
		[[maybe_unused]] const ctag beginTag = rdser.GetCTag();
		assertrx(beginTag.Type() == TAG_OBJECT);

		tagsLengths_.reserve(kMaxIndexes);
		tagsLengths_.push_back(StartObject);

		while (collectTagsSizes(pl, rdser)) {
		}

		if (ds && ds->GetJoinedRowsCount() > 0) {
			for (size_t i = 0, rows = ds->GetJoinedRowsCount(); i < rows; ++i) {
				collectJoinedItemsTagsSizes(ds, i);
			}
		}

		size_t endPos = 0;
		rx_unused = computeObjectLength(tagsLengths_, 0, endPos);
	}
	return tagsLengths_;
}

template <typename Builder>
void BaseEncoder<Builder>::collectJoinedItemsTagsSizes(IEncoderDatasourceWithJoins* ds, size_t rowid) {
	const size_t itemsCount = ds->GetJoinedRowItemsCount(rowid);
	if (!itemsCount) {
		return;
	}

	BaseEncoder<Builder> subEnc(&ds->GetJoinedItemTagsMatcher(rowid), &ds->GetJoinedItemFieldsFilter(rowid));
	for (size_t i = 0; i < itemsCount; ++i) {
		ConstPayload pl(ds->GetJoinedItemPayload(rowid, i));
		rx_unused = subEnc.GetTagsMeasures(pl, nullptr);
	}
}

template <typename Builder>
void BaseEncoder<Builder>::encodeJoinedItems(Builder& builder, IEncoderDatasourceWithJoins* ds, size_t rowid) {
	const size_t itemsCount = ds->GetJoinedRowItemsCount(rowid);
	if (!itemsCount) {
		return;
	}

	std::string nsTagName("joined_" + ds->GetJoinedItemNamespace(rowid));
	auto arrNode = builder.Array(nsTagName);

	BaseEncoder<Builder> subEnc(&ds->GetJoinedItemTagsMatcher(rowid), &ds->GetJoinedItemFieldsFilter(rowid));
	for (size_t i = 0; i < itemsCount; ++i) {
		ConstPayload pl(ds->GetJoinedItemPayload(rowid, i));
		subEnc.Encode(pl, arrNode);
	}
}

template <typename Builder>
bool BaseEncoder<Builder>::encode(ConstPayload* pl, Serializer& rdser, Builder& builder, bool visible) {
	const ctag tag = rdser.GetCTag();

	if (tag == kCTagEnd) {
		if constexpr (kWithFieldExtractor) {
			if (visible && filter_ && indexedTagsPath_.size() && indexedTagsPath_.back().IsWithIndex()) {
				const auto field = builder.TargetField();
				if (field >= 0 && !builder.IsHavingOffset() && filter_->Match(indexedTagsPath_)) {
					builder.OnScopeEnd(fieldsoutcnt_[field]);
				}
			}
		}
		return false;
	}

	const TagName tagName = tag.Name();
	PathScopeT pathScope(curTagsPath_, tagName);
	TagsPathScope<IndexedTagsPathInternalT> indexedPathScope(indexedTagsPath_, visible ? tagName : TagName::Empty());
	const int tagField = tag.Field();

	if (tagField >= 0) {
		if (!pl) {
			throw Error(errParams, "Trying to encode index field {} without payload", tagField);
		}
		assertrx(tagField < pl->NumFields());
	}

	if (visible && filter_ && !tagName.IsEmpty()) {
		if (tagField >= 0 && pl->Field(tagField).t_.IsFloatVector()) {
			visible = filter_->ContainsVector(tagField);
		} else {
			visible = filter_->Match(indexedTagsPath_);
		}
	}

	const TagType tagType = tag.Type();
	// get field from indexed field
	if (tagField >= 0) {
		const auto& f = pl->Type().Field(tagField);
		if (!f.IsArray() && objectScalarIndexes_.test(tagField)) {
			throw Error(errParams, "Non-array field '{}' [{}] from '{}' can only be encoded once.", f.Name(), tagField, pl->Type().Name());
		}
		int& cnt = fieldsoutcnt_[tagField];
		switch (tagType) {
			case TAG_ARRAY: {
				auto count = rdser.GetVarUInt();
				if (visible) {
					f.Type().EvaluateOneOf(
						[&](KeyValueType::Bool) { builder.Array(tagName, pl->GetArray<bool>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Int) { builder.Array(tagName, pl->GetArray<int>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Int64) { builder.Array(tagName, pl->GetArray<int64_t>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Double) { builder.Array(tagName, pl->GetArray<double>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::String) { builder.Array(tagName, pl->GetArray<p_string>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Uuid) { builder.Array(tagName, pl->GetArray<Uuid>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::FloatVector) {
							objectScalarIndexes_.set(tagField);	 // Currently float vector is always single-value scalar
							auto view = ConstFloatVectorView(pl->Get(tagField, 0));
							if rx_unlikely (view.IsStripped()) {
								throw Error(errLogic, "Attempt to serialize stripped vector");
							}
							assertrx_dbg(unsigned(view.Dimension()) == count);
							builder.Array(tagName, view.Span(), 0);	 // Offset is always zero
						},
						[&](KeyValueType::Float) {
							// Indexed field can not contain float array now
							assertrx(false);
							abort();
						},
						[](OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite>) noexcept {
							assertrx(false);
							abort();
						});
				}
				cnt += int(count);
				break;
			}
			case TAG_NULL:
				objectScalarIndexes_.set(tagField);
				if (visible) {
					builder.Null(tagName);
				}
				break;
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_END:
			case TAG_OBJECT:
			case TAG_UUID:
			case TAG_FLOAT:
				objectScalarIndexes_.set(tagField);
				if (visible) {
					builder.Put(tagName, pl->Get(tagField, cnt), cnt);
				}
				++cnt;
				break;
		}
	} else {
		switch (tagType) {
			case TAG_ARRAY: {
				const carraytag atag = rdser.GetCArrayTag();
				const TagType atagType = atag.Type();
				const auto atagCount = atag.Count();
				if (atagType == TAG_OBJECT) {
					if (visible) {
						auto arrNode = builder.Array(tagName);
						for (size_t i = 0; i < atagCount; ++i) {
							indexedTagsPath_.back().SetIndex(i);
							rx_unused = encode(pl, rdser, arrNode, true);
						}
					} else {
						thread_local static Builder arrNode;
						for (size_t i = 0; i < atagCount; ++i) {
							rx_unused = encode(pl, rdser, arrNode, false);
						}
					}
				} else if (visible) {
					builder.Array(tagName, rdser, atagType, atagCount);
				} else {
					const KeyValueType kvt{atagType};
					for (size_t i = 0; i < atagCount; ++i) {
						rdser.SkipRawVariant(kvt);
					}
				}
				break;
			}
			case TAG_OBJECT: {
				if (visible) {
					auto objNode = builder.Object(tagName);
					while (encode(pl, rdser, objNode, true));
				} else {
					thread_local static Builder objNode;
					while (encode(pl, rdser, objNode, false));
				}
				break;
			}
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_NULL:
			case TAG_END:
			case TAG_UUID:
			case TAG_FLOAT: {
				const KeyValueType kvt{tagType};
				if (visible) {
					Variant value = rdser.GetRawVariant(kvt);
					builder.Put(tagName, std::move(value), 0);
				} else {
					rdser.SkipRawVariant(kvt);
				}
			}
		}
	}

	return true;
}

template <typename Builder>
bool BaseEncoder<Builder>::collectTagsSizes(ConstPayload& pl, Serializer& rdser) {
	const ctag tag = rdser.GetCTag();
	if (tag == kCTagEnd) {
		tagsLengths_.push_back(EndObject);
		return false;
	}
	const TagName tagName = tag.Name();

	if (!tagName.IsEmpty() && filter_) {
		curTagsPath_.push_back(tagName);
	}

	const int tagField = tag.Field();
	const TagType tagType = tag.Type();
	tagsLengths_.push_back(kStandardFieldSize);

	// get field from indexed field
	if (tagField >= 0) {
		assertrx(tagField < pl.NumFields());
		switch (tagType) {
			case TAG_ARRAY: {
				const auto count = rdser.GetVarUInt();
				const bool isFloatVector = pl.Field(tagField).t_.IsFloatVector();
				if (!isFloatVector || !filter_ || filter_->ContainsVector(tagField)) {
#ifdef RX_WITH_STDLIB_DEBUG
					if (isFloatVector) {
						assertrx_dbg(count == ConstFloatVectorView(pl.Get(tagField, 0)).Dimension().Value());
					}
#endif
					tagsLengths_.back() = count;
				} else {
					tagsLengths_.pop_back();
				}
				break;
			}
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_NULL:
			case TAG_OBJECT:
			case TAG_END:
			case TAG_UUID:
			case TAG_FLOAT:
				break;
		}
	} else {
		switch (tagType) {
			case TAG_ARRAY: {
				const carraytag atag = rdser.GetCArrayTag();
				const auto atagCount = atag.Count();
				const auto atagType = atag.Type();
				tagsLengths_.back() = atagCount;
				if (atagType == TAG_OBJECT) {
					for (size_t i = 0; i < atagCount; i++) {
						tagsLengths_.push_back(StartArrayItem);
						rx_unused = collectTagsSizes(pl, rdser);
						tagsLengths_.push_back(EndArrayItem);
					}
				} else {
					const KeyValueType kvt{atagType};
					for (size_t i = 0; i < atagCount; i++) {
						rdser.SkipRawVariant(kvt);
					}
				}
				break;
			}
			case TAG_OBJECT:
				tagsLengths_.back() = StartObject;
				while (collectTagsSizes(pl, rdser)) {
				}
				break;
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_NULL:
			case TAG_END:
			case TAG_UUID:
			case TAG_FLOAT:
				rdser.SkipRawVariant(KeyValueType{tagType});
		}
	}
	if (!tagName.IsEmpty() && filter_) {
		curTagsPath_.pop_back();
	}

	return true;
}

template <typename Builder>
std::string_view BaseEncoder<Builder>::getPlTuple(ConstPayload& pl) {
	VariantArray kref;
	pl.Get(0, kref);

	std::string_view tuple(kref[0]);

	if (tagsMatcher_ && tuple.empty()) {
		tmpPlTuple_.Reset();
		buildPayloadTuple(pl, tagsMatcher_, tmpPlTuple_);
		return tmpPlTuple_.Slice();
	}

	return tuple;
}

template class BaseEncoder<JsonBuilder>;
template class BaseEncoder<CJsonBuilder>;
template class BaseEncoder<MsgPackBuilder>;
template class BaseEncoder<ProtobufBuilder>;
template class BaseEncoder<FieldsExtractor>;
template class BaseEncoder<CsvBuilder>;

}  // namespace reindexer
