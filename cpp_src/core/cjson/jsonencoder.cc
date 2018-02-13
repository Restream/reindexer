#include "jsonencoder.h"
#include "core/query/queryresults.h"

namespace reindexer {

JsonPrintFilter::JsonPrintFilter(const TagsMatcher tagsMatcher, const h_vector<string, 4>& filter) {
	for (auto& str : filter) {
		int tag = tagsMatcher.name2tag(str.c_str());
		if (!filter_.size()) filter_.push_back(true);
		if (tag) {
			if (tag >= int(filter_.size())) {
				filter_.insert(filter_.end(), 1 + tag - filter_.size(), false);
			}
			filter_[tag] = true;
		} else {
		}
	}
}

JsonEncoder::JsonEncoder(const TagsMatcher& tagsMatcher, const JsonPrintFilter& filter) : tagsMatcher_(tagsMatcher), filter_(filter) {}

void JsonEncoder::Encode(ConstPayload* pl, WrSerializer& wrSer) {
	key_string plTuple;
	getPlTuple(pl, plTuple);
	Serializer rdser(plTuple->data(), plTuple->length());

	for (int i = 0; i < pl->NumFields(); ++i) fieldsoutcnt_[i] = 0;
	bool first = true;
	encodeJson(pl, rdser, wrSer, first, true);
}

void JsonEncoder::Encode(ConstPayload* pl, WrSerializer& wrSer, IJsonEncoderDatasourceWithJoins& ds) {
	Encode(pl, wrSer);
	if (!ds.GetJoinedRowsCount()) return;

	if (wrSer.Len() > 0) {
		uint8_t* eof = wrSer.Buf() + wrSer.Len() - 1;
		*eof = ',';
	}

	const size_t joinedItemsCount = ds.GetJoinedRowsCount();
	for (size_t i = 0; i < joinedItemsCount; ++i) {
		encodeJoinedItems(wrSer, ds, i);
		if (i != joinedItemsCount - 1) {
			wrSer.PutChar(',');
		}
	}

	wrSer.PutChar('}');
}

static inline void encodeValue(int tagType, Serializer& rdser, WrSerializer& wrser, bool visible) {
	switch (tagType) {
		case TAG_DOUBLE:
			if (visible)
				wrser.Printf("%.20g", rdser.GetDouble());
			else
				rdser.GetDouble();
			break;
		case TAG_VARINT:
			if (visible)
				wrser.Print(rdser.GetVarint());
			else
				rdser.GetVarint();
			break;
		case TAG_BOOL:
			if (visible)
				wrser.PutChars(rdser.GetBool() ? "true" : "false");
			else
				rdser.GetBool();
			break;
		case TAG_NULL:
			if (visible) wrser.PutChars("null");
			break;
		case TAG_STRING:
			if (visible)
				wrser.PrintJsonString(rdser.GetVString());
			else
				rdser.GetVString();
			break;
		default:
			assertf(0, "Unexpected cjson typeTag '%d' while parsing value", tagType);
	}
}

static void encodeKeyRef(WrSerializer& wrser, KeyRef kr, int tagType) {
	if (tagType == TAG_NULL) {
		wrser.PutChars("null");
		return;
	}

	switch (kr.Type()) {
		case KeyValueInt:
			if (tagType != TAG_BOOL)
				wrser.Print(int(kr));
			else
				wrser.PutChars(int(kr) ? "true" : "false");
			break;
		case KeyValueInt64:
			wrser.Print(int64_t(kr));
			break;
		case KeyValueDouble:
			wrser.Printf("%.20g", double(kr));
			break;
		case KeyValueString:
			wrser.PrintJsonString(p_string(kr));
			break;
		default:
			wrser.PutChars("null");
	}
}

bool JsonEncoder::encodeJoinedItem(WrSerializer& wrSer, ConstPayload& pl) {
	key_string plTuple;
	getPlTuple(&pl, plTuple);
	Serializer rdser(plTuple->data(), plTuple->length());

	bool first = true;
	for (int i = 0; i < pl.NumFields(); ++i) fieldsoutcnt_[i] = 0;
	return encodeJson(&pl, rdser, wrSer, first, true);
}

bool JsonEncoder::encodeJoinedItems(WrSerializer& wrSer, IJsonEncoderDatasourceWithJoins& ds, size_t rowid) {
	const size_t itemsCount = ds.GetJoinedRowItemsCount(rowid);
	if (!itemsCount) return false;

	string nsTagName("joined." + ds.GetJoinedItemNamespace(rowid));
	wrSer.PrintJsonString(nsTagName);
	wrSer.PutChar(':');
	wrSer.PutChar('[');

	for (size_t i = 0; i < itemsCount; ++i) {
		ConstPayload pl(ds.GetJoinedItemPayload(rowid, i));
		encodeJoinedItem(wrSer, pl);
		if (i != itemsCount - 1) {
			wrSer.PutChar(',');
		}
	}

	wrSer.PutChar(']');

	return true;
}

bool JsonEncoder::encodeJson(ConstPayload* pl, Serializer& rdser, WrSerializer& wrser, bool& first, bool visible) {
	ctag tag = rdser.GetVarUint();
	int tagType = tag.Type();

	if (tagType == TAG_END) return false;

	int tagName = tag.Name();

	visible = visible && filter_.Match(tagName);

	if (!first && visible) wrser.PutChar(',');
	if (visible) first = false;
	bool nfirst = true;

	if (tagName && visible) {
		wrser.PrintJsonString(tagsMatcher_.tag2name(tagName));
		wrser.PutChar(':');
	}
	int tagField = tag.Field();

	// get field from indexed field
	if (tagField >= 0) {
		KeyRefs kr;
		int* cnt = &fieldsoutcnt_[tagField];

		switch (tagType) {
			case TAG_ARRAY: {
				if (visible) {
					wrser.PutChar('[');
					int count = rdser.GetVarUint();
					if (count) pl->Get(tagField, kr);
					while (count--) {
						assertf(*cnt < int(kr.size()), "No data in field '%s.%s', got %d items.", pl->Type().Name().c_str(),
								pl->Type().Field(tagField).Name().c_str(), *cnt);

						encodeKeyRef(wrser, kr[(*cnt)++], tagType);
						if (count) wrser.PutChar(',');
					}
					wrser.PutChar(']');
				} else
					(*cnt) += rdser.GetVarUint();
				break;
			}
			case TAG_NULL:
				if (visible) wrser.PutChars("null");
				break;
			default:
				if (visible) {
					pl->Get(tagField, kr);
					assertf(*cnt < int(kr.size()), "No data in field '%s.%s', got %d items.", pl->Type().Name().c_str(),
							pl->Type().Field(tagField).Name().c_str(), *cnt);

					encodeKeyRef(wrser, kr[(*cnt)++], tagType);
				} else
					(*cnt)++;
				break;
		}
		return true;
	}

	switch (tagType) {
		case TAG_ARRAY: {
			if (visible) wrser.PutChar('[');
			carraytag atag = rdser.GetUInt32();
			for (int i = 0; i < atag.Count(); i++) {
				if (i != 0 && atag.Tag() != TAG_OBJECT && visible) wrser.PutChar(',');
				switch (atag.Tag()) {
					case TAG_OBJECT:
						encodeJson(pl, rdser, wrser, nfirst, visible);
						break;
					default:
						encodeValue(atag.Tag(), rdser, wrser, visible);
						break;
				}
			}
			if (visible) wrser.PutChar(']');
			break;
		}
		case TAG_OBJECT:
			if (visible) wrser.PutChar('{');
			while (encodeJson(pl, rdser, wrser, nfirst, visible))
				;
			if (visible) wrser.PutChar('}');
			break;
		default:
			encodeValue(tagType, rdser, wrser, visible);
	}
	return true;
}

key_string JsonEncoder::buildPlTuple(ConstPayload* pl) {
	WrSerializer wrser;
	wrser.PutVarUint(ctag(TAG_OBJECT, 0));

	for (int idx = 1; idx < pl->NumFields(); ++idx) {
		KeyRefs keyRefs;
		pl->Get(idx, keyRefs);

		string fieldName = pl->Type().Field(idx).Name();
		int tagName = tagsMatcher_.name2tag(fieldName.c_str());
		if (!tagName) {
			tagName = const_cast<TagsMatcher&>(tagsMatcher_).name2tag(fieldName.c_str(), true);
		}
		assert(tagName);

		int field = idx;
		if (pl->Type().Field(field).IsArray()) {
			wrser.PutVarUint(ctag(TAG_ARRAY, tagName, field));
			wrser.PutVarUint(keyRefs.size());
		} else {
			for (const KeyRef& keyRef : keyRefs) {
				switch (keyRef.Type()) {
					case KeyValueInt:
						wrser.PutVarUint(ctag(TAG_VARINT, tagName, field));
						break;
					case KeyValueInt64:
						wrser.PutVarUint(ctag(TAG_VARINT, tagName, static_cast<int64_t>(field)));
						break;
					case KeyValueDouble:
						wrser.PutVarUint(ctag(TAG_DOUBLE, tagName, field));
						break;
					case KeyValueString:
						wrser.PutVarUint(ctag(TAG_STRING, tagName, field));
						break;
					case KeyValueUndefined:
					case KeyValueEmpty:
						wrser.PutVarUint(ctag(TAG_NULL, tagName));
						break;
					default:
						std::abort();
				}
			}
		}
	}

	wrser.PutVarUint(ctag(TAG_END, 0));
	return make_key_string(reinterpret_cast<const char*>(wrser.Buf()), wrser.Len());
}

key_string& JsonEncoder::getPlTuple(ConstPayload* pl, key_string& plTuple) {
	KeyRefs kref;
	pl->Get(0, kref);
	plTuple = static_cast<key_string>(kref[0]);

	if (plTuple->size() == 0) {
		plTuple = buildPlTuple(pl).get();
	}

	return plTuple;
}

}  // namespace reindexer
