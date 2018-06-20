#include "jsonencoder.h"
#include <cstdlib>
#include "core/payload/payloadtuple.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

JsonEncoder::JsonEncoder(const TagsMatcher& tagsMatcher, const JsonPrintFilter& filter) : tagsMatcher_(tagsMatcher), filter_(filter) {}

void JsonEncoder::Encode(ConstPayload* pl, WrSerializer& wrSer) {
	string_view tuple = getPlTuple(pl);
	Serializer rdser(tuple.data(), tuple.length());

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
	bool first = true;
	for (size_t i = 0; i < joinedItemsCount; ++i) {
		encodeJoinedItems(wrSer, ds, i, first);
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
			std::abort();
	}
}

bool JsonEncoder::encodeJoinedItem(WrSerializer& wrSer, ConstPayload& pl) {
	string_view tuple = getPlTuple(&pl);
	Serializer rdser(tuple.data(), tuple.length());

	bool first = true;
	for (int i = 0; i < pl.NumFields(); ++i) fieldsoutcnt_[i] = 0;
	return encodeJson(&pl, rdser, wrSer, first, true);
}

void JsonEncoder::encodeJoinedItems(WrSerializer& wrSer, IJsonEncoderDatasourceWithJoins& ds, size_t rowid, bool& first) {
	const size_t itemsCount = ds.GetJoinedRowItemsCount(rowid);
	if (!itemsCount) return;

	if (!first) {
		wrSer.PutChar(',');
	} else
		first = false;

	string nsTagName("joined." + ds.GetJoinedItemNamespace(rowid));
	wrSer.PrintJsonString(nsTagName);
	wrSer.PutChar(':');
	wrSer.PutChar('[');

	JsonEncoder subEnc(ds.GetJoinedItemTagsMatcher(rowid), ds.GetJoinedItemJsonFilter(rowid));
	for (size_t i = 0; i < itemsCount; ++i) {
		if (i) {
			wrSer.PutChar(',');
		}
		ConstPayload pl(ds.GetJoinedItemPayload(rowid, i));
		subEnc.encodeJoinedItem(wrSer, pl);
	}

	wrSer.PutChar(']');
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
		assert(tagField < pl->NumFields());

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

string_view JsonEncoder::getPlTuple(ConstPayload* pl) {
	KeyRefs kref;
	pl->Get(0, kref);

	p_string tuple(kref[0]);

	if (tuple.size() == 0) {
		tmpPlTuple_ = BuildPayloadTuple(*pl, tagsMatcher_);
		return string_view(*tmpPlTuple_);
	}

	return string_view(tuple);
}

}  // namespace reindexer
