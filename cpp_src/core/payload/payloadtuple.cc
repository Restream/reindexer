#include "payloadtuple.h"
#include "core/cjson/ctag.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadiface.h"
#include "tools/serializer.h"

namespace reindexer {
key_string BuildPayloadTuple(ConstPayload &pl, const TagsMatcher &tagsMatcher) {
	WrSerializer wrser;
	wrser.PutVarUint(ctag(TAG_OBJECT, 0));

	for (int idx = 1; idx < pl.NumFields(); ++idx) {
		KeyRefs keyRefs;
		pl.Get(idx, keyRefs);

		string fieldName = pl.Type().Field(idx).Name();
		int tagName = tagsMatcher.name2tag(fieldName.c_str());
		if (!tagName) {
			tagName = const_cast<TagsMatcher &>(tagsMatcher).name2tag(fieldName.c_str(), true);
		}
		assert(tagName);

		int field = idx;
		if (pl.Type().Field(field).IsArray()) {
			wrser.PutVarUint(ctag(TAG_ARRAY, tagName, field));
			wrser.PutVarUint(keyRefs.size());
		} else {
			for (const KeyRef &keyRef : keyRefs) {
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
	return make_key_string(reinterpret_cast<const char *>(wrser.Buf()), wrser.Len());
}

}  // namespace reindexer
