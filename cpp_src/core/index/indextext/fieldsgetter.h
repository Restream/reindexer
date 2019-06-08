#pragma once
#include "core/index/payload_map.h"
#include "core/payload/fieldsset.h"
#include "vendor/utf8cpp/utf8.h"

namespace reindexer {

class FieldsGetter {
public:
	FieldsGetter(const FieldsSet &fields, const PayloadType &plt, KeyValueType type) : fields_(fields), plt_(plt), type_(type) {}

	h_vector<pair<string_view, uint32_t>, 8> getDocFields(const key_string &doc, vector<std::unique_ptr<string>> &) {
		if (!utf8::is_valid(doc->cbegin(), doc->cend())) throw Error(errParams, "Invalid UTF8 string in FullText index");

		return {{string_view(*doc.get()), 0}};
	}

	VariantArray krefs;

	// Specific implemetation for composite index
	h_vector<pair<string_view, uint32_t>, 8> getDocFields(const PayloadValue &doc, vector<std::unique_ptr<string>> &strsBuf) {
		ConstPayload pl(plt_, doc);

		uint32_t fieldPos = 0;
		size_t tagsPathIdx = 0;
		h_vector<pair<string_view, uint32_t>, 8> ret;

		for (auto field : fields_) {
			krefs.resize(0);
			bool fieldFromCjson = (field == IndexValueType::SetByJsonPath);
			if (fieldFromCjson) {
				assert(tagsPathIdx < fields_.getTagsPathsLength());
				pl.GetByJsonPath(fields_.getTagsPath(tagsPathIdx++), krefs, type_);
			} else {
				pl.Get(field, krefs);
			}
			for (Variant kref : krefs) {
				if (kref.Type() != KeyValueString) {
					strsBuf.emplace_back(std::unique_ptr<string>(new string(kref.As<string>())));
					ret.push_back({*strsBuf.back().get(), fieldPos});
				} else {
					const string_view stringRef(kref);
					if (!utf8::is_valid(stringRef.data(), stringRef.data() + stringRef.size()))
						throw Error(errParams, "Invalid UTF8 string in FullTextindex");
					ret.push_back({stringRef, fieldPos});
				}
			}
			fieldPos++;
		}
		return ret;
	}

private:
	const FieldsSet &fields_;
	const PayloadType &plt_;

	KeyValueType type_;
};
}  // namespace reindexer
