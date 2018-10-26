#pragma once
#include "core/index/payload_map.h"
#include "core/payload/fieldsset.h"
#include "vendor/utf8cpp/utf8.h"

namespace reindexer {

template <typename T>
class FieldsGetter {
public:
	FieldsGetter(const FieldsSet &fields, const PayloadType &plt, KeyValueType type) : fields_(fields), plt_(plt), type_(type) {}

	template <typename U = T>
	h_vector<pair<string_view, uint32_t>, 8> getDocFields(const typename T::key_type &doc, vector<unique_ptr<string>> &,
														  typename std::enable_if<!is_payload_unord_map_key<U>::value>::type * = nullptr) {
		if (!utf8::is_valid(doc->cbegin(), doc->cend())) throw Error(errParams, "Invalid UTF8 string in FullText index");

		return {{string_view(*doc.get()), 0}};
	}

	// Specific implemetation for composite index

	template <typename U = T>
	h_vector<pair<string_view, uint32_t>, 8> getDocFields(const typename T::key_type &doc, vector<unique_ptr<string>> &strsBuf,
														  typename std::enable_if<is_payload_unord_map_key<U>::value>::type * = nullptr) {
		ConstPayload pl(plt_, doc);

		uint32_t fieldPos = 0;
		size_t tagsPathIdx = 0;
		h_vector<pair<string_view, uint32_t>, 8> ret;

		for (auto field : fields_) {
			VariantArray krefs;
			bool fieldFromCjson = (field == IndexValueType::SetByJsonPath);
			if (fieldFromCjson) {
				assert(tagsPathIdx < fields_.getTagsPathsLength());
				pl.GetByJsonPath(fields_.getTagsPath(tagsPathIdx++), krefs, type_);
			} else {
				pl.Get(field, krefs);
			}
			for (auto kref : krefs) {
				if (kref.Type() != KeyValueString) {
					strsBuf.emplace_back(unique_ptr<string>(new string(kref.As<string>())));
					ret.push_back({*strsBuf.back().get(), fieldPos});
				} else {
					p_string pstr(kref);
					const string_view stringRef(pstr.data(), pstr.length());
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
