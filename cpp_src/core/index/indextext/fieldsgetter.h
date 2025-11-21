#pragma once
#include "core/keyvalue/key_string.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadiface.h"
#include "vendor/utf8cpp/utf8/core.h"

namespace reindexer {

class [[nodiscard]] FieldsGetter {
public:
	FieldsGetter(const FieldsSet& fields, const PayloadType& plt, KeyValueType type) : fields_(fields), plt_(plt), type_(type) {}

	h_vector<std::pair<std::string_view, uint32_t>, 8> getDocFields(const key_string& doc, std::vector<std::unique_ptr<std::string>>&) {
		if (!utf8::is_valid(doc.cbegin(), doc.cend())) {
			throw Error(errParams, "Invalid UTF8 string in FullText index");
		}

		return {{std::string_view(doc), 0}};
	}

	VariantArray krefs;

	// Specific implementation for composite index
	h_vector<std::pair<std::string_view, uint32_t>, 8> getDocFields(const PayloadValue& doc,
																	std::vector<std::unique_ptr<std::string>>& strsBuf) {
		ConstPayload pl(plt_, doc);

		uint32_t fieldPos = 0;
		size_t tagsPathIdx = 0;

		h_vector<std::pair<std::string_view, uint32_t>, 8> ret;

		for (auto field : fields_) {
			krefs.clear<false>();
			bool fieldFromCjson = (field == IndexValueType::SetByJsonPath);
			if (fieldFromCjson) {
				assertrx(tagsPathIdx < fields_.getTagsPathsLength());
				pl.GetByJsonPath(fields_.getTagsPath(tagsPathIdx++), krefs, type_);
			} else {
				pl.Get(field, krefs);
			}
			for (const Variant& kref : krefs) {
				if (!kref.Type().Is<KeyValueType::String>()) {
					auto& str = strsBuf.emplace_back(std::make_unique<std::string>(kref.As<std::string>()));
					ret.emplace_back(*str, fieldPos);
				} else {
					const std::string_view stringRef(kref);
					if (!utf8::is_valid(stringRef.data(), stringRef.data() + stringRef.size())) [[likely]] {
						throw Error(errParams, "Invalid UTF8 string in FullText index");
					}
					ret.emplace_back(stringRef, fieldPos);
				}
			}
			++fieldPos;
		}
		return ret;
	}

private:
	const FieldsSet& fields_;
	const PayloadType& plt_;

	KeyValueType type_;
};
}  // namespace reindexer
