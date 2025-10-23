#pragma once

#include "core/cjson/baseencoder.h"
#include "core/cjson/field_extractor_grouping.h"
#include "core/cjson/jsondecoder.h"
#include "reindexer_api.h"

class [[nodiscard]] FieldExtractorEqApi : public ReindexerApi {
public:
	void SetUp() override {}
	void Test(std::string_view json, std::string_view pathString, const std::vector<VariantArray>& resultVals) {
		reindexer::PayloadTypeImpl pti("ns");
		reindexer::PayloadFieldType ft(reindexer::KeyValueType::String{}, "-tuple", {}, reindexer::IsArray_False);
		pti.Add(ft);
		reindexer::PayloadValue value(100);	 // for '-tuple' this is more than enough
		reindexer::Payload pl(pti, value);
		reindexer::TagsMatcher tm;

		reindexer::JsonDecoder decoder(tm);
		reindexer::WrSerializer wrser;
		reindexer::Serializer rdser(json);
		gason::JsonNode node;
		gason::JsonParser parser;
		try {
			node = parser.Parse(std::string_view(json));
			ASSERT_EQ(node.value.getTag(), gason::JsonTag::OBJECT);
		} catch (gason::Exception& e) {
			ASSERT_TRUE(false);
		}
		reindexer::WrSerializer ser;
		reindexer::FloatVectorsHolderVector vectors;
		ser.PutUInt32(0);
		reindexer::Error err = decoder.Decode(pl, ser, node.value, vectors);
		std::unique_ptr<uint8_t[]> tupleData = ser.DetachLStr();
		pl.Set(0, Variant(reindexer::p_string(reinterpret_cast<reindexer::l_string_hdr*>(tupleData.get())), Variant::noHold));

		ASSERT_TRUE(err.ok()) << err.what();
		reindexer::FieldPath path;
		reindexer::equal_position_helpers::ParseStrPath(pathString, path);
		for (auto& v : path) {
			v.tag = tm.name2tag(v.name);
		}
		reindexer::BaseEncoder<reindexer::FieldsExtractorGrouping> encoder(&tm, nullptr);
		const std::span<reindexer::FieldPathPart> p(path);
		reindexer::h_vector<reindexer::VariantArray, 2> vals;
		reindexer::FieldsExtractorGroupingState state;
		unsigned int index = 0;
		state.path = p;
		state.values = &vals;
		state.arrayIndex = &index;
		reindexer::FieldsExtractorGrouping extractor{state};
		reindexer::ConstPayload cpl(pti, value);
		encoder.Encode(cpl, extractor);
		ASSERT_EQ(vals.size(), resultVals.size());
		for (size_t i = 0; i < vals.size(); i++) {
			ASSERT_EQ(vals[i].size(), resultVals[i].size());
			for (size_t j = 0; j < vals[i].size(); j++) {
				if (vals[i][j].Type().IsSame(reindexer::KeyValueType::Null{}) &&
					resultVals[i][j].Type().IsSame(reindexer::KeyValueType::Null{})) {
					continue;
				}
				ASSERT_EQ(vals[i][j], resultVals[i][j]) << "i=" << i << " j=" << j << " vals=" << vals[i][j].As<std::string>()
														<< " resultVals=" << resultVals[i][j].As<std::string>();
			}
		}
	}
};
