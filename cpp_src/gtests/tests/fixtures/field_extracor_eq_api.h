#pragma once

#include "core/cjson/baseencoder.h"
#include "core/cjson/field_extractor_grouping.h"
#include "core/cjson/jsondecoder.h"
#include "reindexer_api.h"
#include "tools/serilize/serializer.h"

#include "core/cjson/jsonbuilder.h"

class [[nodiscard]] FieldExtractorEqApi : public ReindexerApi {
public:
	void SetUp() override {
		ReindexerApi::SetUp();
		rt.OpenNamespace(default_namespace);

		DefineNamespaceDataset(default_namespace, {
													  IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},

												  });
	}
	void Test(std::string_view json, std::string_view pathString, const std::vector<VariantArray>& resultVals) {
		reindexer::PayloadTypeImpl pti("ns");
		reindexer::PayloadFieldType ft(reindexer::KeyValueType::String{}, "-tuple", {}, reindexer::IsArray_False);
		pti.Add(ft);
		reindexer::PayloadValue value(100);	 // for '-tuple' this is more than enough
		reindexer::Payload pl(pti, value);
		reindexer::TagsMatcher tm;

		reindexer::FloatVectorsHolderVector vectors;
		reindexer::ScalarIndexesSetT objectScalarIndexes;
		reindexer::JsonDecoder decoder(tm, vectors, objectScalarIndexes);
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
		ser.PutUInt32(0);
		reindexer::Error err = decoder.Decode(pl, ser, node.value);
		auto tupleData = ser.DetachLStr();
		pl.Set(0, Variant(reindexer::p_string(reinterpret_cast<const reindexer::l_string_hdr*>(tupleData.Get())), Variant::noHold));

		ASSERT_TRUE(err.ok()) << err.what();
		reindexer::FieldPath path;
		reindexer::equal_position_helpers::ParseStrPath(pathString, path);
		for (auto& v : path) {
			v.tag = tm.name2tag(v.name);
		}
		reindexer::BaseEncoder<reindexer::FieldsExtractorGrouping> encoder(&tm, nullptr);
		const std::span<reindexer::FieldPathPart> p(path);
		unsigned int index = 0;
		reindexer::FieldEqPosCacheImpl cache;
		cache.Clear(1);
		reindexer::FieldEqPosCache vals{cache, 0};
		reindexer::FieldsExtractorGroupingState state{vals, index, p};
		reindexer::FieldsExtractorGrouping extractor{state};
		reindexer::ConstPayload cpl(pti, value);
		encoder.Encode(cpl, extractor);
		ASSERT_EQ(cache.LevelSize(0), resultVals.size());
		for (size_t i = 0; i < cache.LevelSize(0); i++) {
			ASSERT_EQ(cache.ValuesSize(0, i), resultVals[i].size()) << "index=" << i;
			for (size_t j = 0; j < cache.ValuesSize(0, i); j++) {
				if (cache.Value(0, i, j).Type().IsSame(reindexer::KeyValueType::Null{}) &&
					resultVals[i][j].Type().IsSame(reindexer::KeyValueType::Null{})) {
					continue;
				}
				ASSERT_EQ(cache.Value(0, i, j), resultVals[i][j])
					<< "i=" << i << " j=" << j << " vals=" << cache.Value(0, i, j).As<std::string>()
					<< " resultVals=" << resultVals[i][j].As<std::string>();
			}
		}
	}
	Variant i64v(int v) { return Variant(int64_t(v)); }
};
