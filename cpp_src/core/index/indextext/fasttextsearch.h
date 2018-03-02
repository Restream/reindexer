#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "indextext.tcc"
namespace reindexer {
using std::pair;
using std::unique_ptr;

template <typename T>
class FastIndexText : public IndexText<T> {
public:
	FastIndexText(IndexType _type, const string& _name) : IndexText<T>(_type, _name) { CreateConfig(); }
	FastIndexText(const FastIndexText<T>& other) : IndexText<T>(other) { CreateConfig(other.GetConfig()); }

	template <typename U = T>
	FastIndexText(IndexType _type, const string& _name, const IndexOpts& opts, const PayloadType payloadType, const FieldsSet& fields,
				  typename std::enable_if<is_payload_unord_map_key<U>::value>::type* = 0)
		: IndexText<T>(_type, _name, opts, payloadType, fields) {
		CreateConfig();
	}
	Index* Clone() override;
	IdSet::Ptr Select(FullTextCtx::Ptr fctx, FtDSLQuery& dsl) override final;
	void Commit() override final;

protected:
	FtFastConfig* GetConfig() const;
	void CreateConfig(const FtFastConfig* cfg = nullptr);

	typedef int WordIdType;

	struct TextSearchResult {
		const PackedIdRelSet* vids_;
		int proc_;
	};

	class TextSearchResults : public h_vector<TextSearchResult, 8> {
	public:
		int idsCnt_ = 0;
		FtDSLEntry term;
	};

	struct FtVariantEntry {
		string pattern;
		FtDslOpts opts;
		int proc;
	};

	struct FtSelectContext {
		vector<FtVariantEntry> variants;
		fast_hash_map<WordIdType, pair<size_t, size_t>> foundWords;
		vector<TextSearchResults> rawResults;
	};

	struct MergedIdRel {
		IdRelType cur;
		IdRelType next;
		int rank;
		int qpos;
	};

	IdSet::Ptr mergeResults(vector<TextSearchResults>& rawResults, FullTextCtx::Ptr ctx);
	void mergeItaration(TextSearchResults& rawRes, vector<bool>& exists, vector<pair<IdType, int>>& merged, vector<MergedIdRel>& merged_rd,
						h_vector<int16_t>& idoffsets);

	void processVariants(FtSelectContext&);
	void prepareVariants(FtSelectContext&, FtDSLEntry&, std::vector<string>& langs);
	void processTypos(FtSelectContext&, FtDSLEntry&);

	void buildWordsMap(fast_hash_map<string, WordEntry>& m);
	h_vector<pair<const string*, int>, 8> getDocFields(const typename T::key_type&, vector<unique_ptr<string>>& bufStrs);

	void buildTyposMap();
	void initSearchers();

	// Key Entries corresponding to words. Addresable by WordIdType
	vector<PackedWordEntry> words_;
	// Typos map. typo string <-> original word id
	flat_str_multimap<string, WordIdType> typos_;
	// Suffix map. suffix <-> original word id
	suffix_map<string, WordIdType> suffixes_;
	// Virtual documents, merged. Addresable by VDocIdType
	vector<double> avgWordsCount_;
};

}  // namespace reindexer
