#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "core/ft/typos.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "indextext.h"

namespace reindexer {
using std::pair;
using std::unique_ptr;

template <typename T>
class FastIndexText : public IndexText<T> {
public:
	FastIndexText(IndexType _type, const string& _name, const IndexOpts& opts) : IndexText<T>(_type, _name, opts) { CreateConfig(); }
	FastIndexText(const FastIndexText<T>& other) : IndexText<T>(other) { CreateConfig(other.GetConfig()); }

	template <typename U = T>
	FastIndexText(IndexType _type, const string& _name, const IndexOpts& opts, const PayloadType payloadType, const FieldsSet& fields,
				  typename std::enable_if<is_payload_unord_map_key<U>::value>::type* = 0)
		: IndexText<T>(_type, _name, opts, payloadType, fields) {
		CreateConfig();
	}
	Index* Clone() override;
	IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery& dsl) override final;
	void Commit() override final;
	IndexMemStat GetMemStat() override;

protected:
	struct MergeInfo {
		IdType id;
		int proc;
		AreaHolder::UniquePtr holder;
	};

	FtFastConfig* GetConfig() const;
	void CreateConfig(const FtFastConfig* cfg = nullptr);

	typedef int WordIdType;

	struct TextSearchResult {
		const PackedIdRelSet* vids_;
		const char* pattern;
		int proc_;
		int16_t wordLen_;
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

	IdSet::Ptr mergeResults(vector<TextSearchResults>& rawResults, FtCtx::Ptr ctx);
	void mergeItaration(TextSearchResults& rawRes, vector<bool>& exists, vector<MergeInfo>& merged, vector<MergedIdRel>& merged_rd,
						h_vector<int16_t>& idoffsets, bool need_area);

	void debugMergeStep(const char* msg, int vid, float normBm25, float normDist, int finalRank, int prevRank);
	void processVariants(FtSelectContext&);
	void prepareVariants(FtSelectContext&, FtDSLEntry&, std::vector<string>& langs);
	void processTypos(FtSelectContext&, FtDSLEntry&);

	void buildWordsMap(fast_hash_map<string, WordEntry>& m);
	void buildVirtualWord(const string& word, fast_hash_map<string, WordEntry>& words_um, VDocIdType docType, int rfield, size_t insertPos,
						  std::vector<string>& output);

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

Index* FastIndexText_New(IndexType type, const string& _name, const IndexOpts& opts, const PayloadType payloadType,
						 const FieldsSet& fields_);

}  // namespace reindexer
