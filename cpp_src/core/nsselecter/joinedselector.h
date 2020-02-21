#pragma once
#include "core/joincache.h"
#include "core/nsselecter/selectiteratorcontainer.h"

namespace reindexer {

struct JoinPreResult {
	class Values : public vector<ItemRef> {
	public:
		Values(Values &&other)
			: vector<ItemRef>(std::move(other)),
			  payloadType(std::move(other.payloadType)),
			  tagsMatcher(std::move(other.tagsMatcher)),
			  locked_(other.locked_) {
			other.locked_ = false;
		}
		Values() : locked_(false) {}
		Values(const Values &) = delete;
		Values &operator=(const Values &) = delete;
		Values &operator=(Values &&) = delete;
		~Values() {
			if (locked_) {
				for (size_t i = 0; i < size(); ++i) Payload{payloadType, (*this)[i].Value()}.ReleaseStrings();
			}
		}
		bool Locked() const { return locked_; }
		void Lock() {
			assert(!locked_);
			for (size_t i = 0; i < size(); ++i) Payload{payloadType, (*this)[i].Value()}.AddRefStrings();
			locked_ = true;
		}

		PayloadType payloadType;
		TagsMatcher tagsMatcher;

	private:
		bool locked_ = false;
	};

	typedef shared_ptr<JoinPreResult> Ptr;
	IdSet ids;
	SelectIteratorContainer iterators;
	Values values;
	enum { ModeEmpty, ModeBuild, ModeExecute } executionMode = ModeEmpty;
	enum { ModeIterators, ModeIdSet, ModeValues } dataMode = ModeIterators;
	bool enableSortOrders = false;
	bool btreeIndexOptimizationEnabled = true;
	bool enableStoredValues = false;
};

struct SortExpressionJoinedIndex;
class NsSelecter;

class JoinedSelector {
	friend SortExpressionJoinedIndex;
	friend NsSelecter;

public:
	JoinedSelector(JoinType joinType, std::shared_ptr<NamespaceImpl> leftNs, std::shared_ptr<NamespaceImpl> rightNs, JoinCacheRes &&joinRes,
				   Query &&itemQuery, QueryResults &result, const JoinedQuery &joinQuery, JoinPreResult::Ptr preResult,
				   size_t joinedFieldIdx, SelectFunctionsHolder &selectFunctions, int joinedSelectorsCount, const RdxContext &rdxCtx)
		: joinType_(joinType),
		  called_(0),
		  matched_(0),
		  leftNs_(std::move(leftNs)),
		  rightNs_(std::move(rightNs)),
		  joinRes_(std::move(joinRes)),
		  itemQuery_(std::move(itemQuery)),
		  result_(result),
		  joinQuery_(joinQuery),
		  preResult_(preResult),
		  joinedFieldIdx_(joinedFieldIdx),
		  selectFunctions_(selectFunctions),
		  joinedSelectorsCount_(joinedSelectorsCount),
		  rdxCtx_(rdxCtx),
		  optimized_(false) {}

	JoinedSelector(JoinedSelector &&) = default;
	JoinedSelector &operator=(JoinedSelector &&) = delete;
	JoinedSelector(const JoinedSelector &) = delete;
	JoinedSelector &operator=(const JoinedSelector &) = delete;

	bool Process(IdType, int nsId, ConstPayload, bool match);
	JoinType Type() const { return joinType_; }
	const string &RightNsName() const { return itemQuery_._namespace; }
	int Called() const { return called_; }
	int Matched() const { return matched_; }
	void AppendSelectIteratorOfJoinIndexData(SelectIteratorContainer &, int *maxIterations, unsigned sortId, SelectFunction::Ptr,
											 const RdxContext &);
	static constexpr int MaxIterationsForPreResultStoreValuesOptimization() { return 200; }

private:
	template <bool byJsonPath>
	void readValuesFromRightNs(VariantArray &values, const Index &leftIndex, int rightIdxNo, const std::string &rightIndex) const;
	template <bool byJsonPath>
	void readValuesFromPreResult(VariantArray &values, const Index &leftIndex, int rightIdxNo, const std::string &rightIndex) const;
	void selectFromRightNs(QueryResults &joinItemR, bool &found, bool &matchedAtLeastOnce);
	void selectFromPreResultValues(QueryResults &joinItemR, bool &found, bool &matchedAtLeastOnce) const;

	JoinType joinType_;
	int called_, matched_;
	std::shared_ptr<NamespaceImpl> leftNs_;
	std::shared_ptr<NamespaceImpl> rightNs_;
	JoinCacheRes joinRes_;
	Query itemQuery_;
	QueryResults &result_;
	const JoinedQuery &joinQuery_;
	JoinPreResult::Ptr preResult_;
	size_t joinedFieldIdx_;
	SelectFunctionsHolder &selectFunctions_;
	int joinedSelectorsCount_;
	const RdxContext &rdxCtx_;
	bool optimized_;
};

}  // namespace reindexer
