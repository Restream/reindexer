#pragma once

#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "tools/logger.h"

namespace reindexer {

namespace composite_substitution_helpers {

class [[nodiscard]] CompositeValuesCountLimits {
public:
	uint32_t operator[](uint32_t fieldsCount) const noexcept {
		if (fieldsCount >= limits_.size()) [[unlikely]] {
			return kMaxValuesCount;
		}
		return limits_[fieldsCount];
	}

private:
	constexpr static uint32_t kMaxValuesCount = 4000;

	std::array<uint32_t, 6> limits_ = {0, 0, 300, 1000, 2000, 4000};
};

class [[nodiscard]] CompositeSearcher {
public:
	struct [[nodiscard]] IndexData {
		IndexData(int field, int _idx, uint16_t entry) : fields(field), idx(_idx), entries{entry} {}

		IndexesFieldsSet fields;
		int idx;
		h_vector<uint16_t, 8> entries;
	};

	CompositeSearcher(const NamespaceImpl& ns) noexcept : ns_(ns) {}

	void Add(int field, const std::vector<int>& composites, unsigned entry) {
		assertrx_throw(entry < std::numeric_limits<uint16_t>::max());
		const auto compositesBeg = ns_.indexes_.firstCompositePos();
		const auto compositesEnd = compositesBeg + ns_.indexes_.compositeIndexesSize();
		for (auto composite : composites) {
			if (composite < compositesBeg || composite >= compositesEnd) [[unlikely]] {
				// TODO: this may be removed later (somewhere around v3.31/v3.32) after some extra investigations (relates to #1830)
				logFmt(LogError,
					   "<assertion failed>: Unexpected composite index identifier during substitution attempt: {}. Composites range is "
					   "[{}, {});\n(field: {}; {})",
					   composite, compositesBeg, compositesEnd, field, ns_.payloadType_.Field(field).ToString());
				assertrx_dbg(false);
				continue;
			}
			auto compositePtr = ns_.indexes_[composite].get();
			const auto idxType = compositePtr->Type();
			if (idxType != IndexCompositeBTree && idxType != IndexCompositeHash) {
				continue;
			}
			if (auto& idxFields = compositePtr->Fields(); !idxFields.contains(field)) {
				// TODO: this may be removed later (somewhere around v3.31/v3.32) after some extra investigations (relates to #1830)
				logFmt(LogError,
					   "<assertion failed>: Unexpected field {} in composite index {}:{} during substitution attempt. Actual composite "
					   "fields: {}",
					   field, composite, compositePtr->Name(), idxFields.ToString(DumpWithMask_False));
				assertrx_dbg(false);
				continue;
			}
			bool found = false;
			for (auto& d : d_) {
				if (d.idx == composite) {
					d.fields.push_back(field);
					d.entries.push_back(entry);
					found = true;
					break;
				}
			}
			if (!found) {
				d_.emplace_back(field, composite, entry);
			}
		}
	}
	int GetResult() {
		int res = -1;
		unsigned maxSize = 0;
		for (int i = 0; i < int(d_.size()); ++i) {
			auto& data = d_[i];
			const auto& idxFields = ns_.indexes_[data.idx]->Fields();
			// If all of the composite fields were found in query
			const auto dfCnt = data.fields.count();
			if (dfCnt == idxFields.size() && idxFields.contains(data.fields)) {
				if (dfCnt > maxSize) {
					maxSize = dfCnt;
					res = i;
				}
			} else {
				remove(i--);
			}
		}
		return res;
	}
	int RemoveUnusedAndGetNext(uint16_t curId) noexcept {
		if (unsigned(curId) + 1 != d_.size()) {
			std::swap(d_[curId], d_.back());
		}
		d_.pop_back();
		return GetResult();
	}
	int RemoveUsedAndGetNext(uint16_t curId) noexcept {
		int res = -1;
		unsigned deleted = 1;
		unsigned maxSize = 0;
		if (unsigned(curId) + 1 != d_.size()) {
			std::swap(d_[curId], d_.back());
		}
		const auto& cur = d_.back();
		for (unsigned i = 0, sz = d_.size(); i < sz - deleted; ++i) {
			auto& data = d_[i];
			if (haveIntersection(data.entries, cur.entries)) {
				std::swap(data, d_[sz - ++deleted]);
				--i;
			} else {
				const auto dfCnt = data.fields.count();
				if (dfCnt > maxSize) {
					res = i;
					maxSize = dfCnt;
				}
			}
		}
		while (deleted--) {
			d_.pop_back();
		}
		return res;
	}
	const IndexData& operator[](uint16_t i) const noexcept { return d_[i]; }

private:
	void remove(uint16_t i) noexcept {
		if (unsigned(i) + 1 != d_.size()) {
			std::swap(d_[i], d_.back());
		}
		d_.pop_back();
	}
	static bool haveIntersection(const h_vector<uint16_t, 8>& lEntries, const h_vector<uint16_t, 8>& rEntries) noexcept {
		for (auto lit = lEntries.begin(), rit = rEntries.begin(); lit != lEntries.end() && rit != rEntries.end();) {
			if (*lit < *rit) {
				++lit;
			} else if (*rit < *lit) {
				++rit;
			} else {
				return true;
			}
		}
		return false;
	}

	h_vector<IndexData, 6> d_;
	const NamespaceImpl& ns_;
};

// EntriesRange - query entries range. [from; to)
class [[nodiscard]] EntriesRange {
public:
	EntriesRange(uint16_t from, uint16_t to) : from_(from), to_(to) {
		if (to_ <= from_) {
			throw Error(errLogic, "Unexpected range boarders during indexes substitution: [{},{})", from_, to_);
		}
	}
	uint16_t From() const noexcept { return from_; }
	uint16_t To() const noexcept { return to_; }
	void ExtendRight() noexcept { ++to_; }
	void ExtendLeft() {
		if (!from_) {
			throw Error(errLogic, "Unable to extend left range's bound during indexes substitution: [{},{})", from_, to_);
		}
		--from_;
	}
	bool Append(const EntriesRange& r) noexcept {
		if (to_ == r.from_) {
			to_ = r.to_;
			return true;
		}
		return false;
	}
	uint16_t Size() const noexcept { return to_ - from_; }

private:
	uint16_t from_;
	uint16_t to_;
};

// EntriesRanges - contains ordered vector of entries ranges. Ranges can not intercept with each other
class [[nodiscard]] EntriesRanges : h_vector<EntriesRange, 8> {
public:
	using Base = h_vector<EntriesRange, 8>;

	Base::const_reverse_iterator rbegin() const noexcept { return Base::rbegin(); }
	Base::const_reverse_iterator rend() const noexcept { return Base::rend(); }

	void Add(std::span<const uint16_t> entries) {
		for (auto entry : entries) {
			auto insertionPos = Base::end();
			bool wasMerged = false;
			for (auto it = Base::begin(); it != Base::end(); ++it) {
				if (entry > it->To()) {
					// Insertion point is further
					continue;
				} else if (entry < it->From()) {
					// Insertion point is between current and previous ranges
					if (entry + 1 == it->From()) {
						wasMerged = true;
						it->ExtendLeft();
						// Merge current and previous range if possible
						if (it != Base::begin()) {
							auto prev = it - 1;
							if (prev->Append(*it)) {
								Base::erase(it);
							}
						}
					} else {
						insertionPos = it;
					}
					break;
				} else if (entry == it->To()) {
					// Current range can be extended
					wasMerged = true;
					it->ExtendRight();
					auto next = it + 1;
					// Merge current and next range if possible
					if (next != Base::end()) {
						if (it->Append(*next)) {
							Base::erase(next);
						}
					}
					break;
				} else {
					// Range already contains required value
					wasMerged = true;
					break;
				}
			}
			if (!wasMerged) {
				Base::insert(insertionPos, EntriesRange{entry, uint16_t(entry + 1)});
			}
		}
	}
};

}  // namespace composite_substitution_helpers
}  // namespace reindexer
