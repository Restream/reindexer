#pragma once

#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"

namespace reindexer {

namespace composite_substitution_helpers {

class CompositeSearcher {
public:
	struct IndexData {
		IndexData(int field, int _idx, unsigned entry) : fields{field}, idx{_idx}, entries{entry} {}

		IndexesFieldsSet fields;
		int idx;
		h_vector<unsigned, 6> entries;
	};

	CompositeSearcher(const NamespaceImpl &ns) noexcept : ns_(ns) {}

	void Add(int field, const std::vector<int> &composites, unsigned entry) {
		for (auto composite : composites) {
			const auto idxType = ns_.indexes_[composite]->Type();
			if (idxType != IndexCompositeBTree && idxType != IndexCompositeHash) {
				continue;
			}
			bool found = false;
			for (auto &d : d_) {
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
			auto &data = d_[i];
			const auto &idxFields = ns_.indexes_[data.idx]->Fields();
			// If all of the composite fields were found in query
			if (data.fields.size() == idxFields.size() && idxFields.contains(data.fields)) {
				if (data.fields.size() > maxSize) {
					maxSize = data.fields.size();
					res = i;
				}
			} else {
				remove(i--);
			}
		}
		return res;
	}
	int RemoveAndGetNext(unsigned curId) noexcept {
		int res = -1;
		unsigned deleted = 1;
		unsigned maxSize = 0;
		if (curId + 1 != d_.size()) {
			std::swap(d_[curId], d_.back());
		}
		const auto &cur = d_.back();
		for (unsigned i = 0; i < d_.size() - deleted; ++i) {
			auto &data = d_[i];
			if (haveIntersection(data.entries, cur.entries)) {
				std::swap(data, d_[d_.size() - ++deleted]);
				--i;
			} else if (data.fields.size() > maxSize) {
				res = i;
				maxSize = data.fields.size();
			}
		}
		while (deleted--) {
			d_.pop_back();
		}
		return res;
	}
	const IndexData &operator[](unsigned i) const noexcept { return d_[i]; }

private:
	void remove(unsigned i) noexcept {
		if (i + 1 != d_.size()) {
			std::swap(d_[i], d_.back());
		}
		d_.pop_back();
	}
	static bool haveIntersection(const h_vector<unsigned, 6> &lEntries, const h_vector<unsigned, 6> &rEntries) noexcept {
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

	h_vector<IndexData, 8> d_;
	const NamespaceImpl &ns_;
};

// EntriesRange - query entries range. [from; to)
class EntriesRange {
public:
	EntriesRange(unsigned from, unsigned to) : from_(from), to_(to) {
		if (to_ <= from_) {
			throw Error(errLogic, "Unexpected range boarders during indexes substitution: [%u,%u)", from_, to_);
		}
	}
	unsigned From() const noexcept { return from_; }
	unsigned To() const noexcept { return to_; }
	void ExtendRight() noexcept { ++to_; }
	void ExtendLeft() {
		if (!from_) {
			throw Error(errLogic, "Unable to extend left range's bound during indexes substitution: [%u,%u)", from_, to_);
		}
		--from_;
	}
	bool Append(const EntriesRange &r) noexcept {
		if (to_ == r.from_) {
			to_ = r.to_;
			return true;
		}
		return false;
	}
	unsigned Size() const noexcept { return to_ - from_; }

private:
	unsigned from_;
	unsigned to_;
};

// EntriesRanges - contains ordered vector of entries ranges. Ranges can not intercept with each other
class EntriesRanges : h_vector<EntriesRange, 8> {
public:
	using Base = h_vector<EntriesRange, 8>;

	Base::const_reverse_iterator rbegin() const noexcept { return Base::rbegin(); }
	Base::const_reverse_iterator rend() const noexcept { return Base::rend(); }

	void Add(span<unsigned> entries) {
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
				Base::insert(insertionPos, EntriesRange{entry, entry + 1});
			}
		}
	}
};

}  // namespace composite_substitution_helpers
}  // namespace reindexer
