#include "check_string_refs.h"

#if REINDEX_WITH_STRHOLDER_CHECKS

#include <atomic>
#include <cstdint>
#include <tuple>
#include "core/id_type.h"
#include "core/index/index.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"
#include "estl/fast_hash_map.h"
#include "estl/sparse_hash_int.h"
#include "tools/errors.h"
#include "tools/unaligned.h"

namespace reindexer::debug {
namespace {
std::atomic<int32_t> g_stringRefsChecksDisabled{0};
}

CounterGuardAIR32 DisableStringRefsChecks() noexcept { return CounterGuardAIR32{g_stringRefsChecksDisabled}; }

void CheckStringRefs(std::span<const PayloadValue> items, std::span<const std::unique_ptr<Index>> indexes, const PayloadType& pt) {
	if (g_stringRefsChecksDisabled.load(std::memory_order_relaxed) > 0) {
		return;
	}

	struct [[nodiscard]] StringBufPtrHash {
		size_t operator()(const key_string_impl* p) const noexcept { return hash_int<uint64_t>{}(uint64_t(uintptr_t(p))); }
	};
	struct [[nodiscard]] IdCheck {
		const void* ids{nullptr};
		Index::OwnedStringPtrCollector::ContainsIdFn containsId{nullptr};
	};
	using OwnedPtrMap = fast_hash_map<const key_string_impl*, IdCheck, StringBufPtrHash>;

	class [[nodiscard]] PtrCollector : public Index::OwnedStringPtrCollector {
	public:
		explicit PtrCollector(OwnedPtrMap& dst) noexcept : dst_{dst} {}
		void Add(const key_string_impl* impl) override { std::ignore = dst_.try_emplace(impl, IdCheck{}); }
		void Add(const key_string_impl* impl, const void* ids, ContainsIdFn containsId) override {
			std::ignore = dst_.insert_or_assign(impl, IdCheck{ids, containsId});
		}

	private:
		OwnedPtrMap& dst_;
	};

	for (int field : pt.StrFields()) {
		if (const auto* column = static_cast<const std::string_view*>(indexes[field]->ColumnData()); column) {
			for (size_t rowId = 0; rowId < items.size(); ++rowId) {
				auto& item = items[rowId];
				if (item.IsFree()) {
					continue;
				}
				ConstPayload pl(pt, item);
				const auto ps = unaligned::read<p_string>(pl.Field(field).p_);
				assertf(ps.type() == p_string::tagKeyString, "String for index '{}' is not a key string. Item id: {}; array index: {}",
						indexes[field]->Name(), rowId, 0);
				const auto* impl = ps.getBaseKeyString();
				const char* data = impl ? impl->data() : nullptr;
				assertf(data == column[rowId].data(), "String does not belong to index '{}'. Item id: {}", indexes[field]->Name(), rowId);
			}
			continue;
		}

		OwnedPtrMap ownedPtrs;
		{
			PtrCollector collector{ownedPtrs};
			indexes[field]->CollectOwnedStringPtrs(collector);
		}
		size_t valueCount = 0;
		for (size_t rowId = 0; rowId < items.size(); ++rowId) {
			auto& item = items[rowId];
			if (item.IsFree()) {
				continue;
			}
			ConstPayload pl(pt, item);
			const auto cells = pl.GetView<p_string>(field);
			valueCount += cells.size();
			const auto id = IdType::FromNumber(rowId);
			size_t i = 0;
			for (const auto ps : cells) {
				assertf(ps.type() == p_string::tagKeyString, "String for index '{}' is not a key string. Item id: {}; array index: {}",
						indexes[field]->Name(), rowId, i);
				bool ok = false;
				const auto it = ownedPtrs.find(ps.getBaseKeyString());
				if (it != ownedPtrs.end()) {
					const auto& chk = it->second;
					ok = !chk.containsId || chk.containsId(chk.ids, id);
				}
				if (!ok) {
					assertf(false, "String does not belong to index '{}'. Item id: {}; array index: {}", indexes[field]->Name(), rowId, i);
				}
				++i;
			}
		}
		assertf(ownedPtrs.empty() == (valueCount == 0), "Index '{}': {} interned strings vs {} payload values", indexes[field]->Name(),
				ownedPtrs.size(), valueCount);
	}
}

}  // namespace reindexer::debug

#endif	// REINDEX_WITH_STRHOLDER_CHECKS
