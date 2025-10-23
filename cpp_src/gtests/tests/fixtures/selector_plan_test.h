#pragma once

#include "core/cjson/jsonbuilder.h"
#include "core/system_ns_names.h"
#include "reindexer_api.h"

class [[nodiscard]] SelectorPlanTest : public ReindexerApi {
public:
	void SetUp() override {
		ReindexerApi::SetUp();

		rt.OpenNamespace(btreeNs);
		DefineNamespaceDataset(
			btreeNs,
			{IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kFieldTree1, "tree", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldTree2, "tree", "int", IndexOpts(), 0}, IndexDeclaration{kFieldHash, "hash", "int", IndexOpts(), 0}});
		rt.OpenNamespace(unbuiltBtreeNs);
		DefineNamespaceDataset(
			unbuiltBtreeNs,
			{IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kFieldTree1, "tree", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldTree2, "tree", "int", IndexOpts(), 0}, IndexDeclaration{kFieldHash, "hash", "int", IndexOpts(), 0}});
		changeNsOptimizationTimeout();
	}

	void FillNs(const char* ns) {
		for (int i = 0; i < kNsSize; ++i) {
			Item item = makeItem(ns, i);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			Upsert(ns, item);
		}
	}

	static int RandInt() noexcept {
		// Using 49 here for more predictable selection plan
		return rand() % 49;
	}

	const char* const btreeNs = "selector_plan_with_index_sort_optimization_ns";
	const char* const unbuiltBtreeNs = "selector_plan_with_unbuilt_index_sort_optimization_ns";
	const char* const kFieldId = "id";
	const char* const kFieldTree1 = "data_tree_1";
	const char* const kFieldTree2 = "data_tree_2";
	const char* const kFieldHash = "data_hash";
	constexpr static int kNsSize = 2000;

private:
	Item makeItem(const char* ns, int id) {
		Item item = NewItem(ns);
		if (item.Status().ok()) {
			item[kFieldId] = id;
			item[kFieldTree1] = RandInt();
			item[kFieldTree2] = RandInt();
			item[kFieldHash] = RandInt();
		}
		return item;
	}

	void changeNsOptimizationTimeout() {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);

		jb.Put("type", "namespaces");
		auto nsArray = jb.Array("namespaces");
		auto ns1 = nsArray.Object();

		ns1.Put("namespace", unbuiltBtreeNs);
		ns1.Put("log_level", "none");
		ns1.Put("join_cache_mode", "off");
		ns1.Put("start_copy_policy_tx_size", 10000);
		ns1.Put("optimization_timeout_ms", 0);
		ns1.End();

		auto ns2 = nsArray.Object();
		ns2.Put("namespace", btreeNs);
		ns2.Put("log_level", "none");
		ns2.Put("join_cache_mode", "off");
		ns2.Put("start_copy_policy_tx_size", 10000);
		ns2.Put("optimization_timeout_ms", 800);
		ns2.End();

		nsArray.End();
		jb.End();

		rt.UpsertJSON(reindexer::kConfigNamespace, ser.Slice());
	}
};
