#pragma once

#include "core/cjson/jsonbuilder.h"
#include "core/system_ns_names.h"
#include "reindexer_api.h"

class SelectorPlanTest : public ReindexerApi {
public:
	void SetUp() override {
		Error err = rt.reindexer->OpenNamespace(btreeNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(
			btreeNs,
			{IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kFieldTree1, "tree", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldTree2, "tree", "int", IndexOpts(), 0}, IndexDeclaration{kFieldHash, "hash", "int", IndexOpts(), 0}});
		err = rt.reindexer->OpenNamespace(unbuiltBtreeNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(
			unbuiltBtreeNs,
			{IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kFieldTree1, "tree", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldTree2, "tree", "int", IndexOpts(), 0}, IndexDeclaration{kFieldHash, "hash", "int", IndexOpts(), 0}});
		err = rt.reindexer->InitSystemNamespaces();
		ASSERT_TRUE(err.ok()) << err.what();
		changeNsOptimizationTimeout();
	}

	void FillNs(const char* ns) {
		for (int i = 0; i < kNsSize; ++i) {
			Item item = makeItem(ns, i);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			Upsert(ns, item);
		}
	}

	template <typename T>
	static void AssertJsonFieldEqualTo(const std::string& str, const char* fieldName, std::initializer_list<T> v) {
		const auto values = adoptValuesType(v);
		std::string::size_type pos = findField(str, fieldName, 0);
		size_t i = 0;
		for (auto it = values.begin(); it != values.end(); ++i, ++it) {
			ASSERT_NE(pos, std::string::npos) << str << ": Field '" << fieldName << "' found less then expected (Expected " << values.size()
											  << ')';
			const auto fieldValue = readFieldValue<typename decltype(values)::value_type>(str, pos);
			ASSERT_EQ(*it, fieldValue) << str << ": Field '" << fieldName << "' value number " << i << " missmatch. Expected: '" << *it
									   << "', got '" << fieldValue << '\'';
			pos = findField(str, fieldName, pos + 1);
		}
		ASSERT_EQ(pos, std::string::npos) << str << ": Field '" << fieldName << "' found more then expected (Expected " << values.size()
										  << ')';
	}

	template <typename T>
	static std::vector<T> GetJsonFieldValues(const std::string& str, const char* fieldName) {
		std::vector<T> result;
		std::string::size_type pos = findField(str, fieldName, 0);
		while (pos != std::string::npos) {
			result.push_back(readFieldValue<T>(str, pos));
			pos = findField(str, fieldName, pos + 1);
		}
		return result;
	}

	static void AssertJsonFieldAbsent(const std::string& str, const char* fieldName) {
		const std::string::size_type pos = findField(str, fieldName, 0);
		ASSERT_EQ(pos, std::string::npos) << str << ": Field '" << fieldName << "' found";
	}

	int RandInt() const {
		static_assert(kNsSize > 100, "Division by zero");
		return rand() % (kNsSize / 100);
	}

	const char* const btreeNs = "selector_plan_with_index_sort_optimization_ns";
	const char* const unbuiltBtreeNs = "selector_plan_with_unbuilt_index_sort_optimization_ns";
	const char* const kFieldId = "id";
	const char* const kFieldTree1 = "data_tree_1";
	const char* const kFieldTree2 = "data_tree_2";
	const char* const kFieldHash = "data_hash";
	constexpr static int kNsSize = 1000;

private:
	static std::string::size_type findField(const std::string& str, const char* fieldName, std::string::size_type pos) {
		return str.find('"' + std::string(fieldName) + "\":", pos);
	}

	static std::string::size_type findFieldValueStart(const std::string& str, std::string::size_type pos) {
		assertrx(pos + 1 < str.size());
		pos = str.find("\":", pos + 1);
		assertrx(pos != std::string::npos);
		assertrx(pos + 2 < str.size());
		pos = str.find_first_not_of(" \t\n", pos + 2);
		assertrx(pos != std::string::npos);
		return pos;
	}

	template <typename T>
	static T readFieldValue(const std::string&, std::string::size_type);

	template <typename T>
	static std::vector<T> adoptValuesType(std::initializer_list<T> values) {
		std::vector<T> result;
		result.reserve(values.size());
		for (const T& v : values) {
			result.push_back(v);
		}
		return result;
	}

	static std::vector<std::string> adoptValuesType(std::initializer_list<const char*> values) {
		std::vector<std::string> result;
		result.reserve(values.size());
		for (const char* v : values) {
			result.emplace_back(v);
		}
		return result;
	}

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
		ns1.Put("lazyload", false);
		ns1.Put("unload_idle_threshold", 0);
		ns1.Put("join_cache_mode", "off");
		ns1.Put("start_copy_policy_tx_size", 10000);
		ns1.Put("optimization_timeout_ms", 0);
		ns1.End();

		auto ns2 = nsArray.Object();
		ns2.Put("namespace", btreeNs);
		ns2.Put("log_level", "none");
		ns2.Put("lazyload", false);
		ns2.Put("unload_idle_threshold", 0);
		ns2.Put("join_cache_mode", "off");
		ns2.Put("start_copy_policy_tx_size", 10000);
		ns2.Put("optimization_timeout_ms", 800);
		ns2.End();

		nsArray.End();
		jb.End();

		rt.UpsertJSON(reindexer::kConfigNamespace, ser.Slice());
	}
};

template <>
std::string SelectorPlanTest::readFieldValue<std::string>(const std::string&, std::string::size_type);
template <>
bool SelectorPlanTest::readFieldValue<bool>(const std::string&, std::string::size_type);
template <>
int SelectorPlanTest::readFieldValue<int>(const std::string&, std::string::size_type);
template <>
int64_t SelectorPlanTest::readFieldValue<int64_t>(const std::string&, std::string::size_type);
