#pragma once
#include <gtest/gtest.h>
#include <chrono>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include "core/indexdef.h"
#include "core/indexopts.h"
#include "core/query/query.h"
#include "gtests/tests/gtest_cout.h"
#include "tools/errors.h"
#include "tools/stringstools.h"
#include "vendor/utf8cpp/utf8.h"

struct IndexDeclaration {
	std::string_view indexName;
	std::string_view fieldType;
	std::string_view indexType;
	IndexOpts indexOpts;
	int64_t expireAfter;
};

template <typename DB>
class ReindexerTestApi {
public:
	using ItemType = typename DB::ItemT;
	using QueryResultsType = typename DB::QueryResultsT;

	static constexpr auto kBasicTimeout = std::chrono::seconds(200);

	ReindexerTestApi() : reindexer(std::make_shared<DB>()) {}
	template <typename ConfigT>
	ReindexerTestApi(const ConfigT& cfg) : reindexer(std::make_shared<DB>(cfg)) {}

	template <typename FieldsT>
	static void DefineNamespaceDataset(DB& rx, std::string_view ns, const FieldsT& fields) {
		auto err = reindexer::Error();
		for (const auto& field : fields) {
			if (field.indexType != "composite") {
				err = rx.AddIndex(ns, {std::string{field.indexName},
									   {std::string{field.indexName}},
									   std::string{field.fieldType},
									   std::string{field.indexType},
									   field.indexOpts});
			} else {
				std::string indexName{field.indexName};
				std::string idxContents{field.indexName};
				auto eqPos = indexName.find_first_of('=');
				if (eqPos != std::string::npos) {
					idxContents = indexName.substr(0, eqPos);
					indexName = indexName.substr(eqPos + 1);
				}
				reindexer::JsonPaths jsonPaths;
				jsonPaths = reindexer::split(idxContents, "+", true, jsonPaths);

				err = rx.AddIndex(ns, {indexName, jsonPaths, std::string{field.fieldType}, std::string{field.indexType}, field.indexOpts,
									   field.expireAfter});
			}
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
	void DefineNamespaceDataset(std::string_view ns, std::initializer_list<const IndexDeclaration> fields) {
		DefineNamespaceDataset(*reindexer, ns, fields);
	}
	void DefineNamespaceDataset(std::string_view ns, const std::vector<IndexDeclaration>& fields) {
		DefineNamespaceDataset(*reindexer, ns, fields);
	}

	ItemType NewItem(std::string_view ns) {
		ItemType item = reindexer->NewItem(ns);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what() << "; namespace: " << ns;
		return item;
	}
	void Upsert(std::string_view ns, ItemType& item) {
		assertrx(!!item);
		auto err = reindexer->WithTimeout(kBasicTimeout).Upsert(ns, item);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	}
	void OpenNamespace(std::string_view ns) {
		auto err = reindexer->WithTimeout(kBasicTimeout).OpenNamespace(ns);
		ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns;
	}
	void AddIndex(std::string_view ns, const reindexer::IndexDef& idef) {
		auto err = reindexer->WithTimeout(kBasicTimeout).AddIndex(ns, idef);
		if (!err.ok()) {
			reindexer::WrSerializer ser;
			idef.GetJSON(ser);
			ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns << "; def: " << ser.Slice();
		}
	}
	void DropIndex(std::string_view ns, std::string_view name) {
		auto err = reindexer->WithTimeout(kBasicTimeout).DropIndex(ns, reindexer::IndexDef(std::string(name)));
		ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns << "; name: " << name;
	}
	void Upsert(std::string_view ns, ItemType& item, std::function<void(const reindexer::Error&)> cmpl) {
		assertrx(!!item);
		auto err = reindexer->WithTimeout(kBasicTimeout).WithCompletion(cmpl).Upsert(ns, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	void UpsertJSON(std::string_view ns, std::string_view json) {
		auto item = NewItem(ns);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what() << "; " << json;
		auto err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what() << "; " << json;
		err = reindexer->WithTimeout(kBasicTimeout).Upsert(ns, item);
		ASSERT_TRUE(err.ok()) << err.what() << "; " << json;
	}
	void Update(const reindexer::Query& q, QueryResultsType& qr) {
		auto err = reindexer->WithTimeout(kBasicTimeout).Update(q, qr);
		ASSERT_TRUE(err.ok()) << err.what() << "; " << q.GetSQL(QueryUpdate);
	}
	size_t Update(const reindexer::Query& q) {
		QueryResultsType qr;
		Update(q, qr);
		return qr.Count();
	}
	QueryResultsType UpdateQR(const reindexer::Query& q) {
		QueryResultsType qr;
		Update(q, qr);
		return qr;
	}
	void Select(const reindexer::Query& q, QueryResultsType& qr) {
		auto err = reindexer->WithTimeout(kBasicTimeout).Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what() << "; " << q.GetSQL();
	}
	QueryResultsType Select(const reindexer::Query& q) {
		QueryResultsType qr;
		Select(q, qr);
		return qr;
	}
	void Delete(std::string_view ns, ItemType& item) {
		assertrx(!!item);
		auto err = reindexer->WithTimeout(kBasicTimeout).Delete(ns, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	size_t Delete(const reindexer::Query& q) {
		QueryResultsType qr;
		auto err = reindexer->WithTimeout(kBasicTimeout).Delete(q, qr);
		EXPECT_TRUE(err.ok()) << err.what() << "; " << q.GetSQL(QueryDelete);
		return qr.Count();
	}
	reindexer::Error DumpIndex(std::ostream& os, std::string_view ns, std::string_view index) {
		return reindexer->DumpIndex(os, ns, index);
	}
	void PrintQueryResults(const std::string& ns, const QueryResultsType& res) {
		if (!verbose) {
			return;
		}
		{
			ItemType rdummy(reindexer->NewItem(ns));
			std::string outBuf;
			for (auto idx = 1; idx < rdummy.NumFields(); idx++) {
				outBuf += "\t";
				auto sv = rdummy[idx].Name();
				outBuf.append(sv.begin(), sv.end());
			}
			TestCout() << outBuf << std::endl;
		}

		for (auto it : res) {
			ItemType ritem(it.GetItem(false));
			std::string outBuf = "";
			for (auto idx = 1; idx < ritem.NumFields(); idx++) {
				outBuf += "\t";
				outBuf += ritem[idx].template As<std::string>();
			}
			TestCout() << outBuf << std::endl;
		}
		TestCout() << std::endl;
	}
	std::string PrintItem(ItemType& item) {
		std::string outBuf = "";
		for (auto idx = 1; idx < item.NumFields(); idx++) {
			outBuf += std::string(item[idx].Name()) + "=";
			outBuf += item[idx].template As<std::string>() + " ";
		}
		return outBuf;
	}
	std::string RandString() { return RandString(4, 4); }
	std::string RandString(unsigned minLen, unsigned maxRandLen) {
		return RandString(maxRandLen ? (rand() % maxRandLen + minLen) : minLen);
	}
	std::string RandString(unsigned len) {
		std::string res;
		res.resize(len);
		for (unsigned i = 0; i < len; ++i) {
			int f = rand() % letters.size();
			res[i] = letters[f];
		}
		return res;
	}
	std::string RandLikePattern() {
		std::string res;
		const uint8_t len = rand() % 4 + 4;
		res.reserve(len);
		for (uint8_t i = 0; i < len;) {
			if (rand() % 3 == 0) {
				res += '%';
				const uint8_t skipLen = rand() % (len - i + 1);
				i += skipLen;
			} else {
				if (rand() % 3 == 0) {
					res += '_';
				} else {
					int f = rand() % letters.size();
					res += letters[f];
				}
				++i;
			}
		}
		return res;
	}
	std::string RuRandString() {
		std::string res;
		uint8_t len = rand() % 20 + 4;
		res.resize(len * 3);
		auto it = res.begin();
		for (int i = 0; i < len; ++i) {
			int f = rand() % ru_letters.size();
			it = utf8::append(ru_letters[f], it);
		}
		res.erase(it, res.end());
		return res;
	}
	std::vector<int> RandIntVector(size_t size, int start, int range) {
		std::vector<int> vec;
		vec.reserve(size);
		for (size_t i = 0; i < size; ++i) {
			vec.push_back(start + rand() % range);
		}
		return vec;
	}
	void SetVerbose(bool v) noexcept { verbose = v; }
	std::shared_ptr<DB> reindexer;

private:
	const std::string letters = "abcdefghijklmnopqrstuvwxyz";
	const std::wstring ru_letters = L"абвгдеёжзийклмнопрстуфхцчшщъыьэюя";
	bool verbose = false;
};
