#include "ft_api.h"

void FTApi::Init(const reindexer::FtFastConfig& ftCfg, unsigned nses, const std::string& storage) {
	rt.reindexer.reset(new reindexer::Reindexer);
	if (!storage.empty()) {
		auto err = rt.reindexer->Connect("builtin://" + storage);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	if (nses & NS1) {
		const reindexer::Error err = rt.reindexer->OpenNamespace("nm1");
		ASSERT_TRUE(err.ok()) << err.what();
		rt.DefineNamespaceDataset(
			"nm1", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
		SetFTConfig(ftCfg);
	}
	if (nses & NS2) {
		const reindexer::Error err = rt.reindexer->OpenNamespace("nm2");
		ASSERT_TRUE(err.ok()) << err.what();
		rt.DefineNamespaceDataset(
			"nm2", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
	}
	if (nses & NS3) {
		reindexer::Error err = rt.reindexer->OpenNamespace("nm3");
		ASSERT_TRUE(err.ok()) << err.what();
		rt.DefineNamespaceDataset(
			"nm3", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0}, IndexDeclaration{"ft3", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2+ft3=ft", "text", "composite", IndexOpts(), 0}});
		err = SetFTConfig(ftCfg, "nm3", "ft", {"ft1", "ft2", "ft3"});
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

reindexer::FtFastConfig FTApi::GetDefaultConfig(size_t fieldsCount) {
	reindexer::FtFastConfig cfg(fieldsCount);
	cfg.enableNumbersSearch = true;
	cfg.enableWarmupOnNsCopy = true;
	cfg.logLevel = 5;
	cfg.mergeLimit = 20000;
	cfg.maxStepSize = 100;
	cfg.optimization = GetParam();
	return cfg;
}

void FTApi::SetFTConfig(const reindexer::FtFastConfig& ftCfg) {
	const reindexer::Error err = SetFTConfig(ftCfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
}

reindexer::Error FTApi::SetFTConfig(const reindexer::FtFastConfig& ftCfg, const std::string& ns, const std::string& index,
									const std::vector<std::string>& fields) {
	assertrx(!ftCfg.fieldsCfg.empty());
	assertrx(ftCfg.fieldsCfg.size() >= fields.size());
	reindexer::fast_hash_map<std::string, int> fieldsMap;
	for (size_t i = 0, size = fields.size(); i < size; ++i) {
		fieldsMap.emplace(fields[i], i);
	}
	std::vector<reindexer::NamespaceDef> nses;
	auto err = rt.reindexer->EnumNamespaces(nses, reindexer::EnumNamespacesOpts().WithFilter(ns));
	if (!err.ok()) {
		return err;
	}
	const auto it = std::find_if(nses[0].indexes.begin(), nses[0].indexes.end(),
								 [&index](const reindexer::IndexDef& idef) { return idef.name_ == index; });
	it->opts_.SetConfig(ftCfg.GetJSON(fieldsMap));

	return rt.reindexer->UpdateIndex(ns, *it);
}

void FTApi::FillData(int64_t count) {
	for (int i = 0; i < count; ++i) {
		reindexer::Item item = rt.NewItem(GetDefaultNamespace());
		item["id"] = counter_;
		auto ft1 = rt.RandString();
		counter_++;
		item["ft1"] = ft1;
		rt.Upsert(GetDefaultNamespace(), item);
		rt.Commit(GetDefaultNamespace());
	}
}

void FTApi::Add(std::string_view ft1, std::string_view ft2, unsigned nses) {
	using namespace std::string_view_literals;
	if (nses & NS1) {
		Add("nm1"sv, ft1, ft2);
	}
	if (nses & NS2) {
		Add("nm2"sv, ft1, ft2);
	}
}

std::pair<std::string_view, int> FTApi::Add(std::string_view ft1) {
	reindexer::Item item = rt.NewItem("nm1");
	item["id"] = counter_;
	counter_++;
	item["ft1"] = std::string{ft1};

	rt.Upsert("nm1", item);
	rt.Commit("nm1");
	return make_pair(ft1, counter_ - 1);
}

void FTApi::Add(std::string_view ns, std::string_view ft1, std::string_view ft2) {
	reindexer::Item item = rt.NewItem(ns);
	item["id"] = counter_;
	++counter_;
	item["ft1"] = std::string{ft1};
	item["ft2"] = std::string{ft2};

	rt.Upsert(ns, item);
	rt.Commit(ns);
}

void FTApi::Add(std::string_view ns, std::string_view ft1, std::string_view ft2, std::string_view ft3) {
	reindexer::Item item = rt.NewItem(ns);
	item["id"] = counter_;
	++counter_;
	item["ft1"] = std::string{ft1};
	item["ft2"] = std::string{ft2};
	item["ft3"] = std::string{ft3};

	rt.Upsert(ns, item);
	rt.Commit(ns);
}

void FTApi::AddInBothFields(std::string_view w1, std::string_view w2, unsigned nses) {
	using namespace std::string_view_literals;
	if (nses & NS1) {
		AddInBothFields("nm1"sv, w1, w2);
	}
	if (nses & NS2) {
		AddInBothFields("nm2"sv, w1, w2);
	}
}

void FTApi::AddInBothFields(std::string_view ns, std::string_view w1, std::string_view w2) {
	reindexer::Item item = rt.NewItem(ns);
	item["id"] = counter_;
	++counter_;
	item["ft1"] = std::string{w1};
	item["ft2"] = std::string{w1};
	rt.Upsert(ns, item);

	item = rt.NewItem(ns);
	item["id"] = counter_;
	++counter_;
	item["ft1"] = std::string{w2};
	item["ft2"] = std::string{w2};
	rt.Upsert(ns, item);

	rt.Commit(ns);
}

reindexer::QueryResults FTApi::SimpleSelect(std::string word, bool withHighlight) {
	auto q{reindexer::Query("nm1").Where("ft3", CondEq, std::move(word)).WithRank()};
	reindexer::QueryResults res;
	if (withHighlight) {
		q.AddFunction("ft3 = highlight(!,!)");
	}
	auto err = rt.reindexer->Select(q, res);
	EXPECT_TRUE(err.ok()) << err.what();

	return res;
}

reindexer::QueryResults FTApi::SimpleSelect3(std::string word) {
	auto qr{reindexer::Query("nm3").Where("ft", CondEq, std::move(word))};
	reindexer::QueryResults res;
	qr.AddFunction("ft = highlight(!,!)");
	auto err = rt.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();
	return res;
}

reindexer::Error FTApi::Delete(int id) {
	reindexer::Item item = rt.NewItem("nm1");
	item["id"] = id;

	return this->rt.reindexer->Delete("nm1", item);
}

reindexer::QueryResults FTApi::SimpleCompositeSelect(std::string word) {
	auto qr{reindexer::Query("nm1").Where("ft3", CondEq, word)};
	reindexer::QueryResults res;
	auto mqr{reindexer::Query("nm2").Where("ft3", CondEq, std::move(word))};
	mqr.AddFunction("ft1 = snippet(<b>,\"\"</b>,3,2,,d)");

	qr.Merge(std::move(mqr));
	qr.AddFunction("ft3 = highlight(<b>,</b>)");
	auto err = rt.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();

	return res;
}

reindexer::QueryResults FTApi::CompositeSelectField(const std::string& field, std::string word) {
	word = '@' + field + ' ' + word;
	auto qr{reindexer::Query("nm1").Where("ft3", CondEq, word)};
	reindexer::QueryResults res;
	auto mqr{reindexer::Query("nm2").Where("ft3", CondEq, std::move(word))};
	mqr.AddFunction(field + " = snippet(<b>,\"\"</b>,3,2,,d)");

	qr.Merge(std::move(mqr));
	qr.AddFunction(field + " = highlight(<b>,</b>)");
	auto err = rt.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();

	return res;
}

reindexer::QueryResults FTApi::StressSelect(std::string word) {
	const auto qr{reindexer::Query("nm1").Where("ft3", CondEq, std::move(word))};
	reindexer::QueryResults res;
	auto err = rt.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();

	return res;
}

std::vector<std::string> FTApi::CreateAllPermutatedQueries(const std::string& queryStart, std::vector<std::string> words,
														   const std::string& queryEnd, const std::string& sep) {
	std::vector<std::pair<size_t, std::string>> indexedWords;
	indexedWords.reserve(words.size());
	for (std::string& w : words) {
		indexedWords.emplace_back(indexedWords.size(), std::move(w));
	}
	std::vector<std::string> result;
	do {
		result.push_back(queryStart);
		std::string& query = result.back();
		for (auto it = indexedWords.cbegin(); it != indexedWords.cend(); ++it) {
			if (it != indexedWords.cbegin()) query += sep;
			query += it->second;
		}
		query += queryEnd;
	} while (std::next_permutation(
		indexedWords.begin(), indexedWords.end(),
		[](const std::pair<size_t, std::string>& a, const std::pair<size_t, std::string>& b) { return a.first < b.first; }));
	return result;
}

void FTApi::CheckAllPermutations(const std::string& queryStart, const std::vector<std::string>& words, const std::string& queryEnd,
								 const std::vector<std::tuple<std::string, std::string>>& expectedResults, bool withOrder,
								 const std::string& sep, bool withHighlight) {
	for (const auto& query : CreateAllPermutatedQueries(queryStart, words, queryEnd, sep)) {
		CheckResults(query, expectedResults, withOrder, withHighlight);
	}
}

void FTApi::CheckResults(const std::string& query, std::vector<std::tuple<std::string, std::string>> expectedResults, bool withOrder,
						 bool withHighlight) {
	const auto qr = SimpleSelect(query, withHighlight);
	CheckResults(query, qr, expectedResults, withOrder);
}

void FTApi::CheckResults(const std::string& query, const reindexer::QueryResults& qr,
						 std::vector<std::tuple<std::string, std::string, std::string>> expectedResults, bool withOrder) {
	CheckResults<std::tuple<std::string, std::string, std::string>>(query, qr, expectedResults, withOrder);
}

std::vector<std::tuple<std::string, std::string>>& FTApi::DelHighlightSign(std::vector<std::tuple<std::string, std::string>>& in) {
	for (auto& v : in) {
		std::string& v2 = std::get<0>(v);
		v2.erase(std::remove(v2.begin(), v2.end(), '!'), v2.end());
	}
	return in;
}
