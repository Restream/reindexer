#include "ft_api.h"

void FTApi::Init(const reindexer::FtFastConfig& ftCfg, unsigned nses, const std::string& storage) {
	rt.reindexer = std::make_shared<reindexer::Reindexer>();
	rt.Connect("builtin://" + storage);

	if (nses & NS1) {
		rt.OpenNamespace("nm1");
		rt.DefineNamespaceDataset(
			"nm1", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
		SetFTConfig(ftCfg);
	}
	if (nses & NS2) {
		rt.OpenNamespace("nm2");
		rt.DefineNamespaceDataset(
			"nm2", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
	}
	if (nses & NS3) {
		rt.OpenNamespace("nm3");
		rt.DefineNamespaceDataset(
			"nm3", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0}, IndexDeclaration{"ft3", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2+ft3=ft", "text", "composite", IndexOpts(), 0}});
		auto err = SetFTConfig(ftCfg, "nm3", "ft", {"ft1", "ft2", "ft3"});
		ASSERT_TRUE(err.ok()) << err.what();
	}
	if (nses & NS4) {
		rt.OpenNamespace("nm4");
		rt.DefineNamespaceDataset(
			"nm4", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts().Array(), 0},
					IndexDeclaration{"ft3", "text", "string", IndexOpts().Array(), 0},
					IndexDeclaration{"ft1+ft2=ft", "text", "composite", IndexOpts(), 0}});
		auto err = SetFTConfig(ftCfg, "nm4", "ft3", {"text"});
		ASSERT_TRUE(err.ok()) << err.what();
		err = SetFTConfig(ftCfg, "nm4", "ft", {"ft1", "ft2"});
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

reindexer::FtFastConfig FTApi::GetDefaultConfig(size_t fieldsCount) {
	reindexer::FtFastConfig cfg(fieldsCount);
	cfg.enableNumbersSearch = true;
	cfg.logLevel = 5;
	cfg.mergeLimit = 20000;
	cfg.maxStepSize = 100;
	cfg.optimization = GetParam();
	return cfg;
}

void FTApi::SetFTConfig(const reindexer::FtFastConfig& ftCfg) {
	auto err = SetFTConfig(ftCfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
}

reindexer::Error FTApi::SetFTConfig(const reindexer::FtFastConfig& ftCfg, std::string_view ns, const std::string& index,
									const std::vector<std::string>& fields) {
	assertrx(!ftCfg.fieldsCfg.empty());
	assertrx(ftCfg.fieldsCfg.size() >= fields.size());
	auto nses = rt.EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(ns));
	const auto it = std::find_if(nses[0].indexes.begin(), nses[0].indexes.end(),
								 [&index](const reindexer::IndexDef& idef) { return idef.Name() == index; });
	assertrx(it != nses[0].indexes.end());
	auto opts = it->Opts();
	opts.SetConfig(IndexFastFT, GetFTConfigJSON(ftCfg, fields));
	it->SetOpts(std::move(opts));

	return rt.reindexer->UpdateIndex(ns, *it);
}

std::string FTApi::GetFTConfigJSON(const reindexer::FtFastConfig& ftCfg, const std::vector<std::string>& fields) {
	reindexer::fast_hash_map<std::string, int> fieldsMap;
	for (size_t i = 0, size = fields.size(); i < size; ++i) {
		fieldsMap.emplace(fields[i], i);
	}
	return ftCfg.GetJSON(fieldsMap);
}

void FTApi::FillData(int64_t count) {
	for (int i = 0; i < count; ++i) {
		reindexer::Item item = rt.NewItem(GetDefaultNamespace());
		item["id"] = counter_;
		auto ft1 = rt.RandString();
		counter_++;
		item["ft1"] = ft1;
		rt.Upsert(GetDefaultNamespace(), item);
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
	return make_pair(ft1, counter_ - 1);
}

void FTApi::AddNs4(const std::string& ft1, const std::vector<std::string>& ft2, const std::vector<std::string>& ft3) {
	reindexer::Item item = rt.NewItem("nm4");
	item["id"] = counter_;
	++counter_;
	item["ft1"] = ft1;
	item["ft2"] = ft2;
	item["ft3"] = ft3;

	rt.Upsert("nm4", item);
}

void FTApi::Add(std::string_view ns, std::string_view ft1, std::string_view ft2) {
	reindexer::Item item = rt.NewItem(ns);
	item["id"] = counter_;
	++counter_;
	item["ft1"] = std::string{ft1};
	item["ft2"] = std::string{ft2};

	rt.Upsert(ns, item);
}

void FTApi::Add(std::string_view ns, std::string_view ft1, std::string_view ft2, std::string_view ft3) {
	reindexer::Item item = rt.NewItem(ns);
	item["id"] = counter_;
	++counter_;
	item["ft1"] = std::string{ft1};
	item["ft2"] = std::string{ft2};
	item["ft3"] = std::string{ft3};

	rt.Upsert(ns, item);
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
}

reindexer::QueryResults FTApi::SimpleSelect(std::string_view ns, std::string_view index, std::string_view dsl, bool withHighlight) {
	auto q{reindexer::Query(ns).Where(index, CondEq, std::string(dsl)).WithRank()};
	if (withHighlight) {
		q.AddFunction(fmt::format("{} = highlight(!,!)", index));
	}
	return rt.Select(q);
}

void FTApi::Delete(int id) {
	auto item = rt.NewItem("nm1");
	item["id"] = id;
	rt.Delete("nm1", item);
}

reindexer::QueryResults FTApi::SimpleCompositeSelect(std::string_view word) {
	auto qr{reindexer::Query("nm1").Where("ft3", CondEq, word)};
	auto mqr{reindexer::Query("nm2").Where("ft3", CondEq, word)};
	mqr.AddFunction("ft1 = snippet(<b>,\"\"</b>,3,2,,d)");

	qr.Merge(std::move(mqr));
	qr.AddFunction("ft3 = highlight(<b>,</b>)");
	return rt.Select(qr);
}

reindexer::QueryResults FTApi::CompositeSelectField(const std::string& field, std::string_view word) {
	const auto query = fmt::format("@{} {}", field, word);
	auto qr{reindexer::Query("nm1").Where("ft3", CondEq, query)};
	auto mqr{reindexer::Query("nm2").Where("ft3", CondEq, query)};
	mqr.AddFunction(field + " = snippet(<b>,\"\"</b>,3,2,,d)");

	qr.Merge(std::move(mqr));
	qr.AddFunction(field + " = highlight(<b>,</b>)");
	return rt.Select(qr);
}

reindexer::QueryResults FTApi::StressSelect(std::string_view word) {
	const auto qr{reindexer::Query("nm1").Where("ft3", CondEq, word)};
	return rt.Select(qr);
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
			if (it != indexedWords.cbegin()) {
				query += sep;
			}
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
	CheckResults(query, qr, std::move(expectedResults), withOrder);
}

void FTApi::CheckResults(const std::string& query, const reindexer::QueryResults& qr,
						 std::vector<std::tuple<std::string, std::string, std::string>> expectedResults, bool withOrder) {
	CheckResults<std::tuple<std::string, std::string, std::string>>(query, qr, expectedResults, withOrder);
}

void FTApi::CheckResults(const std::string& query, const reindexer::QueryResults& qr,
						 std::vector<std::tuple<std::string, std::string>> expectedResults, bool withOrder) {
	CheckResults<std::tuple<std::string, std::string>>(query, qr, expectedResults, withOrder);
}

std::vector<std::tuple<std::string, std::string>>& FTApi::DelHighlightSign(std::vector<std::tuple<std::string, std::string>>& in) {
	for (auto& v : in) {
		std::string& v2 = std::get<0>(v);
		v2.erase(std::remove(v2.begin(), v2.end(), '!'), v2.end());
	}
	return in;
}

template <typename ResType>
void FTApi::CheckResults(const std::string& query, const reindexer::QueryResults& qr, std::vector<ResType>& expectedResults,
						 bool withOrder) {
	constexpr bool kTreeFields = std::tuple_size<ResType>{} == 3;
	EXPECT_EQ(qr.Count(), expectedResults.size()) << "Query: " << query;
	for (auto itRes : qr) {
		const auto item = itRes.GetItem(false);
		const auto it = std::find_if(expectedResults.begin(), expectedResults.end(), [&item](const ResType& p) {
			if constexpr (kTreeFields) {
				return std::get<0>(p) == item["ft1"].As<std::string>() && std::get<1>(p) == item["ft2"].As<std::string>() &&
					   std::get<2>(p) == item["ft3"].As<std::string>();
			}
			return std::get<0>(p) == item["ft1"].As<std::string>() && std::get<1>(p) == item["ft2"].As<std::string>();
		});
		if (it == expectedResults.end()) {
			if constexpr (kTreeFields) {
				ADD_FAILURE() << "Found not expected: \"" << item["ft1"].As<std::string>() << "\" \"" << item["ft2"].As<std::string>()
							  << "\" \"" << item["ft3"].As<std::string>() << "\"\nQuery: " << query;
			} else {
				ADD_FAILURE() << "Found not expected: \"" << item["ft1"].As<std::string>() << "\" \"" << item["ft2"].As<std::string>()
							  << "\"\nQuery: " << query;
			}
		} else {
			if (withOrder) {
				if constexpr (kTreeFields) {
					EXPECT_EQ(it, expectedResults.begin())
						<< "Found not in order: \"" << item["ft1"].As<std::string>() << "\" \"" << item["ft2"].As<std::string>() << "\" \""
						<< item["ft3"].As<std::string>() << "\"\nQuery: " << query;
				} else {
					EXPECT_EQ(it, expectedResults.begin()) << "Found not in order: \"" << item["ft1"].As<std::string>() << "\" \""
														   << item["ft2"].As<std::string>() << "\"\nQuery: " << query;
				}
			}
			expectedResults.erase(it);
		}
	}
	for (const auto& expected : expectedResults) {
		if constexpr (kTreeFields) {
			ADD_FAILURE() << "Not found: \"" << std::get<0>(expected) << "\" \"" << std::get<1>(expected) << "\" \""
						  << std::get<2>(expected) << "\"\nQuery: " << query;
		} else {
			ADD_FAILURE() << "Not found: \"" << std::get<0>(expected) << "\" \"" << std::get<1>(expected) << "\"\nQuery: " << query;
		}
	}
	if (!expectedResults.empty()) {
		ADD_FAILURE() << "Query: " << query;
	}
}
