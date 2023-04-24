#pragma once
#include <gtest/gtest.h>
#include "client/cororeindexer.h"
#include "client/queryresults.h"
#include "client/rpcclientmock.h"
#include "core/cjson/jsonbuilder.h"
#include "core/indexdef.h"
#include "core/reindexer.h"

template <typename Client>
struct QueryResType {
	using type = reindexer::client::QueryResults;
};

template <>
struct QueryResType<reindexer::client::CoroReindexer> {
	using type = reindexer::client::CoroQueryResults;
};

template <typename Client>
void QueryAggStrictModeTest(const std::unique_ptr<Client>& client) {
	using QueryResType = typename QueryResType<Client>::type;
	constexpr bool isMockClient = std::is_same_v<Client, reindexer::client::RPCClientMock>;

	const reindexer::client::InternalRdxContext ctx;

	const std::string kNsName = "agg_ns";
	const std::string kFieldId = "id";
	const std::string kNonIndexedField = "NonIndexedField";
	const std::string kNonExistentField = "nonExistentField";

	reindexer::Error err;
	if constexpr (isMockClient) {
		err = client->OpenNamespace(kNsName, ctx);
	} else {
		err = client->OpenNamespace(kNsName);
	}

	ASSERT_TRUE(err.ok()) << err.what();

	if constexpr (isMockClient) {
		err = client->AddIndex(kNsName, reindexer::IndexDef(kFieldId, "hash", "int", IndexOpts().PK()), ctx);
	} else {
		err = client->AddIndex(kNsName, reindexer::IndexDef(kFieldId, "hash", "int", IndexOpts().PK()));
	}

	ASSERT_TRUE(err.ok()) << err.what();

	for (size_t i = 0; i < 1000; ++i) {
		reindexer::WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, i);
		jsonBuilder.Put(kNonIndexedField, i);
		jsonBuilder.End();

		char* endp = nullptr;
		auto item = client->NewItem(kNsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		auto err = item.FromJSON(wrser.Slice(), &endp);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		ASSERT_TRUE(err.ok()) << err.what();

		if constexpr (isMockClient) {
			client->Upsert(kNsName, item, ctx);
		} else {
			client->Upsert(kNsName, item);
		}

		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// To verify that when aggregating by a nonexistent field, null optional will be received in the AggregationResult
	const std::array<AggType, 6> aggTypes{AggType::AggSum, AggType::AggAvg,	  AggType::AggMin,
										  AggType::AggMax, AggType::AggCount, AggType::AggCountCached};
	enum StrictError { Ok, ErrName, ErrIndex };

	const std::map<AggType, double> results{
		{AggType::AggSum, 499500.},	 // 1/2 * (0 + 999) * 1000 - by the formula of the sum of the arithmetic progression
		{AggType::AggAvg, 499.5},	{AggType::AggMin, 0.},			{AggType::AggMax, 999.},
		{AggType::AggCount, 1000},	{AggType::AggCountCached, 1000}};

	const std::map<StrictError, std::string> errors{
		{ErrName, "Current query strict mode allows aggregate existing fields only. There are no fields with name '%s' in namespace '%s'"},
		{ErrIndex, "Current query strict mode allows aggregate index fields only. There are no indexes with name '%s' in namespace '%s'"}};

	const std::map<std::string, std::map<StrictMode, StrictError>> scenarios{
		{kFieldId, {{StrictMode::StrictModeNone, Ok}, {StrictMode::StrictModeNames, Ok}, {StrictMode::StrictModeIndexes, Ok}}},
		{kNonIndexedField,
		 {{StrictMode::StrictModeNone, Ok}, {StrictMode::StrictModeNames, Ok}, {StrictMode::StrictModeIndexes, ErrIndex}}},
		{kNonExistentField,
		 {{StrictMode::StrictModeNone, Ok}, {StrictMode::StrictModeNames, ErrName}, {StrictMode::StrictModeIndexes, ErrIndex}}},
	};

	auto testUnit = [&](const std::string& field, AggType type, StrictMode mode, [[maybe_unused]] StrictError expectedError) {
		QueryResType qr;
		auto query = reindexer::Query(kNsName);

		query.Strict(mode);
		switch (type) {
			case AggCount:
			case AggCountCached:
				// execution of count-queries does not depend on fields and StrictMode and is checked separately
				query.Aggregate(type, {});
				break;
			default:
				query.Aggregate(type, {field});
		}

		reindexer::Error err;
		if constexpr (isMockClient) {
			err = client->Select(query, qr, ctx, nullptr, FormatMsgPack);
		} else {
			err = client->Select(query, qr);
		}

		switch (type) {
			case AggCount:
			case AggCountCached: {
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(qr.GetAggregationResults().size(), 1);
				auto value = qr.GetAggregationResults()[0].GetValue();

				ASSERT_TRUE(value);
				ASSERT_EQ(results.at(type), *value);
				return;
			}
			default:
				switch (expectedError) {
					case Ok: {
						ASSERT_TRUE(err.ok()) << "AggType: " << type << "; " << err.what();
						ASSERT_EQ(qr.GetAggregationResults().size(), 1);
						auto value = qr.GetAggregationResults()[0].GetValue();

						if (field != kNonExistentField) {
							ASSERT_TRUE(value);
							ASSERT_EQ(results.at(type), *value);
						} else {
							ASSERT_FALSE(value);
						}
						break;
					}
					case ErrName:
						ASSERT_EQ(err.what(), fmt::sprintf(errors.at(ErrName), field, kNsName))
							<< "AggType: " << type << "; " << err.what();
						break;
					case ErrIndex:
						ASSERT_EQ(err.what(), fmt::sprintf(errors.at(ErrIndex), field, kNsName))
							<< "AggType: " << type << "; " << err.what();
						break;
				}
		}
	};

	for (const auto& aggType : aggTypes)
		for (const auto& [field, modes] : scenarios)
			for (const auto& [mode, err] : modes) testUnit(field, aggType, mode, err);
}
