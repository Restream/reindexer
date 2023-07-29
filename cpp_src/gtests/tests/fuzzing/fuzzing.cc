#include "fuzzing/fuzzing.h"
#include "fuzzing/ns.h"
#include "fuzzing/query_generator.h"

TEST_F(Fuzzing, BaseTest) {
	try {
		const fuzzing::RandomGenerator::ErrFactorType errorFactor{0, 1};
		reindexer::WrSerializer ser;
		std::unordered_set<std::string> generatedNames;
		fuzzing::RandomGenerator rnd(std::cout, errorFactor);
		std::vector<fuzzing::Ns> namespaces_;
		const size_t N = 1;
		for (size_t i = 0; i < N; ++i) {
			namespaces_.emplace_back(rnd.NsName(generatedNames), std::cout, errorFactor);
			fuzzing::Ns& ns = namespaces_.back();
			auto err = rx_.OpenNamespace(ns.GetName());
			EXPECT_TRUE(err.ok()) << err.what();
			if (!err.ok()) continue;
			auto& indexes = ns.GetIndexes();
			for (size_t i = 0; i < indexes.size();) {
				const auto idxDef = indexes[i].IndexDef(ns.GetRandomGenerator(), ns.GetScheme());
				err = rx_.AddIndex(ns.GetName(), idxDef);
				EXPECT_TRUE(err.ok()) << err.what();
				if (err.ok()) {
					ns.AddIndex(indexes[i], !idxDef.opts_.IsDense() && idxDef.opts_.IsSparse() && !idxDef.opts_.IsPK());
					std::vector<FieldData> fields;
					std::visit(reindexer::overloaded{
								   [&](const fuzzing::Index::Child& c) {
									   fields.push_back(FieldData{ns.GetScheme().GetJsonPath(c.fieldPath),
																  ToKeyValueType(ns.GetScheme().GetFieldType(c.fieldPath))});
								   },
								   [&](const fuzzing::Index::Children& c) {
									   for (const auto& child : c) {
										   fields.push_back(FieldData{ns.GetScheme().GetJsonPath(child.fieldPath),
																	  ToKeyValueType(ns.GetScheme().GetFieldType(child.fieldPath))});
									   }
								   }},
							   indexes[i].content);
					if (indexes[i].isPk) {
						setPkFields(ns.GetName(), fields);
					}

					addIndexFields(ns.GetName(), indexes[i].name, std::move(fields));
					++i;
				} else {
					indexes.erase(indexes.begin() + i);
				}
			}
			for (size_t i = 0, s = ns.GetRandomGenerator().RndItemsCount(); i < s; ++i) {
				auto item = rx_.NewItem(ns.GetName());
				err = item.Status();
				EXPECT_TRUE(err.ok()) << err.what();
				if (!err.ok()) continue;
				ns.NewItem(ser);
				err = item.FromJSON(ser.Slice());
				EXPECT_TRUE(err.ok()) << err.what() << std::endl << "size: " << ser.Slice().size() << std::endl << ser.Slice();
				if (!err.ok()) continue;
				err = item.Status();
				EXPECT_TRUE(err.ok()) << err.what();
				if (!err.ok()) continue;
				enum Op : uint8_t { Insert, Upsert, Update, Delete, END = Delete };
				switch (rnd.RndWhich<Op, 10, 100, 1, 1>()) {
					case Insert:
						err = rx_.Insert(rnd.NsName(ns.GetName(), generatedNames), item);
						if (err.ok() && item.GetID() != -1) {
							saveItem(std::move(item), ns.GetName());
						}
						break;
					case Upsert:
						err = rx_.Upsert(rnd.NsName(ns.GetName(), generatedNames), item);
						if (err.ok()) {
							saveItem(std::move(item), ns.GetName());
						}
						break;
					case Update:
						err = rx_.Update(rnd.NsName(ns.GetName(), generatedNames), item);
						if (err.ok() && item.GetID() != -1) {
							saveItem(std::move(item), ns.GetName());
						}
						break;
					case Delete: {
						const auto id = item.GetID();
						err = rx_.Delete(rnd.NsName(ns.GetName(), generatedNames), item);
						if (err.ok() && item.GetID() != id) {
							deleteItem(item, ns.GetName());
						}
					} break;
					default:
						assertrx(0);
				}
				EXPECT_TRUE(err.ok()) << err.what();
			}
			reindexer::QueryResults qr;
			err = rx_.Select(reindexer::Query(ns.GetName()).ReqTotal(), qr);
			EXPECT_TRUE(err.ok()) << err.what();
		}
		fuzzing::QueryGenerator queryGenerator{namespaces_, std::cout, errorFactor};
		for (size_t i = 0; i < 100; ++i) {
			auto query = queryGenerator();
			reindexer::QueryResults qr;
			auto err = rx_.Select(query, qr);
			EXPECT_TRUE(err.ok()) << err.what();
			if (err.ok()) {
				Verify(qr, query, rx_);
			}
		}
	} catch (const std::exception& err) {
		FAIL() << err.what();
	} catch (const reindexer::Error& err) {
		FAIL() << err.what();
	}
}

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
