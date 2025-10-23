#include "fuzzing/fuzzing.h"
#include "args/args.hpp"
#include "fuzzing/index.h"
#include "fuzzing/ns.h"
#include "fuzzing/query_generator.h"

TEST_F(Fuzzing, BaseTest) {
	try {
		const fuzzing::RandomGenerator::ErrFactorType errorFactor{0, 1};
		reindexer::WrSerializer ser;
		fuzzing::RandomGenerator rnd(errorFactor);
		std::vector<fuzzing::Ns> namespaces;
		const size_t N = 1;
		for (size_t i = 0; i < N; ++i) {
			namespaces.emplace_back(rnd.GenerateNsName(), errorFactor);
			fuzzing::Ns& ns = namespaces.back();
			auto err = rx_.OpenNamespace(ns.GetName());
			EXPECT_TRUE(err.ok()) << err.what();
			if (!err.ok()) {
				continue;
			}
			auto& indexes = ns.GetIndexes();
			for (size_t j = 0; j < indexes.size();) {
				const fuzzing::Index& idx = indexes[j];
				const auto idxDef = idx.IndexDef(ns.GetRandomGenerator(), ns.GetScheme(), indexes);
				err = rx_.AddIndex(ns.GetName(), idxDef);
				EXPECT_TRUE(err.ok()) << err.what();
				if (err.ok()) {
					ns.AddIndexToScheme(idx, j);  // TODO move to fuzzing::Ns
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
							   idx.Content());
					if (idx.IsPk()) {
						setPkFields(ns.GetName(), fields);
					}
					addIndexFields(ns.GetName(), idx.Name(), std::move(fields));
					++j;
				} else {
					indexes.erase(indexes.begin() + j);
				}
			}
			for (size_t j = 0, s = ns.GetRandomGenerator().RndItemsCount(); j < s; ++j) {
				auto item = rx_.NewItem(ns.GetName());
				err = item.Status();
				EXPECT_TRUE(err.ok()) << err.what();
				if (!err.ok()) {
					continue;
				}
				ns.NewItem(ser);  // TODO not json
				err = item.FromJSON(ser.Slice());
				EXPECT_TRUE(err.ok()) << err.what() << std::endl << "size: " << ser.Slice().size() << std::endl << ser.Slice();
				if (!err.ok()) {
					continue;
				}
				err = item.Status();
				EXPECT_TRUE(err.ok()) << err.what();
				if (!err.ok()) {
					continue;
				}
				enum [[nodiscard]] Op : uint8_t { Insert, Upsert, Update, Delete, END = Delete };
				switch (rnd.RndWhich<Op, 10, 100, 1, 1>()) {
					case Insert:
						err = rx_.Insert(rnd.NsName(ns.GetName()), item);
						if (err.ok() && item.GetID() != -1) {
							saveItem(std::move(item), ns.GetName());
						}
						break;
					case Upsert:
						err = rx_.Upsert(rnd.NsName(ns.GetName()), item);
						if (err.ok()) {
							saveItem(std::move(item), ns.GetName());
						}
						break;
					case Update:
						err = rx_.Update(rnd.NsName(ns.GetName()), item);
						if (err.ok() && item.GetID() != -1) {
							saveItem(std::move(item), ns.GetName());
						}
						break;
					case Delete: {
						const auto id = item.GetID();
						err = rx_.Delete(rnd.NsName(ns.GetName()), item);
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
		fuzzing::QueryGenerator queryGenerator{namespaces, errorFactor};
		for (size_t i = 0; i < 100; ++i) {
			auto query = queryGenerator();
			reindexer::QueryResults qr;
			auto err = rx_.Select(query, qr);
			EXPECT_TRUE(err.ok()) << err.what();
			if (err.ok()) {
				Verify(qr.ToLocalQr(), std::move(query), rx_);
			}
		}
	} catch (const std::exception& err) {
		FAIL() << err.what();
	}
}

// NOLINTNEXTLINE (bugprone-exception-escape) Get stacktrace is probably better, than generic error-message
int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);

	args::ArgumentParser parser("Reindexer fuzzing tests");
	args::HelpFlag help(parser, "help", "show this message", {'h', "help"});
	args::Group progOptions("options");
	args::ValueFlag<std::string> dbDsn(progOptions, "DSN",
									   "DSN to 'reindexer'. Can be 'cproto://[user@password:]<ip>:<port>/<dbname>' or 'builtin://<path>'",
									   {'d', "dsn"}, args::Options::Single | args::Options::Global);
	args::ValueFlag<std::string> output(progOptions, "FILENAME", "A file for saving initial states of random engines", {'s', "save"},
										args::Options::Single | args::Options::Global);
	args::ValueFlag<std::string> input(progOptions, "FILENAME", "A file for initial states of random engines recovery", {'r', "repeat"},
									   args::Options::Single | args::Options::Global);
	args::GlobalOptions globals(parser, progOptions);
	try {
		parser.ParseCLI(argc, argv);
	} catch (const args::Help&) {
		std::cout << parser.Help() << std::endl;
		return 1;
	} catch (const args::Error& e) {
		std::cerr << "ERROR: " << e.what() << std::endl;
		std::cout << parser.Help() << std::endl;
		return 1;
	}
	std::string out = args::get(output);
	if (!out.empty()) {
		fuzzing::RandomGenerator::SetOut(std::move(out));
	}
	const std::string in = args::get(input);
	if (!in.empty()) {
		fuzzing::RandomGenerator::SetIn(in);
	}
	std::string dsn = args::get(dbDsn);
	if (!dsn.empty()) {
		Fuzzing::SetDsn(std::move(dsn));
	}

	return RUN_ALL_TESTS();
}
