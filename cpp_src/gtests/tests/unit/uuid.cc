#include <gtest/gtest.h>
#include <algorithm>

#include "core/cjson/jsonbuilder.h"
#include "core/keyvalue/uuid.h"
#include "core/keyvalue/variant.h"
#include "core/reindexer.h"
#include "gmock/gmock.h"
#include "gtests/tools.h"
#include "tools/stringstools.h"

static constexpr int kItemsCount = 100;
static constexpr const char* nsName = "ns_uuid";

TEST(UUID, FromString) {
	reindexer::Uuid uuid(nilUUID);
	EXPECT_EQ(std::string(uuid), nilUUID);

	for (int j = 0; j < 1000; ++j) {
		const std::string strUuid = randStrUuid();
		uuid = reindexer::Uuid{strUuid};
		EXPECT_EQ(std::string(uuid), reindexer::toLower(strUuid));
	}
}

TEST(UUID, FromString_InvalidChar) {
	for (int j = 0; j < 1000; ++j) {
		std::string strUuid = randStrUuid();
		unsigned i;
		do {
			i = rand() % strUuid.size();
		} while (isUuidDelimPos(i));
		char ch;
		do {
			ch = rand() % 256;
		} while (hexChars.find(ch) != std::string_view::npos);
		strUuid[i] = ch;
		[[maybe_unused]] reindexer::Uuid uuid;
		EXPECT_THROW(uuid = reindexer::Uuid{strUuid}, reindexer::Error) << strUuid;
	}
}

TEST(UUID, FromString_InvalidSize) {
	for (int j = 0; j < 1000; ++j) {
		std::string strUuid = randStrUuid();
		std::vector<unsigned> delimPos(std::begin(uuidDelimPositions), std::end(uuidDelimPositions));
		const bool del = rand() % 2;
		const unsigned count = rand() % 3 + 1;
		for (unsigned i = 0; i < count; ++i) {
			unsigned idx;
			do {
				idx = rand() % strUuid.size();
			} while (std::find(delimPos.begin(), delimPos.end(), idx) != delimPos.end());
			if (del) {
				strUuid.erase(idx, 1);
			} else {
				strUuid.insert(idx, 1, hexChars[rand() % hexChars.size()]);
			}
			std::transform(delimPos.begin(), delimPos.end(), delimPos.begin(),
						   [idx, del](unsigned i) { return idx < i ? (del ? i - 1 : i + 1) : i; });
		}
		[[maybe_unused]] reindexer::Uuid uuid;
		EXPECT_THROW(uuid = reindexer::Uuid{strUuid}, reindexer::Error) << strUuid;
	}
}

TEST(UUID, FromString_InvalidVariant) {
	for (int j = 0; j < 1000; ++j) {
		std::string strUuid = randStrUuid();
		if (strUuid == nilUUID) {
			strUuid[19] = hexChars[1 + rand() % 7];
		} else {
			strUuid[19] = hexChars[rand() % 8];
		}
		[[maybe_unused]] reindexer::Uuid uuid;
		EXPECT_THROW(uuid = reindexer::Uuid{strUuid}, reindexer::Error) << strUuid;
	}
}

TEST(UUID, ToVariant) {
	for (int i = 0; i < 1000; ++i) {
		const std::string strUuid = randStrUuid();
		const reindexer::Uuid uuid{strUuid};
		reindexer::Variant varUuid{uuid};
		EXPECT_EQ(reindexer::Uuid(varUuid), uuid);
		EXPECT_EQ(std::string(reindexer::Uuid(varUuid)), reindexer::toLower(strUuid));
	}
}

TEST(UUID, ConvertVariant) {
	for (int i = 0; i < 1000; ++i) {
		const std::string strUuid = randStrUuid();
		const reindexer::Uuid uuid{strUuid};
		const reindexer::Variant varStr{strUuid};
		reindexer::Variant varUuid{uuid};
		const auto uuidFromVariant = varStr.As<reindexer::Uuid>();
		EXPECT_EQ(uuid, uuidFromVariant);
		const auto strFromVariant = varUuid.As<std::string>();
		EXPECT_EQ(strFromVariant, reindexer::toLower(strUuid));
		const auto varConverted = varStr.convert(reindexer::KeyValueType::Uuid{});
		EXPECT_EQ(reindexer::toLower(strUuid), varConverted.As<std::string>());
		EXPECT_EQ(uuid, varConverted.As<reindexer::Uuid>());
	}
}

template <typename T1, typename T2>
struct [[nodiscard]] Values {
	Values() = default;
	template <typename U1, typename U2>
	Values(U1&& v1, U2&& v2) : scalar{std::forward<U1>(v1)}, array{std::forward<U2>(v2)} {}
	std::optional<T1> scalar;
	std::optional<std::vector<T2>> array;
};

template <typename T1, typename T2>
static void fillRndValue(Values<T1, T2>& value) {
	value.scalar = (rand() % 10 == 0) ? std::nullopt : std::optional{T1{randStrUuid()}};
	if (std::is_same_v<T1, std::string> || (rand() % 10 != 0)) {  // TODO maybe allow to convert empty string into nil uuid?
		value.scalar = T1{randStrUuid()};
	} else {
		value.scalar = std::nullopt;
	}
	if (rand() % 10 == 0) {
		value.array = std::nullopt;
	} else {
		value.array = std::vector<T2>{};
		const int s = rand() % 10;
		value.array->reserve(s);
		for (int j = 0; j < s; ++j) {
			value.array->emplace_back(randStrUuid());
		}
	}
}

template <typename T1, typename T2>
static void fillItemThroughJson(reindexer::Item& item, int id, Values<T1, T2>& value) {
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	fillRndValue(value);
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder builder{ser};
		builder.Put("id", id);
		if (value.scalar) {
			builder.Put("uuid", std::string{*value.scalar});
		}
		if (value.array) {
			auto arr = builder.Array("uuid_a");
			for (const auto& uuid : *value.array) {
				arr.Put(reindexer::TagName::Empty(), std::string{uuid});
			}
		} else if (rand() % 2 == 0) {
			auto arr = builder.Array("uuid_a");
		}
	}
	const auto err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
}

template <typename T1, typename T2>
static void fillItem(reindexer::Item& item, int id, Values<T1, T2>& value) {
	if (rand() % 3 == 0) {
		return fillItemThroughJson(item, id, value);
	}
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	fillRndValue(value);
	item["id"] = id;
	if (value.scalar) {
		if (std::is_same_v<T1, std::string> || rand() % 2 == 0) {
			item["uuid"] = *value.scalar;
		} else {
			item["uuid"] = std::string{*value.scalar};
		}
	}
	if (value.array) {
		if (std::is_same_v<T2, std::string> || rand() % 2 == 0) {
			item["uuid_a"] = *value.array;
		} else {
			std::vector<std::string> strArray;
			strArray.reserve(value.array->size());
			for (const T2& u : *value.array) {
				strArray.emplace_back(u);
			}
			item["uuid_a"] = strArray;
		}
	}
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
}

template <typename T>
static void init(reindexer::Reindexer& rx, std::vector<Values<T, T>>& values, bool withCompositeIndex = true) {
	auto err = rx.Connect("builtin://");
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::NamespaceDef nsDef{nsName};
	nsDef.AddIndex("id", "hash", "int", IndexOpts{}.PK());
	if constexpr (std::is_same_v<T, std::string>) {
		nsDef.AddIndex("uuid", "hash", "string");
		nsDef.AddIndex("uuid_a", "hash", "string", IndexOpts{}.Array());
	} else {
		nsDef.AddIndex("uuid", "hash", "uuid");
		nsDef.AddIndex("uuid_a", "hash", "uuid", IndexOpts{}.Array());
	}
	if (withCompositeIndex) {
		nsDef.AddIndex("id_uuid", {"id", "uuid"}, "hash", "composite", IndexOpts{});
	}
	err = rx.AddNamespace(nsDef);
	ASSERT_TRUE(err.ok()) << err.what();

	values.resize(kItemsCount);
	for (int i = 0; i < kItemsCount; ++i) {
		auto item = rx.NewItem(nsName);
		fillItem(item, i, values[i]);

		err = rx.Insert(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	}
}

template <typename T>
static void initWithoutIndexes(reindexer::Reindexer& rx, std::vector<Values<T, T>>& values) {
	auto err = rx.Connect("builtin://");
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::NamespaceDef nsDef{nsName};
	nsDef.AddIndex("id", "hash", "int", IndexOpts{}.PK());
	err = rx.AddNamespace(nsDef);
	ASSERT_TRUE(err.ok()) << err.what();

	values.resize(kItemsCount);
	for (int i = 0; i < kItemsCount; ++i) {
		auto item = rx.NewItem(nsName);
		fillItemThroughJson(item, i, values[i]);

		err = rx.Insert(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	}
}

template <typename>
struct TypeToKVT;

template <>
struct [[nodiscard]] TypeToKVT<std::string> {
	using type = reindexer::KeyValueType::String;
};

template <>
struct [[nodiscard]] TypeToKVT<reindexer::Uuid> {
	using type = reindexer::KeyValueType::Uuid;
};

enum class [[nodiscard]] EmptyValues { ShouldBeFilled, ShouldBeEmpty, CouldBeEmpty };  // TODO delete CouldBeEmpty after #1353

template <typename T1, typename T2>
static void test(reindexer::Reindexer& rx, const std::vector<Values<T1, T2>>& values,
				 EmptyValues emptyValue = EmptyValues::ShouldBeFilled) {
	reindexer::QueryResults qr;
	const auto err = rx.Select(reindexer::Query{nsName}, qr);
	ASSERT_TRUE(err.ok()) << err.what();

	EXPECT_EQ(qr.Count(), kItemsCount);
	assert(values.size() == kItemsCount);
	assert(values.size() >= qr.Count());
	size_t i = 0;
	for (auto it = qr.begin(), end = qr.end(); it != end; ++it, ++i) {
		const auto item = it.GetItem(false);
		const reindexer::VariantArray v = item["uuid"];
		if (values[i].scalar || emptyValue != EmptyValues::ShouldBeEmpty) {
			if (emptyValue == EmptyValues::ShouldBeFilled || !v.empty()) {
				EXPECT_EQ(v.size(), 1);
				if (!v.empty()) {
					EXPECT_TRUE(v[0].Type().Is<typename TypeToKVT<T1>::type>()) << v[0].Type().Name();
					if (values[i].scalar) {
						EXPECT_EQ(v[0].As<T1>(), *values[i].scalar) << i;  // NOLINT(bugprone-unchecked-optional-access)
					} else {
						EXPECT_TRUE(v[0].As<T1>() == T1{nilUUID} || v[0].As<T1>() == T1{})
							<< i << ' ' << v[0].As<T1>();  // TODO delete '|| v[0].As<T1>() == T1{}' after #1353
					}
				}
			}
		} else {
			EXPECT_TRUE(v.empty());
		}

		const reindexer::VariantArray va = item["uuid_a"];
		if (values[i].array) {
			EXPECT_EQ(va.size(), values[i].array->size()) << i;	 // NOLINT(bugprone-unchecked-optional-access)
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			for (size_t j = 0, s1 = std::min<size_t>(va.size(), values[i].array->size()); j < s1; ++j) {
				EXPECT_TRUE(va[j].Type().Is<typename TypeToKVT<T2>::type>()) << va[j].Type().Name();
				EXPECT_EQ(va[j].As<T2>(), (*values[i].array)[j]) << i << ' ' << j;	// NOLINT(bugprone-unchecked-optional-access)
			}
		} else {
			EXPECT_EQ(va.size(), 0) << i;
		}
	}
}

TEST(UUID, CreateIndex) {
	try {
		reindexer::Reindexer rx;
		std::vector<Values<reindexer::Uuid, reindexer::Uuid>> values;
		init(rx, values);
		test(rx, values);
	} catch (const reindexer::Error& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (const std::exception& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (...) {
		ASSERT_TRUE(false);
	}
}

TEST(UUID, UpdateItem) {
	try {
		reindexer::Reindexer rx;
		std::vector<Values<reindexer::Uuid, reindexer::Uuid>> values;
		init(rx, values);
		test(rx, values);

		for (int i = 0; i < kItemsCount; ++i) {
			auto item = rx.NewItem(nsName);
			const int id = rand() % kItemsCount;
			fillItem(item, id, values[id]);

			const auto err = rx.Update(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		}

		test(rx, values);
	} catch (const reindexer::Error& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (const std::exception& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (...) {
		ASSERT_TRUE(false);
	}
}

TEST(UUID, DropIndex) {
	try {
		reindexer::Reindexer rx;
		std::vector<Values<reindexer::Uuid, reindexer::Uuid>> uuidValues;
		init(rx, uuidValues);
		test(rx, uuidValues);

		auto err = rx.DropIndex(nsName, reindexer::IndexDef{"id_uuid"});
		ASSERT_TRUE(err.ok()) << err.what();
		test(rx, uuidValues);

		err = rx.DropIndex(nsName, reindexer::IndexDef{"uuid"});
		ASSERT_TRUE(err.ok()) << err.what();

		std::vector<Values<std::string, reindexer::Uuid>> testValues1;
		testValues1.reserve(uuidValues.size());
		for (const auto& value : uuidValues) {
			testValues1.emplace_back(value.scalar, value.array);
		}
		test(rx, testValues1, EmptyValues::CouldBeEmpty);

		err = rx.DropIndex(nsName, reindexer::IndexDef{"uuid_a"});
		ASSERT_TRUE(err.ok()) << err.what();

		std::vector<Values<std::string, std::string>> testValues2;
		testValues2.reserve(testValues1.size());
		for (const auto& value : testValues1) {
			if (value.array) {
				std::vector<std::string> strUuidArray;
				strUuidArray.reserve(value.array->size());
				for (auto uuid : *value.array) {
					strUuidArray.emplace_back(uuid);
				}
				testValues2.emplace_back(value.scalar, std::move(strUuidArray));
			} else {
				testValues2.emplace_back(value.scalar, std::nullopt);
			}
		}
		test(rx, testValues2, EmptyValues::CouldBeEmpty);

	} catch (const reindexer::Error& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (const std::exception& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (...) {
		ASSERT_TRUE(false);
	}
}

TEST(UUID, AddIndex) {
	try {
		reindexer::Reindexer rx;
		std::vector<Values<reindexer::Uuid, reindexer::Uuid>> uuidValues;
		initWithoutIndexes(rx, uuidValues);

		std::vector<Values<std::string, std::string>> testValues1;
		testValues1.reserve(uuidValues.size());
		for (const auto& value : uuidValues) {
			if (value.array) {
				std::vector<std::string> strValues;
				strValues.reserve(value.array->size());
				for (auto uuid : *value.array) {
					strValues.emplace_back(uuid);
				}
				testValues1.emplace_back(value.scalar, std::move(strValues));
			} else {
				testValues1.emplace_back(value.scalar, std::nullopt);
			}
		}
		test(rx, testValues1, EmptyValues::ShouldBeEmpty);

		auto err = rx.AddIndex(nsName, reindexer::IndexDef{"uuid", "hash", "uuid", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();

		std::vector<Values<reindexer::Uuid, std::string>> testValues2;
		testValues2.reserve(uuidValues.size());
		for (size_t i = 0, s = uuidValues.size(); i < s; ++i) {
			testValues2.emplace_back(uuidValues[i].scalar, testValues1[i].array);
		}
		test(rx, testValues2);

		err = rx.AddIndex(nsName, reindexer::IndexDef{"id_uuid", {"id", "uuid"}, "hash", "composite", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();
		test(rx, testValues2);

		err = rx.AddIndex(nsName, reindexer::IndexDef{"uuid_a", "hash", "uuid", IndexOpts().Array()});
		ASSERT_TRUE(err.ok()) << err.what();
		test(rx, uuidValues);
	} catch (const reindexer::Error& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (const std::exception& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (...) {
		ASSERT_TRUE(false);
	}
}

TEST(UUID, UpdateIndexUuidToString) {
	try {
		reindexer::Reindexer rx;
		std::vector<Values<reindexer::Uuid, reindexer::Uuid>> uuidValues;
		init(rx, uuidValues, false);
		test(rx, uuidValues);

		auto err = rx.UpdateIndex(nsName, reindexer::IndexDef{"uuid", "hash", "string", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();

		std::vector<Values<std::string, reindexer::Uuid>> testValues1;
		testValues1.reserve(uuidValues.size());
		for (const auto& value : uuidValues) {
			testValues1.emplace_back(value.scalar, value.array);
		}
		test(rx, testValues1);

		err = rx.UpdateIndex(nsName, reindexer::IndexDef{"uuid_a", "hash", "string", IndexOpts().Array()});
		ASSERT_TRUE(err.ok()) << err.what();

		std::vector<Values<std::string, std::string>> testValues2;
		testValues2.reserve(testValues1.size());
		for (const auto& value : testValues1) {
			if (value.array) {
				std::vector<std::string> strUuidArray;
				strUuidArray.reserve(value.array->size());
				for (auto uuid : *value.array) {
					strUuidArray.emplace_back(uuid);
				}
				testValues2.emplace_back(value.scalar, std::move(strUuidArray));
			} else {
				testValues2.emplace_back(value.scalar, std::nullopt);
			}
		}
		test(rx, testValues2);

	} catch (const reindexer::Error& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (const std::exception& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (...) {
		ASSERT_TRUE(false);
	}
}

TEST(UUID, UpdateIndexStringToUuid) {
	try {
		reindexer::Reindexer rx;
		std::vector<Values<std::string, std::string>> strUuidValues;
		init(rx, strUuidValues, false);
		test(rx, strUuidValues);

		auto err = rx.UpdateIndex(nsName, reindexer::IndexDef{"uuid", "hash", "uuid", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();

		std::vector<Values<reindexer::Uuid, std::string>> testValues1;
		testValues1.reserve(strUuidValues.size());
		for (const auto& value : strUuidValues) {
			testValues1.emplace_back(value.scalar, value.array);
		}
		test(rx, testValues1);

		err = rx.UpdateIndex(nsName, reindexer::IndexDef{"uuid_a", "hash", "uuid", IndexOpts().Array()});
		ASSERT_TRUE(err.ok()) << err.what();

		std::vector<Values<reindexer::Uuid, reindexer::Uuid>> testValues2;
		testValues2.reserve(testValues1.size());
		for (const auto& value : testValues1) {
			if (value.array) {
				std::vector<reindexer::Uuid> uuidArray;
				uuidArray.reserve(value.array->size());
				for (const auto& uuid : *value.array) {
					uuidArray.emplace_back(uuid);
				}
				testValues2.emplace_back(value.scalar, std::move(uuidArray));
			} else {
				testValues2.emplace_back(value.scalar, std::nullopt);
			}
		}
		test(rx, testValues2);

	} catch (const reindexer::Error& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (const std::exception& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (...) {
		ASSERT_TRUE(false);
	}
}

TEST(UUID, AddArrayUuidIndexOnNotArrayField) {
	try {
		reindexer::Reindexer rx;
		std::vector<Values<std::string, std::string>> strUuidValues;
		initWithoutIndexes(rx, strUuidValues);
		test(rx, strUuidValues);

		const auto err = rx.AddIndex(nsName, reindexer::IndexDef{"uuid", "hash", "uuid", IndexOpts().Array()});
		ASSERT_TRUE(err.ok()) << err.what();

		std::vector<Values<reindexer::Uuid, std::string>> testValues;
		testValues.reserve(strUuidValues.size());
		for (const auto& value : strUuidValues) {
			testValues.emplace_back(value.scalar, value.array);
		}
		test(rx, testValues);
	} catch (const reindexer::Error& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (const std::exception& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (...) {
		ASSERT_TRUE(false);
	}
}

TEST(UUID, AddNotArrayUuidIndexOnArrayField) {
	try {
		reindexer::Reindexer rx;
		std::vector<Values<std::string, std::string>> strUuidValues;
		initWithoutIndexes(rx, strUuidValues);
		test(rx, strUuidValues);

		const auto err = rx.AddIndex(nsName, reindexer::IndexDef{"uuid_a", "hash", "uuid", IndexOpts()});
		ASSERT_FALSE(err.ok());
		ASSERT_THAT(err.what(), testing::MatchesRegex(".*: Cannot convert array field to not array UUID"));

		test(rx, strUuidValues);
	} catch (const std::exception& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	}
}
