#ifdef WITH_PROTOBUF

#include "conversion.pb.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "easyarrays.pb.h"
#include "reindexer_api.h"
#include "schema.pb.h"

using reindexer::string_view;

const int64_t KIdValue = 13;
const string kNameValue = "John White Snow";
const int64_t kAgeValue = 21;
const string kEmailValue = "john_white_as_hell@mail.ru";
const int64_t kWeightValue = 95;
const string kNumberValue = "8-800-2000-600";
const int64_t kTypeValue = 1;
const string kCityValue = "Mapletown";
const string kStreetValue = "Miracle Street, ";
const string kPostalCodeValue = "9745 123 ";
const double kSalaryValue = 11238761238768.232342342;

TEST_F(ReindexerApi, ProtobufConvesrionTest) {
	// clang-froamt off
	const string schema = R"z({
                            "type": "object",
                            "required": [
                              "id",
                              "numbers"
                            ],
                            "properties": {
                              "id": {
                                "type": "integer"
                              },
                              "numbers": {
                                "items": {
                                  "type": "integer"
                                },
                                "type": "array"
                              }
                            }
                          })z";
	// clang-format on

	const string_view nsName = "conversion_namespace";
	Error err = rt.reindexer->OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->SetSchema(nsName, schema);
	ASSERT_TRUE(err.ok()) << err.what();

	std::string protobufSchema;
	err = rt.reindexer->GetSchema(nsName, ProtobufSchemaType, protobufSchema);
	ASSERT_TRUE(err.ok()) << err.what();

	vector<double> numbers;

	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder jsonBuilder(wrser);
	jsonBuilder.Put("id", 1.1111f);
	{
		auto nums = jsonBuilder.Array("numbers");
		for (int i = 0; i < 10; ++i) {
			numbers.emplace_back(double(rand() + 10 + i) + 0.11111f);
			nums.Put(0, numbers.back());
		}
	}
	jsonBuilder.End();

	Item item = rt.reindexer->NewItem(nsName);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::WrSerializer rrser;
	err = item.GetProtobuf(rrser);
	ASSERT_TRUE(err.ok()) << err.what();

	conversion_namespace testNs;
	ASSERT_TRUE(testNs.ParseFromArray(rrser.Buf(), rrser.Len()));

	ASSERT_TRUE(testNs.id() == 1) << testNs.id();
	ASSERT_TRUE(testNs.numbers_size() == int(numbers.size())) << testNs.numbers_size();
	for (size_t i = 0; i < numbers.size(); ++i) {
		ASSERT_TRUE(testNs.numbers(i) == int64_t(numbers[i]));
	}
}

TEST_F(ReindexerApi, ProtobufEasyArrayTest) {
	// clang-froamt off
	const string schema = R"z(
                               {
                                   "type": "object",
                                   "required": [
                                       "id",
                                       "object_of_array"
                                   ],
                                   "properties": {
                                       "id": {
                                           "type": "integer"
                                       },
                                       "object_of_array": {
                                           "additionalProperties": false,
                                           "type": "object",
                                           "required": ["nums"],
                                           "properties": {
                                               "nums": {
                                                   "items": {
                                                       "type": "integer"
                                                   },
                                                   "type": "array"
                                               },
                                               "strings": {
                                                   "type": "array",
                                                   "items": {
                                                       "type": "string"
                                                   }
                                               }
                                           }
                                       }
                                   }
                               }
                               )z";
	// clang-format on
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, reindexer::IndexDef("id", {"id"}, "hash", "int", IndexOpts().PK()));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->SetSchema(default_namespace, schema);
	ASSERT_TRUE(err.ok()) << err.what();

	std::string protobufSchema;
	err = rt.reindexer->GetSchema(default_namespace, ProtobufSchemaType, protobufSchema);
	ASSERT_TRUE(err.ok()) << err.what();

	vector<int> numVals;
	vector<string> stringVals;

	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder jsonBuilder(wrser);
	jsonBuilder.Put("id", 1);
	{
		auto nested = jsonBuilder.Object("object_of_array");
		{
			auto nums = nested.Array("nums");
			for (int i = 0; i < 10; ++i) {
				numVals.emplace_back(rand() + 10 + i);
				nums.Put(0, numVals.back());
			}
		}

		{
			auto strings = nested.Array("strings");
			for (int i = 0; i < 10; ++i) {
				stringVals.emplace_back(RandString());
				strings.Put(0, stringVals.back());
			}
		}
	}
	jsonBuilder.End();

	Item item = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::WrSerializer rrser;
	err = item.GetProtobuf(rrser);
	ASSERT_TRUE(err.ok()) << err.what();

	Item item2 = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item2.Status().ok()) << item2.Status().what();
	err = item2.FromProtobuf(rrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what() << wrser.Slice();
	ASSERT_TRUE(item.GetJSON() == item2.GetJSON()) << item.GetJSON() << std::endl << std::endl << item2.GetJSON() << std::endl;

	test_namespace testNs;
	ASSERT_TRUE(testNs.ParseFromArray(rrser.Buf(), rrser.Len()));

	EXPECT_TRUE(testNs.id() == 1);
	EXPECT_TRUE(testNs.object_of_array().strings().size() == int(stringVals.size()));
	for (size_t i = 0; i < stringVals.size(); ++i) {
		EXPECT_TRUE(testNs.object_of_array().strings(i) == stringVals[i]);
	}
	EXPECT_TRUE(testNs.object_of_array().nums().size() == int(numVals.size()));
	for (size_t i = 0; i < numVals.size(); ++i) {
		EXPECT_TRUE(testNs.object_of_array().nums(i) == numVals[i]);
	}
}

TEST_F(ReindexerApi, ProtobufSchemaFromNsSchema) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	// clang-format off
    const std::string jsonschema = R"xxx(
                                   {
                                     "required": [
                                       "Collection",
                                       "floatField",
                                       "intField",
                                       "stringField",
                                       "boolField",
                                       "nested1",
                                       "nested2",
                                       "nested3"
                                     ],
                                     "properties": {
                                       "Collection": {
                                         "items": {
                                           "type": "integer"
                                         },
                                         "type": "array"
                                       },
                                       "floatField": {
                                         "type": "number"
                                       },
                                       "intField": {
                                         "type": "integer"
                                       },
                                       "stringField": {
                                         "type": "string"
                                       },
                                       "boolField": {
                                         "type": "boolean"
                                       },
                                       "nested3": {
                                         "required": [
                                           "bigField",
                                           "biggerField",
                                           "hugeField"
                                         ],
                                         "properties": {
                                           "bigField": {
                                             "type": "string"
                                           },
                                           "biggerField": {
                                             "type": "number"
                                           },
                                           "hugeField": {
                                             "type": "integer"
                                           }
                                         },
                                         "additionalProperties": false,
                                         "type": "object",
                                         "x-go-type": "NestedStruct3"
                                       },
                                       "nested1": {
                                         "required": [
                                           "field1",
                                           "field2",
                                           "field3",
                                           "nested2"
                                         ],
                                         "properties": {
                                           "field1": {
                                             "type": "string"
                                           },
                                           "field2": {
                                             "type": "number"
                                           },
                                           "field3": {
                                             "type": "integer"
                                           },
                                           "nested2": {
                                             "required": [
                                               "field4",
                                               "field5",
                                               "field6",
                                               "oneMoreNested"
                                             ],
                                             "properties": {
                                               "field4": {
                                                 "type": "string"
                                               },
                                               "field5": {
                                                 "type": "number"
                                               },
                                               "field6": {
                                                 "type": "integer"
                                               },
                                               "oneMoreNested": {
                                                 "required": [
                                                   "one",
                                                   "two",
                                                   "three",
                                                   "four"
                                                 ],
                                                 "properties": {
                                                   "one": {
                                                     "type": "integer"
                                                   },
                                                   "two": {
                                                     "type": "number"
                                                   },
                                                   "three": {
                                                     "type": "boolean"
                                                   },
                                                   "four": {
                                                     "type": "array",
                                                     "items": {
                                                       "required": [
                                                         "bigField",
                                                         "biggerField",
                                                         "hugeField"
                                                       ],
                                                       "properties": {
                                                         "bigField": {
                                                           "type": "string"
                                                         },
                                                         "biggerField": {
                                                           "type": "number"
                                                         },
                                                         "hugeField": {
                                                           "type": "integer"
                                                         }
                                                       },
                                                       "additionalProperties": false,
                                                       "type": "object",
                                                       "x-go-type": "NestedStruct3"
                                                     }
                                                   }
                                                 },
                                                 "additionalProperties": false,
                                                 "type": "object",
                                                 "x-go-type": "NNested"
                                               }
                                             },
                                             "additionalProperties": false,
                                             "type": "object"
                                           }
                                         },
                                         "additionalProperties": false,
                                         "type": "object"
                                       }
                                     },
                                     "additionalProperties": false,
                                     "type": "object"
                                   }    )xxx";
	// clang-format on

	err = rt.reindexer->SetSchema(default_namespace, jsonschema);
	ASSERT_TRUE(err.ok()) << err.what();

	std::string protobufSchema;
	err = rt.reindexer->GetSchema(default_namespace, ProtobufSchemaType, protobufSchema);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder jsonBuilder(wrser);
	jsonBuilder.Put("floatField", 5.55f);
	jsonBuilder.Put("intField", 5);
	jsonBuilder.Put("stringField", "five");
	jsonBuilder.Put("boolField", true);
	{
		auto nested3 = jsonBuilder.Object("nested3");
		nested3.Put("bigField", "big big real big");
		nested3.Put("biggerField", 77.77);
		nested3.Put("hugeField", 33);
	}
	{
		auto nested1 = jsonBuilder.Object("nested1");
		nested1.Put("field1", "one");
		nested1.Put("field2", 222.222);
		nested1.Put("field3", 333);
		{
			auto nested2 = nested1.Object("nested2");
			nested2.Put("field4", "four");
			nested2.Put("field5", 55.55);
			nested2.Put("field6", 66);
			{
				auto oneMoreNested = nested2.Object("oneMoreNested");
				oneMoreNested.Put("one", 1);
				oneMoreNested.Put("two", 2.22);
				oneMoreNested.Put("three", true);
				{
					auto four = oneMoreNested.Array("four");
					for (size_t i = 0; i < 10; ++i) {
						auto item = four.Object(0);
						item.Put("bigField", RandString());
						item.Put("biggerField", double(11.11 + rand()));
						item.Put("hugeField", int(33 + rand()));
					}
				}
			}
		}
	}
	auto collection = jsonBuilder.Array("Collection");
	for (int i = 0; i < 10; ++i) collection.Put(0, i);
	collection.End();
	jsonBuilder.End();

	Item item = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::WrSerializer rrser;
	err = item.GetProtobuf(rrser);
	ASSERT_TRUE(err.ok()) << err.what();

	Item item2 = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item2.Status().ok()) << item2.Status().what();
	err = item2.FromProtobuf(rrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(item.GetJSON() == item2.GetJSON());
}

TEST_F(ReindexerApi, ProtobufEncodingTest) {
	Person person;
	person.set_id(KIdValue);
	person.set_name(kNameValue);
	person.set_age(kAgeValue);
	person.set_email(kEmailValue);
	person.set_weight(kWeightValue);
	person.set_salary(kSalaryValue);

	Person::Address* address = person.mutable_address();
	for (size_t j = 0; j < 10; ++j) {
		Person::Address::PhoneNumber* phoneNumber = address->add_phones();
		phoneNumber->set_number(kNumberValue + std::to_string(j));
		phoneNumber->set_type(kTypeValue + j);
	}
	for (size_t j = 0; j < 20; ++j) {
		Person::Address::Home* home = address->add_homes();
		home->set_city(kCityValue + std::to_string(j));
		home->set_street(kStreetValue + std::to_string(j));
	}
	for (size_t i = 0; i < 20; ++i) {
		string* postalCodeItem = address->add_postalcodes();
		*postalCodeItem = kPostalCodeValue + std::to_string(i);
	}
	for (size_t i = 0; i < 5; ++i) {
		person.add_friends(i);
	}
	for (size_t i = 0; i < 10; ++i) {
		person.add_bonuses(i);
	}
	for (int i = 0; i < 10; ++i) {
		person.add_indexedpackeddouble(double(i) + 0.55f);
	}
	for (int i = 0; i < 10; ++i) {
		person.add_indexedunpackeddouble(std::to_string(i + 5));
	}
	person.set_enabled(true);

	reindexer::WrSerializer wrser;
	reindexer::ProtobufBuilder builder(&wrser);
	builder.Put(person.kNameFieldNumber, kNameValue);
	builder.Put(person.kIdFieldNumber, KIdValue);
	builder.Put(person.kAgeFieldNumber, kAgeValue);
	builder.Put(person.kWeightFieldNumber, kWeightValue);
	builder.Put(person.kEmailFieldNumber, kEmailValue);

	auto addressBuilder = builder.Object(person.kAddressFieldNumber);
	auto phones = addressBuilder.ArrayNotPacked(address->kPhonesFieldNumber);
	for (size_t i = 0; i < 10; ++i) {
		auto phone = phones.Object(0);
		phone.Put(Person_Address_PhoneNumber::kNumberFieldNumber, kNumberValue + std::to_string(i));
		phone.Put(Person_Address_PhoneNumber::kTypeFieldNumber, int64_t(kTypeValue + i));
	}
	phones.End();
	auto homes = addressBuilder.ArrayNotPacked(address->kHomesFieldNumber);
	for (size_t i = 0; i < 20; ++i) {
		auto home = homes.Object(0);
		home.Put(Person_Address_Home::kCityFieldNumber, kCityValue + std::to_string(i));
		home.Put(Person_Address_Home::kStreetFieldNumber, kStreetValue + std::to_string(i));
	}
	homes.End();
	auto postalCodes = addressBuilder.ArrayNotPacked(address->kPostalcodesFieldNumber);
	for (size_t i = 0; i < 20; ++i) {
		postalCodes.Put(0, kPostalCodeValue + std::to_string(i));
	}
	postalCodes.End();
	addressBuilder.End();
	auto friends = builder.ArrayNotPacked(person.kFriendsFieldNumber);
	for (int64_t i = 0; i < 5; ++i) {
		friends.Put(0, i);
	}
	friends.End();
	builder.Put(person.kSalaryFieldNumber, kSalaryValue);
	auto bonuses = builder.ArrayPacked(person.kBonusesFieldNumber);
	for (int64_t i = 0; i < 10; ++i) {
		bonuses.Put(person.kBonusesFieldNumber, i);
	}
	bonuses.End();

	auto indexedPackedDouble = builder.ArrayPacked(person.kIndexedPackedDoubleFieldNumber);
	for (int i = 0; i < 10; ++i) {
		indexedPackedDouble.Put(person.kIndexedPackedDoubleFieldNumber, double(i) + 0.55f);
	}
	indexedPackedDouble.End();

	auto indexedUnpackedDouble = builder.ArrayNotPacked(person.kIndexedUnpackedDoubleFieldNumber);
	for (int i = 0; i < 10; ++i) {
		indexedUnpackedDouble.Put(0, std::to_string(5 + i));
	}
	indexedUnpackedDouble.End();

	builder.Put(person.kEnabledFieldNumber, true);

	builder.End();

	Person person2;
	person2.ParseFromArray(wrser.Buf(), wrser.Len());
	EXPECT_TRUE(person.id() == person2.id());
	EXPECT_TRUE(person.name() == person2.name());
	EXPECT_TRUE(person.age() == person2.age());
	EXPECT_TRUE(person.email() == person2.email());
	EXPECT_TRUE(person.weight() == person2.weight());
	EXPECT_TRUE(person.salary() == person2.salary());
	ASSERT_TRUE(person.address().homes_size() == person2.address().homes_size());
	for (int j = 0; j < person.address().homes_size(); ++j) {
		const auto& home = person.address().homes(j);
		const auto& home2 = person2.address().homes(j);
		EXPECT_TRUE(home.city() == home2.city());
		EXPECT_TRUE(home.street() == home2.street());
	}
	ASSERT_TRUE(person.address().phones_size() == person2.address().phones_size());
	for (int j = 0; j < person.address().phones_size(); ++j) {
		const auto& phone = person.address().phones(j);
		const auto& phone2 = person2.address().phones(j);
		EXPECT_TRUE(phone.number() == phone2.number());
		EXPECT_TRUE(phone.type() == phone2.type());
	}
	ASSERT_TRUE(person.address().postalcodes_size() == person2.address().postalcodes_size());
	for (int j = 0; j < person.address().postalcodes_size(); ++j) {
		EXPECT_TRUE(person.address().postalcodes(j) == person2.address().postalcodes(j));
	}
	ASSERT_TRUE(person.friends_size() == person2.friends_size());
	for (int j = 0; j < person.friends_size(); ++j) {
		EXPECT_TRUE(person.friends(j) == person2.friends(j));
	}
	ASSERT_TRUE(person.bonuses_size() == person2.bonuses_size());
	for (int j = 0; j < person.bonuses_size(); ++j) {
		EXPECT_TRUE(person.bonuses(j) == person2.bonuses(j));
	}
	ASSERT_TRUE(person.indexedpackeddouble_size() == person2.indexedpackeddouble_size());
	for (int j = 0; j < person.indexedpackeddouble_size(); ++j) {
		EXPECT_TRUE(person.indexedpackeddouble(j) == person2.indexedpackeddouble(j));
	}
	ASSERT_TRUE(person.indexedunpackeddouble_size() == person2.indexedunpackeddouble_size());
	for (int j = 0; j < person.indexedunpackeddouble_size(); ++j) {
		EXPECT_TRUE(person.indexedunpackeddouble(j) == person2.indexedunpackeddouble(j));
	}
	ASSERT_TRUE(person.enabled() == person2.enabled());
}

TEST_F(ReindexerApi, ProtobufDecodingTest) {
	// clang-format off
    const string jsonSchema = R"xxx(
                          {
                            "required": [
                              "name",
                              "id",
                              "age",
                              "weight",
                              "email",
                              "address",
                              "friends",
                              "salary",
                              "bonuses",
                              "indexedPackedDouble",
                              "indexedUnpackedDouble",
                              "enabled"
                            ],
                            "properties": {
                              "name": {
                                "type": "string"
                              },
                              "id": {
                                "type": "integer"
                              },
                              "age": {
                                "type": "integer"
                              },
                              "weight": {
                                "type": "integer"
                              },
                              "email": {
                                "type": "string"
                              },
                              "address": {
                                "required": [
                                  "phones",
                                  "homes",
                                  "postalcodes"
                                ],
                                "properties": {
                                  "phones": {
                                    "items": {
                                      "type": "object",
                                      "required": [
                                        "number",
                                        "type"
                                      ],
                                      "properties": {
                                        "number": {
                                          "type": "string"
                                        },
                                        "type": {
                                          "type": "integer"
                                        }
                                      },
                                      "additionalProperties": false,
                                      "x-go-type": "PhoneNumber"
                                    },
                                    "type": "array"
                                  },
                                  "homes": {
                                    "items": {
                                      "type": "object",
                                      "required": [
                                        "city",
                                        "street"
                                      ],
                                      "properties": {
                                        "city": {
                                          "type": "string"
                                        },
                                        "street": {
                                          "type": "string"
                                        }
                                      },
                                      "additionalProperties": false,
                                      "x-go-type": "Home"
                                    },
                                    "type": "array"
                                  },
                                  "postalcodes": {
                                    "items": {
                                      "type": "string"
                                    },
                                    "type": "array"
                                  }
                                },
                                "additionalProperties": false,
                                "type": "object",
                                "x-go-type": "Address"
                              },
                              "friends": {
                                "items": {
                                  "type": "integer"
                                },
                                "type": "array"
                              },
                              "salary": {
                                "type": "number"
                              },
                              "bonuses": {
                                "items": {
                                  "type": "integer"
                                },
                                "type": "array"
                              },
                              "indexedPackedDouble": {
                                "items": {
                                  "type": "number"
                                },
                                "type": "array"
                              },
                              "indexedUnpackedDouble": {
                                "items": {
                                  "type": "string"
                                },
                                "type": "array"
                              },
                              "enabled": {
                                "type": "boolean"
                              }
                            },
                            "additionalProperties": false,
                            "type": "object",
                            "x-go-type": "TestStruct"
                          }
                          )xxx";
	// clang-format on

	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(
		default_namespace, reindexer::IndexDef("indexedPackedDouble", {"indexedPackedDouble"}, "tree", "double", IndexOpts().Array()));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(
		default_namespace, reindexer::IndexDef("indexedUnpackedDouble", {"indexedUnpackedDouble"}, "tree", "string", IndexOpts().Array()));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->SetSchema(default_namespace, jsonSchema);
	ASSERT_TRUE(err.ok()) << err.what();

	std::string protobufSchema;
	err = rt.reindexer->GetSchema(default_namespace, ProtobufSchemaType, protobufSchema);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::Item nsItem = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(nsItem.Status().ok()) << nsItem.Status().what();

	reindexer::WrSerializer wrser;
	reindexer::ProtobufBuilder builder(&wrser);
	builder.Put(nsItem.GetFieldTag("name"), kNameValue);
	builder.Put(nsItem.GetFieldTag("id"), KIdValue);
	builder.Put(nsItem.GetFieldTag("age"), kAgeValue);
	builder.Put(nsItem.GetFieldTag("weight"), kWeightValue);
	builder.Put(nsItem.GetFieldTag("email"), kEmailValue);

	auto addressBuilder = builder.Object(nsItem.GetFieldTag("address"));
	auto phones = addressBuilder.ArrayNotPacked(nsItem.GetFieldTag("phones"));
	for (size_t i = 0; i < 10; ++i) {
		auto phone = phones.Object(0);
		phone.Put(nsItem.GetFieldTag("number"), kNumberValue + std::to_string(i));
		phone.Put(nsItem.GetFieldTag("type"), int64_t(kTypeValue + i));
	}
	phones.End();
	auto homes = addressBuilder.ArrayNotPacked(nsItem.GetFieldTag("homes"));
	for (size_t i = 0; i < 20; ++i) {
		auto home = homes.Object(0);
		home.Put(nsItem.GetFieldTag("city"), kCityValue + std::to_string(i));
		home.Put(nsItem.GetFieldTag("street"), kStreetValue + std::to_string(i));
	}
	homes.End();
	auto postalCodes = addressBuilder.ArrayNotPacked(nsItem.GetFieldTag("postalcodes"));
	for (size_t i = 0; i < 20; ++i) {
		postalCodes.Put(0, kPostalCodeValue + std::to_string(i));
	}
	postalCodes.End();
	addressBuilder.End();
	auto friends = builder.ArrayNotPacked(nsItem.GetFieldTag("friends"));
	for (int64_t i = 0; i < 5; ++i) {
		friends.Put(0, i);
	}
	friends.End();
	builder.Put(nsItem.GetFieldTag("salary"), kSalaryValue);
	auto bonuses = builder.ArrayPacked(nsItem.GetFieldTag("bonuses"));
	for (int64_t i = 0; i < 10; ++i) {
		bonuses.Put(9, i);
	}
	bonuses.End();

	auto indexedPackedDouble = builder.ArrayPacked(nsItem.GetFieldTag("indexedPackedDouble"));
	for (int i = 0; i < 10; ++i) {
		indexedPackedDouble.Put(0, double(i) + 0.55f);
	}
	indexedPackedDouble.End();

	vector<string> strings;
	auto indexedUnpackedDouble = builder.ArrayNotPacked(nsItem.GetFieldTag("indexedUnpackedDouble"));
	for (int i = 0; i < 10; ++i) {
		strings.emplace_back(string("BIG_DATA") + std::to_string(i + 1));
		indexedUnpackedDouble.Put(0, strings.back());
	}
	indexedUnpackedDouble.End();

	builder.Put(nsItem.GetFieldTag("enabled"), false);

	builder.End();

	Item item1 = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item1.Status().ok()) << item1.Status().what();

	err = item1.FromProtobuf(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();

	Item item2 = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item2.Status().ok()) << item2.Status().what();

	err = item2.FromJSON(item1.GetJSON());
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(item1.GetJSON() == item2.GetJSON());

	Item item3 = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item3.Status().ok()) << item3.Status().what();

	reindexer::WrSerializer protobufSer;
	err = item2.GetProtobuf(protobufSer);
	ASSERT_TRUE(err.ok()) << err.what();

	err = item3.FromProtobuf(protobufSer.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(item2.GetJSON() == item3.GetJSON());
}

#endif
