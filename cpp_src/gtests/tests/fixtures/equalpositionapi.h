#pragma once

#include "reindexer_api.h"

class [[nodiscard]] EqualPositionApi : public ReindexerApi {
public:
	void SetUp() override {
		ReindexerApi::SetUp();
		rt.OpenNamespace(default_namespace);
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{kFieldA1, "hash", "int", IndexOpts().Array(), 0},
												   IndexDeclaration{kFieldA2, "tree", "int", IndexOpts().Array(), 0},
												   IndexDeclaration{kFieldA3, "hash", "int", IndexOpts().Array(), 0}});
		fillNs();
	}

protected:
	void fillNs() {
		int initValue = 100;
		auto a1Val = randIntVec(10, initValue * 1, 5);
		auto a2Val = randIntVec(10, initValue * 2, 5);
		auto a3Val = randIntVec(10, initValue * 3, 5);
		for (size_t i = 0; i < 100; ++i) {
			Item item = NewItem(default_namespace);
			item[kFieldId] = static_cast<int>(i);
			item[kFieldA1] = a1Val;
			item[kFieldA2] = a2Val;
			item[kFieldA3] = a3Val;
			if (i % 20 == 0) {
				initValue += 10;
				a1Val = randIntVec(10, initValue * 1, 5);
				a2Val = randIntVec(10, initValue * 2, 5);
				a3Val = randIntVec(10, initValue * 3, 5);
			}
			Upsert(default_namespace, item);
		}
	}

	std::vector<int> randIntVec(int length, int initVal, int multipleCond) {
		int val = initVal;
		std::vector<int> vec;
		vec.reserve(length);
		for (size_t i = 0; i < static_cast<size_t>(length); ++i) {
			vec.emplace_back(val);
			if (i % multipleCond) {
				val += initVal;
			}
		}
		return vec;
	}

	const char* kFieldId = "id";
	const char* kFieldA1 = "a1";
	const char* kFieldA2 = "a2";
	const char* kFieldA3 = "a3";
};
