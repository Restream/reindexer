#pragma once

#include <cassert>
#include <random>
#include <unordered_set>
#include "core/type_consts.h"
#include "tools/assertrx.h"

namespace reindexer {

class Query;
class Uuid;
class KeyValueType;

}  // namespace reindexer

namespace fuzzing {

struct Index;
class NsScheme;

enum class FieldType { Bool, Int, Int64, Double, String, Uuid, Point, Struct, END = Struct };
reindexer::KeyValueType ToKeyValueType(FieldType);
std::ostream& operator<<(std::ostream&, FieldType);
using FieldPath = std::vector<size_t>;

class RandomGenerator {
	using ErrFactorInt = uint32_t;

public:
	using ErrFactorType = std::pair<ErrFactorInt, ErrFactorInt>;
	RandomGenerator(std::ostream&, ErrFactorType errorFactor);
	RandomGenerator(std::istream&);

	size_t FieldsCount(bool firstLevel) {
		if (RndErr()) {
			enum Err : uint8_t { Zero, TooMany, END = TooMany };
			switch (RndWhich<Err, 1, 1>()) {
				case Zero:
					return 0;
				case TooMany:
					return RndInt(0, 10'000);
				default:
					assertrx(0);
			}
		}
		if (firstLevel) {
			enum Size : uint8_t { Normal, Long, END = Long };
			switch (RndWhich<Size, 10'000, 1>()) {
				case Normal:
					return RndInt(1, 9);
				case Long:
					return RndInt(10, 100);
				default:
					assertrx(false);
					std::abort();
			}
		}
		return RndInt(1, 5);
	}
	fuzzing::FieldType RndFieldType(unsigned level) {
		const bool withoutStruct = level > 2 && (level > 5 || !RndBool(1.0 / (2 << (2 * level))));
		return static_cast<fuzzing::FieldType>(
			RndInt(static_cast<int>(fuzzing::FieldType::Bool), static_cast<int>(fuzzing::FieldType::Struct) - withoutStruct));
	}
	fuzzing::FieldType RndFieldType() {
		return static_cast<fuzzing::FieldType>(
			RndInt(static_cast<int>(fuzzing::FieldType::Bool), static_cast<int>(fuzzing::FieldType::Point)));
	}
	fuzzing::FieldType RndPkIndexFieldType() {
		return static_cast<fuzzing::FieldType>(
			RndInt(static_cast<int>(fuzzing::FieldType::Int), static_cast<int>(fuzzing::FieldType::Uuid)));
	}
	std::string IndexFieldType(fuzzing::FieldType);
	fuzzing::FieldType RndFieldType(fuzzing::FieldType type) {
		if (RndErr()) {
			return RndWhich<fuzzing::FieldType, 1, 1, 1, 1, 1, 1, 1, 1>();
		}
		return type;
	}
	std::string RndIndexType(fuzzing::FieldType, bool pk);
	bool RndArrayField() { return RndBool(0.2); }
	bool RndArrayField(bool array) { return RndErr() ? !array : array; }
	size_t ArraySize() {
		if (RndErr()) return RndInt(0, 100'000);
		enum Size : uint8_t { Short, Normal, Long, VeryLong, END = VeryLong };
		switch (RndWhich<Size, 10'000, 100'000, 10, 1>()) {
			case Short:
				return RndInt(0, 5);
			case Normal:
				return RndInt(6, 20);
			case Long:
				return RndInt(21, 200);
			case VeryLong:
				return RndInt(201, 10'000);
			default:
				assertrx(false);
				std::abort();
		}
	}
	bool PkIndex(bool pk) { return RndErr() ? RndBool(0.5) : pk; }
	bool SparseIndex(bool pk) { return pk ? RndErr() : RndBool(0.2); }
	bool DenseIndex() { return RndBool(0.2); }
	int64_t ExpiredIndex() { return RndInt(0, 100'000); }  // TODO
	size_t IndexesCount() {
		if (RndErr()) {
			enum Err : uint8_t { Zero, TooMany, END = TooMany };
			switch (RndWhich<Err, 1, 1>()) {
				case Zero:
					return 0;
				case TooMany:
					return RndInt(0, 1'000);
				default:
					assertrx(0);
			}
		}
		enum Count : uint8_t { Few, Normal, Many, TooMany, END = TooMany };
		switch (RndWhich<Count, 500, 1'000, 10, 1>()) {
			case Few:
				return RndInt(1, 3);
			case Normal:
				return RndInt(4, 6);
			case Many:
				return RndInt(7, 20);
			case TooMany:
				return RndInt(21, 63);
			default:
				assertrx(false);
				std::abort();
		}
	}
	bool CompositeIndex() { return RndBool(0.2); }
	bool UniqueName() { return RndBool(0.5); }
	size_t CompositeIndexSize() {
		if (RndErr()) {
			enum Err : uint8_t { Zero, One, TooMany, END = TooMany };
			switch (RndWhich<Err, 1, 1, 1>()) {
				case Zero:
					return 0;
				case One:
					return 1;
				case TooMany:
					return RndInt(0, 10'000);
				default:
					assertrx(0);
			}
		}
		return RndInt(2, 5);
	}
	std::string FieldName(std::unordered_set<std::string>& generatedNames);
	std::string FieldName(const std::string& fieldName, std::unordered_set<std::string>& generatedNames) {
		if (RndErr()) return FieldName(generatedNames);
		return fieldName;
	}
	FieldPath RndField(const NsScheme&);
	FieldPath RndScalarField(const NsScheme&);
	std::string IndexName(std::unordered_set<std::string>& generatedNames) { return FieldName(generatedNames); }  // TODO
	std::string NsName(std::unordered_set<std::string>& generatedNames) { return FieldName(generatedNames); }	  // TODO
	std::string NsName(const std::string& nsName, std::unordered_set<std::string>& generatedNames) {
		if (RndErr()) return NsName(generatedNames);
		return nsName;
	}
	int RndInt(int min, int max) { return rndInt_(gen_, IntRndParams(min, max)); }
	size_t RndItemsCount() {
		enum Count : uint8_t { Short, Normal, Long, VeryLong, END = VeryLong };
		switch (RndWhich<Count, 100, 100'000, 100, 1>()) {
			case Short:
				return RndInt(0, 10);
			case Normal:
				return RndInt(11, 1'000);
			case Long:
				return RndInt(1'001, 100'000);
			case VeryLong:
				return RndInt(100'001, 10'000'000);
			default:
				assertrx(false);
				std::abort();
		}
	}
	template <typename T>
	static constexpr size_t itemsCount = (static_cast<size_t>(T::END) + 1);
	template <typename T, size_t... P>
	T RndWhich() {
		static_assert(sizeof...(P) == itemsCount<T>);
		static std::discrete_distribution<std::underlying_type_t<T>> rndDiscr({P...});
		return static_cast<T>(rndDiscr(gen_));
	}
	bool RndErr() {
		const bool err = rndError_(gen_, errParams_);
		if (err) {
			if (errFactor_.second == std::numeric_limits<ErrFactorInt>::max()) {
				errFactor_.first >>= 1;
			} else if (errFactor_.second < (std::numeric_limits<ErrFactorInt>::max() >> 1)) {
				errFactor_.second <<= 1;
			} else {
				errFactor_.second = std::numeric_limits<ErrFactorInt>::max();
			}
			errParams_ = {static_cast<double>(errFactor_.second - errFactor_.first), static_cast<double>(errFactor_.first)};
		}
		return err;
	}
	char RndChar() { return rndChar_(gen_); }
	bool NeedThisNode(bool sparse) { return sparse ? RndBool(0.5) : !RndErr(); }
	int RndIntValue() {
		enum Size : uint8_t { Short, Long, END = Long };
		switch (RndWhich<Size, 1, 1>()) {
			case Short:
				return RndInt(-50, 50);
			case Long:
				return RndInt(std::numeric_limits<int>::min(), std::numeric_limits<int>::max());
			default:
				assertrx(false);
				std::abort();
		}
	}
	int64_t RndInt64Value() {
		enum Size : uint8_t { Short, Long, END = Long };
		switch (RndWhich<Size, 1, 1>()) {
			case Short:
				return rndInt64(-50, 50);
			case Long:
				return rndInt64(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
			default:
				assertrx(false);
				std::abort();
		}
	}
	bool RndBool(double p) { return rndBool_(gen_, BoolRndParams{p}); }
	double RndDoubleValue() {
		enum Size : uint8_t { Short, Long, END = Long };
		switch (RndWhich<Size, 1, 1>()) {
			case Short:
				return rndDouble_(gen_, DoubleRndParams{0.0, 10.0});
			case Long:
				return rndDouble_(gen_, DoubleRndParams{0.0, 100'000.0});
			default:
				assertrx(false);
				std::abort();
		}
	}
	std::string RndStringValue() {
		static constexpr char availableChars[] = "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		static const IntRndParams availableRndParams{0, sizeof(availableChars) - 2};
		std::string res;
		enum Size : uint8_t { Short, Normal, Long, VeryLong, END = VeryLong };
		switch (RndWhich<Size, 10'000, 100'000, 10, 1>()) {
			case Short:
				res.resize(RndInt(0, 5));
				break;
			case Normal:
				res.resize(RndInt(6, 50));
				break;
			case Long:
				res.resize(RndInt(51, 1'000));
				break;
			case VeryLong:
				res.resize(RndInt(1'001, 100'000));
				break;
			default:
				assertrx(false);
				std::abort();
		}
		for (char& ch : res) ch = availableChars[rndInt(availableRndParams)];  // TODO
		return res;
	}
	std::string RndStrUuidValue() { return rndStrUuidValue(false); }
	template <typename Cont>
	auto& RndWhich(Cont& cont) {
		assertrx(!std::empty(cont));
		auto it = std::begin(cont);
		std::advance(it, rndSize(0, std::size(cont) - 1));
		return *it;
	}
	template <typename Cont>
	const auto& RndWhich(const Cont& cont) {
		assert(!std::empty(cont));
		auto it = std::begin(cont);
		std::advance(it, rndSize(0, std::size(cont) - 1));
		return *it;
	}
	void RndWhere(reindexer::Query&, const std::string& field, const std::vector<fuzzing::FieldType>&);

private:
	using IntRndParams = std::uniform_int_distribution<>::param_type;
	using SizeRndParams = std::uniform_int_distribution<size_t>::param_type;
	using Int64RndParams = std::uniform_int_distribution<int64_t>::param_type;
	using BoolRndParams = std::bernoulli_distribution::param_type;
	using DoubleRndParams = std::normal_distribution<double>::param_type;
	using ErrorParams = std::discrete_distribution<int>::param_type;

	int rndInt(IntRndParams params) { return rndInt_(gen_, params); }
	int64_t rndInt64(int64_t min, int64_t max) { return rndInt64_(gen_, Int64RndParams(min, max)); }
	size_t rndSize(size_t min, size_t max) { return rndSize_(gen_, SizeRndParams(min, max)); }
	CondType rndCond(fuzzing::FieldType);
	std::string rndStrUuidValue(bool noErrors);
	reindexer::Uuid rndUuidValue();

	std::default_random_engine gen_;
	ErrFactorType errFactor_;
	ErrorParams errParams_;
	std::uniform_int_distribution<> rndInt_;
	std::uniform_int_distribution<size_t> rndSize_;
	std::uniform_int_distribution<int64_t> rndInt64_;
	std::uniform_int_distribution<int> rndChar_ =
		std::uniform_int_distribution<int>{std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max()};
	std::bernoulli_distribution rndBool_;
	std::normal_distribution<double> rndDouble_;
	std::discrete_distribution<int> rndError_;
};

}  // namespace fuzzing
