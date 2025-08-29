#pragma once

#include <memory>
#include <optional>
#include <random>
#include <unordered_set>
#include "core/enums.h"
#include "core/type_consts.h"
#include "tools/assertrx.h"
#include "types.h"

namespace reindexer {

class Query;
class Uuid;
class KeyValueType;

}  // namespace reindexer

namespace fuzzing {

class NsScheme;

class [[nodiscard]] RandomGenerator {
	using ErrFactorInt = uint32_t;

public:
	using ErrFactorType = std::pair<ErrFactorInt, ErrFactorInt>;
	RandomGenerator(ErrFactorType errorFactor);

	size_t FieldsCount(bool firstLevel);
	FieldType RndFieldType(unsigned level) {
		const bool withoutStruct = level > 2 && (level > 5 || !RndBool(1.0 / (2 << (2 * level))));
		return static_cast<FieldType>(RndInt(static_cast<int>(FieldType::Bool), static_cast<int>(FieldType::Struct) - withoutStruct));
	}
	FieldType RndFieldType() {
		return static_cast<FieldType>(RndInt(static_cast<int>(FieldType::Bool), static_cast<int>(FieldType::Point)));
	}
	FieldType RndPkIndexFieldType() {
		return static_cast<FieldType>(RndInt(static_cast<int>(FieldType::Int), static_cast<int>(FieldType::Uuid)));
	}
	std::string IndexFieldType(FieldType);
	FieldType RndFieldType(FieldType type) {
		if (RndErr()) {
			return RndWhich<FieldType, 1, 1, 1, 1, 1, 1, 1, 1>();
		}
		return type;
	}
	std::vector<FieldType> RndFieldTypesArray(std::vector<FieldType>&& types) {
		if (!RndErr()) {
			return std::move(types);
		}
		if (RndBool(0.5)) {
			types.resize(compositeIndexSize(types.size()));
		}
		for (auto& t : types) {
			t = RndFieldType();
		}
		return std::move(types);
	}
	IndexType RndIndexType(const std::vector<FieldType>&);
	IndexType RndPkIndexType(const std::vector<FieldType>&);
	IndexType RndIndexType(IndexType);
	reindexer::IsArray RndArrayField() { return reindexer::IsArray(RndBool(0.2)); }
	reindexer::IsArray RndArrayField(reindexer::IsArray array) { return reindexer::IsArray(*array != RndErr()); }
	size_t ArraySize();
	bool PkIndex(bool pk) { return RndErr() ? RndBool(0.5) : pk; }
	reindexer::IsSparse RndSparseIndex(FieldType fldType) {
		const bool couldBeSparse = fldType != FieldType::Struct && fldType != FieldType::Uuid;	// TODO remove uuid #1470
		return reindexer::IsSparse(couldBeSparse ? RndBool(0.2) : RndErr());
	}
	reindexer::IsSparse RndSparseIndex(reindexer::IsSparse isSparse) { return reindexer::IsSparse(*isSparse != RndErr()); }
	bool DenseIndex() { return RndBool(0.2); }
	int64_t ExpiredIndex() { return RndInt(0, 100'000); }  // TODO
	size_t IndexesCount();
	bool CompositeIndex(size_t scalarIndexesCount) { return scalarIndexesCount < 1 ? RndErr() : RndBool(0.2); }
	bool UniqueName() { return RndBool(0.5); }
	size_t compositeIndexSize(size_t scalarIndexesCount);
	std::vector<size_t> RndFieldsForCompositeIndex(const std::vector<size_t>& scalarIndexes);
	std::string FieldName(std::unordered_set<std::string>& generatedNames);
	std::string FieldName(const std::string& fieldName, std::unordered_set<std::string>& generatedNames) {
		if (RndErr()) {
			return FieldName(generatedNames);
		}
		return fieldName;
	}
	FieldPath RndField(const NsScheme&);
	FieldPath RndScalarField(const NsScheme&);
	std::string IndexName(std::unordered_set<std::string>& generatedNames) { return FieldName(generatedNames); }  // TODO
	std::string GenerateNsName() {																				  // TODO
		std::unordered_set<std::string> generatedNames;
		return FieldName(generatedNames);
	}
	std::string NsName(const std::string& nsName) {
		if (RndErr()) {
			return GenerateNsName();
		}
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
	bool NeedThisNode(reindexer::IsSparse sparse) { return sparse ? RndBool(0.5) : !RndErr(); }
	int RndIntValue() {
		enum [[nodiscard]] Size : uint8_t { Short, Long, END = Long };
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
		enum [[nodiscard]] Size : uint8_t { Short, Long, END = Long };
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
	int64_t RndTtlValue();
	bool RndBool(double p) { return rndBool_(gen_, BoolRndParams{p}); }
	double RndDoubleValue() {
		enum [[nodiscard]] Size : uint8_t { Short, Long, END = Long };
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
		enum [[nodiscard]] Size : uint8_t { Short, Normal, Long, VeryLong, END = VeryLong };
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
		for (char& ch : res) {
			ch = availableChars[rndInt(availableRndParams)];  // TODO
		}
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
		assertrx(!std::empty(cont));
		auto it = std::begin(cont);
		std::advance(it, rndSize(0, std::size(cont) - 1));
		return *it;
	}
	void RndWhere(reindexer::Query&, const std::string& field, FieldType, std::optional<IndexType>);
	void RndWhereComposite(reindexer::Query&, const std::string& field, std::vector<FieldType>&&, std::optional<IndexType>);

	static void SetOut(std::string);
	static void SetIn(const std::string&);

private:
	using RandomEngine = std::default_random_engine;
	using IntRndParams = std::uniform_int_distribution<>::param_type;
	using SizeRndParams = std::uniform_int_distribution<size_t>::param_type;
	using Int64RndParams = std::uniform_int_distribution<int64_t>::param_type;
	using BoolRndParams = std::bernoulli_distribution::param_type;
	using DoubleRndParams = std::normal_distribution<double>::param_type;
	using ErrorParams = std::discrete_distribution<int>::param_type;

	int rndInt(IntRndParams params) { return rndInt_(gen_, params); }
	int64_t rndInt64(int64_t min, int64_t max) { return rndInt64_(gen_, Int64RndParams(min, max)); }
	size_t rndSize(size_t min, size_t max) { return rndSize_(gen_, SizeRndParams(min, max)); }
	CondType rndCond(FieldType);
	std::string rndStrUuidValue(bool noErrors);
	reindexer::Uuid rndUuidValue();
	template <size_t N, const std::vector<IndexType>* Availables>
	IndexType rndIndexType(const std::vector<FieldType>&);
	static std::string& out() noexcept;
	static std::unique_ptr<std::ifstream>& in() noexcept;
	static RandomEngine createRandomEngine();

	RandomEngine gen_;
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
