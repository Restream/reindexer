#include "random_generator.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <fstream>
#include "core/query/query.h"
#include "index.h"
#include "ns_scheme.h"
#include "tools/clock.h"

namespace fuzzing {

std::string& RandomGenerator::out() noexcept {
	static std::string outStr;
	return outStr;
}

std::unique_ptr<std::ifstream>& RandomGenerator::in() noexcept {
	static std::unique_ptr<std::ifstream> f;
	return f;
}

void RandomGenerator::SetOut(std::string o) {
	ASSERT_TRUE(out().empty());
	ASSERT_FALSE(in());
	out() = std::move(o);
	{
		std::ifstream f{out()};
		ASSERT_FALSE(f.is_open()) << "File '" << out() << "' already exists";
	}
}

void RandomGenerator::SetIn(const std::string& i) {
	ASSERT_FALSE(in());
	ASSERT_TRUE(out().empty());
	in() = std::make_unique<std::ifstream>(i);
	ASSERT_TRUE(in()->is_open()) << "Cannot open file '" << i << '\'';
	in()->exceptions(std::ios_base::badbit | std::ios_base::failbit | std::ios_base::eofbit);
}

RandomGenerator::RandomEngine RandomGenerator::createRandomEngine() {
	if (in()) {
		RandomEngine ret;
		std::string buf;
		std::getline(*in(), buf);
		std::istringstream ss{buf};
		ss >> ret;
		return ret;
	} else {
		RandomEngine ret(reindexer::system_clock_w::now().time_since_epoch().count());
		if (!out().empty()) {
			std::ofstream file{out(), std::ios_base::app};
			if (file.is_open()) {
				file.exceptions(std::ios_base::badbit | std::ios_base::failbit | std::ios_base::eofbit);
				file << ret << std::endl;
			} else {
				EXPECT_TRUE(false) << "Cannot open file '" << out() << '\'';
			}
		}
		return ret;
	}
}

RandomGenerator::RandomGenerator(ErrFactorType errorFactor) : gen_{createRandomEngine()}, errFactor_{errorFactor} {
	assertrx(errFactor_.first < errFactor_.second);
	errParams_ = {static_cast<double>(errFactor_.second - errFactor_.first), static_cast<double>(errFactor_.first)};
}

size_t RandomGenerator::FieldsCount(bool firstLevel) {
	if (RndErr()) {
		enum [[nodiscard]] Err : uint8_t { Zero, TooMany, END = TooMany };
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
		enum [[nodiscard]] Size : uint8_t { Normal, Long, END = Long };
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

std::string RandomGenerator::FieldName(std::unordered_set<std::string>& generatedNames) {  // TODO
	static constexpr char alfas[] = "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static constexpr char digits[] = "1234567890";
	static constexpr size_t alfasWeight = sizeof(alfas) - 1;
	static constexpr size_t digitsWeight = sizeof(digits) - 1;
	static const IntRndParams alfasRndParams{0, sizeof(alfas) - 2};
	static const IntRndParams digitsRndParams{0, sizeof(digits) - 2};
	std::string res;
	do {
		const bool withErr = RndErr();
		if (withErr) {
			size_t len;
			enum [[nodiscard]] Err : uint8_t { Dublicate, ZeroLength, TooLong, NormalLength, END = NormalLength };
			switch (RndWhich<Err, 1, 1, 1, 1>()) {
				case Dublicate:
					if (!generatedNames.empty()) {
						return RndWhich(generatedNames);
					} else {
						return {};
					}
				case ZeroLength:
					return {};
				case TooLong:
					len = RndInt(0, 10'000);
					break;
				case NormalLength:
					len = RndInt(1, 30);
					break;
				default:
					assertrx(false);
					std::abort();
			}
			res.resize(len);
			{
				enum Chars : uint8_t { All, Printable, Available, END = Available };
				switch (RndWhich<Chars, 1, 1, 1>()) {
					case All:
						for (auto& ch : res) {
							ch = rndChar_(gen_);
						}
						break;
					case Printable:
						for (size_t i = 0; i < len;) {
							res[i] = rndChar_(gen_);
							if (true) {
								++i;  // TODO
							}
						}
						break;
					case Available:
						for (auto& ch : res) {
							enum Chars : uint8_t { Alfas, Digits, END = Digits };
							switch (RndWhich<Chars, alfasWeight, digitsWeight>()) {
								case Alfas:
									ch = alfas[rndInt(alfasRndParams)];
									break;
								case Digits:
									ch = digits[rndInt(digitsRndParams)];
									break;
								default:
									assertrx(0);
									std::abort();
							}
						}
						break;
					default:
						assertrx(0);
						std::abort();
				}
			}
		} else {
			const size_t len = RndInt(5, 20);
			res.resize(len);
			res[0] = alfas[rndInt(alfasRndParams)];
			for (size_t i = 1; i < len; ++i) {
				enum Chars : uint8_t { Alfas, Digits, END = Digits };
				switch (RndWhich<Chars, alfasWeight, digitsWeight>()) {
					case Alfas:
						res[i] = alfas[rndInt(alfasRndParams)];
						break;
					case Digits:
						res[i] = digits[rndInt(digitsRndParams)];
						break;
					default:
						assertrx(0);
				}
			}
		}
	} while (!generatedNames.insert(res).second);
	return res;
}

FieldPath RandomGenerator::RndField(const NsScheme& nsScheme) {
	const bool withErr = RndErr();
	FieldPath res;
	do {
		if (withErr) {
			enum [[nodiscard]] Err : uint8_t { Break, Continue, END = Continue };
			switch (RndWhich<Err, 1, 1>()) {
				case Break:
					return res;
				case Continue:
					break;
				default:
					assertrx(0);
			}
		}
		const auto size = nsScheme.FieldsCount(res);
		if (size == 0) {
			return res;
		}
		const int idx = RndInt(0, size - 1);
		res.push_back(idx);
	} while (nsScheme.IsStruct(res));
	return res;
}

FieldPath RandomGenerator::RndScalarField(const NsScheme& nsScheme) {
	const bool withErr = RndErr();
	FieldPath res;
	do {
		if (withErr) {
			enum [[nodiscard]] Err : uint8_t { Break, Continue, END = Continue };
			switch (RndWhich<Err, 1, 1>()) {
				case Break:
					return res;
				case Continue:
					break;
				default:
					assertrx(0);
			}
		}
		const auto size = nsScheme.FieldsCount(res);
		if (size == 0) {
			return res;
		}
		int idx = RndInt(0, size - 1);
		res.push_back(idx);
		const int end = idx + size;
		while (idx < end) {
			res.back() = idx % size;
			if (!nsScheme.IsArray(res) && !nsScheme.IsPoint(res)) {
				break;
			}
			++idx;
		}
		if (idx == end) {
			return {};
		}
	} while (nsScheme.IsStruct(res));
	return res;
}

std::string RandomGenerator::IndexFieldType(FieldType ft) {
	static const std::string types[] = {"bool", "int", "int64", "double", "string", "uuid", "point", "composite"};
	if (RndErr()) {
		// TODO rnd string
		return RndWhich(types);
	}
	const size_t i = static_cast<size_t>(ft);
	assertrx(i < std::size(types));
	return types[i];
}

IndexType RandomGenerator::RndIndexType(IndexType it) {
	if (RndErr()) {
		return RndWhich<IndexType, 1, 1, 1, 1, 1, 1, 1>();	// TODO
	}
	return it;
}

template <size_t N, const std::vector<IndexType>* Availables>
IndexType RandomGenerator::rndIndexType(const std::vector<FieldType>& fieldTypes) {
	if (RndErr()) {
		// TODO rnd string
		return RndWhich<IndexType, 1, 1, 1, 1, 1, 1, 1>();	// TODO
	}
	assertrx(!fieldTypes.empty());
	std::vector<IndexType> availables;
	{
		const size_t f = static_cast<size_t>(fieldTypes[0]);
		assertrx(f < N);
		availables = Availables[f];
	}
	for (size_t i = 1, s = fieldTypes.size(); i < s; ++i) {
		const size_t f = static_cast<size_t>(fieldTypes[i]);
		std::vector<IndexType> tmp;
		tmp.reserve(availables.size());
		assertrx(f < N);
		std::set_intersection(availables.begin(), availables.end(), Availables[f].begin(), Availables[f].end(), std::back_inserter(tmp));
		availables = tmp;
	}
	if (availables.empty()) {
		return RndWhich<IndexType, 1, 1, 1, 1, 1, 1, 1>();	// TODO
	} else {
		return RndWhich(availables);
	}
}

IndexType RandomGenerator::RndIndexType(const std::vector<FieldType>& fieldTypes) {
	static const std::vector<IndexType> availableTypes[] = {
		{IndexType::Store},													   // Bool
		{IndexType::Store, IndexType::Hash, IndexType::Tree},				   // Int
		{IndexType::Store, IndexType::Hash, IndexType::Tree, IndexType::Ttl},  // Int64
		{IndexType::Store, IndexType::Tree},								   // Double
		{IndexType::Store, IndexType::Hash, IndexType::Tree},				   // String // TODO IndexType::FastFT IndexType::FuzzyFT
		{IndexType::Hash},													   // Uuid
		{IndexType::RTree},													   // Point
		{IndexType::Hash, IndexType::Tree}									   // Struct // TODO  IndexType::FastFT IndexType::FuzzyFT
	};
	return rndIndexType<std::extent_v<decltype(availableTypes)>, availableTypes>(fieldTypes);
}

IndexType RandomGenerator::RndPkIndexType(const std::vector<FieldType>& fieldTypes) {
	static const std::vector<IndexType> availablePkTypes[] = {
		{},													 // Bool
		{IndexType::Hash, IndexType::Tree},					 // Int
		{IndexType::Hash, IndexType::Tree, IndexType::Ttl},	 // Int64
		{IndexType::Tree},									 // Double
		{IndexType::Hash, IndexType::Tree},					 // String
		{IndexType::Hash},									 // Uuid
		{},													 // Point
		{IndexType::Hash, IndexType::Tree}					 // Struct
	};
	return rndIndexType<std::extent_v<decltype(availablePkTypes)>, availablePkTypes>(fieldTypes);
}

size_t RandomGenerator::ArraySize() {
	if (RndErr()) {
		return RndInt(0, 100'000);
	}
	enum [[nodiscard]] Size : uint8_t { Short, Normal, Long, VeryLong, END = VeryLong };
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

size_t RandomGenerator::IndexesCount() {
	if (RndErr()) {
		enum [[nodiscard]] Err : uint8_t { Zero, TooMany, END = TooMany };
		switch (RndWhich<Err, 1, 1>()) {
			case Zero:
				return 0;
			case TooMany:
				return RndInt(kMaxIndexes, 5 + kMaxIndexes);
			default:
				assertrx(0);
		}
	}
	enum Count : uint8_t { Few, Normal, Many, TooMany, END = TooMany };
	switch (RndWhich<Count, 500, 1'000, 10, 1>()) {
		case Few:
			return RndInt(1, 3);
		case Normal:
			return RndInt(4, 20);
		case Many:
			return RndInt(21, 63);
		case TooMany:
			return RndInt(64, kMaxIndexes);
		default:
			assertrx(false);
			std::abort();
	}
}

size_t RandomGenerator::compositeIndexSize(size_t scalarIndexesCount) {
	if (RndErr()) {
		enum [[nodiscard]] Err : uint8_t { Zero, /*One,*/ TooMany, END = TooMany };
		switch (RndWhich<Err, 1, /*1,*/ 1>()) {
			case Zero:
				return 0;
			/*case One:
				return 1;*/
			case TooMany:
				return RndInt(0, 10'000);
			default:
				assertrx(0);
		}
	}
	assertrx(scalarIndexesCount >= 1);
	return RndInt(1, scalarIndexesCount);
}

std::vector<size_t> RandomGenerator::RndFieldsForCompositeIndex(const std::vector<size_t>& scalarIndexes) {
	std::vector<size_t> result;
	const size_t count = compositeIndexSize(scalarIndexes.size());
	result.reserve(count);
	const bool uniqueFields = count <= scalarIndexes.size() && !RndErr();
	// TODO non-existent and not indexed fields
	if (uniqueFields) {
		auto scalars = scalarIndexes;
		while (result.size() < count) {
			const size_t idx = rndSize(0, scalars.size() - 1);
			result.push_back(scalars[idx]);
			scalars.erase(scalars.begin() + idx);
		}
	} else {
		while (result.size() < count) {
			result.push_back(scalarIndexes[rndSize(0, scalarIndexes.size() - 1)]);
		}
	}
	return result;
}

template <>
constexpr size_t RandomGenerator::itemsCount<CondType> = CondType::CondDWithin + 1;

CondType RandomGenerator::rndCond(FieldType ft) {  // TODO array
	if (RndErr()) {
		return RndWhich<CondType, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1>();
	}
	static const std::vector<CondType> availableConds[] = {
		{CondEq, CondLt, CondLe, CondGt, CondGe, CondSet, CondAllSet},			  // Bool
		{CondEq, CondLt, CondLe, CondGt, CondGe, CondSet, CondAllSet},			  // Int
		{CondEq, CondLt, CondLe, CondGt, CondGe, CondSet, CondAllSet},			  // Int64
		{CondEq, CondLt, CondLe, CondGt, CondGe, CondSet, CondAllSet},			  // Double
		{CondEq, CondLt, CondLe, CondGt, CondGe, CondSet, CondAllSet, CondLike},  // String
		{CondEq, CondLt, CondLe, CondGt, CondGe, CondSet, CondAllSet},			  // Uuid
		{CondDWithin},															  // Point
		{CondEq, CondLt, CondLe, CondGt, CondGe, CondSet, CondAllSet}			  // Struct
	};
	const size_t i = static_cast<size_t>(ft);
	assertrx(i < std::size(availableConds));
	return RndWhich(availableConds[i]);
}

std::string RandomGenerator::rndStrUuidValue(bool noErrors) {
	static constexpr std::string_view hexChars = "0123456789aAbBcCdDeEfF";
	static constexpr std::string_view notAvailableChars = "_ghijklmnopqrstuvwxyzGHIJKLMNOPQRSTUVWXYZ";
	static constexpr unsigned uuidDelimPositions[] = {8, 13, 18, 23};
	enum [[nodiscard]] Err : uint8_t { NoErrors, Empty, Short, Long, TooLong, WrongVariant, WrongChar, END = WrongChar };
	Err err = NoErrors;
	if (!noErrors && RndErr()) {
		err = RndWhich<Err, 0, 1, 1, 1, 1, 1, 1>();
	}
	size_t size = 32;
	switch (err) {
		case Short:
			size = RndInt(1, 31);
			break;
		case Long:
			size = RndInt(33, 50);
			break;
		case TooLong:
			size = RndInt(51, 100'000);
			break;
		case Empty:
			return {};
		case NoErrors:
		case WrongVariant:
		case WrongChar:
			break;
		default:
			assertrx(0);
			abort();
	}
	std::string res;
	res.reserve(size + 4);
	if (RndBool(0.001)) {
		res = std::string(std::string::size_type{size}, '0');
	} else {
		for (size_t i = 0; i < size; ++i) {
			if (i == 16) {
				if (err == WrongVariant) {
					res.append(1, RndWhich(hexChars.substr(0, 8)));
				} else {
					res.append(1, RndWhich(hexChars.substr(8)));
				}
			} else {
				res.append(1, RndWhich(hexChars));
			}
		}
	}
	if (err == WrongChar) {
		for (int i = 0, count = RndInt(1, 5); i < count; ++i) {
			RndWhich(res) = RndWhich(notAvailableChars);
		}
	}
	for (unsigned i : uuidDelimPositions) {
		if (i < res.size()) {
			res.insert(i, 1, '-');
		}
	}
	return res;
}

reindexer::Uuid RandomGenerator::rndUuidValue() { return reindexer::Uuid{rndStrUuidValue(true)}; }

int64_t RandomGenerator::RndTtlValue() {
	const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(reindexer::system_clock_w::now().time_since_epoch()).count();
	// TODO uncomment this after TTL subscribe done
	/*enum Size : uint8_t { Negative, FarPast, Past, Now, Future, FarFuture, AnyShort, Any, END = Any };
	switch (RndWhich<Size, 1, 1>()) { case Negative: return rndInt64(std::numeric_limits<int64_t>::min(), 0); case FarPast:
			return rndInt64(0, now - 10'000);
		case Past:
			return rndInt64(now - 10'000, now - 10);
		case Now:
			return rndInt64(now - 10, now + 10);
		case Future:
			return rndInt64(now + 10, now + 10'000);
		case FarFuture:
			return rndInt64(now + 10'000, std::numeric_limits<int64_t>::max());
		case AnyShort:
			return rndInt64(-50, 50);
		case Any:
			return rndInt64(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
		default:
			assertrx(false);
			std::abort();
	}*/
	return rndInt64(now + 10'000, std::numeric_limits<int64_t>::max());
}

void RandomGenerator::RndWhere(reindexer::Query& query, const std::string& field, FieldType fieldType,
							   std::optional<IndexType> indexType) {  // TODO array
	if (RndErr()) {
		return RndWhereComposite(query, field, RndFieldTypesArray({fieldType}), indexType);
	}
	std::unordered_set<std::string> generatedNames;
	const std::string fldName = FieldName(field, generatedNames);
	const auto cond = rndCond(fieldType);
	switch (RndFieldType(fieldType)) {
		case FieldType::Bool:
			query.Where(fldName, cond, RndBool(0.5));
			break;
		case FieldType::Int:
			query.Where(fldName, cond, RndIntValue());
			break;
		case FieldType::Int64:
			if (indexType == IndexType::Ttl) {
				query.Where(fldName, cond, RndTtlValue());
			} else {
				query.Where(fldName, cond, RndInt64Value());
			}
			break;
		case FieldType::Double:
			query.Where(fldName, cond, RndDoubleValue());
			break;
		case FieldType::String:
			query.Where(fldName, cond, RndStringValue());
			break;
		case FieldType::Uuid:
			if (RndBool(0.5)) {
				query.Where(fldName, cond, rndUuidValue());
			} else {
				query.Where(fldName, cond, rndStrUuidValue(false));
			}
			break;
		case FieldType::Point:
			query.Where(fldName, cond,
						{reindexer::Variant{reindexer::Point{RndDoubleValue(), RndDoubleValue()}},
						 reindexer::Variant{RndErr() ? RndDoubleValue() : std::abs(RndDoubleValue())}});
			break;
		case FieldType::Struct:	 // TODO
			break;
		default:
			assertrx(0);
	}
}

void RandomGenerator::RndWhereComposite(reindexer::Query& query, const std::string& field, std::vector<FieldType>&& fieldTypes,
										std::optional<IndexType> indexType) {  // TODO array
	if (RndErr()) {
		return RndWhere(query, field, RndFieldType(), indexType);
	}
	std::unordered_set<std::string> generatedNames;
	const std::string fldName = FieldName(field, generatedNames);
	fieldTypes = RndFieldTypesArray(std::move(fieldTypes));
	const auto cond = rndCond(FieldType::Struct);
	reindexer::VariantArray keys;
	keys.reserve(fieldTypes.size());
	for (const FieldType ft : fieldTypes) {
		switch (ft) {
			case FieldType::Bool:
				keys.emplace_back(RndBool(0.5));
				break;
			case FieldType::Int:
				keys.emplace_back(RndIntValue());
				break;
			case FieldType::Int64:
				if (indexType == IndexType::Ttl) {
					keys.emplace_back(RndTtlValue());
				} else {
					keys.emplace_back(RndInt64Value());
				}
				break;
			case FieldType::Double:
				keys.emplace_back(RndDoubleValue());
				break;
			case FieldType::String:
				keys.emplace_back(RndStringValue());
				break;
			case FieldType::Uuid:
				if (RndBool(0.5)) {
					keys.emplace_back(rndUuidValue());
				} else {
					keys.emplace_back(rndStrUuidValue(false));
				}
				break;
			case FieldType::Point:
				keys.emplace_back(reindexer::Point{RndDoubleValue(), RndDoubleValue()});
				break;
			case FieldType::Struct:	 // TODO
				break;
			default:
				assertrx(0);
		}
	}
	query.WhereComposite(fldName, cond, {std::move(keys)});
}

}  // namespace fuzzing
