#include "random_generator.h"
#include <chrono>
#include "core/query/query.h"
#include "ns_scheme.h"

namespace fuzzing {

RandomGenerator::RandomGenerator(std::ostream& os, ErrFactorType errorFactor)
	: gen_(std::chrono::system_clock::now().time_since_epoch().count()), errFactor_{errorFactor} {
	assertrx(errFactor_.first < errFactor_.second);
	errParams_ = {static_cast<double>(errFactor_.second - errFactor_.first), static_cast<double>(errFactor_.first)};
	os << gen_ << std::endl;
}
RandomGenerator::RandomGenerator(std::istream& is) { is >> gen_; }

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
			enum Err : uint8_t { Dublicate, ZeroLength, TooLong, NormalLength, END = NormalLength };
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
							if (true) ++i;	// TODO
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
			enum Err : uint8_t { Break, Continue, END = Continue };
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
		if (size == 0) return res;
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
			enum Err : uint8_t { Break, Continue, END = Continue };
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
		if (size == 0) return res;
		int idx = RndInt(0, size - 1);
		res.push_back(idx);
		const int end = idx + size;
		while (idx < end) {
			res.back() = idx % size;
			if (!nsScheme.IsArray(res) && !nsScheme.IsPoint(res)) break;
			++idx;
		}
		if (idx == end) return {};
	} while (nsScheme.IsStruct(res));
	return res;
}

std::string RandomGenerator::IndexFieldType(fuzzing::FieldType ft) {
	static const std::string types[] = {"bool", "int", "int64", "double", "string", "uuid", "point", "composite"};
	if (RndErr()) {
		// TODO rnd string
		return RndWhich(types);
	}
	const size_t i = static_cast<size_t>(ft);
	assertrx(i < std::size(types));
	return types[i];
}

std::string RandomGenerator::RndIndexType(fuzzing::FieldType ft, bool pk) {
	static const std::string types[] = {"-", "hash", "tree", "ttl", "text", "fuzzytext", "rtree"};
	static const std::vector<size_t> availableTypes[] = {
		{0},				   // Bool
		{0, 1, 2},			   // Int
		{0, 1, 2, 3},		   // Int64
		{0, 2},				   // Double
		{0, 1, 2 /*, 4, 5*/},  // String // TODO FT indexes
		{1},				   // Uuid
		{6},				   // Point
		{1, 2 /*, 4, 5*/}	   // Struct // TODO FT indexes
	};
	static const std::vector<size_t> availablePkTypes[] = {
		{},			// Bool
		{1, 2},		// Int
		{1, 2, 3},	// Int64
		{2},		// Double
		{1, 2},		// String
		{1},		// Uuid
		{},			// Point
		{1, 2}		// Struct
	};
	if (RndErr()) {
		// TODO rnd string
		return RndWhich(types);
	}
	const size_t i = static_cast<size_t>(ft);
	size_t n;
	if (pk) {
		assertrx(i < std::size(availablePkTypes));
		if (availablePkTypes[i].empty()) {
			return RndWhich(types);
		}
		n = RndWhich(availablePkTypes[i]);
	} else {
		assertrx(i < std::size(availableTypes));
		n = RndWhich(availableTypes[i]);
	}
	assertrx(n < std::size(types));
	return types[n];
}

template <>
constexpr size_t RandomGenerator::itemsCount<CondType> = CondType::CondDWithin + 1;

CondType RandomGenerator::rndCond(fuzzing::FieldType ft) {	// TODO array
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
	enum Err : uint8_t { NoErrors, Empty, Short, Long, TooLong, WrongVariant, WrongChar, END = WrongChar };
	Err err = NoErrors;
	if (!noErrors && RndErr()) {
		err = RndWhich<Err, 0, 1, 1, 1, 1, 1, 1>();
	}
	std::string res;
	if (err == Empty) return res;
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
		case NoErrors:
		case Empty:
		case WrongVariant:
		case WrongChar:
			break;
		default:
			assert(0);
			abort();
	}
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

void RandomGenerator::RndWhere(reindexer::Query& query, const std::string& field,
							   const std::vector<fuzzing::FieldType>& types) {	// TODO array
	std::unordered_set<std::string> generatedNames;
	assertrx(!types.empty());
	const std::string fldName = FieldName(field, generatedNames);
	const auto type = types.size() > 1 ? fuzzing::FieldType::Struct : types[0];
	const auto cond = rndCond(type);
	switch (RndFieldType(type)) {
		case fuzzing::FieldType::Bool:
			query.Where(fldName, cond, RndBool(0.5));
			break;
		case fuzzing::FieldType::Int:
			query.Where(fldName, cond, RndIntValue());
			break;
		case fuzzing::FieldType::Int64:
			query.Where(fldName, cond, RndInt64Value());
			break;
		case fuzzing::FieldType::Double:
			query.Where(fldName, cond, RndDoubleValue());
			break;
		case fuzzing::FieldType::String:
			query.Where(fldName, cond, RndStringValue());
			break;
		case fuzzing::FieldType::Uuid:
			if (RndBool(0.5)) {
				query.Where(fldName, cond, rndUuidValue());
			} else {
				query.Where(fldName, cond, rndStrUuidValue(false));
			}
			break;
		case fuzzing::FieldType::Point:
			query.Where(fldName, cond,
						{reindexer::Variant{reindexer::Point{RndDoubleValue(), RndDoubleValue()}},
						 reindexer::Variant{RndErr() ? RndDoubleValue() : std::abs(RndDoubleValue())}});
			break;
		case fuzzing::FieldType::Struct:  // TODO
			if (type == fuzzing::FieldType::Struct) {
			} else {
			}
			break;
		default:
			assertrx(0);
	}
}

std::ostream& operator<<(std::ostream& os, FieldType ft) {
	switch (ft) {
		case FieldType::Bool:
			return os << "bool";
		case FieldType::Int:
			return os << "int";
		case FieldType::Int64:
			return os << "int64";
		case FieldType::Double:
			return os << "double";
		case FieldType::String:
			return os << "string";
		case FieldType::Uuid:
			return os << "uuid";
		case FieldType::Point:
			return os << "point";
		case FieldType::Struct:
			return os << "struct";
		default:
			assertrx(0);
	}
	return os;
}

reindexer::KeyValueType ToKeyValueType(FieldType ft) {
	switch (ft) {
		case FieldType::Bool:
			return reindexer::KeyValueType::Bool{};
		case FieldType::Int:
			return reindexer::KeyValueType::Int{};
		case FieldType::Int64:
			return reindexer::KeyValueType::Int64{};
		case FieldType::Double:
			return reindexer::KeyValueType::Double{};
		case FieldType::String:
			return reindexer::KeyValueType::String{};
		case FieldType::Uuid:
			return reindexer::KeyValueType::Uuid{};
		case FieldType::Point:
		case FieldType::Struct:
		default:
			assertrx(0);
	}
}

}  // namespace fuzzing
