#include "payloadtype.h"
#include <sstream>
#include "core/embedding/embedder.h"
#include "core/keyvalue/key_string.h"
#include "payloadtypeimpl.h"
#include "tools/serializer.h"

namespace reindexer {

size_t PayloadTypeImpl::TotalSize() const noexcept {
	if (fields_.size()) {
		return fields_.back().Offset() + fields_.back().Sizeof();
	}
	return 0;
}

std::string PayloadTypeImpl::ToString() const {
	std::stringstream ret;
	for (auto& f : fields_) {
		ret << f.Type().Name() << (f.IsArray() ? "[]" : "") << " '" << f.Name() << '\'' << " json:\"";
		for (auto& jp : f.JsonPaths()) {
			ret << jp << ';';
		}
		ret << "\"\n";
	}
	return ret.str();
}

void PayloadTypeImpl::Dump(std::ostream& os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << '{';
	for (const auto& f : fields_) {
		os << '\n' << newOffset << f.Type().Name() << (f.IsArray() ? "[]" : "") << " '" << f.Name() << '\'' << " json:\"";
		for (size_t i = 0, s = f.JsonPaths().size(); i < s; ++i) {
			if (i != 0) {
				os << ';';
			}
			os << f.JsonPaths()[i];
		}
		os << '"';
	}
	if (!fields_.empty()) {
		os << '\n' << offset;
	}
	os << '}';
}

void PayloadTypeImpl::Add(PayloadFieldType f) {
	checkNewNameBeforeAdd(f);
	// Unique name -> just add field
	f.SetOffset(TotalSize());
	const int fieldNo = int(fields_.size());
	for (const auto& jp : f.JsonPaths()) {
		if (!jp.length()) {
			continue;
		}
		auto res = fieldsByJsonPath_.emplace(jp, fieldNo);
		if (!res.second && res.first->second != fieldNo) {
			throw Error(errLogic, "Cannot add field with name '{}' to namespace '{}'. Json path '{}' already used in field '{}'", f.Name(),
						Name(), jp, Field(res.first->second).Name());
		}
		checkNewJsonPathBeforeAdd(f, jp);
	}
	fieldsByName_.emplace(f.Name(), fieldNo);
	if (f.Type().Is<KeyValueType::String>()) {
		strFields_.push_back(fieldNo);
	}
	fields_.push_back(std::move(f));
}

void PayloadTypeImpl::Drop(std::string_view name) {
	const auto itField = fieldsByName_.find(name);
	if (itField == fieldsByName_.end()) {
		return;
	}
	const auto fieldIdx = itField->second;
	for (auto& f : fieldsByName_) {
		if (f.second > fieldIdx) {
			--f.second;
		}
	}
	for (auto& f : fieldsByJsonPath_) {
		if (f.second > fieldIdx) {
			--f.second;
		}
	}
	const auto& payloadField = fields_[fieldIdx];
	const auto fieldType = payloadField.Type();
	for (auto it = strFields_.begin(); it != strFields_.end();) {
		if ((*it == fieldIdx) && (fieldType.Is<KeyValueType::String>())) {
			it = strFields_.erase(it);
			continue;
		} else if (*it > fieldIdx) {
			--(*it);
		}
		++it;
	}
	for (const auto& jp : payloadField.JsonPaths()) {
		fieldsByJsonPath_.erase(jp);
	}
	fields_.erase(fields_.begin() + fieldIdx);
	for (size_t idx = static_cast<size_t>(fieldIdx); idx < fields_.size(); ++idx) {
		if (idx == 0) {
			fields_[idx].SetOffset(0);
		} else {
			const PayloadFieldType& plTypePrev(fields_[idx - 1]);
			fields_[idx].SetOffset(plTypePrev.Offset() + plTypePrev.Sizeof());
		}
	}
	fieldsByName_.erase(itField);
}

void PayloadTypeImpl::Replace(int field, PayloadFieldType f) {
	assertrx_throw(field < int(fields_.size()));
	const auto& fld = fields_[field];
	if (fld.Name() != f.Name() || !fld.Type().IsSame(f.Type()) || fld.JsonPaths() != f.JsonPaths() || fld.Offset() != f.Offset() ||
		fld.Sizeof() != f.Sizeof() || fld.ElemSizeof() != f.ElemSizeof() || fld.IsArray() != f.IsArray() || !f.IsFloatVector()) {
		// replacement is expected only to update settings of embedder, now. All other parameters must be same
		assertrx_throw(false);
	}
	checkEmbedderFields(f);
	fields_[field] = std::move(f);
}

int PayloadTypeImpl::FieldByName(std::string_view field) const {
	auto it = fieldsByName_.find(field);
	if (it == fieldsByName_.end()) {
		throw Error(errLogic, "Field '{}' not found in namespace '{}'", field, Name());
	}
	return it->second;
}

bool PayloadTypeImpl::FieldByName(std::string_view name, int& field) const noexcept {
	auto it = fieldsByName_.find(name);
	if (it == fieldsByName_.end()) {
		return false;
	}
	field = it->second;
	return true;
}

int PayloadTypeImpl::FieldByJsonPath(std::string_view jsonPath) const noexcept {
	auto it = fieldsByJsonPath_.find(jsonPath);
	if (it == fieldsByJsonPath_.end()) {
		return -1;
	}
	return it->second;
}

void PayloadTypeImpl::serialize(WrSerializer& ser) const {
	ser.PutVarUint(key_string_impl::export_hdr_offset());
	ser.PutVarUint(NumFields());
	for (int i = 0; i < NumFields(); i++) {
		const auto& field = Field(i);
		ser.PutKeyValueType(field.Type());
		if (field.Type().Is<KeyValueType::FloatVector>()) {
			ser.PutVarUint(field.FloatVectorDimension().Value());
		}
		ser.PutVString(field.Name());
		ser.PutVarUint(field.Offset());
		ser.PutVarUint(field.ElemSizeof());
		ser.PutVarUint(*field.IsArray());
		ser.PutVarUint(field.JsonPaths().size());
		for (auto& jp : field.JsonPaths()) {
			ser.PutVString(jp);
		}
	}
}

void PayloadTypeImpl::deserialize(Serializer& ser) {
	fields_.clear();
	fieldsByName_.clear();
	fieldsByJsonPath_.clear();
	strFields_.clear();

	[[maybe_unused]] const uint64_t exportHdrOffset = ser.GetVarUInt();

	const uint64_t count = ser.GetVarUInt();
	fields_.reserve(count);

	for (uint64_t i = 0; i < count; i++) {
		const auto t = ser.GetKeyValueType();
		const FloatVectorDimension floatVectorDimension =
			t.Is<KeyValueType::FloatVector>() ? FloatVectorDimension(ser.GetVarUInt()) : FloatVectorDimension{};
		std::string name(ser.GetVString());
		std::vector<std::string> jsonPaths;
		const uint64_t offset = ser.GetVarUInt();
		[[maybe_unused]] const uint64_t elemSizeof = ser.GetVarUInt();
		const auto isArray = IsArray(ser.GetVarUInt());
		uint64_t jsonPathsCount = ser.GetVarUInt();
		jsonPaths.reserve(jsonPathsCount);

		while (jsonPathsCount--) {
			jsonPaths.emplace_back(ser.GetVString());
		}

		PayloadFieldType ft(t, std::move(name), std::move(jsonPaths), isArray, floatVectorDimension);
		ft.SetOffset(offset);
		fieldsByName_.emplace(ft.Name(), fields_.size());
		if (t.Is<KeyValueType::String>()) {
			strFields_.push_back(fields_.size());
		}
		for (const auto& jp : ft.JsonPaths()) {
			fieldsByJsonPath_.emplace(jp, fields_.size());
		}
		fields_.push_back(std::move(ft));
	}
}

void PayloadTypeImpl::checkNewJsonPathBeforeAdd(const PayloadFieldType& f, const std::string& jsonPath) const {
	const auto pos = jsonPath.find('.');
	if (pos < jsonPath.length() - 1) {
		for (const auto& fld : fields_) {
			for (const auto& jpfld : fld.JsonPaths()) {
				// new field total overwrites existing one
				if ((jsonPath.rfind(jpfld, 0) == 0) && (jsonPath[jpfld.length()] == '.')) {
					throw Error(errLogic,
								"Cannot add field with name '{}' (jsonpath '{}') and type '{}' to namespace '{}'."
								" Already exists json path '{}' with type '{}' in field '{}'. Rewriting is impossible",
								f.Name(), jsonPath, f.Type().Name(), Name(), jpfld, fld.Type().Name(), fld.Name());
				}
			}
		}
	}
}

void PayloadTypeImpl::checkNewNameBeforeAdd(const PayloadFieldType& f) const {
	if (const auto it = fieldsByName_.find(f.Name()); it != fieldsByName_.end()) {
		// Non unique name -> check type, and upgrade to array if types are the same
		const auto& oldf = fields_[it->second];
		throw Error(errLogic, "Cannot add field with name '{}' and type '{}' to namespace '{}'. It already exists with type '{}'", f.Name(),
					f.Type().Name(), Name(), oldf.Type().Name());
	}
}

std::string_view PayloadTypeImpl::CheckEmbeddersAuxiliaryField(std::string_view fieldName) const {
	for (const auto& field : fields_) {
		auto embedder = field.UpsertEmbedder();
		if (embedder && embedder->IsAuxiliaryField(fieldName)) {
			return embedder->FieldName();
		}
	}
	return {};
}

void PayloadTypeImpl::checkEmbedderFields(const PayloadFieldType& fieldType) {
	auto embedder = fieldType.UpsertEmbedder();
	if (embedder) {
		for (const auto& field : embedder->Fields()) {
			auto itFld = fieldsByName_.find(field);
			if (itFld == fieldsByName_.end()) {
				throw Error(errLogic,
							"Incorrect embedding configuration for field with name '{}' to namespace '{}'. Auxiliary field '{}' not found",
							fieldType.Name(), Name(), field);
			}
		}
	}
}

PayloadType::PayloadType(const std::string& name, std::initializer_list<PayloadFieldType> fields)
	: shared_cow_ptr<PayloadTypeImpl>(make_intrusive<intrusive_atomic_rc_wrapper<PayloadTypeImpl>>(name, fields)) {}
PayloadType::PayloadType(const PayloadTypeImpl& impl)
	: shared_cow_ptr<PayloadTypeImpl>(make_intrusive<intrusive_atomic_rc_wrapper<PayloadTypeImpl>>(impl)) {}
PayloadType::~PayloadType() = default;
const PayloadFieldType& PayloadType::Field(int field) const& noexcept { return get()->Field(field); }
const std::string& PayloadType::Name() const& noexcept { return get()->Name(); }
void PayloadType::SetName(std::string_view name) { clone()->SetName(std::string(name)); }
int PayloadType::NumFields() const noexcept { return get()->NumFields(); }
void PayloadType::Add(PayloadFieldType f) { clone()->Add(std::move(f)); }
void PayloadType::Drop(std::string_view field) { return clone()->Drop(field); }
void PayloadType::Replace(int field, PayloadFieldType f) { clone()->Replace(field, std::move(f)); }
int PayloadType::FieldByName(std::string_view field) const { return get()->FieldByName(field); }
bool PayloadType::FieldByName(std::string_view name, int& field) const noexcept { return get()->FieldByName(name, field); }
bool PayloadType::Contains(std::string_view field) const noexcept { return get()->Contains(field); }
int PayloadType::FieldByJsonPath(std::string_view jsonPath) const noexcept { return get()->FieldByJsonPath(jsonPath); }
const std::vector<int>& PayloadType::StrFields() const& noexcept { return get()->StrFields(); }
size_t PayloadType::TotalSize() const noexcept { return get()->TotalSize(); }
std::string PayloadType::ToString() const { return get()->ToString(); }
std::string_view PayloadType::CheckEmbeddersAuxiliaryField(std::string_view fieldName) const {
	return get()->CheckEmbeddersAuxiliaryField(fieldName);
}

void PayloadType::Dump(std::ostream& os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << "{\n" << newOffset << "name: " << Name() << ",\n" << newOffset;
	get()->Dump(os, step, newOffset);
	os << '\n' << offset << '}';
}

}  // namespace reindexer
