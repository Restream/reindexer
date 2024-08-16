#include "payloadtype.h"
#include <sstream>
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
			ret << jp << ";";
		}
		ret << "\"\n";
	}
	return ret.str();
}

void PayloadTypeImpl::Dump(std::ostream& os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << '{';
	for (auto& f : fields_) {
		os << '\n'
		   << newOffset << f.Type().Name() << (f.IsArray() ? "[]" : "") << " '" << f.Name() << "'"
		   << " json:\"";
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
	auto it = fieldsByName_.find(f.Name());
	if (it != fieldsByName_.end()) {
		// Non unique name -> check type, and upgrade to array if types are the same
		auto& oldf = fields_[it->second];
		throw Error(errLogic, "Cannot add field with name '%s' and type '%s' to namespace '%s'. It already exists with type '%s'", f.Name(),
					f.Type().Name(), Name(), oldf.Type().Name());
	} else {
		// Unique name -> just add field
		f.SetOffset(TotalSize());
		for (auto& jp : f.JsonPaths()) {
			if (!jp.length()) {
				continue;
			}
			auto res = fieldsByJsonPath_.emplace(jp, int(fields_.size()));
			if (!res.second && res.first->second != int(fields_.size())) {
				throw Error(errLogic, "Cannot add field with name '%s' to namespace '%s'. Json path '%s' already used in field '%s'",
							f.Name(), Name(), jp, Field(res.first->second).Name());
			}

			checkNewJsonPathBeforeAdd(f, jp);
		}
		fieldsByName_.emplace(f.Name(), int(fields_.size()));
		if (f.Type().Is<KeyValueType::String>()) {
			strFields_.push_back(int(fields_.size()));
		}
		fields_.push_back(std::move(f));
	}
}

bool PayloadTypeImpl::Drop(std::string_view field) {
	auto itField = fieldsByName_.find(field);
	if (itField == fieldsByName_.end()) {
		return false;
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

	const auto fieldType = fields_[fieldIdx].Type();
	for (auto it = strFields_.begin(); it != strFields_.end();) {
		if ((*it == fieldIdx) && (fieldType.Is<KeyValueType::String>())) {
			it = strFields_.erase(it);
			continue;
		} else if (*it > fieldIdx) {
			--(*it);
		}
		++it;
	}

	for (auto& jp : fields_[fieldIdx].JsonPaths()) {
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

	fieldsByName_.erase(field);

	return true;
}

int PayloadTypeImpl::FieldByName(std::string_view field) const {
	auto it = fieldsByName_.find(field);
	if (it == fieldsByName_.end()) {
		throw Error(errLogic, "Field '%s' not found in namespace '%s'", field, Name());
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
	ser.PutVarUint(base_key_string::export_hdr_offset());
	ser.PutVarUint(NumFields());
	for (int i = 0; i < NumFields(); i++) {
		ser.PutKeyValueType(Field(i).Type());
		ser.PutVString(Field(i).Name());
		ser.PutVarUint(Field(i).Offset());
		ser.PutVarUint(Field(i).ElemSizeof());
		ser.PutVarUint(Field(i).IsArray());
		ser.PutVarUint(Field(i).JsonPaths().size());
		for (auto& jp : Field(i).JsonPaths()) {
			ser.PutVString(jp);
		}
	}
}

void PayloadTypeImpl::deserialize(Serializer& ser) {
	fields_.clear();
	fieldsByName_.clear();
	fieldsByJsonPath_.clear();
	strFields_.clear();

	ser.GetVarUint();

	auto count = ser.GetVarUint();

	for (uint64_t i = 0; i < count; i++) {
		const auto t = ser.GetKeyValueType();
		std::string name(ser.GetVString());
		std::vector<std::string> jsonPaths;
		uint64_t offset = ser.GetVarUint();
		uint64_t elemSizeof = ser.GetVarUint();
		bool isArray = ser.GetVarUint();
		uint64_t jsonPathsCount = ser.GetVarUint();

		while (jsonPathsCount--) {
			jsonPaths.push_back(std::string(ser.GetVString()));
		}

		(void)elemSizeof;

		PayloadFieldType ft(t, name, jsonPaths, isArray);

		if (isArray) {
			ft.SetArray();
		}
		ft.SetOffset(offset);
		fieldsByName_.emplace(name, fields_.size());
		if (t.Is<KeyValueType::String>()) {
			strFields_.push_back(fields_.size());
		}
		fields_.push_back(ft);
	}
}

PayloadType::PayloadType(const std::string& name, std::initializer_list<PayloadFieldType> fields)
	: shared_cow_ptr<PayloadTypeImpl>(make_intrusive<intrusive_atomic_rc_wrapper<PayloadTypeImpl>>(name, fields)) {}
PayloadType::PayloadType(const PayloadTypeImpl& impl)
	: shared_cow_ptr<PayloadTypeImpl>(make_intrusive<intrusive_atomic_rc_wrapper<PayloadTypeImpl>>(impl)) {}
PayloadType::~PayloadType() = default;
const PayloadFieldType& PayloadType::Field(int field) const { return get()->Field(field); }
const std::string& PayloadType::Name() const { return get()->Name(); }
void PayloadType::SetName(const std::string& name) { clone()->SetName(name); }
int PayloadType::NumFields() const { return get()->NumFields(); }
void PayloadType::Add(PayloadFieldType f) { clone()->Add(std::move(f)); }
bool PayloadType::Drop(std::string_view field) { return clone()->Drop(field); }
int PayloadType::FieldByName(std::string_view field) const { return get()->FieldByName(field); }
bool PayloadType::FieldByName(std::string_view name, int& field) const { return get()->FieldByName(name, field); }
bool PayloadType::Contains(std::string_view field) const { return get()->Contains(field); }
int PayloadType::FieldByJsonPath(std::string_view jsonPath) const { return get()->FieldByJsonPath(jsonPath); }
const std::vector<int>& PayloadType::StrFields() const { return get()->StrFields(); }
size_t PayloadType::TotalSize() const { return get()->TotalSize(); }
std::string PayloadType::ToString() const { return get()->ToString(); }

void PayloadType::Dump(std::ostream& os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << "{\n" << newOffset << "name: " << Name() << ",\n" << newOffset;
	get()->Dump(os, step, newOffset);
	os << '\n' << offset << '}';
}

void PayloadTypeImpl::checkNewJsonPathBeforeAdd(const PayloadFieldType& f, const std::string& jsonPath) const {
	const auto pos = jsonPath.find('.');
	if (pos < jsonPath.length() - 1) {
		for (auto& fld : fields_) {
			for (auto& jpfld : fld.JsonPaths()) {
				// new field total overwrites existing one
				if ((jsonPath.rfind(jpfld, 0) == 0) && (jsonPath[jpfld.length()] == '.')) {
					throw Error(errLogic,
								"Cannot add field with name '%s' (jsonpath '%s') and type '%s' to namespace '%s'."
								" Already exists json path '%s' with type '%s' in field '%s'. Rewriting is impossible",
								f.Name(), jsonPath, f.Type().Name(), Name(), jpfld, fld.Type().Name(), fld.Name());
				}
			}
		}
	}
}

}  // namespace reindexer
