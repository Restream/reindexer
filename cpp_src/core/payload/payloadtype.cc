#include "payloadtype.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "core/type_consts_helpers.h"
#include "payloadtypeimpl.h"
#include "tools/serializer.h"

namespace reindexer {

size_t PayloadTypeImpl::TotalSize() const {
	if (fields_.size()) {
		return fields_.back().Offset() + fields_.back().Sizeof();
	}
	return 0;
}

std::string PayloadTypeImpl::ToString() const {
	std::string ret;
	for (auto &f : fields_) {
		ret += std::string(Variant::TypeName(f.Type())) + (f.IsArray() ? "[]" : "") + " '" + f.Name() + "'" + " json:\"";
		for (auto &jp : f.JsonPaths()) ret += jp + ";";
		ret += "\"\n";
	}
	return ret;
}

void PayloadTypeImpl::Dump(std::ostream &os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << '{';
	for (auto &f : fields_) {
		os << '\n'
		   << newOffset << KeyValueTypeToStr(f.Type()) << (f.IsArray() ? "[]" : "") << " '" << f.Name() << "'"
		   << " json:\"";
		for (size_t i = 0, s = f.JsonPaths().size(); i < s; ++i) {
			if (i != 0) os << ';';
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
		auto &oldf = fields_[it->second];
		throw Error(errLogic, "Cannot add field with name '%s' and type '%s' to namespace '%s'. It already exists with type '%s'", f.Name(),
					Variant::TypeName(f.Type()), Name(), Variant::TypeName(oldf.Type()));
	} else {
		// Unique name -> just add field
		f.SetOffset(TotalSize());
		for (auto &jp : f.JsonPaths()) {
			if (!jp.length()) continue;
			auto res = fieldsByJsonPath_.emplace(jp, int(fields_.size()));
			if (!res.second && res.first->second != int(fields_.size())) {
				throw Error(errLogic, "Cannot add field with name '%s' to namespace '%s'. Json path '%s' already used in field '%s'",
							f.Name(), Name(), jp, Field(res.first->second).Name());
			}
		}
		fieldsByName_.emplace(f.Name(), int(fields_.size()));
		if (f.Type() == KeyValueString) {
			strFields_.push_back(int(fields_.size()));
		}
		fields_.push_back(std::move(f));
	}
}

bool PayloadTypeImpl::Drop(std::string_view field) {
	auto it = fieldsByName_.find(field);
	if (it == fieldsByName_.end()) return false;

	int fieldIdx = it->second;
	for (auto &it : fieldsByName_) {
		if (it.second > fieldIdx) --it.second;
	}
	for (auto &it : fieldsByJsonPath_) {
		if (it.second > fieldIdx) --it.second;
	}

	KeyValueType fieldType = fields_[fieldIdx].Type();
	for (auto it = strFields_.begin(); it != strFields_.end();) {
		if ((*it == fieldIdx) && (fieldType == KeyValueString)) {
			it = strFields_.erase(it);
			continue;
		} else if (*it > fieldIdx)
			--(*it);
		++it;
	}

	for (auto &jp : fields_[fieldIdx].JsonPaths()) {
		fieldsByJsonPath_.erase(jp);
	}

	fields_.erase(fields_.begin() + fieldIdx);
	for (size_t idx = static_cast<size_t>(fieldIdx); idx < fields_.size(); ++idx) {
		if (idx == 0) {
			fields_[idx].SetOffset(0);
		} else {
			const PayloadFieldType &plTypePrev(fields_[idx - 1]);
			fields_[idx].SetOffset(plTypePrev.Offset() + plTypePrev.Sizeof());
		}
	}

	fieldsByName_.erase(field);

	return true;
}

bool PayloadTypeImpl::Contains(std::string_view field) const { return fieldsByName_.find(field) != fieldsByName_.end(); }

int PayloadTypeImpl::FieldByName(std::string_view field) const {
	auto it = fieldsByName_.find(field);
	if (it == fieldsByName_.end()) throw Error(errLogic, "Field '%s' not found in namespace '%s'", field, Name());
	return it->second;
}

bool PayloadTypeImpl::FieldByName(std::string_view name, int &field) const {
	auto it = fieldsByName_.find(name);
	if (it == fieldsByName_.end()) return false;
	field = it->second;
	return true;
}

int PayloadTypeImpl::FieldByJsonPath(std::string_view jsonPath) const {
	auto it = fieldsByJsonPath_.find(jsonPath);
	if (it == fieldsByJsonPath_.end()) return -1;
	return it->second;
}

void PayloadTypeImpl::serialize(WrSerializer &ser) const {
	ser.PutVarUint(base_key_string::export_hdr_offset());
	ser.PutVarUint(NumFields());
	for (int i = 0; i < NumFields(); i++) {
		ser.PutVarUint(Field(i).Type());
		ser.PutVString(Field(i).Name());
		ser.PutVarUint(Field(i).Offset());
		ser.PutVarUint(Field(i).ElemSizeof());
		ser.PutVarUint(Field(i).IsArray());
		ser.PutVarUint(Field(i).JsonPaths().size());
		for (auto &jp : Field(i).JsonPaths()) ser.PutVString(jp);
	}
}

void PayloadTypeImpl::deserialize(Serializer &ser) {
	fields_.clear();
	fieldsByName_.clear();
	fieldsByJsonPath_.clear();
	strFields_.clear();

	ser.GetVarUint();

	int count = ser.GetVarUint();

	for (int i = 0; i < count; i++) {
		KeyValueType t = KeyValueType(ser.GetVarUint());
		std::string name(ser.GetVString());
		std::vector<std::string> jsonPaths;
		int offset = ser.GetVarUint();
		int elemSizeof = ser.GetVarUint();
		bool isArray = ser.GetVarUint();
		int jsonPathsCount = ser.GetVarUint();

		while (jsonPathsCount--) jsonPaths.push_back(std::string(ser.GetVString()));

		(void)elemSizeof;

		PayloadFieldType ft(t, name, jsonPaths, isArray);

		if (isArray) ft.SetArray();
		ft.SetOffset(offset);
		fieldsByName_.emplace(name, fields_.size());
		if (t == KeyValueString) strFields_.push_back(fields_.size());
		fields_.push_back(ft);
	}
}

PayloadType::PayloadType(const std::string &name, std::initializer_list<PayloadFieldType> fields)
	: shared_cow_ptr<PayloadTypeImpl>(make_intrusive<intrusive_atomic_rc_wrapper<PayloadTypeImpl>>(name, fields)) {}
PayloadType::PayloadType(const PayloadTypeImpl &impl)
	: shared_cow_ptr<PayloadTypeImpl>(make_intrusive<intrusive_atomic_rc_wrapper<PayloadTypeImpl>>(impl)) {}
PayloadType::~PayloadType() = default;
const PayloadFieldType &PayloadType::Field(int field) const { return get()->Field(field); }
const std::string &PayloadType::Name() const { return get()->Name(); }
void PayloadType::SetName(const std::string &name) { clone()->SetName(name); }
int PayloadType::NumFields() const { return get()->NumFields(); }
void PayloadType::Add(PayloadFieldType f) { clone()->Add(std::move(f)); }
bool PayloadType::Drop(std::string_view field) { return clone()->Drop(field); }
int PayloadType::FieldByName(std::string_view field) const { return get()->FieldByName(field); }
bool PayloadType::FieldByName(std::string_view name, int &field) const { return get()->FieldByName(name, field); }
bool PayloadType::Contains(std::string_view field) const { return get()->Contains(field); }
int PayloadType::FieldByJsonPath(std::string_view jsonPath) const { return get()->FieldByJsonPath(jsonPath); }
const std::vector<int> &PayloadType::StrFields() const { return get()->StrFields(); }
size_t PayloadType::TotalSize() const { return get()->TotalSize(); }
std::string PayloadType::ToString() const { return get()->ToString(); }

void PayloadType::Dump(std::ostream &os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << "{\n" << newOffset << "name: " << Name() << ",\n" << newOffset;
	get()->Dump(os, step, newOffset);
	os << '\n' << offset << '}';
}

}  // namespace reindexer
