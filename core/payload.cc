#include "core/payload.h"
#include "string.h"
#include "tools/errors.h"

namespace reindexer {

void PayloadValue::Set(KeyRef kv) {
	//	TODO: check this code for: 'string' != 'int' and 'int' != 'string'
	if (kv.Type() != t_.Type())
		throw Error(errLogic, "PayloadValue::Set field '%s' type mimatch. passed '%s', expected '%s'\n", t_.Name().c_str(),
					KeyValue::TypeName(kv.Type()), KeyValue::TypeName(t_.Type()));

	switch (t_.Type()) {
		case KeyValueInt:
			*(int *)(p_) = (int)kv;
			break;
		case KeyValueInt64:
			*(int64_t *)(p_) = (int64_t)kv;
			break;
		case KeyValueDouble:
			*(double *)p_ = (double)kv;
			break;
		case KeyValueString:
			*(p_string *)p_ = (p_string)kv;
			break;
		default:
			assert(0);
	}
}

KeyRef PayloadValue::Get() const {
	switch (t_.Type()) {
		case KeyValueInt:
			return KeyRef(*(const int *)p_);
		case KeyValueInt64:
			return KeyRef(*(const int64_t *)p_);
		case KeyValueDouble:
			return KeyRef(*(const double *)p_);
		case KeyValueString:
			return KeyRef(*(const p_string *)p_);
		default:
			abort();
	}
}

size_t PayloadFieldType::Sizeof() const {
	if (IsArray()) return sizeof(PayloadValue::PayloadArray);
	return ElemSizeof();
}

size_t PayloadFieldType::ElemSizeof() const {
	switch (Type()) {
		case KeyValueInt:
			return sizeof(int);
		case KeyValueInt64:
			return sizeof(int64_t);
		case KeyValueDouble:
			return sizeof(double);
		case KeyValueString:
			return sizeof(p_string);
		default:
			assert(0);
	}
	return 0;
}

size_t PayloadFieldType::Alignof() const {
	if (IsArray()) return alignof(PayloadValue::PayloadArray);
	switch (Type()) {
		case KeyValueInt:
			return alignof(int);
		case KeyValueInt64:
			return alignof(int64_t);
		case KeyValueDouble:
			return alignof(double);
		case KeyValueString:
			return alignof(p_string);
		default:
			assert(0);
	}
	return 0;
}

size_t PayloadType::TotalSize() const {
	if (fields_.size()) {
		return fields_.back().Offset() + fields_.back().Sizeof();
	}
	return 0;
}

string PayloadType::ToString() const {
	string ret;

	for (auto f : fields_) {
		ret += string(KeyValue::TypeName(f.Type())) + (f.IsArray() ? "[]" : "") + " '" + f.Name() + "'" + (f.IsPK() ? " pk" : "") +
			   " json:\"" + f.JsonPath() + "\"" + ";";
	}
	return ret;
}

void PayloadType::Add(PayloadFieldType f) {
	auto it = fieldsByName_.find(f.Name());
	if (it != fieldsByName_.end()) {
		// Non unique name -> check type, and upgrade to array if types are the same
		auto &oldf = fields_[it->second];
		if (oldf.Type() != f.Type())
			throw Error(errLogic, "Can't add field with name '%s' and type '%s' to namespace '%s'. It already exists with type '%s'",
						f.Name().c_str(), KeyValue::TypeName(f.Type()), Name().c_str(), KeyValue::TypeName(oldf.Type()));
		// Upgrade to array
		oldf.SetArray();
		// Move offsets of followed fields
		for (size_t i = it->second + 1; i < fields_.size(); ++i) fields_[i].SetOffset(fields_[i - 1].Offset() + fields_[i - 1].Sizeof());
		fieldsByJsonPath_.emplace(f.JsonPath(), it->second);
	} else {
		// Unique name -> just add field
		f.SetOffset(TotalSize());
		fieldsByName_.emplace(f.Name(), (int)fields_.size());
		fieldsByJsonPath_.emplace(f.JsonPath(), (int)fields_.size());
		fields_.push_back(f);
	}
}
int PayloadType::FieldByName(const string &field) const {
	auto it = fieldsByName_.find(field);
	if (it == fieldsByName_.end()) throw Error(errLogic, "Field '%s' not found in namespace '%s'", field.c_str(), Name().c_str());
	return it->second;
}

int PayloadType::FieldByJsonPath(const string &jsonPath) const {
	auto it = fieldsByJsonPath_.find(jsonPath);
	if (it == fieldsByJsonPath_.end()) return -1;
	return it->second;
}

}  // namespace reindexer
