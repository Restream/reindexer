#pragma once

#include "keyref.h"

namespace reindexer {
class PayloadType;
class FieldsSet;

class KeyValue : public KeyRef {
public:
	KeyValue() {}
	explicit KeyValue(const KeyArray<KeyValue, 2> &values) {
		for (const KeyValue &kv : values) {
			h_composite_values.emplace_back(kv);
		}
		type = KeyValueComposite;
	}
	explicit KeyValue(const string &v) : h_value_string(make_key_string(v.data(), v.length())) { type = KeyValueString, relink(); }
	explicit KeyValue(const key_string &v) : h_value_string(v) { type = KeyValueString, relink(); }
	explicit KeyValue(const PayloadValue &v) : KeyRef(h_value_composite), h_value_composite(v) {}
	explicit KeyValue(const int &v) : KeyRef(v) {}
	explicit KeyValue(const int64_t &v) : KeyRef(v) {}
	explicit KeyValue(const double &v) : KeyRef(v) {}
	KeyValue(const KeyRef &other);
	KeyValue(const KeyValue &other);
	KeyValue &operator=(const KeyValue &other);

	bool operator==(const KeyValue &other) const;
	bool operator!=(const KeyValue &other) const;

	explicit operator key_string() const {
		assertKeyType(type, KeyValueString);
		return h_value_string;
	}
	int convert(KeyValueType type);
	void convertToComposite(const PayloadType &, const FieldsSet &);
	const std::vector<KeyValue> &getCompositeValues() const;

protected:
	void relink() {
		if (type == KeyValueComposite) value_composite = &h_value_composite;
		if (type == KeyValueString) value_string = p_string(h_value_string.get());
	}

	PayloadValue h_value_composite;
	key_string h_value_string;
	std::vector<KeyValue> h_composite_values;
};

using KeyValues = KeyArray<KeyValue, 2>;

}  // namespace reindexer

namespace std {
template <>
struct hash<reindexer::KeyValue> {
public:
	size_t operator()(const reindexer::KeyValue &kv) const { return kv.Hash(); }
};

}  // namespace std
