#pragma once

#include <type_traits>
#include "fieldsset.h"
#include "payloadfieldvalue.h"
#include "payloadtype.h"

namespace reindexer {

template <typename T>
class PayloadIface {
	template <typename U>
	friend class PayloadIface;

public:
	PayloadIface(const PayloadType &t, T *v);
	PayloadIface(const PayloadType::Ptr t, T &v);

	void Reset() { memset(v_->Ptr(), 0, t_.TotalSize()); }
	// Get element(s) by field index
	KeyRefs &Get(int field, KeyRefs &) const;
	// Set element or array by field index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(int field, const KeyRefs &keys, bool append = false);
	// Set element or array by field index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(const string &field, const KeyRefs &keys, bool append = false);

	// Get element(s) by field index
	KeyRefs &Get(const string &field, KeyRefs &) const;

	// Get fields count
	int NumFields() const { return t_.NumFields(); }

	// Real size of payload with arrays
	size_t RealSize() const;
	inline const uint8_t *Ptr() const { return v_->Ptr(); }
	const PayloadType &Type() const { return t_; }
	const T *Value() const { return v_; }

	// Get primary key for elem
	string GetPK(const FieldsSet &pkFields) const;
	void GetPK(char *buf, size_t size, const FieldsSet &pkFields) const;

	// Get PK hash
	size_t GetHash(const FieldsSet &fields) const;
	// Compare is EQ by field mask
	bool IsEQ(const T *other, const FieldsSet &fields) const;
	// Compare is EQ
	bool IsEQ(const T *other) const;

	// Compare is PK less
	bool Less(const T *other, const FieldsSet &fields) const;

	// Get PayloadFieldValue by field index
	PayloadFieldValue Field(int field) const;

	// Add refs to strings - make payload value complete self holding
	void AddRefStrings();
	// Release strings
	void ReleaseStrings();

	// Item values' string for printing
	std::string Dump();

protected:
	// Array of elements types , not owning
	const PayloadType &t_;
	// Data of elements, not owning
	T *v_;
};

using Payload = PayloadIface<PayloadValue>;
using ConstPayload = PayloadIface<const PayloadValue>;

}  // namespace reindexer
