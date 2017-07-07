#pragma once

#include <type_traits>
#include "payload.h"

namespace reindexer {

template <typename T>
class BasePayload {
public:
	BasePayload(const PayloadType &t, T *p);

	void Reset() { memset(p_->Ptr(), 0, t_.TotalSize()); }
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
	inline const uint8_t *Ptr() const { return p_->Ptr(); }
	const PayloadType &Type() const { return t_; }
	const T *Data() const { return p_; }

	// Get primary key for elem
	string GetPK() const;
	void GetPK(char *buf, size_t size) const;

	// Compare is PK EQ
	bool IsPKEQ(T *other) const;

	// Get PK hash
	size_t GetHash(const FieldsSet &fields) const;
	// Compare is PK EQ
	bool IsEQ(T *other, const FieldsSet &fields) const;

	// Compare is PK less
	bool Less(T *other, const FieldsSet &fields) const;

	// Get PK hash
	size_t GetPKHash() const;

protected:
	PayloadValue Field(int field) const;
	// Array of elements types , not owning
	const PayloadType &t_;
	// Data of elements, owning
	T *p_;
};

class Payload : public BasePayload<PayloadData> {
public:
	Payload(const PayloadType &t, PayloadData *n) : BasePayload(t, n) {}
};

class ConstPayload : public BasePayload<const PayloadData> {
public:
	ConstPayload(const PayloadType &t, const PayloadData *n) : BasePayload(t, n) {}
};

struct PayloadProvider {
	virtual ConstPayload GetPayload(IdType id) const = 0;
	virtual Payload GetPayload(IdType id) = 0;
};
}  // namespace reindexer
