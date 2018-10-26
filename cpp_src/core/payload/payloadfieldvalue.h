#pragma once

#include "core/keyvalue/variant.h"
#include "payloadfieldtype.h"

namespace reindexer {

// Helper field's' value object
class PayloadFieldValue {
public:
	struct Array {
		unsigned offset;
		int len;
	};
	// Construct object
	PayloadFieldValue(const PayloadFieldType &t, uint8_t *v) : t_(t), p_(v) {}
	// Single value operations
	void Set(Variant kv);
	Variant Get(bool enableHold = false) const;

	// Type of value, not owning
	const PayloadFieldType &t_;
	// Value data, not owning
	uint8_t *p_;
};

}  // namespace reindexer
