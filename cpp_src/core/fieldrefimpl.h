#pragma once

#include "core/keyvalue/keyref.h"

namespace reindexer {

class ItemImpl;

class FieldRefImpl {
public:
	virtual ~FieldRefImpl() = default;
	virtual const std::string &Name() = 0;
	virtual KeyRef GetValue() = 0;
	virtual KeyRefs GetValues() = 0;
	virtual void SetValue(KeyRef &) = 0;
	virtual void SetValue(const KeyRefs &) = 0;
};

class RegularFieldRefImpl : public FieldRefImpl {
public:
	RegularFieldRefImpl(int field, ItemImpl *itemImpl);
	~RegularFieldRefImpl();

	const string &Name() override;
	KeyRef GetValue() override;
	KeyRefs GetValues() override;

	void SetValue(KeyRef &kr) override;
	void SetValue(const KeyRefs &krefs) override;

private:
	int field_;
	ItemImpl *itemImpl_;
};

class CjsonFieldRefImpl : public FieldRefImpl {
public:
	CjsonFieldRefImpl(const std::string &jsonPath, ItemImpl *itemImpl);
	~CjsonFieldRefImpl();

	const string &Name() override;
	KeyRef GetValue() override;
	KeyRefs GetValues() override;

	void SetValue(KeyRef &kr) override;
	void SetValue(const KeyRefs &krefs) override;

private:
	std::string jsonPath_;
	ItemImpl *itemImpl_;
};
}  // namespace reindexer
