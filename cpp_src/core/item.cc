#include "core/item.h"
#include "core/itemimpl.h"
#include "core/keyvalue/p_string.h"
#include "core/namespace/namespace.h"
#include "tools/catch_and_return.h"

namespace reindexer {

Item::Item(ItemImpl* impl, const FieldsFilter& fieldsFilter) : Item{impl} {
	if (impl_) {
		impl_->fieldsFilter_ = &fieldsFilter;
	}
}
Item::Item(PayloadType pt, PayloadValue pv, const TagsMatcher& tm, std::shared_ptr<const Schema> schema, const FieldsFilter& fieldsFilter)
	: Item(new ItemImpl(std::move(pt), std::move(pv), tm, std::move(schema), fieldsFilter)) {}

Item& Item::operator=(Item&& other) noexcept {
	if (&other != this) {
		if (impl_) {
			auto ns = impl_->GetNamespace().lock();
			if (ns) {
				try {
					ns->ToPool(impl_);
					// NOLINTBEGIN(bugprone-empty-catch)
				} catch (...) {
					assertrx_dbg(false);
				}
				// NOLINTEND(bugprone-empty-catch)
				impl_ = nullptr;
			}
		}
		delete impl_;
		impl_ = other.impl_;
		status_ = std::move(other.status_);
		id_ = other.id_;
		other.impl_ = nullptr;
	}
	return *this;
}

KeyValueType Item::GetIndexType(int field) const noexcept {
	if (!impl_ || field < 0 || field >= impl_->Type().NumFields()) {
		return KeyValueType::Undefined{};
	}
	return impl_->Type().Field(field).Type();
}

std::string_view Item::FieldRef::Name() const { return field_ >= 0 ? itemImpl_->Type().Field(field_).Name() : jsonPath(); }

template <>
Point Item::FieldRef::As<Point>() const {
	auto va = (operator VariantArray());
	if (va.size() != 2) {
		throw Error(errParams, "Unable to convert field with {} scalar values to 2D Point", va.size());
	}
	return Point(va[0].As<double>(), va[1].As<double>());
}

Item::FieldRef::operator Variant() const {
	throwIfNotSet();
	VariantArray kr;
	if (field_ >= 0) {
		itemImpl_->GetPayload().Get(field_, kr);
	} else {
		kr = itemImpl_->GetValueByJSONPath(jsonPath());
	}

	if (kr.size() != 1) {
		throw Error(errParams, "Invalid array access");
	}
	return kr[0];
}

Item::FieldRef::operator VariantArray() const {
	throwIfNotSet();
	if (field_ >= 0) {
		VariantArray kr;
		itemImpl_->GetPayload().Get(field_, kr);
		return kr;
	}
	return itemImpl_->GetValueByJSONPath(jsonPath());
}

void Item::FieldRef::throwIfAssignFieldMultyJsonPath() const {
	int field = field_ >= 0 ? field_ : itemImpl_->Type()->FieldByJsonPath(jsonPath());

	if (field < 0) {
		return;
	}

	if (auto& fieldPl = itemImpl_->Type().Field(field); fieldPl.JsonPaths().size() > 1) {
		throw Error(errLogic, "It is not allowed to use fields with multiple json paths in the Item::operator[]. Index name = `{}`",
					fieldPl.Name());
	}
}

Item::FieldRef& Item::FieldRef::operator=(Variant kr) {
	throwIfAssignFieldMultyJsonPath();

	if (field_ >= 0) {
		itemImpl_->SetField(field_, VariantArray{std::move(kr)});
	} else {
		itemImpl_->SetField(jsonPath(), VariantArray{std::move(kr)});
	}

	return *this;
}

Item::FieldRef& Item::FieldRef::operator=(const char* str) { return operator=(p_string(str)); }
Item::FieldRef& Item::FieldRef::operator=(const std::string& str) { return operator=(p_string(&str)); }

Item::FieldRef& Item::FieldRef::operator=(const VariantArray& krs) {
	if (field_ >= 0) {
		itemImpl_->SetField(field_, krs);
	} else {
		itemImpl_->SetField(jsonPath(), krs);
	}
	return *this;
}

template <typename T>
Item::FieldRef& Item::FieldRef::operator=(std::span<const T> arr) {
	constexpr static bool kIsStr = std::is_same_v<T, std::string> || std::is_same_v<T, key_string> || std::is_same_v<T, p_string> ||
								   std::is_same_v<T, std::string_view> || std::is_same_v<T, const char*>;
	if (field_ < 0) {
		VariantArray krs;
		std::ignore = krs.MarkArray();
		krs.reserve(arr.size());
		std::transform(arr.begin(), arr.end(), std::back_inserter(krs), [](const T& t) { return Variant(t); });
		itemImpl_->SetField(jsonPath(), krs);
		return *this;
	}

	auto pl(itemImpl_->GetPayload());
	int pos = pl.ResizeArray(field_, arr.size(), Append_False);

	if constexpr (kIsStr) {
		if (itemImpl_->IsUnsafe() || itemImpl_->Type()->Field(field_).Type().Is<KeyValueType::Uuid>()) {
			for (auto& elem : arr) {
				pl.Set(field_, pos++, Variant(elem));
			}
		} else {
			if (!itemImpl_->holder_) {
				itemImpl_->holder_ = std::make_unique<ItemImplRawData::HolderT>();
			}
			for (auto& elem : arr) {
				if constexpr (std::is_same_v<T, p_string>) {
					itemImpl_->holder_->emplace_back(elem.getKeyString());
				} else if constexpr (std::is_same_v<T, key_string>) {
					itemImpl_->holder_->emplace_back(elem);
				} else {
					itemImpl_->holder_->emplace_back(make_key_string(elem));
				}
				pl.Set(field_, pos++, Variant(p_string{itemImpl_->holder_->back()}, Variant::noHold));
			}
		}
	} else {
		for (auto& elem : arr) {
			pl.Set(field_, pos++, Variant(elem));
		}
	}
	return *this;
}

void Item::FieldRef::throwIfNotSet() const {
	if (notSet_) {
		throw Error{errParams, "Field {} is not set", Name()};
	}
}

Item::~Item() {
	if (impl_) {
		auto ns = impl_->GetNamespace().lock();
		if (ns) {
			try {
				ns->ToPool(impl_);
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
				assertrx_dbg(false);
			}
			// NOLINTEND(bugprone-empty-catch)
		} else {
			delete impl_;
		}
	}
}

Error Item::FromJSON(std::string_view slice, char** endp, bool pkOnly) & noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->FromJSON(slice, endp, pkOnly));
}

Error Item::FromCJSON(std::string_view slice, bool pkOnly) & noexcept {
	try {
		impl_->FromCJSON(slice, pkOnly);
	}
	CATCH_AND_RETURN;
	return {};
}
void Item::FromCJSONImpl(std::string_view slice, bool pkOnly) & { impl_->FromCJSON(slice, pkOnly); }

std::string_view Item::GetCJSON(bool withTagsMatcher) { return impl_->GetCJSON(withTagsMatcher); }

std::string_view Item::GetJSON() { return impl_->GetJSON(); }

Error Item::FromMsgPack(std::string_view buf, size_t& offset) & noexcept { RETURN_RESULT_NOEXCEPT(impl_->FromMsgPack(buf, offset)); }

Error Item::FromProtobuf(std::string_view sbuf) & noexcept { RETURN_RESULT_NOEXCEPT(impl_->FromProtobuf(sbuf)); }

Error Item::GetMsgPack(WrSerializer& wrser) & noexcept { RETURN_RESULT_NOEXCEPT(impl_->GetMsgPack(wrser)); }

std::string_view Item::GetMsgPack() & { return impl_->GetMsgPack(); }

Error Item::GetProtobuf(WrSerializer& wrser) & noexcept { RETURN_RESULT_NOEXCEPT(impl_->GetProtobuf(wrser)); }

int Item::NumFields() const { return impl_->Type().NumFields(); }

Item::FieldRef Item::operator[](int field) const {
	if (field < 0 || field >= impl_->Type().NumFields()) [[unlikely]] {
		throw Error(errLogic, "Item::operator[] requires indexed field. Values range: [0; {}]", impl_->Type().NumFields());
	}
	const bool notSet = impl_->Type().Field(field).Type().Is<KeyValueType::FloatVector>() && impl_->fieldsFilter_ &&
						!impl_->fieldsFilter_->ContainsVector(field);
	return FieldRef(field, impl_, notSet);
}

Item::FieldRef Item::FieldRefByNameOrJsonPath(std::string_view name, ItemImpl& impl) noexcept {
	int field = 0;

	if (!impl.Type().FieldByName(name, field)) {
		field = impl.Type()->FieldByJsonPath(name);
		if (field > 0 && impl.Type().Field(field).JsonPaths().size() > 1) {
			return FieldRef(name, &impl, false);
		}
	}

	if (field > 0) {
		const bool notSet = impl.Type().Field(field).Type().Is<KeyValueType::FloatVector>() && impl.fieldsFilter_ &&
							!impl.fieldsFilter_->ContainsVector(field);
		return FieldRef(field, &impl, notSet);
	} else {
		const auto& sparseIndexes = impl.tagsMatcher().SparseIndexes();
		if (auto it = std::find_if(sparseIndexes.begin(), sparseIndexes.end(), [&name](const auto& data) { return data.name == name; });
			it != sparseIndexes.end()) {
			return FieldRef(impl.tagsMatcher().Path2Name(it->paths[0]), &impl, false);
		}
		return FieldRef(name, &impl, false);
	}
}

void Item::Embed(const RdxContext& ctx) { impl_->Embed(ctx); }

TagName Item::GetFieldTag(std::string_view name) const { return impl_->NameTag(name); }
int Item::GetFieldIndex(std::string_view name) const { return impl_->FieldIndex(name); }
FieldsSet Item::PkFields() const { return impl_->PkFields(); }
void Item::SetPrecepts(std::vector<std::string> precepts) & { impl_->SetPrecepts(std::move(precepts)); }
bool Item::IsTagsUpdated() const noexcept { return impl_->tagsMatcher().isUpdated(); }
int Item::GetStateToken() const noexcept { return impl_->tagsMatcher().stateToken(); }

Item& Item::Unsafe(bool enable) & noexcept {
	impl_->Unsafe(enable);
	return *this;
}

lsn_t Item::GetLSN() { return impl_->RealValue().IsFree() ? impl_->Value().GetLSN() : impl_->RealValue().GetLSN(); }
void Item::setLSN(lsn_t lsn) { impl_->RealValue().IsFree() ? impl_->Value().SetLSN(lsn) : impl_->RealValue().SetLSN(lsn); }
void Item::setFieldsFilter(const FieldsFilter& fieldsFilter) noexcept { impl_->fieldsFilter_ = &fieldsFilter; }

template Item::FieldRef& Item::FieldRef::operator=(std::span<const int> arr);
template Item::FieldRef& Item::FieldRef::operator=(std::span<const int64_t> arr);
template Item::FieldRef& Item::FieldRef::operator=(std::span<const std::string> arr);
template Item::FieldRef& Item::FieldRef::operator=(std::span<const double>);
template Item::FieldRef& Item::FieldRef::operator=(std::span<const Uuid>);

}  // namespace reindexer
