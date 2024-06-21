
#include "core/item.h"
#include "core/itemimpl.h"
#include "core/keyvalue/p_string.h"
#include "core/namespace/namespace.h"
#include "tools/catch_and_return.h"

namespace reindexer {

Item &Item::operator=(Item &&other) noexcept {
	if (&other != this) {
		if (impl_) {
			auto ns = impl_->GetNamespace();
			if (ns) {
				ns->ToPool(impl_);
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

std::string_view Item::FieldRef::Name() const { return field_ >= 0 ? itemImpl_->Type().Field(field_).Name() : jsonPath_; }

template <>
Point Item::FieldRef::As<Point>() const {
	auto va = (operator VariantArray());
	if (va.size() != 2) {
		throw Error(errParams, "Unable to convert field with %d scalar values to 2D Point", va.size());
	}
	return Point(va[0].As<double>(), va[1].As<double>());
}

Item::FieldRef::operator Variant() const {
	VariantArray kr;
	if (field_ >= 0)
		itemImpl_->GetPayload().Get(field_, kr);
	else
		kr = itemImpl_->GetValueByJSONPath(jsonPath_);

	if (kr.size() != 1) {
		throw Error(errParams, "Invalid array access");
	}
	return kr[0];
}

Item::FieldRef::operator VariantArray() const {
	if (field_ >= 0) {
		VariantArray kr;
		itemImpl_->GetPayload().Get(field_, kr);
		return kr;
	}
	return itemImpl_->GetValueByJSONPath(jsonPath_);
}

Item::FieldRef &Item::FieldRef::operator=(Variant kr) {
	if (field_ >= 0) {
		itemImpl_->SetField(field_, VariantArray{std::move(kr)});
	} else {
		itemImpl_->SetField(jsonPath_, VariantArray{std::move(kr)}, nullptr);
	}

	return *this;
}

Item::FieldRef &Item::FieldRef::operator=(const char *str) { return operator=(p_string(str)); }
Item::FieldRef &Item::FieldRef::operator=(const std::string &str) { return operator=(p_string(&str)); }

Item::FieldRef &Item::FieldRef::operator=(const VariantArray &krs) {
	if (field_ >= 0) {
		itemImpl_->SetField(field_, krs);
	} else {
		itemImpl_->SetField(jsonPath_, krs, nullptr);
	}
	return *this;
}

template <typename T>
Item::FieldRef &Item::FieldRef::operator=(span<T> arr) {
	constexpr static bool kIsStr = std::is_same_v<T, std::string> || std::is_same_v<T, key_string> || std::is_same_v<T, p_string> ||
								   std::is_same_v<T, std::string_view> || std::is_same_v<T, const char *>;
	if (field_ < 0) {
		VariantArray krs;
		krs.MarkArray();
		krs.reserve(arr.size());
		std::transform(arr.begin(), arr.end(), std::back_inserter(krs), [](const T &t) { return Variant(t); });
		itemImpl_->SetField(jsonPath_, krs, nullptr);
		return *this;
	}

	auto pl(itemImpl_->GetPayload());
	int pos = pl.ResizeArray(field_, arr.size(), false);

	if constexpr (kIsStr) {
		if (itemImpl_->IsUnsafe() || itemImpl_->Type()->Field(field_).Type().Is<KeyValueType::Uuid>()) {
			for (auto &elem : arr) {
				pl.Set(field_, pos++, Variant(elem));
			}
		} else {
			if (!itemImpl_->holder_) itemImpl_->holder_ = std::make_unique<std::deque<std::string>>();
			for (auto &elem : arr) {
				if constexpr (std::is_same_v<T, p_string>) {
					itemImpl_->holder_->push_back(elem.toString());
				} else {
					itemImpl_->holder_->push_back(elem);
				}
				pl.Set(field_, pos++, Variant(p_string{&itemImpl_->holder_->back()}, Variant::no_hold_t{}));
			}
		}
	} else {
		for (auto &elem : arr) {
			pl.Set(field_, pos++, Variant(elem));
		}
	}
	return *this;
}

Item::~Item() {
	if (impl_) {
		auto ns = impl_->GetNamespace();
		if (ns) {
			ns->ToPool(impl_);
		} else {
			delete impl_;
		}
	}
}

Error Item::FromJSON(std::string_view slice, char **endp, bool pkOnly) & noexcept {
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

std::string_view Item::GetCJSON() { return impl_->GetCJSON(); }
std::string_view Item::GetJSON() { return impl_->GetJSON(); }

Error Item::FromMsgPack(std::string_view buf, size_t &offset) & noexcept { RETURN_RESULT_NOEXCEPT(impl_->FromMsgPack(buf, offset)); }

Error Item::FromProtobuf(std::string_view sbuf) & noexcept { RETURN_RESULT_NOEXCEPT(impl_->FromProtobuf(sbuf)); }

Error Item::GetMsgPack(WrSerializer &wrser) & noexcept { RETURN_RESULT_NOEXCEPT(impl_->GetMsgPack(wrser)); }

std::string_view Item::GetMsgPack() & { return impl_->GetMsgPack(); }

Error Item::GetProtobuf(WrSerializer &wrser) & noexcept { RETURN_RESULT_NOEXCEPT(impl_->GetProtobuf(wrser)); }

int Item::NumFields() const { return impl_->Type().NumFields(); }

Item::FieldRef Item::operator[](int field) const {
	if (rx_unlikely(field < 0 || field >= impl_->Type().NumFields())) {
		throw Error(errLogic, "Item::operator[] requires indexed field. Values range: [0; %d]", impl_->Type().NumFields());
	}
	return FieldRef(field, impl_);
}

Item::FieldRef Item::FieldRefByName(std::string_view name, ItemImpl &impl) {
	int field = 0;
	return (impl.Type().FieldByName(name, field)) ? FieldRef(field, &impl) : FieldRef(name, &impl);
}

int Item::GetFieldTag(std::string_view name) const { return impl_->NameTag(name); }
FieldsSet Item::PkFields() const { return impl_->PkFields(); }
void Item::SetPrecepts(const std::vector<std::string> &precepts) & { impl_->SetPrecepts(precepts); }
bool Item::IsTagsUpdated() const noexcept { return impl_->tagsMatcher().isUpdated(); }
int Item::GetStateToken() const noexcept { return impl_->tagsMatcher().stateToken(); }

Item &Item::Unsafe(bool enable) & noexcept {
	impl_->Unsafe(enable);
	return *this;
}

int64_t Item::GetLSN() { return impl_->Value().GetLSN(); }
void Item::setLSN(int64_t lsn) { impl_->Value().SetLSN(lsn); }

template Item::FieldRef &Item::FieldRef::operator=(span<int> arr);
template Item::FieldRef &Item::FieldRef::operator=(span<int64_t> arr);
template Item::FieldRef &Item::FieldRef::operator=(span<std::string> arr);
template Item::FieldRef &Item::FieldRef::operator=(span<double>);
template Item::FieldRef &Item::FieldRef::operator=(span<Uuid>);

}  // namespace reindexer
