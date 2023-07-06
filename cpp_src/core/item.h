#pragma once

#include "core/keyvalue/geometry.h"
#include "core/keyvalue/variant.h"
#include "estl/span.h"
#include "tools/errors.h"

namespace reindexer {

namespace client {
class ReindexerImpl;
class Namespace;
}  // namespace client

class ItemImpl;
class FieldRefImpl;
class Replicator;
class Schema;

/// Item is the interface for data manipulating. It holds and control one database document (record)<br>
/// *Lifetime*: Item is uses Copy-On-Write semantics, and have independent lifetime and state - e.g., aquired from Reindexer Item will
/// not changed externally, even in case, when data in database was changed, or deleted. *Thread safety*: Item is thread safe against
/// Reindexer, but not thread safe itself. Usage of single Item from different threads will race

class Item {
public:
	/// Construct empty Item
	Item() noexcept : impl_(nullptr), status_(errNotValid) {}
	/// Destroy Item
	~Item();
	Item(const Item &) = delete;
	Item(Item &&other) noexcept : impl_(other.impl_), status_(std::move(other.status_)), id_(other.id_) { other.impl_ = nullptr; }
	Item &operator=(const Item &) = delete;
	Item &operator=(Item &&) noexcept;

	/// Reference to field. Interface for field data manipulation
	class FieldRef {
		friend class Item;

	public:
		/// Get field value. Strong type check. T must be same as field type. Throws reindexer::Error, if type mismatched
		/// If field type is array, and not contains exact 1 element, then throws reindexer::Error
		/// @tparam T - type. Must be one of: int, int64_t, double or string_view
		/// @return value of field
		template <typename T>
		const T Get() {
			return static_cast<T>(operator Variant());
		}
		/// Get field value as specified type, convert type if neccesary. In case when convertion fails throws reindexer::Error
		/// If field is array, and not contains exact 1 element, then throws reindexer::Error
		/// @tparam T - type. Must be one of: int, int64_t, double or std::string
		/// @return value of field
		template <typename T>
		T As() {
			return (operator Variant()).As<T>();
		}
		/// Set single fundamental type value
		/// @tparam T - type. Must be one of: int, int64_t, double
		/// @param val - value, which will be setted to field
		template <typename T>
		FieldRef &operator=(const T &val) {
			return operator=(Variant(val));
		}
		/// Set single point type value
		/// @param p - point value, which will be setted to field
		FieldRef &operator=(Point p) {
			const double arr[]{p.X(), p.Y()};
			return operator=(span<double>(arr, 2));
		}

		/// Set array of values to field
		/// @tparam T - type. Must be one of: int, int64_t, double
		/// @param arr - std::vector of T values, which will be setted to field
		template <typename T>
		FieldRef &operator=(span<T> arr);
		/// Set array of values to field
		/// @tparam T - type. Must be one of: int, int64_t, double
		/// @param arr - std::vector of T values, which will be setted to field
		template <typename T>
		FieldRef &operator=(const std::vector<T> &arr) {
			return operator=(span<T>(arr));
		}
		/// Set string value to field
		/// If Item is in Unsafe Mode, then Item will not store str, but just keep pointer to str,
		/// application *MUST* hold str until end of life of Item
		/// @param str - pointer to C null-terminated string, which will be setted to field
		FieldRef &operator=(const char *str);
		/// Set string value<br>
		/// If Item is in Unsafe Mode, then Item will not store str, but just keep pointer to str,
		/// application *MUST* hold str until end of life of Item
		/// @param str - std::string, which will be setted to field
		FieldRef &operator=(const std::string &str);

		/// Get field index name
		std::string_view Name() const;

		/// Get Variant with field value
		/// If field is array, and contains not exact 1 element, then throws reindexer::Error
		/// @return Variant object with field value
		operator Variant() const;
		/// Get VariantArray with field values. If field is not array, then 1 elemnt will be returned
		/// @return VariantArray with field values
		operator VariantArray() const;
		/// Set field value
		/// @param kr - key reference object, which will be set to field
		FieldRef &operator=(Variant kr);
		/// Set field value
		/// @param krs - key reference object, which will be set to field
		FieldRef &operator=(const VariantArray &krs);

	private:
		FieldRef(int field, ItemImpl *itemImpl);
		FieldRef(std::string_view jsonPath, ItemImpl *itemImpl);
		ItemImpl *itemImpl_;
		std::string_view jsonPath_;
		int field_;
	};

	/// Build item from JSON<br>
	/// If Item is in *Unsafe Mode*, then Item will not store slice, but just keep pointer to data in slice,
	/// application *MUST* hold slice until end of life of Item
	/// @param slice - data slice with Json.
	/// @param endp - pointer to end of parsed part of slice
	/// @param pkOnly - if TRUE, that mean a JSON string will be parse only primary key fields
	[[nodiscard]] Error FromJSON(std::string_view slice, char **endp = nullptr, bool pkOnly = false) &noexcept;

	/// Build item from JSON<br>
	/// If Item is in *Unsafe Mode*, then Item will not store slice, but just keep pointer to data in slice,
	/// application *MUST* hold slice until end of life of Item
	/// @param slice - data slice with CJson
	/// @param pkOnly - if TRUE, that mean a JSON string will be parse only primary key fields
	[[nodiscard]] Error FromCJSON(std::string_view slice, bool pkOnly = false) &noexcept;
	void FromCJSONImpl(std::string_view slice, bool pkOnly = false) &;

	/// Builds item from msgpack::object.
	/// @param buf - msgpack encoded data buffer.
	/// @param offset - position to start from.
	[[nodiscard]] Error FromMsgPack(std::string_view buf, size_t &offset) &noexcept;

	/// Builds item from Protobuf
	/// @param sbuf - Protobuf encoded data
	[[nodiscard]] Error FromProtobuf(std::string_view sbuf) &noexcept;

	/// Packs data in msgpack format
	/// @param wrser - buffer to serialize data to
	[[nodiscard]] Error GetMsgPack(WrSerializer &wrser) &noexcept;

	/// Packs item data to Protobuf
	/// @param wrser - buffer to serialize data to
	[[nodiscard]] Error GetProtobuf(WrSerializer &wrser) &noexcept;

	/// Serialize item to CJSON.<br>
	/// If Item is in *Unsafe Mode*, then returned slice is allocated in temporary buffer, and can be invalidated by any next operation
	/// with Item
	/// @return data slice with CJSON
	[[nodiscard]] std::string_view GetCJSON();
	/// Serialize item to JSON.<br>
	/// @return data slice with JSON. Returned slice is allocated in temporary Item's buffer, and can be invalidated by any next
	/// operation with Item
	[[nodiscard]] std::string_view GetJSON();
	/// Get status of item
	/// @return data slice with JSON. Returned slice is allocated in temporary Item's buffer, and can be invalidated by any next
	/// operation with Item
	[[nodiscard]] Error Status() const noexcept { return status_; }
	/// Get internal ID of item
	/// @return ID of item
	[[nodiscard]] int GetID() const noexcept { return id_; }
	/// Get internal version of item
	/// @return version of item
	[[nodiscard]] int64_t GetLSN();
	/// Get count of indexed field
	/// @return count of  field
	[[nodiscard]] int NumFields() const;
	/// Get field by number
	/// @param field - number of field. Must be >= 0 && < NumFields
	/// @return FieldRef which contains reference to indexed field
	[[nodiscard]] FieldRef operator[](int field) const;
	/// Get field by name
	/// @param name - name of field
	/// @return FieldRef which contains reference to indexed field
	[[nodiscard]] FieldRef operator[](std::string_view name) const;
	/// Get field's name tag
	/// @param name - field name
	/// @return name's numeric tag value
	[[nodiscard]] int GetFieldTag(std::string_view name) const;
	/// Get PK fields
	[[nodiscard]] FieldsSet PkFields() const;
	/// Set additional percepts for modify operation
	/// @param precepts - strings in format "fieldName=Func()"
	void SetPrecepts(const std::vector<std::string> &precepts) &;
	/// Check was names tags updated while modify operation
	/// @return true: tags was updated.
	[[nodiscard]] bool IsTagsUpdated();
	/// Get state token
	/// @return Current state token
	[[nodiscard]] int GetStateToken();
	/// Check is item valid. If is not valid, then any futher operations with item will raise nullptr dereference
	[[nodiscard]] bool operator!() const { return impl_ == nullptr; }
	/// Enable Unsafe Mode<br>.
	/// USE WITH CAUTION. In unsafe mode most of Item methods will not store  strings and slices, passed from/to application.<br>
	/// The advantage of unsafe mode is speed. It does not call extra memory allocation from heap and copying data.<br>
	/// The disadvantage of unsafe mode is potentially danger code. Most of C++ stl containters in many cases invalidates references -
	/// and in unsafe mode caller is responsibe to guarantee, that all resources passed to Item will keep valid
	Item &Unsafe(bool enable = true) &noexcept;
	/// Get index type by field id
	/// @return either index type or Undefined (if index with this number does not exist or PayloadType is not available)
	KeyValueType GetIndexType(int field) const noexcept;

private:
	explicit Item(ItemImpl *impl) : impl_(impl) {}
	explicit Item(const Error &err) : impl_(nullptr), status_(err) {}
	void setID(int id) { id_ = id; }
	void setLSN(int64_t lsn);

	ItemImpl *impl_;
	Error status_;
	int id_ = -1;
	friend class NamespaceImpl;
	friend class TransactionImpl;
	friend class ItemModifier;

	friend class QueryResults;
	friend class ReindexerImpl;
	friend class Replicator;
	friend class TransactionStep;
	friend class client::ReindexerImpl;
	friend class client::Namespace;
};

}  // namespace reindexer
