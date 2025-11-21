#pragma once

#include <span>
#include <variant>
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/variant.h"
#include "core/tag_name_index.h"
#include "tools/errors.h"
#include "tools/lsn.h"

namespace reindexer {

namespace client {
class ReindexerImpl;
class Namespace;
}  // namespace client

class ItemImpl;
class Schema;
class TagsMatcher;
class FieldsFilter;
class RdxContext;

/// Item is the interface for data manipulating. It holds and control one database document (record)<br>
/// *Lifetime*: Item is uses Copy-On-Write semantics, and have independent lifetime and state - e.g., acquired from Reindexer Item will
/// not changed externally, even in case, when data in database was changed, or deleted. *Thread safety*: Item is thread safe against
/// Reindexer, but not thread safe itself. Usage of single Item from different threads will race

class [[nodiscard]] Item {
public:
	/// Construct empty Item
	Item() noexcept : impl_(nullptr), status_(errNotValid) {}
	/// Destroy Item
	~Item();
	Item(const Item&) = delete;
	Item(Item&& other) noexcept : impl_(other.impl_), status_(std::move(other.status_)), id_(other.id_) { other.impl_ = nullptr; }
	Item& operator=(const Item&) = delete;
	Item& operator=(Item&&) noexcept;

	/// Reference to field. Interface for field data manipulation
	class [[nodiscard]] FieldRef {
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
		/// Get field value as specified type, convert type if necessary. In case when conversion fails throws reindexer::Error
		/// If field is array, and not contains exact 1 element, then throws reindexer::Error
		/// @tparam T - type. Must be one of: int, int64_t, double or std::string
		/// @return value of field
		template <typename T>
		std::enable_if_t<!std::is_same_v<T, Point>, T> As() const {
			return (operator Variant()).As<T>();
		}
		template <typename T>
		std::enable_if_t<std::is_same_v<T, Point>, Point> As() const;
		/// Set single fundamental type value
		/// @tparam T - type. Must be one of: int, int64_t, double
		/// @param val - value, which will be set to field
		template <typename T>
		FieldRef& operator=(const T& val) {
			return operator=(Variant(val));
		}
		/// Set single point type value
		/// @param p - point value, which will be set to field
		FieldRef& operator=(Point p) {
			double arr[]{p.X(), p.Y()};
			return operator=(std::span<const double>(arr, 2));
		}

		/// Set array of values to field
		/// @tparam T - type. Must be one of: int, int64_t, double
		/// @param arr - std::vector of T values, which will be set to field
		template <typename T>
		FieldRef& operator=(std::span<const T> arr);
		/// Set array of values to field
		/// @tparam T - type. Must be one of: int, int64_t, double
		/// @param arr - std::vector of T values, which will be set to field
		template <typename T>
		FieldRef& operator=(const std::vector<T>& arr) {
			return operator=(std::span<const std::remove_const_t<T>>(arr));
		}
		/// Set string value to field
		/// If Item is in Unsafe Mode, then Item will not store str, but just keep pointer to str,
		/// application *MUST* hold str until end of life of Item
		/// @param str - pointer to C null-terminated string, which will be set to field
		FieldRef& operator=(const char* str);
		/// Set string value<br>
		/// If Item is in Unsafe Mode, then Item will not store str, but just keep pointer to str,
		/// application *MUST* hold str until end of life of Item
		/// @param str - std::string, which will be set to field
		FieldRef& operator=(const std::string& str);

		/// Get field index name
		std::string_view Name() const;

		/// Get Variant with field value
		/// If field is array, and contains not exact 1 element, then throws reindexer::Error
		/// @return Variant object with field value
		operator Variant() const;
		/// Get VariantArray with field values. If field is not array, then 1 element will be returned
		/// @return VariantArray with field values
		operator VariantArray() const;
		/// Set field value
		/// @param kr - key reference object, which will be set to field
		FieldRef& operator=(Variant kr);
		/// Set field value
		/// @param krs - key reference object, which will be set to field
		FieldRef& operator=(const VariantArray& krs);

	private:
		void throwIfNotSet() const;
		void throwIfAssignFieldMultyJsonPath() const;

		std::string_view jsonPath() const noexcept {
			return std::get_if<std::string>(&jsonPath_) ? std::string_view{std::get<std::string>(jsonPath_)}
														: std::get<std::string_view>(jsonPath_);
		}

		FieldRef(int field, ItemImpl* itemImpl, bool notSet) noexcept : itemImpl_(itemImpl), field_(field), notSet_(notSet) {}
		template <typename Str>
		FieldRef(Str&& jsonPath, ItemImpl* itemImpl, bool notSet) noexcept
			: itemImpl_(itemImpl), jsonPath_(std::forward<Str>(jsonPath)), field_(-1), notSet_(notSet) {}
		ItemImpl* itemImpl_;
		std::variant<std::string_view, std::string> jsonPath_;
		int field_;
		bool notSet_{false};
	};

	/// Build item from JSON<br>
	/// If Item is in *Unsafe Mode*, then Item will not store slice, but just keep pointer to data in slice,
	/// application *MUST* hold slice until end of life of Item
	/// @param slice - data slice with Json.
	/// @param endp - pointer to end of parsed part of slice
	/// @param pkOnly - if TRUE, that mean a JSON string will be parse only primary key fields
	Error FromJSON(std::string_view slice, char** endp = nullptr, bool pkOnly = false) & noexcept;

	/// Build item from JSON<br>
	/// If Item is in *Unsafe Mode*, then Item will not store slice, but just keep pointer to data in slice,
	/// application *MUST* hold slice until end of life of Item
	/// @param slice - data slice with CJson
	/// @param pkOnly - if TRUE, that mean a JSON string will be parse only primary key fields
	Error FromCJSON(std::string_view slice, bool pkOnly = false) & noexcept;
	void FromCJSONImpl(std::string_view slice, bool pkOnly = false) &;

	/// Builds item from msgpack::object.
	/// @param buf - msgpack encoded data buffer.
	/// @param offset - position to start from.
	Error FromMsgPack(std::string_view buf, size_t& offset) & noexcept;

	/// Builds item from Protobuf
	/// @param sbuf - Protobuf encoded data
	Error FromProtobuf(std::string_view sbuf) & noexcept;

	/// Packs data in msgpack format
	/// @param wrser - buffer to serialize data to
	Error GetMsgPack(WrSerializer& wrser) & noexcept;
	/// Packs data in msgpack format
	/// @return data slice with MsgPack
	std::string_view GetMsgPack() &;

	/// Packs item data to Protobuf
	/// @param wrser - buffer to serialize data to
	Error GetProtobuf(WrSerializer& wrser) & noexcept;

	/// Serialize item to CJSON.<br>
	/// If Item is in *Unsafe Mode*, then returned slice is allocated in temporary buffer, and can be invalidated by any next operation
	/// with Item
	/// @param withTagsMatcher - need to serialize TagsMatcher
	/// @return data slice with CJSON
	std::string_view GetCJSON(bool withTagsMatcher = false);
	/// Serialize item to JSON.<br>
	/// @return data slice with JSON. Returned slice is allocated in temporary Item's buffer, and can be invalidated by any next
	/// operation with Item
	std::string_view GetJSON();
	/// Get status of item
	/// @return data slice with JSON. Returned slice is allocated in temporary Item's buffer, and can be invalidated by any next
	/// operation with Item
	Error Status() const noexcept { return status_; }
	/// Get internal ID of item
	/// @return ID of item
	int GetID() const noexcept { return id_; }
	/// Get internal shardId of item
	/// @return shardId of item
	int GetShardID() const noexcept { return shardId_; }
	/// Get internal version of item
	/// @return version of item
	lsn_t GetLSN();
	/// Get count of indexed field
	/// @return count of  field
	int NumFields() const;
	/// Get field by number
	/// @param field - number of field. Must be >= 0 && < NumFields
	/// @return FieldRef which contains reference to indexed field
	FieldRef operator[](int field) const;
	/// Get field by name
	/// @param name - name of field
	/// @return FieldRef which contains reference to indexed field
	FieldRef operator[](std::string_view name) const noexcept { return FieldRefByNameOrJsonPath(name, *impl_); }
	/// Get field's name tag
	/// @param name - field name
	/// @return name's numeric tag value
	TagName GetFieldTag(std::string_view name) const;
	/// Get field's index by name
	/// @param name - field name
	/// @return name's numeric field value
	int GetFieldIndex(std::string_view name) const;
	/// Get PK fields
	FieldsSet PkFields() const;
	/// Set additional percepts for modify operation
	/// @param precepts - strings in format "fieldName=Func()"
	void SetPrecepts(std::vector<std::string> precepts) &;
	/// Check was names tags updated while modify operation
	/// @return true: tags was updated.
	bool IsTagsUpdated() const noexcept;
	/// Get state token
	/// @return Current state token
	int GetStateToken() const noexcept;
	/// Check is item valid. If is not valid, then any further operations with item will raise nullptr dereference
	bool operator!() const noexcept { return impl_ == nullptr; }
	/// Enable Unsafe Mode<br>.
	/// USE WITH CAUTION. In unsafe mode most of Item methods will not store  strings and slices, passed from/to application.<br>
	/// The advantage of unsafe mode is speed. It does not call extra memory allocation from heap and copying data.<br>
	/// The disadvantage of unsafe mode is potentially danger code. Most of C++ stl containers in many cases invalidates references -
	/// and in unsafe mode caller is responsible to guarantee, that all resources passed to Item will keep valid
	Item& Unsafe(bool enable = true) & noexcept;
	/// Get index type by field id
	/// @return either index type or Undefined (if index with this number does not exist or PayloadType is not available)
	KeyValueType GetIndexType(int field) const noexcept;
	/// Get field's ref by name or its jsonpath
	/// @param name - field name or jsonpath
	/// @param itemImpl - item
	/// @return field's ref
	static FieldRef FieldRefByNameOrJsonPath(std::string_view name, ItemImpl& itemImpl) noexcept;

	/// Perform embedding for all fields with automatic embedding configured
	/// @param ctx - context
	void Embed(const RdxContext& ctx);

private:
	explicit Item(ItemImpl* impl) : impl_(impl) {}
	Item(ItemImpl*, const FieldsFilter&);
	explicit Item(const Error& err) : impl_(nullptr), status_(err) {}
	Item(PayloadType, PayloadValue, const TagsMatcher&, std::shared_ptr<const Schema>, const FieldsFilter&);
	void setID(int id) noexcept { id_ = id; }
	void setLSN(lsn_t lsn);
	void setShardID(int id) noexcept { shardId_ = id; }
	void setFieldsFilter(const FieldsFilter&) noexcept;

	ItemImpl* impl_;
	Error status_;
	int id_ = -1;
	int shardId_ = ShardingKeyType::ProxyOff;
	friend class NamespaceImpl;
	friend class ItemModifier;

	friend class Transaction;
	friend class LocalTransaction;
	friend class TransactionImpl;
	friend class ProxiedTransaction;

	friend class QueryResults;
	friend class LocalQueryResults;
	friend class ReindexerImpl;
	friend class TransactionStep;
	friend class TransactionSteps;
	friend class client::ReindexerImpl;
	friend class client::Namespace;
	friend class SnapshotHandler;
	friend class ClusterProxy;
	friend class ShardingProxy;
	friend class Reindexer;
};

}  // namespace reindexer
