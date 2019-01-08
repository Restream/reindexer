#pragma once

#include <memory>
#include <vector>
#include "estl/string_view.h"
#include "tools/errors.h"

namespace reindexer {
class Replicator;
namespace client {

using std::vector;

class ItemImpl;

/// Item is the interface for data manipulating. It holds and control one database document (record)<br>
/// *Lifetime*: Item is uses Copy-On-Write semantics, and have independent lifetime and state - e.g., aquired from Reindexer Item will not
/// changed externally, even in case, when data in database was changed, or deleted.
/// *Thread safety*: Item is thread safe againist Reindexer, but not thread safe itself.
/// Usage of single Item from different threads will race

class Item {
public:
	/// Construct empty Item
	Item();
	/// Destroy Item
	~Item();
	Item(const Item &) = delete;
	Item(Item &&) noexcept;
	Item &operator=(const Item &) = delete;
	Item &operator=(Item &&) noexcept;

	/// Build item from JSON<br>
	/// If Item is in *Unsafe Mode*, then Item will not store slice, but just keep pointer to data in slice,
	/// application *MUST* hold slice until end of life of Item
	/// @param slice - data slice with Json.
	/// @param endp - pounter to end of parsed part of slice
	Error FromJSON(const string_view &slice, char **endp = nullptr, bool = false);

	/// Build item from JSON<br>
	/// If Item is in *Unsafe Mode*, then Item will not store slice, but just keep pointer to data in slice,
	/// application *MUST* hold slice until end of life of Item
	/// @param slice - data slice with CJson
	Error FromCJSON(const string_view &slice);
	/// Serialize item to CJSON.<br>
	/// If Item is in *Unfafe Mode*, then returned slice is allocated in temporary buffer, and can be invalidated by any next operation with
	/// Item
	/// @return data slice with CJSON
	string_view GetCJSON();
	/// Serialize item to JSON.<br>
	/// @return data slice with JSON. Returned slice is allocated in temporary Item's buffer, and can be invalidated by any next operation
	/// with Item
	string_view GetJSON();
	/// Get status of item
	/// @return data slice with JSON. Returned slice is allocated in temporary Item's buffer, and can be invalidated by any next operation
	/// with Item
	Error Status() { return status_; }
	/// Get internal ID of item
	/// @return ID of item
	int GetID() { return id_; }
	/// Get internal version of item
	/// @return version of item
	int NumFields();
	/// Set additional percepts for modify operation
	/// @param precepts - strings in format "fieldName=Func()"
	void SetPrecepts(const vector<string> &precepts);
	/// Check was names tags updated while modify operation
	/// @return true: tags was updated.
	bool IsTagsUpdated();
	/// Get state token
	/// @return Current state token
	int GetStateToken();
	/// Check is item valid. If is not valid, then any futher operations with item will raise nullptr dereference
	operator bool() const;
	/// Enable Unsafe Mode<br>.
	/// USE WITH CAUTION. In unsafe mode most of Item methods will not store  strings and slices, passed from/to application.<br>
	/// The advantage of unsafe mode is speed. It does not call extra memory allocation from heap and copying data.<br>
	/// The disadvantage of unsafe mode is potentially danger code. Most of C++ stl containters in many cases invalidates references -
	/// and in unsafe mode caller is responsibe to guarantee, that all resources passed to Item will keep valid
	Item &Unsafe(bool enable = true);

private:
	explicit Item(ItemImpl *impl);
	explicit Item(const Error &err);
	void setID(int id) { id_ = id; }

	std::unique_ptr<ItemImpl> impl_;
	Error status_;
	int id_ = -1;
	friend class Namespace;
	friend class QueryResults;
	friend class RPCClient;
	friend class reindexer::Replicator;
};
}  // namespace client
}  // namespace reindexer
