#pragma once

#include <core/type_consts.h>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include "estl/h_vector.h"
#include "tools/lsn.h"

namespace reindexer {

enum [[nodiscard]] WALRecType : unsigned {
	WalEmpty = 0,
	WalReplState = 1,
	WalItemUpdate = 2,
	WalItemModify = 3,
	WalIndexAdd = 4,
	WalIndexDrop = 5,
	WalIndexUpdate = 6,
	WalPutMeta = 7,
	WalUpdateQuery = 8,
	WalNamespaceAdd = 9,
	WalNamespaceDrop = 10,
	WalNamespaceRename = 11,
	WalInitTransaction = 12,
	WalCommitTransaction = 13,
	WalForceSync = 14,
	WalSetSchema = 15,
	WalWALSync = 16,
	WalTagsMatcher = 17,
	WalResetLocalWal = 18,
	WalRawItem = 19,
	WalShallowItem = 20,
	WalDeleteMeta = 21,
};
inline constexpr int format_as(WALRecType v) noexcept { return int(v); }

class WrSerializer;
struct WALRecord;

struct [[nodiscard]] WALRecord {
	explicit WALRecord(std::span<const uint8_t>);
	explicit WALRecord(std::string_view sv);
	explicit WALRecord(WALRecType _type = WalEmpty, IdType _id = 0, bool inTx = false) : type(_type), id(_id), inTransaction(inTx) {}
	explicit WALRecord(WALRecType _type, std::string_view _data, bool inTx = false) : type(_type), data(_data), inTransaction(inTx) {}
	explicit WALRecord(WALRecType _type, IdType _id, std::string_view _data) : type(_type), rawItem{_id, _data} {}
	explicit WALRecord(WALRecType _type, std::string_view key, std::string_view value, bool inTx)
		: type(_type), itemMeta{key, value}, inTransaction(inTx) {}
	explicit WALRecord(WALRecType _type, std::string_view cjson, int tmVersion, ItemModifyMode modifyMode, bool inTx = false)
		: type(_type), itemModify{cjson, tmVersion, modifyMode}, inTransaction(inTx) {}
	void Dump(WrSerializer& ser, const std::function<std::string(std::string_view)>& cjsonViewer) const;
	void GetJSON(JsonBuilder& jb, const std::function<std::string(std::string_view)>& cjsonViewer) const;
	void Pack(WrSerializer& ser) const;

	WALRecType type;
	union {
		IdType id;
		std::string_view data;
		struct {
			std::string_view itemCJson;
			int tmVersion;
			ItemModifyMode modifyMode;
		} itemModify;
		struct {
			std::string_view key;
			std::string_view value;
		} itemMeta;
		struct {
			IdType id;
			std::string_view itemCJson;
		} rawItem;
	};
	bool inTransaction = false;
};

struct [[nodiscard]] PackedWALRecord : public h_vector<uint8_t, 12> {
	using h_vector<uint8_t, 12>::h_vector;
	void Pack(const WALRecord& rec);
};

#pragma pack(push, 1)
struct [[nodiscard]] MarkedPackedWALRecord : public PackedWALRecord {
	MarkedPackedWALRecord(int16_t s) noexcept : server{s} {
		assertrx_dbg(server >= lsn_t::kMinServerIDValue);
		assertrx_dbg(server <= lsn_t::kMaxServerIDValue);
	}
	template <typename RecordT>
	MarkedPackedWALRecord(int16_t s, RecordT&& rec) : PackedWALRecord(std::forward<RecordT>(rec)), server(s) {
		assertrx_dbg(server >= lsn_t::kMinServerIDValue);
		assertrx_dbg(server <= lsn_t::kMaxServerIDValue);
	}

	int16_t server = -1;
	void Pack(int16_t _serverId, const WALRecord& rec);
};
#pragma pack(pop)

}  // namespace reindexer
