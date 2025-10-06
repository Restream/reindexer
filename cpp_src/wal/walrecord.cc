
#include "walrecord.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/logger.h"
#include "tools/serializer.h"

namespace reindexer {

constexpr unsigned kTxBit = (1 << 7);

void PackedWALRecord::Pack(const WALRecord& rec) {
	WrSerializer ser;
	rec.Pack(ser);
	assign(ser.Buf(), ser.Buf() + ser.Len());
}

void WALRecord::Pack(WrSerializer& ser) const {
	if (type == WalEmpty) {
		return;
	}
	ser.PutVarUint(inTransaction ? (unsigned(type) | kTxBit) : unsigned(type));
	switch (type) {
		case WalItemUpdate:
		case WalShallowItem:
			ser.PutUInt32(id);
			return;
		case WalUpdateQuery:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalReplState:
		case WalNamespaceRename:
		case WalForceSync:
		case WalWALSync:
		case WalSetSchema:
		case WalTagsMatcher:
			ser.PutVString(data);
			return;
		case WalPutMeta:
			ser.PutVString(itemMeta.key);
			ser.PutVString(itemMeta.value);
			return;
		case WalDeleteMeta:
			ser.PutVString(itemMeta.key);
			return;
		case WalItemModify:
			ser.PutVString(itemModify.itemCJson);
			ser.PutVarUint(itemModify.modifyMode);
			ser.PutVarUint(itemModify.tmVersion);
			return;
		case WalRawItem:
			ser.PutUInt32(rawItem.id);
			ser.PutVString(rawItem.itemCJson);
			return;
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalResetLocalWal:
			return;
	}
	fprintf(stderr, "reindexer error: unexpected WAL rec type %d\n", int(type));
	std::abort();
}

WALRecord::WALRecord(std::span<const uint8_t> packed) {
	if (!packed.size()) {
		type = WalEmpty;
		return;
	}
	Serializer ser(packed.data(), packed.size());
	const unsigned unpackedType = ser.GetVarUInt();
	if (unpackedType & kTxBit) {
		inTransaction = true;
		type = static_cast<WALRecType>(unpackedType ^ kTxBit);
	} else {
		inTransaction = false;
		type = static_cast<WALRecType>(unpackedType);
	}
	switch (type) {
		case WalItemUpdate:
		case WalShallowItem:
			id = ser.GetUInt32();
			return;
		case WalUpdateQuery:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalReplState:
		case WalNamespaceRename:
		case WalForceSync:
		case WalWALSync:
		case WalSetSchema:
		case WalTagsMatcher:
			data = ser.GetVString();
			return;
		case WalPutMeta:
			itemMeta.key = ser.GetVString();
			itemMeta.value = ser.GetVString();
			return;
		case WalDeleteMeta:
			itemMeta.key = ser.GetVString();
			return;
		case WalItemModify:
			itemModify.itemCJson = ser.GetVString();
			itemModify.modifyMode = ItemModifyMode(ser.GetVarUInt());
			itemModify.tmVersion = ser.GetVarUInt();
			return;
		case WalRawItem:
			rawItem.id = ser.GetUInt32();
			rawItem.itemCJson = ser.GetVString();
			return;
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalResetLocalWal:
			return;
	}
	logFmt(LogError, "Unexpected WAL rec type {}\n", int(type));
}

static std::string_view wrecType2Str(WALRecType t) {
	using namespace std::string_view_literals;
	switch (t) {
		case WalEmpty:
			return "<WalEmpty>"sv;
		case WalItemUpdate:
			return "WalItemUpdate"sv;
		case WalUpdateQuery:
			return "WalUpdateQuery"sv;
		case WalIndexAdd:
			return "WalIndexAdd"sv;
		case WalIndexDrop:
			return "WalIndexDrop"sv;
		case WalIndexUpdate:
			return "WalIndexUpdate"sv;
		case WalReplState:
			return "WalReplState"sv;
		case WalPutMeta:
			return "WalPutMeta"sv;
		case WalDeleteMeta:
			return "WalDeleteMeta"sv;
		case WalNamespaceAdd:
			return "WalNamespaceAdd"sv;
		case WalNamespaceDrop:
			return "WalNamespaceDrop"sv;
		case WalNamespaceRename:
			return "WalNamespaceRename"sv;
		case WalItemModify:
			return "WalItemModify"sv;
		case WalInitTransaction:
			return "WalInitTransaction"sv;
		case WalCommitTransaction:
			return "WalCommitTransaction"sv;
		case WalForceSync:
			return "WalForceSync"sv;
		case WalWALSync:
			return "WalWALSync"sv;
		case WalSetSchema:
			return "WalSetSchema"sv;
		case WalRawItem:
			return "WalRawItem"sv;
		case WalShallowItem:
			return "WalShallowItem"sv;
		case WalTagsMatcher:
			return "WalTagsMatcher"sv;
		case WalResetLocalWal:
			return "WalResetLocalWal"sv;
	}
	return "<Unknown>"sv;
}

void WALRecord::Dump(WrSerializer& ser, const std::function<std::string(std::string_view)>& cjsonViewer) const {
	ser << wrecType2Str(type);
	if (inTransaction) {
		ser << " InTransaction";
	}
	switch (type) {
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalResetLocalWal:
			return;
		case WalItemUpdate:
		case WalShallowItem:
			ser << " rowId=" << id;
			return;
		case WalNamespaceRename:
		case WalTagsMatcher:
			ser << ' ' << data;
			return;
		case WalUpdateQuery:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalReplState:
		case WalForceSync:
		case WalWALSync:
		case WalSetSchema:
			ser << ' ' << data;
			return;
		case WalPutMeta:
			ser << ' ' << itemMeta.key << "=" << itemMeta.value;
			return;
		case WalDeleteMeta:
			ser << ' ' << itemMeta.key;
			return;
		case WalItemModify:
			ser << (itemModify.modifyMode == ModeDelete ? " Delete " : " Update ") << cjsonViewer(itemModify.itemCJson);
			return;
		case WalRawItem:
			ser << (" rowId=") << rawItem.id << ": " << cjsonViewer(rawItem.itemCJson);
			return;
	}
	fprintf(stderr, "reindexer error: unexpected WAL rec type %d\n", int(type));
	std::abort();
}

void WALRecord::GetJSON(JsonBuilder& jb, const std::function<std::string(std::string_view)>& cjsonViewer) const {
	jb.Put("type", wrecType2Str(type));
	jb.Put("in_transaction", inTransaction);

	switch (type) {
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalResetLocalWal:
			return;
		case WalItemUpdate:
		case WalShallowItem:
			jb.Put("row_id", id);
			return;
		case WalUpdateQuery:
			jb.Put("query", data);
			return;
		case WalNamespaceRename:
			jb.Put("dst_ns_name", data);
			return;
		case WalForceSync:
		case WalWALSync:
			jb.Put("ns_def", data);
			return;
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
			jb.Raw("index", data);
			return;
		case WalReplState:
			jb.Raw("state", data);
			return;
		case WalPutMeta:
			jb.Put("key", itemMeta.key);
			jb.Put("value", itemMeta.value);
			return;
		case WalDeleteMeta:
			jb.Put("key", itemMeta.key);
			return;
		case WalItemModify:
			jb.Put("mode", uint64_t(itemModify.modifyMode));
			jb.Raw("item", cjsonViewer(itemModify.itemCJson));
			return;
		case WalSetSchema:
			jb.Raw("schema", data);
			return;
		case WalRawItem:
			jb.Put("row_id", rawItem.id);
			jb.Raw("item", cjsonViewer(rawItem.itemCJson));
			return;
		case WalTagsMatcher:
			jb.Put("tagsmatcher", data);
			return;
	}
	fprintf(stderr, "reindexer error: unexpected WAL rec type %d\n", int(type));
	std::abort();
}

WALRecord::WALRecord(std::string_view data)
	: WALRecord(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(data.data()), data.size())) {}

void MarkedPackedWALRecord::Pack(int16_t _server, const WALRecord& rec) {
	server = _server;
	PackedWALRecord::Pack(rec);
}

}  // namespace reindexer
