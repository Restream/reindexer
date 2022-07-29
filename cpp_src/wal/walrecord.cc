
#include "walrecord.h"
#include "core/cjson/baseencoder.h"
#include "tools/logger.h"
#include "tools/serializer.h"

namespace reindexer {

enum { TxBit = (1 << 7) };

void PackedWALRecord::Pack(const WALRecord &rec) {
	WrSerializer ser;
	rec.Pack(ser);
	assign(ser.Buf(), ser.Buf() + ser.Len());
}

void WALRecord::Pack(WrSerializer &ser) const {
	if (type == WalEmpty) return;
	ser.PutVarUint(inTransaction ? (type | TxBit) : type);
	switch (type) {
		case WalItemUpdate:
		case WalShallowItem:
			ser.PutUInt32(id);
			break;
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
			break;
		case WalPutMeta:
			ser.PutVString(putMeta.key);
			ser.PutVString(putMeta.value);
			break;
		case WalItemModify:
			ser.PutVString(itemModify.itemCJson);
			ser.PutVarUint(itemModify.modifyMode);
			ser.PutVarUint(itemModify.tmVersion);
			break;
		case WalRawItem:
			ser.PutUInt32(rawItem.id);
			ser.PutVString(rawItem.itemCJson);
			break;
		case WalEmpty:
			ser.Reset();
			break;
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalResetLocalWal:
			break;
		default:
			fprintf(stderr, "Unexpected WAL rec type %d\n", int(type));
			std::abort();
	}
}

WALRecord::WALRecord(span<uint8_t> packed) {
	if (!packed.size()) {
		type = WalEmpty;
		return;
	}
	Serializer ser(packed.data(), packed.size());
	const unsigned unpackedType = ser.GetVarUint();
	if (unpackedType & TxBit) {
		inTransaction = true;
		type = static_cast<WALRecType>(unpackedType ^ TxBit);
	} else {
		inTransaction = false;
		type = static_cast<WALRecType>(unpackedType);
	}
	switch (type) {
		case WalItemUpdate:
		case WalShallowItem:
			id = ser.GetUInt32();
			break;
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
			break;
		case WalPutMeta:
			putMeta.key = ser.GetVString();
			putMeta.value = ser.GetVString();
			break;
		case WalItemModify:
			itemModify.itemCJson = ser.GetVString();
			itemModify.modifyMode = ser.GetVarUint();
			itemModify.tmVersion = ser.GetVarUint();
			break;
		case WalRawItem:
			rawItem.id = ser.GetUInt32();
			rawItem.itemCJson = ser.GetVString();
			break;
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalResetLocalWal:
			break;
		default:
			logPrintf(LogWarning, "Unexpected WAL rec type %d\n", int(type));
			break;
	}
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
		default:
			return "<Unknown>"sv;
	}
}

WrSerializer &WALRecord::Dump(WrSerializer &ser, std::function<string(std::string_view)> cjsonViewer) const {
	ser << wrecType2Str(type);
	if (inTransaction) ser << " InTransaction";
	switch (type) {
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
			return ser;
		case WalItemUpdate:
			return ser << " rowId=" << id;
		case WalNamespaceRename:
			return ser << ' ' << data;
		case WalUpdateQuery:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalReplState:
		case WalForceSync:
		case WalWALSync:
		case WalSetSchema:
			return ser << ' ' << data;
		case WalPutMeta:
			return ser << ' ' << putMeta.key << "=" << putMeta.value;
		case WalItemModify:
			return ser << (itemModify.modifyMode == ModeDelete ? " Delete " : " Update ") << cjsonViewer(itemModify.itemCJson);
		default:
			return ser << " <unhandled record of type " << int(type) << '>';
	}
}

void WALRecord::GetJSON(JsonBuilder &jb, std::function<string(std::string_view)> cjsonViewer) const {
	jb.Put("type", wrecType2Str(type));
	jb.Put("in_transaction", inTransaction);

	switch (type) {
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
			return;
		case WalItemUpdate:
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
			jb.Put("key", putMeta.key);
			jb.Put("value", putMeta.value);
			return;
		case WalItemModify:
			jb.Put("mode", itemModify.modifyMode);
			jb.Raw("item", cjsonViewer(itemModify.itemCJson));
			return;
		case WalSetSchema:
			jb.Raw("schema", data);
			return;
		default:
			fprintf(stderr, "Unexpected WAL rec type %d\n", int(type));
			std::abort();
	}
	return;
}

WALRecord::WALRecord(std::string_view data) : WALRecord(span<uint8_t>(reinterpret_cast<const uint8_t *>(data.data()), data.size())) {}

void MarkedPackedWALRecord::Pack(int16_t _server, const WALRecord &rec) {
	server = _server;
	PackedWALRecord::Pack(rec);
}

}  // namespace reindexer
