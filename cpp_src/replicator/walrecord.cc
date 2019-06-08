
#include "walrecord.h"
#include "core/cjson/baseencoder.h"
#include "tools/serializer.h"

namespace reindexer {

void PackedWALRecord::Pack(const WALRecord &rec) {
	WrSerializer ser;
	ser.PutVarUint(rec.type);
	switch (rec.type) {
		case WalItemUpdate:
			ser.PutUInt32(rec.id);
			break;
		case WalUpdateQuery:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalReplState:
			ser.PutVString(rec.data);
			break;
		case WalPutMeta:
			ser.PutVString(rec.putMeta.key);
			ser.PutVString(rec.putMeta.value);
			break;
		case WalItemModify:
			ser.PutVString(rec.itemModify.itemCJson);
			ser.PutVarUint(rec.itemModify.modifyMode);
			ser.PutVarUint(rec.itemModify.tmVersion);
			break;
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
			break;
		default:
			fprintf(stderr, "Unexpected WAL rec type %d\n", int(rec.type));
			std::abort();
	}
	assign(ser.Buf(), ser.Buf() + ser.Len());
}

WALRecord::WALRecord(span<uint8_t> packed) {
	if (!packed.size()) {
		type = WalEmpty;
		return;
	}
	Serializer ser(packed.data(), packed.size());
	type = WALRecType(ser.GetVarUint());
	switch (type) {
		case WalItemUpdate:
			id = ser.GetUInt32();
			break;
		case WalUpdateQuery:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalReplState:
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
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
			break;
		default:
			fprintf(stderr, "Unexpected WAL rec type %d\n", int(type));
			std::abort();
	}
}

string_view wrecType2Str(WALRecType t) {
	switch (t) {
		case WalEmpty:
			return "<WalEmpty>"_sv;
		case WalItemUpdate:
			return "WalItemUpdate"_sv;
		case WalUpdateQuery:
			return "WalUpdateQuery"_sv;
		case WalIndexAdd:
			return "WalIndexAdd"_sv;
		case WalIndexDrop:
			return "WalIndexDrop"_sv;
		case WalIndexUpdate:
			return "WalIndexUpdate"_sv;
		case WalReplState:
			return "WalReplState"_sv;
		case WalPutMeta:
			return "WalPutMeta"_sv;
		case WalNamespaceAdd:
			return "WalNamespaceAdd"_sv;
		case WalNamespaceDrop:
			return "WalNamespaceDrop"_sv;
		case WalItemModify:
			return "WalItemMofify"_sv;
		default:
			return "<Unknown"_sv;
	}
}

WrSerializer &WALRecord::Dump(WrSerializer &ser, std::function<string(string_view)> cjsonViewer) const {
	ser << wrecType2Str(type);
	switch (type) {
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
			return ser;
		case WalItemUpdate:
			return ser << " rowId=" << id;
		case WalUpdateQuery:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalReplState:
			return ser << ' ' << data;
		case WalPutMeta:
			return ser << ' ' << putMeta.key << "=" << putMeta.value;
		case WalItemModify:
			return ser << (itemModify.modifyMode == ModeDelete ? " Delete " : " Update ") << cjsonViewer(itemModify.itemCJson);
		default:
			fprintf(stderr, "Unexpected WAL rec type %d\n", int(type));
			std::abort();
	}
	return ser;
}

void WALRecord::GetJSON(JsonBuilder &jb, std::function<string(string_view)> cjsonViewer) const {
	jb.Put("type", wrecType2Str(type));

	switch (type) {
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
			return;
		case WalItemUpdate:
			jb.Put("row_id", id);
			return;
		case WalUpdateQuery:
			jb.Put("query", data);
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
			break;
		case WalItemModify:
			jb.Put("mode", itemModify.modifyMode);
			jb.Raw("item", cjsonViewer(itemModify.itemCJson));
			return;
		default:
			fprintf(stderr, "Unexpected WAL rec type %d\n", int(type));
			std::abort();
	}
	return;
}

WALRecord::WALRecord(string_view data) : WALRecord(span<uint8_t>(reinterpret_cast<const uint8_t *>(data.data()), data.size())) {}

}  // namespace reindexer
