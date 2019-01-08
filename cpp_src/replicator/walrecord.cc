
#include "walrecord.h"
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

WrSerializer &WALRecord::Dump(WrSerializer &ser, std::function<string(string_view)> cjsonViewer) const {
	switch (type) {
		case WalEmpty:
			return ser << "<WalEmpty>";
		case WalItemUpdate:
			return ser << "WalItemUpdate rowId=" << id;
		case WalUpdateQuery:
			return ser << "WalUpdateQuery " << data;
		case WalIndexAdd:
			return ser << "WalIndexAdd " << data;
		case WalIndexDrop:
			return ser << "WalIndexDrop " << data;
		case WalIndexUpdate:
			return ser << "WalIndexUpdate " << data;
		case WalReplState:
			return ser << "WalReplState " << data;
		case WalPutMeta:
			return ser << "WalPutMeta " << putMeta.key << "=" << putMeta.value;
		case WalNamespaceAdd:
			return ser << "WalNamespaceAdd ";
		case WalNamespaceDrop:
			return ser << "WalNamespaceDrop ";
		case WalItemModify:
			return ser << (itemModify.modifyMode == ModeDelete ? "WalItemDelete " : "WalItemUpdate ") << cjsonViewer(itemModify.itemCJson);
		default:
			fprintf(stderr, "Unexpected WAL rec type %d\n", int(type));
			std::abort();
	}
	return ser;
}

WALRecord::WALRecord(string_view data) : WALRecord(span<uint8_t>(reinterpret_cast<const uint8_t *>(data.data()), data.size())) {}

}  // namespace reindexer
