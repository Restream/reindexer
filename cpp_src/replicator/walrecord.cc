
#include "walrecord.h"
#include "core/cjson/baseencoder.h"
#include "core/transactionimpl.h"
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
			ser.PutUInt32(id);
			break;
		case WalUpdateQuery:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalReplState:
		case WalNamespaceRename:
		case WalForceSync:
		case WalSetSchema:
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
		case WalEmpty:
			ser.Reset();
			break;
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
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
			id = ser.GetUInt32();
			break;
		case WalUpdateQuery:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalReplState:
		case WalNamespaceRename:
		case WalForceSync:
		case WalSetSchema:
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
		case WalInitTransaction:
		case WalCommitTransaction:
			break;
		default:
			logPrintf(LogWarning, "Unexpected WAL rec type %d\n", int(type));
			break;
	}
}

static string_view wrecType2Str(WALRecType t) {
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
		case WalNamespaceRename:
			return "WalNamespaceRename"_sv;
		case WalItemModify:
			return "WalItemModify"_sv;
		case WalInitTransaction:
			return "WalInitTransaction"_sv;
		case WalCommitTransaction:
			return "WalCommitTransaction"_sv;
		case WalForceSync:
			return "WalForceSync"_sv;
		case WalSetSchema:
			return "WalSetSchema"_sv;
		default:
			return "<Unknown>"_sv;
	}
}

WrSerializer &WALRecord::Dump(WrSerializer &ser, std::function<string(string_view)> cjsonViewer) const {
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
		case WalSetSchema:
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

WALRecord::WALRecord(string_view data) : WALRecord(span<uint8_t>(reinterpret_cast<const uint8_t *>(data.data()), data.size())) {}

SharedWALRecord WALRecord::GetShared(int64_t lsn, int64_t upstreamLSN, string_view nsName) const {
	if (!shared_.packed_) {
		shared_ = SharedWALRecord(lsn, upstreamLSN, nsName, *this);
	}
	return shared_;
}
SharedWALRecord::SharedWALRecord(int64_t lsn, int64_t originLSN, string_view nsName, const WALRecord &rec) {
	WrSerializer ser;
	ser.PutVarint(lsn);
	ser.PutVarint(originLSN);
	ser.PutVString(nsName);
	{
		auto sl = ser.StartSlice();
		rec.Pack(ser);
	}
	packed_.reset(new intrusive_atomic_rc_wrapper<chunk>(ser.DetachChunk()));
}

SharedWALRecord::Unpacked SharedWALRecord::Unpack() {
	Serializer rdser(packed_->data(), packed_->size());
	int64_t lsn = rdser.GetVarint();
	int64_t originLSN = rdser.GetVarint();
	p_string nsName = rdser.GetPVString();
	p_string pwal = rdser.GetPSlice();
	return {lsn, originLSN, nsName, pwal};
}

}  // namespace reindexer
