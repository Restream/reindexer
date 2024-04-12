
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
	fprintf(stderr, "Unexpected WAL rec type %d\n", int(type));
	std::abort();
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
			itemModify.modifyMode = ser.GetVarUint();
			itemModify.tmVersion = ser.GetVarUint();
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
	logPrintf(LogError, "Unexpected WAL rec type %d\n", int(type));
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

WrSerializer &WALRecord::Dump(WrSerializer &ser, const std::function<std::string(std::string_view)> &cjsonViewer) const {
	ser << wrecType2Str(type);
	if (inTransaction) ser << " InTransaction";
	switch (type) {
		case WalEmpty:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalResetLocalWal:
			return ser;
		case WalItemUpdate:
		case WalShallowItem:
			return ser << " rowId=" << id;
		case WalNamespaceRename:
		case WalTagsMatcher:
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
			return ser << ' ' << itemMeta.key << "=" << itemMeta.value;
		case WalDeleteMeta:
			return ser << ' ' << itemMeta.key;
		case WalItemModify:
			return ser << (itemModify.modifyMode == ModeDelete ? " Delete " : " Update ") << cjsonViewer(itemModify.itemCJson);
		case WalRawItem:
			return ser << (" rowId=") << rawItem.id << ": " << cjsonViewer(rawItem.itemCJson);
	}
	fprintf(stderr, "Unexpected WAL rec type %d\n", int(type));
	std::abort();
}

void WALRecord::GetJSON(JsonBuilder &jb, const std::function<std::string(std::string_view)> &cjsonViewer) const {
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
			jb.Put("mode", itemModify.modifyMode);
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
	fprintf(stderr, "Unexpected WAL rec type %d\n", int(type));
	std::abort();
}

WALRecord::WALRecord(std::string_view data) : WALRecord(span<uint8_t>(reinterpret_cast<const uint8_t *>(data.data()), data.size())) {}

SharedWALRecord WALRecord::GetShared(int64_t lsn, int64_t upstreamLSN, std::string_view nsName) const {
	if (!shared_.packed_) {
		shared_ = SharedWALRecord(lsn, upstreamLSN, nsName, *this);
	}
	return shared_;
}
SharedWALRecord::SharedWALRecord(int64_t lsn, int64_t originLSN, std::string_view nsName, const WALRecord &rec) {
	const size_t kCapToSizeRelation = 4;
	WrSerializer ser;
	ser.PutVarint(lsn);
	ser.PutVarint(originLSN);
	ser.PutVString(nsName);
	{
		auto sl = ser.StartSlice();
		rec.Pack(ser);
	}

	auto ch = ser.DetachChunk();
	ch.shrink(kCapToSizeRelation);

	packed_ = make_intrusive<intrusive_atomic_rc_wrapper<chunk>>(std::move(ch));
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
