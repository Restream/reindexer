#include "item_storage_writer.h"

#include "asyncstorage.h"
#include "core/itemimpl.h"
#include "core/payload/payloadiface.h"
#include "core/storage/storage_prefixes.h"
#include "core/type_consts.h"
#include "tools/lsn.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer {

std::string_view ItemStorageWriter::EncodeKey(const ConstPayload& payload, const FieldsSet& pk, WrSerializer& buffer) {
	buffer.Reset();
	buffer << kRxStorageItemPrefix;
	payload.SerializeFields(buffer, pk);
	return buffer.Slice();
}

std::string_view ItemStorageWriter::EncodeValue(lsn_t lsn, std::string_view cjson, WrSerializer& buffer) {
	buffer.Reset();
	buffer.PutUInt64(static_cast<uint64_t>(lsn));
	buffer.Write(cjson);
	return buffer.Slice();
}

std::string_view ItemStorageWriter::EncodeValue(lsn_t lsn, ItemImpl& item, WrSerializer& buffer) {
	buffer.Reset();
	buffer.PutUInt64(static_cast<uint64_t>(lsn));
	return item.GetCJSON(buffer, WithTagsMatcher_False);
}

void ItemStorageWriter::WriteEncoded(std::string_view key, std::string_view value, WriteMode mode) const {
	switch (mode) {
		case WriteMode::Queued:
			storage_.Write(key, value);
			break;
		case WriteMode::Immediate:
			storage_.WriteSync(StorageOpts{}, key, value);
			break;
	}
}

void ItemStorageWriter::RemoveEncoded(std::string_view key) const { storage_.Remove(key); }

}  // namespace reindexer
