#include "updaterecord.h"

namespace reindexer {
namespace cluster {

UpdateRecord::UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId)
	: type(_type), nsName(std::move(_nsName)), lsn(_lsn), emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::Truncate:
		case Type::BeginTx:
		case Type::CommitTx:
		case Type::DropNamespace:
			break;
		default:
			assert(false);
	}
}

UpdateRecord::UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, std::string _data)
	: type(_type), nsName(std::move(_nsName)), lsn(_lsn), emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::ItemUpdate:
		case Type::ItemUpsert:
		case Type::ItemDelete:
		case Type::ItemInsert:
		case Type::ItemUpdateTx:
		case Type::ItemUpsertTx:
		case Type::ItemDeleteTx:
		case Type::ItemInsertTx:
			data.emplace<std::unique_ptr<ItemReplicationRecord>>(new ItemReplicationRecord{std::move(_data)});
			break;
		case Type::RenameNamespace:
			data.emplace<std::unique_ptr<RenameNamespaceReplicationRecord>>(new RenameNamespaceReplicationRecord{std::move(_data)});
			break;
		case Type::SetSchema:
			data.emplace<std::unique_ptr<SchemaReplicationRecord>>(new SchemaReplicationRecord{std::move(_data)});
			break;
		default:
			assert(false);
	}
}

UpdateRecord::UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, IndexDef _idef)
	: type(_type),
	  nsName(std::move(_nsName)),
	  lsn(_lsn),
	  data(std::unique_ptr<IndexReplicationRecord>(new IndexReplicationRecord{std::move(_idef)})),
	  emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::IndexAdd:
		case Type::IndexDrop:
		case Type::IndexUpdate:
			break;
		default:
			assert(false);
	}
}

UpdateRecord::UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, Query _q)
	: type(_type),
	  nsName(std::move(_nsName)),
	  lsn(_lsn),
	  data(std::unique_ptr<QueryReplicationRecord>(new QueryReplicationRecord{std::move(_q)})),
	  emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::UpdateQuery:
		case Type::DeleteQuery:
		case Type::UpdateQueryTx:
		case Type::DeleteQueryTx:
			break;
		default:
			assert(false);
	}
}

UpdateRecord::UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, NamespaceDef _def)
	: type(_type), nsName(std::move(_nsName)), lsn(_lsn), emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::AddNamespace:
			data.emplace<std::unique_ptr<AddNamespaceReplicationRecord>>(new AddNamespaceReplicationRecord{std::move(_def)});
			break;
		default:
			assert(false);
	}
}

UpdateRecord::UpdateRecord(UpdateRecord::Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, std::string _k, std::string _v)
	: type(_type), nsName(std::move(_nsName)), lsn(_lsn), emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::PutMeta:
			data.emplace<std::unique_ptr<MetaReplicationRecord>>(new MetaReplicationRecord{std::move(_k), std::move(_v)});
			break;
		default:
			assert(false);
	}
}

}  // namespace cluster
}  // namespace reindexer
