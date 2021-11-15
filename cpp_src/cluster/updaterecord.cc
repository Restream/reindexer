#include "updaterecord.h"

namespace reindexer {
namespace cluster {

UpdateRecord::UpdateRecord(UpdateRecord::Type _type, uint32_t _nodeUid, bool online) : type(_type), emmiterServerId(-1) {
	assert(_type == Type::NodeNetworkCheck);
	data.emplace<std::unique_ptr<NodeNetworkCheckRecord>>(new NodeNetworkCheckRecord{_nodeUid, online});
}

UpdateRecord::UpdateRecord(UpdateRecord::Type _type, std::string _nsName, int _emmiterServerId)
	: type(_type), nsName(std::move(_nsName)), emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::EmptyUpdate:
			break;
		default:
			assert(false);
	}
}

UpdateRecord::UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emmiterServerId)
	: type(_type), nsName(std::move(_nsName)), extLsn(_nsVersion, _lsn), emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::Truncate:
		case Type::BeginTx:
		case Type::CommitTx:
		case Type::DropNamespace:
		case Type::ResyncNamespaceGeneric:
		case Type::ResyncNamespaceLeaderInit:
			break;
		default:
			assert(false);
	}
}

UpdateRecord::UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emmiterServerId, std::string _data)
	: type(_type), nsName(std::move(_nsName)), extLsn(_nsVersion, _lsn), emmiterServerId(_emmiterServerId) {
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
		case Type::UpdateQuery:
		case Type::DeleteQuery:
		case Type::UpdateQueryTx:
		case Type::DeleteQueryTx:
			data.emplace<std::unique_ptr<QueryReplicationRecord>>(new QueryReplicationRecord{std::move(_data)});
			break;
		default:
			assert(false);
	}
}

UpdateRecord::UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emmiterServerId, IndexDef _idef)
	: type(_type),
	  nsName(std::move(_nsName)),
	  extLsn(_nsVersion, _lsn),
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

UpdateRecord::UpdateRecord(Type _type, std::string _nsName, lsn_t _nsVersion, int _emmiterServerId, NamespaceDef _def, int64_t _stateToken)
	: type(_type), nsName(std::move(_nsName)), extLsn(_nsVersion, lsn_t(0, 0)), emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::AddNamespace:
			data.emplace<std::unique_ptr<AddNamespaceReplicationRecord>>(new AddNamespaceReplicationRecord{std::move(_def), _stateToken});
			break;
		default:
			assert(false);
	}
}

UpdateRecord::UpdateRecord(UpdateRecord::Type _type, std::string _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emmiterServerId,
						   std::string _k, std::string _v)
	: type(_type), nsName(std::move(_nsName)), extLsn(_nsVersion, _lsn), emmiterServerId(_emmiterServerId) {
	switch (type) {
		case Type::PutMeta:
		case Type::PutMetaTx:
			data.emplace<std::unique_ptr<MetaReplicationRecord>>(new MetaReplicationRecord{std::move(_k), std::move(_v)});
			break;
		default:
			assert(false);
	}
}

size_t UpdateRecord::DataSize() const noexcept {
	switch (type) {
		case Type::ItemUpdate:
		case Type::ItemUpsert:
		case Type::ItemDelete:
		case Type::ItemInsert:
		case Type::ItemUpdateTx:
		case Type::ItemUpsertTx:
		case Type::ItemDeleteTx:
		case Type::ItemInsertTx:
			return std::get<std::unique_ptr<ItemReplicationRecord>>(data)->Size();
		case Type::IndexAdd:
		case Type::IndexDrop:
		case Type::IndexUpdate:
			return std::get<std::unique_ptr<IndexReplicationRecord>>(data)->Size();
		case Type::PutMeta:
		case Type::PutMetaTx:
			return std::get<std::unique_ptr<MetaReplicationRecord>>(data)->Size();
		case Type::UpdateQuery:
		case Type::DeleteQuery:
		case Type::UpdateQueryTx:
		case Type::DeleteQueryTx:
			return std::get<std::unique_ptr<QueryReplicationRecord>>(data)->Size();
		case Type::SetSchema:
			return std::get<std::unique_ptr<SchemaReplicationRecord>>(data)->Size();
		case Type::Truncate:
		case Type::BeginTx:
		case Type::CommitTx:
		case Type::DropNamespace:
		case Type::ResyncNamespaceGeneric:
		case Type::ResyncNamespaceLeaderInit:
		case Type::ResyncOnUpdatesDrop:
		case Type::EmptyUpdate:
			return 0;
		case Type::AddNamespace: {
			return std::get<std::unique_ptr<AddNamespaceReplicationRecord>>(data)->Size();
		}
		case Type::RenameNamespace: {
			return std::get<std::unique_ptr<RenameNamespaceReplicationRecord>>(data)->Size();
		}
		case Type::NodeNetworkCheck: {
			return std::get<std::unique_ptr<NodeNetworkCheckRecord>>(data)->Size();
		}
		default:
			std::abort();
	}
}

}  // namespace cluster
}  // namespace reindexer
