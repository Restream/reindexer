#pragma once

#include <string_view>
#include <variant>
#include "client/cororeindexer.h"
#include "tools/lsn.h"

namespace reindexer {
namespace cluster {

struct ItemReplicationRecord {
	size_t Size() const noexcept { return sizeof(ItemReplicationRecord) + cjson.size(); }

	std::string cjson;
};

struct IndexReplicationRecord {
	size_t Size() const noexcept { return sizeof(IndexReplicationRecord) + idef.HeapSize(); }

	IndexDef idef;
};

struct MetaReplicationRecord {
	size_t Size() const noexcept { return sizeof(MetaReplicationRecord) + key.size() + value.size(); }

	std::string key;
	std::string value;
};

struct QueryReplicationRecord {
	size_t Size() const noexcept { return sizeof(QueryReplicationRecord) + sql.size(); }

	std::string sql;
};

struct SchemaReplicationRecord {
	size_t Size() const noexcept { return sizeof(SchemaReplicationRecord) + schema.size(); }

	std::string schema;
};

struct AddNamespaceReplicationRecord {
	size_t Size() const noexcept { return sizeof(AddNamespaceReplicationRecord) + def.HeapSize(); }

	NamespaceDef def;
	int64_t stateToken;
};

struct RenameNamespaceReplicationRecord {
	size_t Size() const noexcept { return sizeof(RenameNamespaceReplicationRecord) + dstNsName.size(); }

	std::string dstNsName;
};

struct NodeNetworkCheckRecord {
	size_t Size() const noexcept { return sizeof(NodeNetworkCheckRecord); }

	uint32_t nodeUid;
	bool online;
};

struct UpdateRecord {
	enum class Type {
		None = 0,
		ItemUpdate = 1,
		ItemUpsert = 2,
		ItemDelete = 3,
		ItemInsert = 4,
		ItemUpdateTx = 5,
		ItemUpsertTx = 6,
		ItemDeleteTx = 7,
		ItemInsertTx = 8,
		IndexAdd = 9,
		IndexDrop = 10,
		IndexUpdate = 11,
		PutMeta = 12,
		PutMetaTx = 13,
		UpdateQuery = 14,
		DeleteQuery = 15,
		UpdateQueryTx = 16,
		DeleteQueryTx = 17,
		SetSchema = 18,
		Truncate = 19,
		BeginTx = 20,
		CommitTx = 21,
		AddNamespace = 22,
		DropNamespace = 23,
		RenameNamespace = 24,
		ResyncNamespaceGeneric = 25,
		ResyncNamespaceLeaderInit = 26,
		ResyncOnUpdatesDrop = 27,
		EmptyUpdate = 28,
		NodeNetworkCheck = 29,
	};

	UpdateRecord() = default;
	UpdateRecord(Type _type, uint32_t _nodeUid, bool online);
	UpdateRecord(Type _type, std::string _nsName, int _emmiterServerId);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emmiterServerId);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emmiterServerId, std::string _data);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emmiterServerId, IndexDef _idef);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _nsVersion, int _emmiterServerId, NamespaceDef _def, int64_t _stateToken);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emmiterServerId, std::string _k, std::string _v);

	const std::string& GetNsName() const noexcept { return nsName; }
	bool IsDbRecord() const noexcept {
		return type == Type::AddNamespace || type == Type::DropNamespace || type == Type::RenameNamespace ||
			   type == Type::ResyncNamespaceGeneric || type == Type::ResyncNamespaceLeaderInit;
	}
	bool IsRequiringTmUpdate() const noexcept {
		return type == Type::IndexAdd || type == Type::SetSchema || type == Type::IndexDrop || type == Type::IndexUpdate || IsDbRecord();
	}
	bool IsRequiringTx() const noexcept {
		return type == Type::CommitTx || type == Type::ItemUpsertTx || type == Type::ItemInsertTx || type == Type::ItemDeleteTx ||
			   type == Type::ItemUpdateTx || type == Type::UpdateQueryTx || type == Type::DeleteQueryTx || type == Type::PutMetaTx;
	}
	bool IsTxBeginning() const noexcept { return type == Type::BeginTx; }
	bool IsEmptyRecord() const noexcept { return type == Type::EmptyUpdate; }
	bool IsBatchingAllowed() const noexcept {
		switch (type) {
			case Type::ItemUpdate:
			case Type::ItemUpsert:
			case Type::ItemDelete:
			case Type::ItemInsert: {
				auto& d = std::get<std::unique_ptr<ItemReplicationRecord>>(data);
				Serializer rdser(d->cjson);
				// check tags matcher update
				return (rdser.GetVarUint() != TAG_END);
			}
			case Type::ItemUpdateTx:
			case Type::ItemUpsertTx:
			case Type::ItemDeleteTx:
			case Type::ItemInsertTx:
			case Type::PutMeta:
			case Type::PutMetaTx:
			case Type::UpdateQueryTx:
			case Type::DeleteQueryTx:
			case Type::Truncate:
				return true;
			default:
				return false;
		}
	}
	bool IsNetworkCheckRecord() const noexcept { return type == Type::NodeNetworkCheck; }
	size_t DataSize() const noexcept;
	bool HasEmmiterID() const noexcept { return emmiterServerId != -1; }

	Type type = Type::None;
	std::string nsName;
	ExtendedLsn extLsn;
	std::variant<std::unique_ptr<ItemReplicationRecord>, std::unique_ptr<IndexReplicationRecord>, std::unique_ptr<MetaReplicationRecord>,
				 std::unique_ptr<QueryReplicationRecord>, std::unique_ptr<SchemaReplicationRecord>,
				 std::unique_ptr<AddNamespaceReplicationRecord>, std::unique_ptr<RenameNamespaceReplicationRecord>,
				 std::unique_ptr<NodeNetworkCheckRecord>>
		data;
	int emmiterServerId = -1;
};

}  // namespace cluster
}  // namespace reindexer
