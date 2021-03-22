#pragma once

#include "client/cororeindexer.h"
#include "estl/string_view.h"
#include "tools/lsn.h"
#include "vendor/mpark/variant.h"

namespace reindexer {
namespace cluster {

struct ItemReplicationRecord {
	std::string cjson;
};

struct IndexReplicationRecord {
	IndexDef idef;
};

struct MetaReplicationRecord {
	std::string key;
	std::string value;
};

struct QueryReplicationRecord {
	Query q;
};

struct SchemaReplicationRecord {
	std::string schema;
};

struct AddNamespaceReplicationRecord {
	NamespaceDef def;
};

struct RenameNamespaceReplicationRecord {
	std::string dstNsName;
};

struct UpdateRecord {
	enum class Type {
		None,
		ItemUpdate,
		ItemUpsert,
		ItemDelete,
		ItemInsert,
		ItemUpdateTx,
		ItemUpsertTx,
		ItemDeleteTx,
		ItemInsertTx,
		IndexAdd,
		IndexDrop,
		IndexUpdate,
		PutMeta,
		UpdateQuery,
		DeleteQuery,
		UpdateQueryTx,
		DeleteQueryTx,
		SetSchema,
		Truncate,
		BeginTx,
		CommitTx,
		AddNamespace,
		DropNamespace,
		RenameNamespace
	};

	UpdateRecord() = default;
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, std::string _cjson);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, IndexDef _idef);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, Query _q);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, NamespaceDef _q);
	UpdateRecord(Type _type, std::string _nsName, lsn_t _lsn, int _emmiterServerId, std::string _k, std::string _v);

	const std::string& GetNsName() const noexcept { return nsName; }

	Type type = Type::None;
	std::string nsName;
	lsn_t lsn;
	mpark::variant<std::unique_ptr<ItemReplicationRecord>, std::unique_ptr<IndexReplicationRecord>, std::unique_ptr<MetaReplicationRecord>,
				   std::unique_ptr<QueryReplicationRecord>, std::unique_ptr<SchemaReplicationRecord>,
				   std::unique_ptr<AddNamespaceReplicationRecord>, std::unique_ptr<RenameNamespaceReplicationRecord> >
		data;
	int emmiterServerId = -1;
};

}  // namespace cluster
}  // namespace reindexer
