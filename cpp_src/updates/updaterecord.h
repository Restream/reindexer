#pragma once

#include <variant>
#include "core/cjson/tagsmatcher.h"
#include "core/indexdef.h"
#include "core/namespace/namespacename.h"
#include "core/namespacedef.h"
#include "tools/lsn.h"

namespace reindexer {
namespace updates {

struct [[nodiscard]] ItemReplicationRecord {
	ItemReplicationRecord(chunk&& _ch) noexcept : ch(std::move(_ch)) {}
	ItemReplicationRecord(const ItemReplicationRecord&) = delete;
	ItemReplicationRecord& operator=(const ItemReplicationRecord&) = delete;
	size_t Size() const noexcept { return sizeof(ItemReplicationRecord) + ch.size(); }

	const chunk ch;
};

struct [[nodiscard]] TagsMatcherReplicationRecord {
	TagsMatcherReplicationRecord(TagsMatcher&& t, size_t ts) noexcept : tm(std::move(t)), tmSize(ts) {}
	TagsMatcherReplicationRecord(const TagsMatcherReplicationRecord&) = delete;
	TagsMatcherReplicationRecord& operator=(const TagsMatcherReplicationRecord&) = delete;
	size_t Size() const noexcept { return sizeof(TagsMatcherReplicationRecord) + tmSize; }

	const TagsMatcher tm;
	const size_t tmSize;
};

struct [[nodiscard]] IndexReplicationRecord {
	IndexReplicationRecord(IndexDef&& i) noexcept : idef(std::make_unique<const IndexDef>(std::move(i))) {}
	IndexReplicationRecord(const IndexReplicationRecord&) = delete;
	IndexReplicationRecord& operator=(const IndexReplicationRecord&) = delete;
	size_t Size() const noexcept { return sizeof(IndexReplicationRecord) + idef->HeapSize(); }

	std::unique_ptr<const IndexDef> idef;
};

struct [[nodiscard]] MetaReplicationRecord {
	MetaReplicationRecord(std::string&& k, std::string&& v) noexcept : key(std::move(k)), value(std::move(v)) {}
	MetaReplicationRecord(const MetaReplicationRecord&) = delete;
	MetaReplicationRecord& operator=(const MetaReplicationRecord&) = delete;
	size_t Size() const noexcept { return sizeof(MetaReplicationRecord) + key.size() + value.size(); }

	const std::string key;
	const std::string value;
};

struct [[nodiscard]] QueryReplicationRecord {
	QueryReplicationRecord(std::string&& s) noexcept : sql(std::move(s)) {}
	QueryReplicationRecord(const QueryReplicationRecord&) = delete;
	QueryReplicationRecord& operator=(const QueryReplicationRecord&) = delete;
	size_t Size() const noexcept { return sizeof(QueryReplicationRecord) + sql.size(); }

	const std::string sql;
};

struct [[nodiscard]] SchemaReplicationRecord {
	SchemaReplicationRecord(std::string&& s) noexcept : schema(std::move(s)) {}
	SchemaReplicationRecord(const SchemaReplicationRecord&) = delete;
	SchemaReplicationRecord& operator=(const SchemaReplicationRecord&) = delete;
	size_t Size() const noexcept { return sizeof(SchemaReplicationRecord) + schema.size(); }

	const std::string schema;
};

struct [[nodiscard]] AddNamespaceReplicationRecord {
	AddNamespaceReplicationRecord(NamespaceDef&& nd, int64_t st) noexcept
		: def(std::make_unique<const NamespaceDef>(std::move(nd))), stateToken(st) {}
	AddNamespaceReplicationRecord(const AddNamespaceReplicationRecord&) = delete;
	AddNamespaceReplicationRecord& operator=(const AddNamespaceReplicationRecord&) = delete;
	size_t Size() const noexcept { return sizeof(AddNamespaceReplicationRecord) + def->HeapSize(); }

	std::unique_ptr<const NamespaceDef> def;
	const int64_t stateToken;
};

struct [[nodiscard]] RenameNamespaceReplicationRecord {
	RenameNamespaceReplicationRecord(std::string&& dst) noexcept : dstNsName(std::move(dst)) {}
	RenameNamespaceReplicationRecord(const RenameNamespaceReplicationRecord&) = delete;
	RenameNamespaceReplicationRecord& operator=(const RenameNamespaceReplicationRecord&) = delete;
	size_t Size() const noexcept { return sizeof(RenameNamespaceReplicationRecord) + dstNsName.size(); }

	const std::string dstNsName;
};

struct [[nodiscard]] NodeNetworkCheckRecord {
	NodeNetworkCheckRecord(uint32_t nu, bool o) noexcept : nodeUid(nu), online(o) {}
	NodeNetworkCheckRecord(const NodeNetworkCheckRecord&) = delete;
	NodeNetworkCheckRecord& operator=(const NodeNetworkCheckRecord&) = delete;
	size_t Size() const noexcept { return sizeof(NodeNetworkCheckRecord); }

	const uint32_t nodeUid;
	const bool online;
};

struct [[nodiscard]] SaveNewShardingCfgRecord {
	SaveNewShardingCfgRecord(std::string&& c, int64_t s) noexcept : config(std::move(c)), sourceId(s) {}
	SaveNewShardingCfgRecord(const SaveNewShardingCfgRecord&) = delete;
	SaveNewShardingCfgRecord& operator=(const SaveNewShardingCfgRecord&) = delete;
	size_t Size() const noexcept { return sizeof(SaveNewShardingCfgRecord) + config.size(); }

	const std::string config;
	const int64_t sourceId;
};

struct [[nodiscard]] ApplyNewShardingCfgRecord {
	ApplyNewShardingCfgRecord(int64_t s) noexcept : sourceId(s) {}
	ApplyNewShardingCfgRecord(const ApplyNewShardingCfgRecord&) = delete;
	ApplyNewShardingCfgRecord& operator=(const ApplyNewShardingCfgRecord&) = delete;
	size_t Size() const noexcept { return sizeof(ApplyNewShardingCfgRecord); }

	const int64_t sourceId;
};

struct [[nodiscard]] ResetShardingCfgRecord {
	ResetShardingCfgRecord(int64_t s) noexcept : sourceId(s) {}
	ResetShardingCfgRecord(const ResetShardingCfgRecord&) = delete;
	ResetShardingCfgRecord& operator=(const ResetShardingCfgRecord&) = delete;
	size_t Size() const noexcept { return sizeof(ResetShardingCfgRecord); }

	const int64_t sourceId;
};

enum class [[nodiscard]] URType : int {
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
	CloseNamespace = 24,
	RenameNamespace = 25,
	ResyncNamespaceGeneric = 26,
	ResyncNamespaceLeaderInit = 27,
	ResyncOnUpdatesDrop = 28,
	EmptyUpdate = 29,
	NodeNetworkCheck = 30,
	SetTagsMatcher = 31,
	SetTagsMatcherTx = 32,
	SaveShardingConfig = 33,
	ApplyShardingConfig = 34,
	ResetOldShardingConfig = 35,
	ResetCandidateConfig = 36,
	RollbackCandidateConfig = 37,
	DeleteMeta = 38,
};

struct [[nodiscard]] UpdateRecord {
	enum class [[nodiscard]] ClonePolicy : bool {
		WithoutEmitter = 0,
		WithEmitter = 1,
	};
	using ImplT = const intrusive_atomic_rc_wrapper<
		std::variant<ItemReplicationRecord, IndexReplicationRecord, MetaReplicationRecord, QueryReplicationRecord, SchemaReplicationRecord,
					 AddNamespaceReplicationRecord, RenameNamespaceReplicationRecord, NodeNetworkCheckRecord, TagsMatcherReplicationRecord,
					 SaveNewShardingCfgRecord, ApplyNewShardingCfgRecord, ResetShardingCfgRecord>>;
	struct [[nodiscard]] UpdatesDropT {};

	UpdateRecord() = default;
	UpdateRecord(UpdatesDropT) noexcept : type_(URType::ResyncOnUpdatesDrop) {}
	UpdateRecord(URType _type, uint32_t _nodeUid, bool online);
	UpdateRecord(URType _type, NamespaceName _nsName, int _emitterServerId);
	UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId);
	UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, std::string _data);
	UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, WrSerializer&& _data);
	UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, const TagsMatcher& _tm);
	UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, IndexDef _idef);
	UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _nsVersion, int _emitterServerId, NamespaceDef _def, int64_t _stateToken);
	UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, std::string _k, std::string _v);
	UpdateRecord(URType _type, int _emitterServerId, std::string _data, int64_t sourceId);
	UpdateRecord(URType _type, int _emitterServerId, int64_t sourceId);
	UpdateRecord(const UpdateRecord&) = delete;
	UpdateRecord(UpdateRecord&&) = default;
	UpdateRecord& operator=(const UpdateRecord&) = delete;
	UpdateRecord& operator=(UpdateRecord&&) = default;

	const NamespaceName& NsName() const noexcept { return nsName_; }
	URType Type() const noexcept { return type_; }
	const ExtendedLsn& ExtLSN() const noexcept { return extLsn_; }
	const intrusive_ptr<ImplT>& Data() const noexcept { return data_; }
	bool IsDbRecord() const noexcept {
		return type_ == URType::AddNamespace || type_ == URType::DropNamespace || type_ == URType::CloseNamespace ||
			   type_ == URType::RenameNamespace || type_ == URType::ResyncNamespaceGeneric || type_ == URType::ResyncNamespaceLeaderInit ||
			   type_ == URType::SaveShardingConfig || type_ == URType::ApplyShardingConfig || type_ == URType::ResetOldShardingConfig ||
			   type_ == URType::RollbackCandidateConfig || type_ == URType::ResetCandidateConfig;
	}
	bool IsRequiringTmUpdate() const noexcept {
		return type_ == URType::IndexAdd || type_ == URType::SetSchema || type_ == URType::IndexDrop || type_ == URType::IndexUpdate ||
			   IsDbRecord();
	}
	bool IsRequiringTx() const noexcept {
		return type_ == URType::CommitTx || type_ == URType::ItemUpsertTx || type_ == URType::ItemInsertTx ||
			   type_ == URType::ItemDeleteTx || type_ == URType::ItemUpdateTx || type_ == URType::UpdateQueryTx ||
			   type_ == URType::DeleteQueryTx || type_ == URType::PutMetaTx || type_ == URType::SetTagsMatcherTx;
	}
	bool IsTxBeginning() const noexcept { return type_ == URType::BeginTx; }
	bool IsTxRecord() const noexcept { return IsTxBeginning() || IsRequiringTx(); }
	bool IsTxCommit() const noexcept { return type_ == URType::CommitTx; }
	bool IsEmptyRecord() const noexcept { return type_ == URType::EmptyUpdate; }
	bool IsResyncOnUpdatesDropRecord() const noexcept { return type_ == URType::ResyncOnUpdatesDrop; }
	bool IsBatchingAllowed() const noexcept {
		switch (type_) {
			case URType::ItemUpdate:
			case URType::ItemUpsert:
			case URType::ItemDelete:
			case URType::ItemInsert:
			case URType::ItemUpdateTx:
			case URType::ItemUpsertTx:
			case URType::ItemDeleteTx:
			case URType::ItemInsertTx:
			case URType::PutMeta:
			case URType::PutMetaTx:
			case URType::DeleteMeta:
			case URType::UpdateQueryTx:
			case URType::DeleteQueryTx:
			case URType::Truncate:
				return true;
			case URType::None:
			case URType::IndexAdd:
			case URType::IndexDrop:
			case URType::IndexUpdate:
			case URType::UpdateQuery:
			case URType::DeleteQuery:
			case URType::SetSchema:
			case URType::BeginTx:
			case URType::CommitTx:
			case URType::AddNamespace:
			case URType::CloseNamespace:
			case URType::DropNamespace:
			case URType::RenameNamespace:
			case URType::ResyncNamespaceGeneric:
			case URType::ResyncNamespaceLeaderInit:
			case URType::ResyncOnUpdatesDrop:
			case URType::EmptyUpdate:
			case URType::NodeNetworkCheck:
			case URType::SetTagsMatcher:
			case URType::SetTagsMatcherTx:
			case URType::SaveShardingConfig:
			case URType::ApplyShardingConfig:
			case URType::ResetOldShardingConfig:
			case URType::ResetCandidateConfig:
			case URType::RollbackCandidateConfig:
			default:
				return false;
		}
	}
	bool IsNetworkCheckRecord() const noexcept { return type_ == URType::NodeNetworkCheck; }
	size_t DataSize() const noexcept;
	bool HasEmitterID() const noexcept { return emitterServerId_ != -1; }
	int EmitterServerID() const noexcept { return emitterServerId_; }
	template <ClonePolicy policy>
	UpdateRecord Clone() const noexcept {
		// Explicit copy instead of builtin operators
		UpdateRecord rec;
		rec.type_ = type_;
		rec.nsName_ = nsName_;
		rec.extLsn_ = extLsn_;
		rec.data_ = data_;
		if constexpr (policy == ClonePolicy::WithEmitter) {
			rec.emitterServerId_ = emitterServerId_;
		} else {
			rec.emitterServerId_ = -1;
		}
		return rec;
	}

private:
	URType type_ = URType::None;
	int emitterServerId_ = -1;
	NamespaceName nsName_;
	ExtendedLsn extLsn_;
	intrusive_ptr<ImplT> data_;
};

}  // namespace updates

using UpdatesContainer = h_vector<updates::UpdateRecord, 2>;

}  // namespace reindexer
