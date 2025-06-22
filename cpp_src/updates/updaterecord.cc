#include "updaterecord.h"

namespace reindexer {
namespace updates {

UpdateRecord::UpdateRecord(URType _type, uint32_t _nodeUid, bool online)
	: type_(_type), data_(make_intrusive<ImplT>(std::in_place_type<NodeNetworkCheckRecord>, _nodeUid, online)) {
	assertrx(_type == URType::NodeNetworkCheck);
}

UpdateRecord::UpdateRecord(URType _type, NamespaceName _nsName, int _emitterServerId)
	: type_(_type), emitterServerId_(_emitterServerId), nsName_(std::move(_nsName)) {
	switch (type_) {
		case URType::EmptyUpdate:
			break;
		case URType::None:
		case URType::ItemUpdate:
		case URType::ItemUpsert:
		case URType::ItemDelete:
		case URType::ItemInsert:
		case URType::ItemUpdateTx:
		case URType::ItemUpsertTx:
		case URType::ItemDeleteTx:
		case URType::ItemInsertTx:
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
		case URType::PutMeta:
		case URType::PutMetaTx:
		case URType::DeleteMeta:
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
		case URType::SetSchema:
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::AddNamespace:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::RenameNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
		case URType::SaveShardingConfig:
		case URType::ApplyShardingConfig:
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			assertrx(false);
	}
}

UpdateRecord::UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId)
	: type_(_type), emitterServerId_(_emitterServerId), nsName_(std::move(_nsName)), extLsn_(_nsVersion, _lsn) {
	switch (type_) {
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
			break;
		case URType::None:
		case URType::ItemUpdate:
		case URType::ItemUpsert:
		case URType::ItemDelete:
		case URType::ItemInsert:
		case URType::ItemUpdateTx:
		case URType::ItemUpsertTx:
		case URType::ItemDeleteTx:
		case URType::ItemInsertTx:
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
		case URType::PutMeta:
		case URType::PutMetaTx:
		case URType::DeleteMeta:
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
		case URType::SetSchema:
		case URType::AddNamespace:
		case URType::RenameNamespace:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
		case URType::EmptyUpdate:
		case URType::SaveShardingConfig:
		case URType::ApplyShardingConfig:
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			assertrx(false);
	}
}

UpdateRecord::UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, std::string _data)
	: type_(_type), emitterServerId_(_emitterServerId), nsName_(std::move(_nsName)), extLsn_(_nsVersion, _lsn) {
	switch (type_) {
		case URType::RenameNamespace:
			data_ = make_intrusive<ImplT>(std::in_place_type<RenameNamespaceReplicationRecord>, std::move(_data));
			break;
		case URType::SetSchema:
			data_ = make_intrusive<ImplT>(std::in_place_type<SchemaReplicationRecord>, std::move(_data));
			break;
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
			data_ = make_intrusive<ImplT>(std::in_place_type<QueryReplicationRecord>, std::move(_data));
			break;
		case URType::None:
		case URType::ItemUpdate:
		case URType::ItemUpsert:
		case URType::ItemDelete:
		case URType::ItemInsert:
		case URType::ItemUpdateTx:
		case URType::ItemUpsertTx:
		case URType::ItemDeleteTx:
		case URType::ItemInsertTx:
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
		case URType::PutMeta:
		case URType::PutMetaTx:
		case URType::DeleteMeta:
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::AddNamespace:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
		case URType::EmptyUpdate:
		case URType::SaveShardingConfig:
		case URType::ApplyShardingConfig:
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			assertrx(false);
	}
}

UpdateRecord::UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, WrSerializer&& _data)
	: type_(_type), emitterServerId_(_emitterServerId), nsName_(std::move(_nsName)), extLsn_(_nsVersion, _lsn) {
	switch (type_) {
		case URType::ItemUpdate:
		case URType::ItemUpsert:
		case URType::ItemDelete:
		case URType::ItemInsert:
		case URType::ItemUpdateTx:
		case URType::ItemUpsertTx:
		case URType::ItemDeleteTx:
		case URType::ItemInsertTx: {
			auto ch = _data.DetachChunk();
			ch.shrink(1);
			data_ = make_intrusive<ImplT>(std::in_place_type<ItemReplicationRecord>, std::move(ch));
			break;
		}
		case URType::None:
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
		case URType::PutMeta:
		case URType::PutMetaTx:
		case URType::DeleteMeta:
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
		case URType::SetSchema:
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::AddNamespace:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::RenameNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
		case URType::EmptyUpdate:
		case URType::SaveShardingConfig:
		case URType::ApplyShardingConfig:
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			assertrx(false);
	}
}

UpdateRecord::UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, const TagsMatcher& _tm)
	: type_(_type), emitterServerId_(_emitterServerId), nsName_(std::move(_nsName)), extLsn_(_nsVersion, _lsn) {
	switch (type_) {
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx: {
			TagsMatcher tm;
			WrSerializer wser;
			_tm.serialize(wser);
			Serializer ser(wser.Slice());
			tm.deserialize(ser, _tm.version(), _tm.stateToken());
			data_ = make_intrusive<ImplT>(std::in_place_type<TagsMatcherReplicationRecord>, std::move(tm), wser.Slice().size() * 4);
			break;
		}
		case URType::None:
		case URType::ItemUpdate:
		case URType::ItemUpsert:
		case URType::ItemDelete:
		case URType::ItemInsert:
		case URType::ItemUpdateTx:
		case URType::ItemUpsertTx:
		case URType::ItemDeleteTx:
		case URType::ItemInsertTx:
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
		case URType::PutMeta:
		case URType::PutMetaTx:
		case URType::DeleteMeta:
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
		case URType::SetSchema:
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::AddNamespace:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::RenameNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::EmptyUpdate:
		case URType::SaveShardingConfig:
		case URType::ApplyShardingConfig:
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			assertrx(false);
	}
}

UpdateRecord::UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, IndexDef _idef)
	: type_(_type),
	  emitterServerId_(_emitterServerId),
	  nsName_(std::move(_nsName)),
	  extLsn_(_nsVersion, _lsn),
	  data_(make_intrusive<ImplT>(std::in_place_type<IndexReplicationRecord>, std::move(_idef))) {
	switch (type_) {
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
			break;
		case URType::None:
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
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
		case URType::SetSchema:
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::AddNamespace:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::RenameNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
		case URType::EmptyUpdate:
		case URType::SaveShardingConfig:
		case URType::ApplyShardingConfig:
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			assertrx(false);
	}
}

UpdateRecord::UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _nsVersion, int _emitterServerId, NamespaceDef _def,
						   int64_t _stateToken)
	: type_(_type), emitterServerId_(_emitterServerId), nsName_(std::move(_nsName)), extLsn_(_nsVersion, lsn_t(0, 0)) {
	switch (type_) {
		case URType::AddNamespace:
			data_ = make_intrusive<ImplT>(std::in_place_type<AddNamespaceReplicationRecord>, std::move(_def), _stateToken);
			break;
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
		case URType::None:
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
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
		case URType::SetSchema:
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::RenameNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
		case URType::EmptyUpdate:
		case URType::SaveShardingConfig:
		case URType::ApplyShardingConfig:
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			assertrx(false);
	}
}

UpdateRecord::UpdateRecord(URType _type, NamespaceName _nsName, lsn_t _lsn, lsn_t _nsVersion, int _emitterServerId, std::string _k,
						   std::string _v)
	: type_(_type), emitterServerId_(_emitterServerId), nsName_(std::move(_nsName)), extLsn_(_nsVersion, _lsn) {
	switch (type_) {
		case URType::PutMeta:
		case URType::PutMetaTx:
		case URType::DeleteMeta:
			data_ = make_intrusive<ImplT>(std::in_place_type<MetaReplicationRecord>, std::move(_k), std::move(_v));
			break;
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
		case URType::None:
		case URType::ItemUpdate:
		case URType::ItemUpsert:
		case URType::ItemDelete:
		case URType::ItemInsert:
		case URType::ItemUpdateTx:
		case URType::ItemUpsertTx:
		case URType::ItemDeleteTx:
		case URType::ItemInsertTx:
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
		case URType::SetSchema:
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::AddNamespace:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::RenameNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
		case URType::EmptyUpdate:
		case URType::SaveShardingConfig:
		case URType::ApplyShardingConfig:
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			assertrx(false);
	}
}

UpdateRecord::UpdateRecord(URType _type, int _emitterServerId, std::string _data, int64_t sourceId)
	: type_(_type), emitterServerId_(_emitterServerId) {
	switch (type_) {
		case URType::SaveShardingConfig:
			data_ = make_intrusive<ImplT>(std::in_place_type<SaveNewShardingCfgRecord>, std::move(_data), sourceId);
			break;
		case URType::PutMeta:
		case URType::PutMetaTx:
		case URType::DeleteMeta:
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
		case URType::None:
		case URType::ItemUpdate:
		case URType::ItemUpsert:
		case URType::ItemDelete:
		case URType::ItemInsert:
		case URType::ItemUpdateTx:
		case URType::ItemUpsertTx:
		case URType::ItemDeleteTx:
		case URType::ItemInsertTx:
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
		case URType::SetSchema:
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::AddNamespace:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::RenameNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
		case URType::EmptyUpdate:
		case URType::ApplyShardingConfig:
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			assertrx(false);
	}
}

UpdateRecord::UpdateRecord(URType _type, int _emitterServerId, int64_t sourceId) : type_(_type), emitterServerId_(_emitterServerId) {
	switch (type_) {
		case URType::ApplyShardingConfig:
			data_ = make_intrusive<ImplT>(std::in_place_type<ApplyNewShardingCfgRecord>, sourceId);
			break;
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			data_ = make_intrusive<ImplT>(std::in_place_type<ResetShardingCfgRecord>, sourceId);
			break;
		case URType::PutMeta:
		case URType::PutMetaTx:
		case URType::DeleteMeta:
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
		case URType::None:
		case URType::ItemUpdate:
		case URType::ItemUpsert:
		case URType::ItemDelete:
		case URType::ItemInsert:
		case URType::ItemUpdateTx:
		case URType::ItemUpsertTx:
		case URType::ItemDeleteTx:
		case URType::ItemInsertTx:
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
		case URType::SetSchema:
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::AddNamespace:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::RenameNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::NodeNetworkCheck:
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
		case URType::EmptyUpdate:
		case URType::SaveShardingConfig:
			assertrx(false);
	}
}

size_t UpdateRecord::DataSize() const noexcept {
	switch (type_) {
		case URType::ItemUpdate:
		case URType::ItemUpsert:
		case URType::ItemDelete:
		case URType::ItemInsert:
		case URType::ItemUpdateTx:
		case URType::ItemUpsertTx:
		case URType::ItemDeleteTx:
		case URType::ItemInsertTx:
			return sizeof(ImplT) + std::get_if<ItemReplicationRecord>(data_.get())->Size();
		case URType::IndexAdd:
		case URType::IndexDrop:
		case URType::IndexUpdate:
			return sizeof(ImplT) + std::get_if<IndexReplicationRecord>(data_.get())->Size();
		case URType::PutMeta:
		case URType::PutMetaTx:
		case URType::DeleteMeta:
			return sizeof(ImplT) + std::get_if<MetaReplicationRecord>(data_.get())->Size();
		case URType::UpdateQuery:
		case URType::DeleteQuery:
		case URType::UpdateQueryTx:
		case URType::DeleteQueryTx:
			return sizeof(ImplT) + std::get_if<QueryReplicationRecord>(data_.get())->Size();
		case URType::SetSchema:
			return sizeof(ImplT) + std::get_if<SchemaReplicationRecord>(data_.get())->Size();
		case URType::Truncate:
		case URType::BeginTx:
		case URType::CommitTx:
		case URType::DropNamespace:
		case URType::CloseNamespace:
		case URType::ResyncNamespaceGeneric:
		case URType::ResyncNamespaceLeaderInit:
		case URType::ResyncOnUpdatesDrop:
		case URType::EmptyUpdate:
			return 0;
		case URType::AddNamespace:
			return sizeof(ImplT) + std::get_if<AddNamespaceReplicationRecord>(data_.get())->Size();
		case URType::RenameNamespace:
			return sizeof(ImplT) + std::get_if<RenameNamespaceReplicationRecord>(data_.get())->Size();
		case URType::NodeNetworkCheck:
			return sizeof(ImplT) + std::get_if<NodeNetworkCheckRecord>(data_.get())->Size();
		case URType::SetTagsMatcher:
		case URType::SetTagsMatcherTx:
			return sizeof(ImplT) + std::get_if<TagsMatcherReplicationRecord>(data_.get())->Size();
		case URType::SaveShardingConfig:
			return sizeof(ImplT) + std::get_if<SaveNewShardingCfgRecord>(data_.get())->Size();
		case URType::ApplyShardingConfig:
			return sizeof(ImplT) + std::get_if<ApplyNewShardingCfgRecord>(data_.get())->Size();
		case URType::ResetOldShardingConfig:
		case URType::ResetCandidateConfig:
		case URType::RollbackCandidateConfig:
			return sizeof(ImplT) + std::get_if<ResetShardingCfgRecord>(data_.get())->Size();
		case URType::None:
		default:
			std::abort();
	}
}

}  // namespace updates
}  // namespace reindexer
