#include "events/subscriber_config.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/catch_and_return.h"
#include "vendor/gason/gason.h"

namespace reindexer {

using namespace std::string_view_literals;

Error EventSubscriberConfig::FromJSON(std::span<char> json) noexcept {
	try {
		gason::JsonParser parser;
		FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "UpdatesFilter: {}", ex.what());
	}
	CATCH_AND_RETURN;
	return {};
}

static SubscriptionDataType SubDataTypeFromStr(std::string_view t) {
	if (t == "none"sv) {
		return kSubscriptionDataTypeNone;
	}
	throw Error(errParams, "Unsupported subscription data type: '{}'", t);
}

static std::string_view SubDataTypeToStr(SubscriptionDataType t) {
	switch (t) {
		case kSubscriptionDataTypeNone:
			return "none"sv;
		default:
			break;
	}
	throw Error(errParams, "Unsupported subscription data type: '{}'", int(t));
}

static updates::URType EventTypeFromStr(std::string_view t) {
	using updates::UpdateRecord;
	if (t == "None"sv) {
		return updates::URType::None;
	} else if (t == "ItemUpdate"sv) {
		return updates::URType::ItemUpdate;
	} else if (t == "ItemUpsert"sv) {
		return updates::URType::ItemUpsert;
	} else if (t == "ItemDelete"sv) {
		return updates::URType::ItemDelete;
	} else if (t == "ItemInsert"sv) {
		return updates::URType::ItemInsert;
	} else if (t == "ItemUpdateTx"sv) {
		return updates::URType::ItemUpdateTx;
	} else if (t == "ItemUpsertTx"sv) {
		return updates::URType::ItemUpsertTx;
	} else if (t == "ItemDeleteTx"sv) {
		return updates::URType::ItemDeleteTx;
	} else if (t == "ItemInsertTx"sv) {
		return updates::URType::ItemInsertTx;
	} else if (t == "IndexAdd"sv) {
		return updates::URType::IndexAdd;
	} else if (t == "IndexDrop"sv) {
		return updates::URType::IndexDrop;
	} else if (t == "IndexUpdate"sv) {
		return updates::URType::IndexUpdate;
	} else if (t == "PutMeta"sv) {
		return updates::URType::PutMeta;
	} else if (t == "PutMetaTx"sv) {
		return updates::URType::PutMetaTx;
	} else if (t == "UpdateQuery"sv) {
		return updates::URType::UpdateQuery;
	} else if (t == "DeleteQuery"sv) {
		return updates::URType::DeleteQuery;
	} else if (t == "UpdateQueryTx"sv) {
		return updates::URType::UpdateQueryTx;
	} else if (t == "DeleteQueryTx"sv) {
		return updates::URType::DeleteQueryTx;
	} else if (t == "SetSchema"sv) {
		return updates::URType::SetSchema;
	} else if (t == "Truncate"sv) {
		return updates::URType::Truncate;
	} else if (t == "BeginTx"sv) {
		return updates::URType::BeginTx;
	} else if (t == "CommitTx"sv) {
		return updates::URType::CommitTx;
	} else if (t == "AddNamespace"sv) {
		return updates::URType::AddNamespace;
	} else if (t == "DropNamespace"sv) {
		return updates::URType::DropNamespace;
	} else if (t == "CloseNamespace"sv) {
		return updates::URType::CloseNamespace;
	} else if (t == "RenameNamespace"sv) {
		return updates::URType::RenameNamespace;
	} else if (t == "NamespaceSync"sv) {
		return updates::URType::ResyncNamespaceGeneric;
	} else if (t == "NamespaceSyncOnLeaderInit"sv) {
		return updates::URType::ResyncNamespaceLeaderInit;
	} else if (t == "UpdatesDrop"sv) {
		return updates::URType::ResyncOnUpdatesDrop;
	} else if (t == "EmptyUpdate"sv) {
		return updates::URType::EmptyUpdate;
	} else if (t == "NodeNetworkCheck"sv) {
		return updates::URType::NodeNetworkCheck;
	} else if (t == "SetTagsMatcher"sv) {
		return updates::URType::SetTagsMatcher;
	} else if (t == "SetTagsMatcherTx"sv) {
		return updates::URType::SetTagsMatcherTx;
	} else if (t == "SaveShardingConfig"sv) {
		return updates::URType::SaveShardingConfig;
	} else if (t == "ApplyShardingConfig"sv) {
		return updates::URType::ApplyShardingConfig;
	} else if (t == "ResetOldShardingConfig"sv) {
		return updates::URType::ResetOldShardingConfig;
	} else if (t == "ResetCandidateConfig"sv) {
		return updates::URType::ResetCandidateConfig;
	} else if (t == "RollbackCandidateConfig"sv) {
		return updates::URType::RollbackCandidateConfig;
	} else if (t == "DeleteMeta") {
		return updates::URType::DeleteMeta;
	}
	throw Error(errParams, "Unknown event type: '{}'", t);
}

static std::string_view EventTypeToStr(updates::URType t) {
	using updates::UpdateRecord;
	switch (t) {
		case updates::URType::None:
			return "None"sv;
		case updates::URType::ItemUpdate:
			return "ItemUpdate"sv;
		case updates::URType::ItemUpsert:
			return "ItemUpsert"sv;
		case updates::URType::ItemDelete:
			return "ItemDelete"sv;
		case updates::URType::ItemInsert:
			return "ItemInsert"sv;
		case updates::URType::ItemUpdateTx:
			return "ItemUpdateTx"sv;
		case updates::URType::ItemUpsertTx:
			return "ItemUpsertTx"sv;
		case updates::URType::ItemDeleteTx:
			return "ItemDeleteTx"sv;
		case updates::URType::ItemInsertTx:
			return "ItemInsertTx"sv;
		case updates::URType::IndexAdd:
			return "IndexAdd"sv;
		case updates::URType::IndexDrop:
			return "IndexDrop"sv;
		case updates::URType::IndexUpdate:
			return "IndexUpdate"sv;
		case updates::URType::PutMeta:
			return "PutMeta"sv;
		case updates::URType::PutMetaTx:
			return "PutMetaTx"sv;
		case updates::URType::UpdateQuery:
			return "UpdateQuery"sv;
		case updates::URType::DeleteQuery:
			return "DeleteQuery"sv;
		case updates::URType::UpdateQueryTx:
			return "UpdateQueryTx"sv;
		case updates::URType::DeleteQueryTx:
			return "DeleteQueryTx"sv;
		case updates::URType::SetSchema:
			return "SetSchema"sv;
		case updates::URType::Truncate:
			return "Truncate"sv;
		case updates::URType::BeginTx:
			return "BeginTx"sv;
		case updates::URType::CommitTx:
			return "CommitTx"sv;
		case updates::URType::AddNamespace:
			return "AddNamespace"sv;
		case updates::URType::DropNamespace:
			return "DropNamespace"sv;
		case updates::URType::CloseNamespace:
			return "CloseNamespace"sv;
		case updates::URType::RenameNamespace:
			return "RenameNamespace"sv;
		case updates::URType::ResyncNamespaceGeneric:
			return "NamespaceSync"sv;
		case updates::URType::ResyncNamespaceLeaderInit:
			return "NamespaceSyncOnLeaderInit"sv;
		case updates::URType::ResyncOnUpdatesDrop:
			return "UpdatesDrop"sv;
		case updates::URType::EmptyUpdate:
			return "EmptyUpdate"sv;
		case updates::URType::NodeNetworkCheck:
			return "NodeNetworkCheck"sv;
		case updates::URType::SetTagsMatcher:
			return "SetTagsMatcher"sv;
		case updates::URType::SetTagsMatcherTx:
			return "SetTagsMatcherTx"sv;
		case updates::URType::SaveShardingConfig:
			return "SaveShardingConfig"sv;
		case updates::URType::ApplyShardingConfig:
			return "ApplyShardingConfig"sv;
		case updates::URType::ResetOldShardingConfig:
			return "ResetOldShardingConfig"sv;
		case updates::URType::ResetCandidateConfig:
			return "ResetCandidateConfig"sv;
		case updates::URType::RollbackCandidateConfig:
			return "RollbackCandidateConfig"sv;
		case updates::URType::DeleteMeta:
			return "DeleteMeta"sv;
		default:
			throw Error(errParams, "Unknown event type: '{}'", int(t));
	}
}

void EventSubscriberConfig::FromJSON(const gason::JsonNode& root) {
	formatVersion_ = root["version"sv].As<int>(-1);
	if (formatVersion_ < kMinSubscribersConfigFormatVersion) {
		throw Error(errParams,
					"EventSubscriberConfig: min supported subscribers config format version is {}, but {} version was found in JSON",
					kMinSubscribersConfigFormatVersion, formatVersion_);
	}
	withDBName_ = root["with_db_name"sv].As<bool>(false);
	withServerID_ = root["with_server_id"sv].As<bool>(false);
	withShardID_ = root["with_shard_id"sv].As<bool>(false);
	withLSN_ = root["with_lsn"sv].As<bool>(false);
	withTimestamp_ = root["with_timestamp"sv].As<bool>(false);
	dataType_ = SubDataTypeFromStr(root["data_type"sv].As<std::string>("none"));

	streams_.clear();
	streams_.reserve(kMaxStreamsPerSub);
	for (const auto& stream : root["streams"sv]) {
		const int id = stream["id"].As<int>(-1);
		if (id < 0 || unsigned(id) >= kMaxStreamsPerSub) {
			throw Error(errParams, "Stream ID {} is out of range [0, {}]", id, kMaxStreamsPerSub - 1);
		}
		if (std::find_if(streams_.begin(), streams_.end(), [id](const StreamConfig& s) noexcept { return s.id == id; }) != streams_.end()) {
			throw Error(errParams, "Stream ID {} is duplicated", id);
		}

		auto& s = streams_.emplace_back(id);
		s.withConfigNamespace = stream["with_config_namespace"sv].As<bool>(false);

		for (const auto& ns : stream["namespaces"sv]) {
			s.nss.emplace(ns["name"sv].As<std::string>());
		}
		const auto& eventTypesNode = stream["event_types"sv];
		if (eventTypesNode.empty() || begin(eventTypesNode) == end(eventTypesNode)) {
			eventTypes_.clear();
		} else {
			const bool addCommonEvents = (!eventTypes_.empty() || streams_.size() == 1);
			for (const auto& eventType : eventTypesNode) {
				const auto et = EventTypeFromStr(eventType.As<std::string>());
				s.eventTypes.emplace(et);
				if (addCommonEvents) {
					eventTypes_.emplace(et);
				}
			}
		}
	}
}

void EventSubscriberConfig::GetJSON(WrSerializer& ser) const {
	JsonBuilder builder(ser);
	{
		builder.Put("version"sv, formatVersion_);
		builder.Put("with_db_name"sv, withDBName_);
		builder.Put("with_server_id"sv, withServerID_);
		builder.Put("with_shard_id"sv, withShardID_);
		builder.Put("with_lsn"sv, withLSN_);
		builder.Put("with_timestamp", withTimestamp_);
		builder.Put("data_type"sv, SubDataTypeToStr(dataType_));

		auto streamArr = builder.Array("streams"sv);
		for (auto& stream : streams_) {
			auto streamObj = streamArr.Object();
			streamObj.Put("id"sv, stream.id);
			streamObj.Put("with_config_namespace"sv, stream.withConfigNamespace);
			{
				auto nssArr = streamObj.Array("namespaces"sv);
				for (const auto& ns : stream.nss) {
					auto obj = nssArr.Object();
					obj.Put("name"sv, ns);
				}
			}
			{
				auto eventTypesArr = builder.Array("event_types"sv);
				for (auto& e : stream.eventTypes) {
					eventTypesArr.Put(TagName::Empty(), EventTypeToStr(e));
				}
			}
		}
	}
}

}  // namespace reindexer
