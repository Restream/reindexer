#include "reindexerservice.h"

#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/queryresults/joinresults.h"
#include "core/reindexer.h"
#include "core/type_consts.h"
#include "server/dbmanager.h"

#include <grpcpp/grpcpp.h>

namespace reindexer {
namespace grpc {

ReindexerService::ReindexerService(reindexer_server::DBManager& dbMgr, std::chrono::seconds txIdleTimeout,
								   reindexer::net::ev::dynamic_loop& loop)
	: Reindexer::Service(), dbMgr_(dbMgr), txID_(0), txIdleTimeout_(txIdleTimeout) {
	expirationChecker_.set<ReindexerService, &ReindexerService::removeExpiredTxCb>(this);
	expirationChecker_.set(loop);
	expirationChecker_.start(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::seconds(1)).count(),
							 std::chrono::duration_cast<std::chrono::seconds>(std::chrono::seconds(1)).count());
}

Error ReindexerService::getDB(const std::string& dbName, int userRole, reindexer::Reindexer** ptr) {
	reindexer_server::AuthContext authCtx;
	Error status = dbMgr_.OpenDatabase(dbName, authCtx, false);
	if (!status.ok()) {
		return status;
	}
	reindexer::Reindexer* db = nullptr;
	status = authCtx.GetDB(reindexer_server::UserRole(userRole), &db);
	if (!status.ok()) {
		return status;
	}
	*ptr = db;
	return errOK;
}

::grpc::Status ReindexerService::Connect(::grpc::ServerContext*, const ConnectRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		std::string dsn;
		if (request->login().size() && request->password().size()) {
			dsn += request->login();
			dsn += ":";
			dsn += request->password();
			dsn += "@";
		}
		dsn += request->url();
		if (dsn.size() && dsn.back() != '\\') {
			dsn += '\\';
		}
		dsn += request->dbname();

		ConnectOpts opts;
		opts.Autorepair(request->connectopts().autorepair());
		opts.OpenNamespaces(request->connectopts().opennamespaces());
		opts.WithStorageType(StorageTypeOpt(request->connectopts().storagetype()));
		opts.DisableReplication(request->connectopts().disablereplication());
		opts.AllowNamespaceErrors(request->connectopts().allownamespaceerrors());

		assertrx(rx);
		status = rx->Connect(dsn, opts);
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::CreateDatabase(::grpc::ServerContext*, const CreateDatabaseRequest* request, ErrorResponse* response) {
	auto dbs = dbMgr_.EnumDatabases();
	for (auto& db : dbs) {
		if (db == request->dbname()) {
			response->set_code(ErrorResponse::ErrorCode(ErrorResponse_ErrorCode_errCodeParams));
			response->set_what("Database already exists");
		}
	}
	reindexer_server::AuthContext actx;
	Error status = dbMgr_.OpenDatabase(request->dbname(), actx, true);
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::OpenNamespace(::grpc::ServerContext*, const OpenNamespaceRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		StorageOpts opts;
		opts.Sync(request->storageoptions().sync());
		opts.Enabled(request->storageoptions().enabled());
		opts.FillCache(request->storageoptions().fillcache());
		opts.SlaveMode(request->storageoptions().slavemode());
		opts.Autorepair(request->storageoptions().autorepair());
		opts.CreateIfMissing(request->storageoptions().createifmissing());
		opts.VerifyChecksums(request->storageoptions().verifychecksums());
		opts.DropOnFileFormatError(request->storageoptions().droponfileformaterror());

		assertrx(rx);
		status = rx->OpenNamespace(request->storageoptions().nsname(), opts);
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::AddNamespace(::grpc::ServerContext*, const AddNamespaceRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		NamespaceDef nsDef;
		nsDef.name = request->namespace_().name();
		nsDef.storage.Sync(request->namespace_().storageoptions().sync());
		nsDef.storage.Enabled(request->namespace_().storageoptions().enabled());
		nsDef.storage.FillCache(request->namespace_().storageoptions().fillcache());
		nsDef.storage.SlaveMode(request->namespace_().storageoptions().slavemode());
		nsDef.storage.Autorepair(request->namespace_().storageoptions().autorepair());
		nsDef.storage.CreateIfMissing(request->namespace_().storageoptions().createifmissing());
		nsDef.storage.VerifyChecksums(request->namespace_().storageoptions().verifychecksums());
		nsDef.storage.DropOnFileFormatError(request->namespace_().storageoptions().droponfileformaterror());
		for (int i = 0; i < request->namespace_().indexesdefinitions().size(); ++i) {
			Index index = request->namespace_().indexesdefinitions(i);
			IndexDef indexDef;
			indexDef.name_ = index.name();
			indexDef.fieldType_ = index.fieldtype();
			indexDef.indexType_ = index.indextype();
			indexDef.expireAfter_ = index.expireafter();
			indexDef.opts_.PK(index.options().ispk());
			indexDef.opts_.Array(index.options().isarray());
			indexDef.opts_.Dense(index.options().isdense());
			indexDef.opts_.Sparse(index.options().issparse());
			indexDef.opts_.SetConfig(index.options().config());
			indexDef.opts_.RTreeType(static_cast<IndexOpts::RTreeIndexType>(index.options().rtreetype()));
			indexDef.opts_.SetCollateMode(CollateMode(index.options().collatemode()));
			for (int j = 0; j < index.jsonpaths().size(); ++j) {
				indexDef.jsonPaths_.emplace_back(index.jsonpaths(j));
			}
			nsDef.indexes.emplace_back(indexDef);
		}
		assertrx(rx);
		status = rx->AddNamespace(nsDef);
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::CloseNamespace(::grpc::ServerContext*, const CloseNamespaceRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		assertrx(rx);
		status = rx->CloseNamespace(request->nsname());
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::DropNamespace(::grpc::ServerContext*, const DropNamespaceRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		assertrx(rx);
		status = rx->DropNamespace(request->nsname());
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::TruncateNamespace(::grpc::ServerContext*, const TruncateNamespaceRequest* request,
												   ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		assertrx(rx);
		status = rx->TruncateNamespace(request->nsname());
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

static IndexDef toIndexDef(const Index& src) {
	IndexDef indexDef;
	indexDef.opts_.PK(src.options().ispk());
	indexDef.opts_.Array(src.options().isarray());
	indexDef.opts_.Dense(src.options().isdense());
	indexDef.opts_.Sparse(src.options().issparse());
	indexDef.opts_.RTreeType(static_cast<IndexOpts::RTreeIndexType>(src.options().rtreetype()));
	if (src.options().sortorderstable().empty()) {
		indexDef.opts_.SetCollateMode(CollateMode(src.options().collatemode()));
	} else {
		indexDef.opts_.collateOpts_ = CollateOpts(src.options().sortorderstable());
	}
	indexDef.opts_.config = src.options().config();
	indexDef.name_ = src.name();
	indexDef.fieldType_ = src.fieldtype();
	indexDef.indexType_ = src.indextype();
	indexDef.expireAfter_ = src.expireafter();
	for (const std::string& jsonPath : src.jsonpaths()) {
		indexDef.jsonPaths_.emplace_back(jsonPath);
	}
	return indexDef;
}

::grpc::Status ReindexerService::AddIndex(::grpc::ServerContext*, const AddIndexRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		assertrx(rx);
		IndexDef indexDef(toIndexDef(request->definition()));
		status = rx->AddIndex(request->nsname(), indexDef);
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::UpdateIndex(::grpc::ServerContext*, const UpdateIndexRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		assertrx(rx);
		IndexDef indexDef(toIndexDef(request->definition()));
		status = rx->UpdateIndex(request->nsname(), indexDef);
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::DropIndex(::grpc::ServerContext*, const DropIndexRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		assertrx(rx);
		IndexDef indexDef(toIndexDef(request->definition()));
		status = rx->DropIndex(request->nsname(), indexDef);
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::SetSchema(::grpc::ServerContext*, const SetSchemaRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDBAdmin, &rx);
	if (status.ok()) {
		assertrx(rx);
		status = rx->SetSchema(request->schemadefinitionrequest().nsname(), request->schemadefinitionrequest().jsondata());
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::GetProtobufSchema(::grpc::ServerContext*, const GetProtobufSchemaRequest* request,
												   ProtobufSchemaResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	ErrorResponse* responseCode = response->errorresponse().New();
	Error status = getDB(request->dbname(), reindexer_server::kRoleDataRead, &rx);
	if (status.ok()) {
		std::vector<std::string> nses;
		for (const std::string& ns : request->namespaces()) {
			nses.emplace_back(ns);
		}
		WrSerializer ser;
		assertrx(rx);
		status = rx->GetProtobufSchema(ser, nses);
		if (status.ok()) {
			std::string_view proto = ser.Slice();
			response->set_proto(proto.data(), proto.length());
		}
	}
	responseCode->set_code(ErrorResponse::ErrorCode(status.code()));
	responseCode->set_what(status.what());
	response->set_allocated_errorresponse(responseCode);
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::EnumNamespaces(::grpc::ServerContext*, const EnumNamespacesRequest* request,
												EnumNamespacesResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	ErrorResponse* responseCode = response->errorresponse().New();
	Error status = getDB(request->dbname(), reindexer_server::kRoleDataRead, &rx);
	if (status.ok()) {
		EnumNamespacesOpts opts;
		opts.OnlyNames(request->options().onlynames());
		opts.HideSystem(request->options().hidesystems());
		opts.WithClosed(request->options().withclosed());
		opts.WithFilter(request->options().filter());

		std::vector<NamespaceDef> nsDefs;
		assertrx(rx);
		status = rx->EnumNamespaces(nsDefs, opts);
		if (status.ok()) {
			for (const NamespaceDef& src : nsDefs) {
				Namespace* nsdef = response->add_namespacesdefinitions();
				nsdef->set_name(src.name);
				nsdef->set_istemporary(src.isTemporary);

				StorageOptions* storageOpts = nsdef->storageoptions().New();
				storageOpts->set_enabled(src.storage.IsEnabled());
				storageOpts->set_droponfileformaterror(src.storage.IsDropOnFileFormatError());
				storageOpts->set_createifmissing(src.storage.IsCreateIfMissing());
				storageOpts->set_verifychecksums(src.storage.IsVerifyChecksums());
				storageOpts->set_fillcache(src.storage.IsFillCache());
				storageOpts->set_sync(src.storage.IsSync());
				storageOpts->set_slavemode(src.storage.IsSlaveMode());
				storageOpts->set_autorepair(src.storage.IsAutorepair());
				nsdef->set_allocated_storageoptions(storageOpts);

				for (const IndexDef& index : src.indexes) {
					Index* indexDef = nsdef->add_indexesdefinitions();
					indexDef->set_name(index.name_);
					indexDef->set_fieldtype(index.fieldType_);
					indexDef->set_indextype(index.indexType_);
					indexDef->set_expireafter(index.expireAfter_);
					for (const std::string& jsonPath : index.jsonPaths_) {
						indexDef->add_jsonpaths(jsonPath);
					}

					IndexOptions* indexOpts = indexDef->options().New();
					indexOpts->set_ispk(index.opts_.IsPK());
					indexOpts->set_config(index.opts_.config);
					indexOpts->set_isarray(index.opts_.IsArray());
					indexOpts->set_isdense(index.opts_.IsDense());
					indexOpts->set_issparse(index.opts_.IsSparse());
					indexOpts->set_collatemode(IndexOptions::CollateMode(index.opts_.GetCollateMode()));
					indexOpts->set_rtreetype(static_cast<reindexer::grpc::IndexOptions_RTreeType>(index.opts_.RTreeType()));
					indexDef->set_allocated_options(indexOpts);
				}
			}
		}
	}
	responseCode->set_code(ErrorResponse::ErrorCode(status.code()));
	responseCode->set_what(status.what());
	response->set_allocated_errorresponse(responseCode);
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::EnumDatabases(::grpc::ServerContext*, const EnumDatabasesRequest*, EnumDatabasesResponse* response) {
	std::vector<std::string> dbNames = dbMgr_.EnumDatabases();
	for (const std::string& dbName : dbNames) {
		*(response->add_names()) = dbName;
	}
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::ModifyItem(::grpc::ServerContext*, ::grpc::ServerReaderWriter<ErrorResponse, ModifyItemRequest>* stream) {
	Error status;
	ErrorResponse response;
	ModifyItemRequest itemRequest;
	while (stream->Read(&itemRequest)) {
		std::string_view data(itemRequest.data().data(), itemRequest.data().length());
		if (data.empty()) {
			status = Error(errParams, "Item could not be empty");
			break;
		}

		reindexer::Reindexer* rx = nullptr;
		status = getDB(itemRequest.dbname(), reindexer_server::kRoleDataWrite, &rx);
		if (!status.ok()) {
			break;
		}

		assertrx(rx);
		Item item = rx->NewItem(itemRequest.nsname());
		if (!item.Status().ok()) {
			status = item.Status();
			break;
		}

		switch (itemRequest.encodingtype()) {
			case EncodingType::CJSON:
				status = item.FromCJSON(data);
				break;
			case EncodingType::JSON:
				status = item.FromJSON(data);
				break;
			case EncodingType::MSGPACK: {
				size_t offset = 0;
				status = item.FromMsgPack(data, offset);
				break;
			}
			case EncodingType::PROTOBUF:
				status = item.FromProtobuf(data);
				break;
			case EncodingType_INT_MAX_SENTINEL_DO_NOT_USE_:
			case EncodingType_INT_MIN_SENTINEL_DO_NOT_USE_:
			default:
				return ::grpc::Status(::grpc::INVALID_ARGUMENT, "Unsupported encoding type");
		}
		if (!status.ok()) {
			break;
		}
		switch (itemRequest.mode()) {
			case ModifyMode::UPDATE:
				status = rx->Update(itemRequest.nsname(), item);
				break;
			case ModifyMode::INSERT:
				status = rx->Insert(itemRequest.nsname(), item);
				break;
			case ModifyMode::UPSERT:
				status = rx->Upsert(itemRequest.nsname(), item);
				break;
			case ModifyMode::DELETE:
				status = rx->Delete(itemRequest.nsname(), item);
				break;
			case ModifyMode_INT_MIN_SENTINEL_DO_NOT_USE_:
			case ModifyMode_INT_MAX_SENTINEL_DO_NOT_USE_:
			default:
				break;
		}
		if (!status.ok()) {
			break;
		}
		response.set_code(ErrorResponse::ErrorCode(status.code()));
		response.set_what(status.what());
		stream->Write(response);
	}
	if (status.ok()) {
		return ::grpc::Status::OK;
	} else {
		response.set_code(ErrorResponse::ErrorCode(status.code()));
		response.set_what(status.what());
		stream->Write(response);
		return ::grpc::Status::CANCELLED;
	}
}

Error ReindexerService::packPayloadTypes(WrSerializer& wrser, const reindexer::QueryResults& qr) {
	wrser.PutVarUint(qr.getMergedNSCount());
	for (int i = 0; i < qr.getMergedNSCount(); ++i) {
		wrser.PutVarUint(i);
		wrser.PutVString(qr.getPayloadType(i).Name());

		const PayloadType& t = qr.getPayloadType(i);
		const TagsMatcher& m = qr.getTagsMatcher(i);

		wrser.PutVarUint(m.stateToken());
		wrser.PutVarUint(m.version());
		m.serialize(wrser);
		t->serialize(wrser);
	}
	return errOK;
}

Error ReindexerService::packCJSONItem(WrSerializer& wrser, reindexer::QueryResults::Iterator& it, const OutputFlags& opts) {
	ItemRef itemRef = it.GetItemRef();
	if (opts.withnsid()) {
		wrser.PutVarUint(itemRef.Nsid());
	}
	if (opts.withitemid()) {
		wrser.PutVarUint(itemRef.Id());
		wrser.PutVarUint(itemRef.Value().GetLSN());
	}
	if (opts.withrank()) {
		wrser.PutVarUint(itemRef.Proc());
	}
	return it.GetCJSON(wrser);
}

Error ReindexerService::buildItems(WrSerializer& wrser, const reindexer::QueryResults& qr, const OutputFlags& opts) {
	Error status;
	switch (opts.encodingtype()) {
		case EncodingType::JSON: {
			JsonBuilder builder(wrser, ObjType::TypeObject);
			if (qr.Count() > 0) {
				JsonBuilder array = builder.Array("items");
				for (auto& item : qr) {
					array.Raw(nullptr, "");
					status = item.GetJSON(wrser, false);
					if (!status.ok()) break;
				}
			}
			if (qr.GetAggregationResults().size() > 0) {
				buildAggregation(builder, wrser, qr, opts);
			}
			break;
		}
		case EncodingType::MSGPACK: {
			int fields = 0;
			bool withItems = (qr.Count() > 0);
			if (withItems) ++fields;
			bool withAggregation = (qr.GetAggregationResults().size() > 0);
			if (withAggregation) ++fields;
			MsgPackBuilder builder(wrser, ObjType::TypeObject, fields);
			if (withItems) {
				MsgPackBuilder array = builder.Array("items", qr.Count());
				for (auto& item : qr) {
					status = item.GetMsgPack(wrser, false);
					if (!status.ok()) break;
				}
			}
			if (withAggregation) {
				buildAggregation(builder, wrser, qr, opts);
			}
			break;
		}
		case EncodingType::PROTOBUF: {
			ProtobufBuilder builder(&wrser, ObjType::TypeObject);
			ProtobufBuilder array = builder.Array("items");
			for (auto& it : qr) {
				status = it.GetProtobuf(wrser, false);
				if (!status.ok()) break;
			}
			break;
		}
		case EncodingType::CJSON: {
			if (qr.Count() > 0) {
				packPayloadTypes(wrser, qr);
			}
			for (auto& item : qr) {
				status = packCJSONItem(wrser, item, opts);
				if (!status.ok()) break;

				auto jIt = item.GetJoined();
				if (opts.withjoineditems() && jIt.getJoinedItemsCount() > 0) {
					wrser.PutVarUint(jIt.getJoinedItemsCount() > 0 ? jIt.getJoinedFieldsCount() : 0);
					if (jIt.getJoinedItemsCount() == 0) continue;

					size_t joinedField = item.qr_->joined_.size();
					for (size_t ns = 0; ns < item.GetItemRef().Nsid(); ++ns) {
						joinedField += item.qr_->joined_[ns].GetJoinedSelectorsCount();
					}
					for (auto it = jIt.begin(); it != jIt.end(); ++it, ++joinedField) {
						wrser.PutVarUint(it.ItemsCount());
						if (it.ItemsCount() == 0) continue;
						QueryResults jqr = it.ToQueryResults();
						jqr.addNSContext(qr.getPayloadType(joinedField), qr.getTagsMatcher(joinedField), qr.getFieldsFilter(joinedField),
										 qr.getSchema(joinedField));
						for (size_t i = 0; i < jqr.Count(); i++) packCJSONItem(wrser, jqr.begin() + i, opts);
					}
				}
			}
			break;
		}
		case EncodingType_INT_MAX_SENTINEL_DO_NOT_USE_:
		case EncodingType_INT_MIN_SENTINEL_DO_NOT_USE_:
		default:
			return Error(errParams, "Unsupported encoding type");
	}
	return status;
}

template <typename Builder>
Error ReindexerService::buildAggregation(Builder& builder, WrSerializer& wrser, const reindexer::QueryResults& qr,
										 const OutputFlags& opts) {
	switch (opts.encodingtype()) {
		case EncodingType::JSON: {
			auto array = builder.Array("aggregations");
			for (size_t i = 0; i < qr.GetAggregationResults().size(); ++i) {
				array.Raw(nullptr, "");
				(qr.GetAggregationResults())[i].GetJSON(wrser);
			}
			break;
		}
		case EncodingType::MSGPACK: {
			auto array = builder.Array("aggregations", qr.Count());
			for (size_t i = 0; i < qr.GetAggregationResults().size(); ++i) {
				(qr.GetAggregationResults())[i].GetMsgPack(wrser);
			}
			break;
		}
		case EncodingType::CJSON:
		case EncodingType::PROTOBUF:
		case EncodingType_INT_MAX_SENTINEL_DO_NOT_USE_:
		case EncodingType_INT_MIN_SENTINEL_DO_NOT_USE_:
		default:
			return Error(errParams, "Unsupported encoding type");
	}
	return errOK;
}

::grpc::Status ReindexerService::buildQueryResults(const reindexer::QueryResults& qr, ::grpc::ServerWriter<QueryResultsResponse>* writer,
												   const OutputFlags& flags) {
	WrSerializer wrser;
	QueryResultsResponse response;
	Error status = buildItems(wrser, qr, flags);
	if (status.ok()) {
		response.set_data(std::string(wrser.Slice().data(), wrser.Slice().length()));
		QueryResultsResponse::QueryResultsOptions* opts = response.options().New();
		bool isWALQuery = (qr.Count() && qr.begin().IsRaw());
		opts->set_cacheenabled(qr.IsCacheEnabled() && !isWALQuery);
		if (!qr.GetExplainResults().empty()) {
			opts->set_explain(qr.GetExplainResults());
		}
		opts->set_totalitems(qr.Count());
		opts->set_querytotalitems(qr.TotalCount());
		response.set_allocated_options(opts);
	}
	ErrorResponse* responseCode = response.errorresponse().New();
	responseCode->set_code(ErrorResponse::ErrorCode(status.code()));
	responseCode->set_what(status.what());
	response.set_allocated_errorresponse(responseCode);
	writer->Write(response);
	return status.ok() ? ::grpc::Status::OK : ::grpc::Status::CANCELLED;
}

Error ReindexerService::executeQuery(const std::string& dbName, const Query& query, QueryType type, reindexer::QueryResults& qr) {
	Error status;
	reindexer::Query q;
	switch (query.encdoingtype()) {
		case EncodingType::JSON:
			status = q.FromJSON(query.data());
			break;
		case EncodingType::MSGPACK:
			// TODO: merge from appropriate MR
			return Error(errLogic, "MSGPACK is not yet supported for Query");
		case EncodingType::CJSON:
		case EncodingType::PROTOBUF:
		case EncodingType_INT_MAX_SENTINEL_DO_NOT_USE_:
		case EncodingType_INT_MIN_SENTINEL_DO_NOT_USE_:
		default:
			return Error(errParams, "Unsupported encoding type");
	}
	if (status.ok()) {
		reindexer::Reindexer* rx = nullptr;
		status = getDB(dbName, type == QueryType::QuerySelect ? reindexer_server::kRoleDataRead : reindexer_server::kRoleDataWrite, &rx);
		if (status.ok()) {
			assertrx(rx);
			switch (type) {
				case QueryType::QuerySelect:
					status = rx->Select(q, qr);
					break;
				case QueryType::QueryUpdate:
					status = rx->Update(q, qr);
					break;
				case QueryType::QueryDelete:
					status = rx->Delete(q, qr);
					break;
				case QueryType::QueryTruncate:
				default:
					return Error(errParams, "Unsupported type of query");
			}
		}
	}
	return status;
}

::grpc::Status ReindexerService::SelectSql(::grpc::ServerContext*, const SelectSqlRequest* request,
										   ::grpc::ServerWriter<QueryResultsResponse>* writer) {
	reindexer::QueryResults qr;
	Error status = execSqlQueryByType(qr, *request);
	if (status.ok()) {
		return buildQueryResults(qr, writer, request->flags());
	}
	QueryResultsResponse response;
	ErrorResponse* errResponse = response.errorresponse().New();
	errResponse->set_code(ErrorResponse::ErrorCode(status.code()));
	errResponse->set_what(status.what());
	response.set_allocated_errorresponse(errResponse);
	writer->Write(response);
	return ::grpc::Status::CANCELLED;
}

::grpc::Status ReindexerService::Select(::grpc::ServerContext*, const SelectRequest* request,
										::grpc::ServerWriter<QueryResultsResponse>* writer) {
	reindexer::QueryResults qr;
	Error status = executeQuery(request->dbname(), request->query(), QueryType::QuerySelect, qr);
	if (status.ok()) {
		return buildQueryResults(qr, writer, request->flags());
	} else {
		QueryResultsResponse response;
		ErrorResponse* errResponse = response.errorresponse().New();
		errResponse->set_code(ErrorResponse::ErrorCode(status.code()));
		errResponse->set_what(status.what());
		response.set_allocated_errorresponse(errResponse);
		writer->Write(response);
		return ::grpc::Status::CANCELLED;
	}
}

::grpc::Status ReindexerService::Update(::grpc::ServerContext*, const UpdateRequest* request,
										::grpc::ServerWriter<QueryResultsResponse>* writer) {
	reindexer::QueryResults qr;
	Error status = executeQuery(request->dbname(), request->query(), QueryType::QueryUpdate, qr);
	if (status.ok()) {
		return buildQueryResults(qr, writer, request->flags());
	} else {
		QueryResultsResponse response;
		ErrorResponse* errResponse = response.errorresponse().New();
		errResponse->set_code(ErrorResponse::ErrorCode(status.code()));
		errResponse->set_what(status.what());
		response.set_allocated_errorresponse(errResponse);
		writer->Write(response);
		return ::grpc::Status::CANCELLED;
	}
}

::grpc::Status ReindexerService::Delete(::grpc::ServerContext*, const DeleteRequest* request,
										::grpc::ServerWriter<QueryResultsResponse>* writer) {
	reindexer::QueryResults qr;
	Error status = executeQuery(request->dbname(), request->query(), QueryType::QueryDelete, qr);
	if (status.ok()) {
		return buildQueryResults(qr, writer, request->flags());
	} else {
		QueryResultsResponse response;
		ErrorResponse* errResponse = response.errorresponse().New();
		errResponse->set_code(ErrorResponse::ErrorCode(status.code()));
		errResponse->set_what(status.what());
		response.set_allocated_errorresponse(errResponse);
		writer->Write(response);
		return ::grpc::Status::CANCELLED;
	}
}

::grpc::Status ReindexerService::GetMeta(::grpc::ServerContext*, const GetMetaRequest* request, MetadataResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDataRead, &rx);
	if (status.ok()) {
		std::string data;
		assertrx(rx);
		status = rx->GetMeta(request->metadata().nsname(), request->metadata().key(), data);
		if (status.ok()) {
			response->set_metadata(data);
		}
	}
	ErrorResponse* errResponse = response->errorresponse().New();
	errResponse->set_code(ErrorResponse::ErrorCode(status.code()));
	errResponse->set_what(status.what());
	response->set_allocated_errorresponse(errResponse);
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::PutMeta(::grpc::ServerContext*, const PutMetaRequest* request, ErrorResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDataWrite, &rx);
	if (status.ok()) {
		assertrx(rx);
		status = rx->PutMeta(request->metadata().nsname(), request->metadata().key(), request->metadata().value());
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::EnumMeta(::grpc::ServerContext*, const EnumMetaRequest* request, MetadataKeysResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDataRead, &rx);
	if (status.ok()) {
		std::vector<std::string> keys;
		assertrx(rx);
		status = rx->EnumMeta(request->nsname(), keys);
		if (status.ok()) {
			for (const std::string& key : keys) {
				*(response->add_keys()) = key;
			}
		}
	}
	ErrorResponse* retStatus = response->errorresponse().New();
	retStatus->set_code(ErrorResponse::ErrorCode(status.code()));
	retStatus->set_what(status.what());
	response->set_allocated_errorresponse(retStatus);
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::BeginTransaction(::grpc::ServerContext*, const BeginTransactionRequest* request,
												  TransactionIdResponse* response) {
	reindexer::Reindexer* rx = nullptr;
	Error status = getDB(request->dbname(), reindexer_server::kRoleDataWrite, &rx);
	if (status.ok()) {
		assertrx(rx);
		reindexer::Transaction tr = rx->NewTransaction(request->nsname());
		status = tr.Status();
		if (status.ok()) {
			uint64_t txID = txID_++;
			response->set_id(txID);
			TxData txData;
			txData.dbName = request->dbname();
			txData.nsName = request->nsname();
			txData.tx = std::make_shared<Transaction>(std::move(tr));
			txData.txDeadline = std::chrono::steady_clock::now() + txIdleTimeout_;
			std::lock_guard<std::mutex> lck(m_);
			transactions_.emplace(txID, std::move(txData));
		}
	}
	ErrorResponse* retStatus = response->status().New();
	retStatus->set_code(ErrorResponse::ErrorCode(status.code()));
	retStatus->set_what(status.what());
	response->set_allocated_status(retStatus);
	return ::grpc::Status::OK;
}

void ReindexerService::removeExpiredTxCb(reindexer::net::ev::periodic&, int) {
	auto now = std::chrono::steady_clock::now();
	std::lock_guard<std::mutex> lck(m_);
	for (auto it = transactions_.begin(); it != transactions_.end();) {
		if (it->second.txDeadline <= now) {
			auto ctx = reindexer_server::MakeSystemAuthContext();
			auto status = dbMgr_.OpenDatabase(it->second.dbName, ctx, false);
			if (status.ok()) {
				reindexer::Reindexer* db = nullptr;
				status = ctx.GetDB(reindexer_server::kRoleSystem, &db);
				if (db) db->RollBackTransaction(*it->second.tx);
			}
			it = transactions_.erase(it);
		} else {
			++it;
		}
	}
}

Error ReindexerService::getTx(uint64_t id, TxData& txData) {
	std::unique_lock<std::mutex> lck(m_);
	auto it = transactions_.find(id);
	if (it == transactions_.end()) {
		lck.unlock();
		return Error(errParams, "No such transaction ID or transaction has expired");
	}
	txData.dbName = it->second.dbName;
	txData.nsName = it->second.nsName;
	txData.tx = it->second.tx;
	return errOK;
}

::grpc::Status ReindexerService::AddTxItem(::grpc::ServerContext*, ::grpc::ServerReaderWriter<ErrorResponse, AddTxItemRequest>* stream) {
	Error status;
	ErrorResponse response;
	AddTxItemRequest request;
	while (stream->Read(&request)) {
		TxData txData;
		status = getTx(request.id(), txData);
		if (!status.ok()) {
			break;
		}

		reindexer::Reindexer* rx = nullptr;
		status = getDB(txData.dbName, reindexer_server::kRoleDataWrite, &rx);
		if (!status.ok()) {
			break;
		}

		assertrx(rx);
		Item item = rx->NewItem(txData.nsName);
		if (!item.Status().ok()) {
			status = item.Status();
			break;
		}

		switch (request.encodingtype()) {
			case EncodingType::CJSON:
				status = item.FromCJSON(request.data());
				break;
			case EncodingType::JSON:
				status = item.FromJSON(request.data());
				break;
			case EncodingType::MSGPACK: {
				size_t offset = 0;
				status = item.FromMsgPack(request.data(), offset);
				break;
			}
			case EncodingType::PROTOBUF:
				status = item.FromProtobuf(request.data());
				break;
			case EncodingType_INT_MAX_SENTINEL_DO_NOT_USE_:
			case EncodingType_INT_MIN_SENTINEL_DO_NOT_USE_:
			default:
				return ::grpc::Status(::grpc::INVALID_ARGUMENT, "Unsupported encoding type");
		}
		if (!status.ok()) {
			break;
		}
		switch (request.mode()) {
			case ModifyMode::UPDATE:
				txData.tx->Update(std::move(item));
				break;
			case ModifyMode::INSERT:
				txData.tx->Insert(std::move(item));
				break;
			case ModifyMode::UPSERT:
				txData.tx->Upsert(std::move(item));
				break;
			case ModifyMode::DELETE:
				txData.tx->Delete(std::move(item));
				break;
			case ModifyMode_INT_MAX_SENTINEL_DO_NOT_USE_:
			case ModifyMode_INT_MIN_SENTINEL_DO_NOT_USE_:
			default:
				break;
		}
		response.set_code(ErrorResponse::ErrorCode(status.code()));
		response.set_what(status.what());
		stream->Write(response);
	}
	if (status.ok()) {
		return ::grpc::Status::OK;
	} else {
		response.set_code(ErrorResponse::ErrorCode(status.code()));
		response.set_what(status.what());
		stream->Write(response);
		return ::grpc::Status::CANCELLED;
	}
}

::grpc::Status ReindexerService::CommitTransaction(::grpc::ServerContext*, const CommitTransactionRequest* request,
												   ErrorResponse* response) {
	TxData txData;
	Error status = getTx(request->id(), txData);
	if (status.ok()) {
		reindexer::Reindexer* rx = nullptr;
		status = getDB(txData.dbName, reindexer_server::kRoleDataWrite, &rx);
		if (status.ok()) {
			reindexer::QueryResults qr;
			assertrx(rx);
			status = rx->CommitTransaction(*txData.tx, qr);
			{
				std::lock_guard<std::mutex> lck(m_);
				transactions_.erase(request->id());
			}
		}
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

::grpc::Status ReindexerService::RollbackTransaction(::grpc::ServerContext*, const RollbackTransactionRequest* request,
													 ErrorResponse* response) {
	TxData txData;
	Error status = getTx(request->id(), txData);
	if (status.ok()) {
		reindexer::Reindexer* rx = nullptr;
		status = getDB(txData.dbName, reindexer_server::kRoleDataWrite, &rx);
		if (status.ok()) {
			assertrx(rx);
			status = rx->RollBackTransaction(*txData.tx);
			{
				std::lock_guard<std::mutex> lck(m_);
				transactions_.erase(request->id());
			}
		}
	}
	response->set_code(ErrorResponse::ErrorCode(status.code()));
	response->set_what(status.what());
	return ::grpc::Status::OK;
}

Error ReindexerService::execSqlQueryByType(QueryResults& res, const SelectSqlRequest& request) {
	try {
		reindexer::Query q;
		reindexer_server::UserRole requiredRole;
		q.FromSQL(request.sql());
		switch (q.Type()) {
			case QuerySelect: {
				requiredRole = reindexer_server::kRoleDataRead;
				break;
			}
			case QueryDelete:
			case QueryUpdate: {
				requiredRole = reindexer_server::kRoleDataWrite;
				break;
			}
			case QueryTruncate: {
				requiredRole = reindexer_server::kRoleDBAdmin;
				break;
			}
			default:
				return Error(errParams, "unknown query type %d", q.Type());
		}
		reindexer::Reindexer* rx = nullptr;
		auto err = getDB(request.dbname(), requiredRole, &rx);
		if (!err.ok()) {
			return err;
		}

		switch (q.Type()) {
			case QuerySelect: {
				return rx->Select(q, res);
			}
			case QueryDelete: {
				return rx->Delete(q, res);
			}
			case QueryUpdate: {
				return rx->Update(q, res);
			}
			case QueryTruncate: {
				return rx->TruncateNamespace(q._namespace);
			}
			default:
				return Error(errParams, "unknown query type %d", q.Type());
		}
	} catch (Error& e) {
		return e;
	}
}

}  // namespace grpc
}  // namespace reindexer

struct grpc_data {
	std::unique_ptr<reindexer::grpc::ReindexerService> service_;
	std::unique_ptr<::grpc::Server> grpcServer_;
};

extern "C" void* start_reindexer_grpc(reindexer_server::DBManager& dbMgr, std::chrono::seconds txIdleTimeout,
									  reindexer::net::ev::dynamic_loop& loop, const std::string& address) {
	auto data = new grpc_data();

	data->service_.reset(new reindexer::grpc::ReindexerService(dbMgr, txIdleTimeout, loop));
	::grpc::ServerBuilder builder;
	builder.AddListeningPort(address, ::grpc::InsecureServerCredentials());
	builder.RegisterService(data->service_.get());
	data->grpcServer_ = builder.BuildAndStart();
	if (!data->grpcServer_) {
		throw reindexer::Error(errLogic, "Unable to create GRPC server. Check logs for details");
	}
	return data;
}

extern "C" void stop_reindexer_grpc(void* pdata) {
	auto data = reinterpret_cast<grpc_data*>(pdata);
	data->grpcServer_->Shutdown();
	delete data;
}
