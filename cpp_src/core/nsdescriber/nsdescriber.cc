#include <sstream>

#include "core/cjson/jsondecoder.h"
#include "core/cjson/jsonencoder.h"
#include "core/namespace.h"
#include "nsdescriber.h"

using std::shared_ptr;
using std::string;
using std::stringstream;

namespace reindexer {
void NsDescriber::operator()(QueryResults &result) {
	PayloadType payloadType;
	TagsMatcher tagsMatcher;
	Namespace::RLock rlock(ns_->mtx_);

	if (result.ctxs.size() == 0) {
		payloadType = PayloadType(ns_->name);
		payloadType.Add(PayloadFieldType(KeyValueString, "-tuple", "", false));
		tagsMatcher = TagsMatcher(payloadType);
		result.addNSContext(payloadType, tagsMatcher, JsonPrintFilter());
	} else {
		payloadType = result.getPayloadType(0);
		tagsMatcher = result.getTagsMatcher(0);
	}

	JsonValue jsonValue;
	JsonAllocator jsonAllocator;
	PayloadValue payloadValue(payloadType.TotalSize());
	char *endptr;

	// prepare json
	stringstream strStream;
	strStream << "{";
	strStream << "\"name\":"
			  << "\"" << ns_->name << "\",";

	string updated = ns_->GetMeta("updated");
	if (updated.empty()) {
		updated = "0";
	}

	strStream << "\"updated_unix_nano\":" << updated << ",";
	strStream << "\"indexes\":[";

	// skip first "-tuple" index
	unsigned indexesLength = ns_->indexes_.size();
	for (unsigned i = 1; i < indexesLength; i++) {
		auto &index = *ns_->indexes_[i];

		bool isSortable =
			index.type == IndexStrBTree || index.type == IndexIntBTree || index.type == IndexDoubleBTree || index.type == IndexInt64BTree;
		bool isFulltext = index.type == IndexFullText || index.type == IndexNewFullText;

		strStream << "{";
		strStream << "\"name\":"
				  << "\"" << index.name << "\",";
		strStream << "\"field_type\":"
				  << "\"" << index.TypeName() << "\",";
		strStream << "\"is_array\":" << (index.opts_.IsArray() ? "true" : "false") << ",";
		strStream << "\"sortable\":" << (isSortable ? "true" : "false") << ",";
		strStream << "\"pk\":" << (index.opts_.IsPK() ? "true" : "false") << ",";
		strStream << "\"fulltext\":" << (isFulltext ? "true" : "false") << ",";
		strStream << "\"collate_mode\":"
				  << "\"" << index.CollateMode() << "\""
				  << ",";

		strStream << "\"conditions\": [";
		auto conds = index.Conds();
		unsigned condsLength = conds.size();
		for (unsigned j = 0; j < condsLength; j++) {
			strStream << "\"" << conds.at(j) << "\"";

			if (j < condsLength - 1) {
				strStream << ",";
			}
		}
		strStream << "]";

		strStream << "}";
		if (i < indexesLength - 1) {
			strStream << ",";
		}
	}
	strStream << "],";
	strStream << "\"storage_enabled\":" << (ns_->dbpath_.length() ? "true" : "false") << ",";
	strStream << "\"storage_ok\":" << (ns_->storage_ != nullptr ? "true" : "false") << ",";

	string storagePath = ns_->dbpath_.length() ? ns_->dbpath_ + '/' : "";
	strStream << "\"storage_path\":"
			  << "\"" << storagePath << "\",";

	strStream << "\"items_count\":" << ns_->items_.size() - ns_->free_.size();
	strStream << "}";

	string json = strStream.str();

	int status = jsonParse(const_cast<char *>(json.c_str()), &endptr, &jsonValue, jsonAllocator);

	if (status != JSON_OK) {
		throw Error(errLogic, "JSON parse: %d - %s\n", status, jsonStrError(status));
	}

	Payload payload(payloadType, payloadValue);

	WrSerializer wrSer;
	JsonDecoder decoder(result.getTagsMatcher(0));

	auto err = decoder.Decode(&payload, wrSer, jsonValue);
	if (!err.ok()) {
		throw Error(errLogic, "JSON decode: %s\n", err.what().c_str());
	}

	auto tupleData_ = make_key_string(reinterpret_cast<const char *>(wrSer.Buf()), wrSer.Len());
	KeyRefs kr;

	kr.push_back(KeyRef(tupleData_));
	payload.Set(0, kr);

	result.Add({static_cast<int>(result.size()), 0, payloadValue, 0, 0});
}
}  // namespace reindexer
