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
	NamespaceDef nsDef = ns_->GetDefinition();

	Namespace::RLock rlock(ns_->mtx_);

	if (result.ctxs.size() == 0) {
		payloadType = PayloadType(nsDef.name);
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
	strStream << "\"name\":\"" << nsDef.name << "\",";

	string updated = ns_->GetMeta("updated");
	if (updated.empty()) {
		updated = "0";
	}

	strStream << "\"updated_unix_nano\":" << updated << ",";
	strStream << "\"indexes\":[";
	strStream << std::boolalpha;

	for (unsigned i = 0; i < nsDef.indexes.size(); i++) {
		auto &index = nsDef.indexes[i];

		IndexType type = index.Type();

		if (i != 0) strStream << ",";

		strStream << "{";
		strStream << "\"name\":\"" << index.name << "\",";
		strStream << "\"field_type\":\"" << index.fieldType << "\",";
		strStream << "\"is_array\":" << index.opts.IsArray() << ",";
		strStream << "\"sortable\":" << isSortable(type) << ",";
		strStream << "\"pk\":" << index.opts.IsPK() << ",";
		strStream << "\"fulltext\":" << isFullText(type) << ",";
		strStream << "\"collate_mode\":\"" << index.getCollateMode() << "\",";

		strStream << "\"conditions\": [";
		auto conds = index.Conditions();
		for (unsigned j = 0; j < conds.size(); j++) {
			if (j != 0) strStream << ",";
			strStream << "\"" << conds.at(j) << "\"";
		}
		strStream << "]}";
	}
	strStream << "],";
	strStream << "\"storage_enabled\":" << nsDef.storage.IsEnabled() << ",";
	strStream << "\"storage_ok\":" << bool(ns_->storage_ != nullptr) << ",";
	strStream << "\"storage_path\":\"" << ns_->dbpath_ << "\",";
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
