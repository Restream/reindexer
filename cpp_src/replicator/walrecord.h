#pragma once

#include <core/type_consts.h>
#include <functional>
#include <string>
#include "estl/h_vector.h"
#include "estl/span.h"
#include "estl/string_view.h"

namespace reindexer {

enum WALRecType {
	WalEmpty,
	WalReplState,
	WalItemUpdate,
	WalItemModify,
	WalIndexAdd,
	WalIndexDrop,
	WalIndexUpdate,
	WalPutMeta,
	WalUpdateQuery,
	WalNamespaceAdd,
	WalNamespaceDrop
};

class WrSerializer;
class JsonBuilder;

struct WALRecord {
	explicit WALRecord(span<uint8_t>);
	explicit WALRecord(string_view sv);
	explicit WALRecord(WALRecType _type = WalEmpty, IdType _id = 0) : type(_type), id(_id){};
	explicit WALRecord(WALRecType _type, string_view _data) : type(_type), data(_data) {}
	explicit WALRecord(WALRecType _type, string_view key, string_view value) : type(_type) { putMeta = {key, value}; }
	explicit WALRecord(WALRecType _type, string_view cjson, int tmVersion, int modifyMode) : type(_type) {
		itemModify = {cjson, tmVersion, modifyMode};
	}
	WrSerializer &Dump(WrSerializer &ser, std::function<std::string(string_view)> cjsonViewer) const;
	void GetJSON(JsonBuilder &jb, std::function<string(string_view)> cjsonViewer) const;
	WALRecType type;
	union {
		IdType id;
		string_view data;
		struct {
			string_view itemCJson;
			int tmVersion;
			int modifyMode;
		} itemModify;
		struct {
			string_view key;
			string_view value;
		} putMeta;
	};
};

struct PackedWALRecord : public h_vector<uint8_t, 12> {
	using h_vector<uint8_t, 12>::h_vector;
	void Pack(const WALRecord &rec);
};
}  // namespace reindexer
