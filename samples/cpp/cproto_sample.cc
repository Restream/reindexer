#include <iostream>
#include <client/reindexer.h>

using namespace reindexer::client;
using reindexer::Error;
using reindexer::Query;

int main() {
	//// Initialize database
	Reindexer db;
	db.Connect("cproto://127.0.0.1:6534/reindex", reindexer::client::ConnectOpts().CreateDBIfMissing());
	//// Create namespace and add index
	Error err = db.OpenNamespace("mytable");
	if (!err.ok()) return -1;
	err = db.AddIndex("mytable", {"id", "hash", "int", IndexOpts().PK()});
	if (!err.ok()) return -2;
	err = db.AddIndex("mytable", {"genre", "hash", "string", IndexOpts()});
	if (!err.ok()) return -3;

	//// Insert some data in JSON format
	Item item = db.NewItem("mytable");
	string data = "{\"id\":100,\"name\":\"Some name\" \"genre\":\"some genre\"}";
	item.FromJSON(data);
	err = db.Upsert("mytable", item);
	if (!err.ok()) return -4;

	//// Build & execute query
	auto query = Query("mytable").Where("id", CondEq, 100);
	QueryResults results;
	err = db.Select(query, results);
	if (!err.ok()) {
		std::cerr << "Select error" << err.what() << std::endl;
		return -5;
	}

	//  // Fetch and print results
	for (auto rowIt : results) {
	std::cout << __LINE__ << std::endl;
		//  // Get complete JSON
		reindexer::WrSerializer ser;
		err = rowIt.GetJSON(ser, false);
		std::cout << "JSON: " << ser.Slice() << std::endl;
	}
	return 0;
}

