#include <iostream>
#include <core/reindexer.h>

using namespace reindexer;

int main() {
	// Initialize database
	Reindexer db;
	// Create namespace and add index
	Error err = db.OpenNamespace("mytable");
	if (!err.ok()) return -1;
	err = db.AddIndex("mytable", {"id", "hash", "int", IndexOpts().PK()});
	if (!err.ok()) return -2;
	err = db.AddIndex("mytable", {"genre", "hash", "string", IndexOpts()});
	if (!err.ok()) return -3;

	//// Insert some data in JSON format
	Item item = db.NewItem("mytable");
	std::string data = "{\"id\":100,\"name\":\"Some name\" \"genre\":\"some genre\"}";
	err = item.FromJSON(data);
	if (!err.ok()) return -4;
	err = db.Upsert("mytable", item);
	if (!err.ok()) return -5;

	// Build & execute query
	auto query = Query("mytable").Where("id", CondEq, 100);
	QueryResults results;
	err = db.Select(query, results);
	if (!err.ok()) {
		std::cerr << "Select error" << err.what() << std::endl;
		return -6;
	}

	// Fetch and print results
	for (auto rowIt : results) {
		Item item = rowIt.GetItem();
		// Get complete JSON
		std::cout << "JSON: " << item.GetJSON() << std::endl;

		// OR Iterate indexed fields
		std::cout << "Fields: ";
		for (int field = 1; field < item.NumFields(); field++) {
			std::cout << item[field].Name() << "=" << item[field].As<std::string>() << "; ";
		}
		std::cout << std::endl;

		// OR Get indexed field by name
		std::cout << "Genre: " << item["genre"].As<std::string>() << std::endl;
	}
	return 0;
}

