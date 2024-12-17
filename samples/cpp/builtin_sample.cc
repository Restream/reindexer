#include <core/reindexer.h>

using namespace reindexer;

int main() {
	// Initialize database
	Reindexer db;
	// Create DB's storage in '/tmp/reindex'
	Error err = db.Connect("builtin:///tmp/reindex");
	if (!err.ok()) {
		std::cerr << "Reindexer.Connect: " << err.what() << std::endl;
		return -1;
	}
	// Create namespace and add index
	err = db.OpenNamespace("mytable");
	if (!err.ok()) {
		std::cerr << "Reindexer.OpenNamespace: " << err.what() << std::endl;
		return -2;
	}
	err = db.AddIndex("mytable", {"id", "hash", "int", IndexOpts().PK()});
	if (!err.ok()) {
		std::cerr << "Reindexer.AddIndex id: " << err.what() << std::endl;
		return -3;
	}
	err = db.AddIndex("mytable", {"genre", "hash", "string", IndexOpts()});
	if (!err.ok()) {
		std::cerr << "Reindexer.AddIndex genre: " << err.what() << std::endl;
		return -4;
	}

	//// Insert some data in JSON format
	for (int i = 0; i < 5; ++i) {
		Item item = db.NewItem("mytable");
		std::string data = "{\"id\":" + std::to_string(i) + ",\"name\":\"Some name " + std::to_string(i) + "\", \"genre\":\"some genre " +
						   std::to_string(i) + "\"}";
		err = item.FromJSON(data);
		if (!err.ok()) {
			std::cerr << "Item.FromJSON error: " << err.what() << std::endl;
			return -5;
		}
		err = db.Upsert("mytable", item);
		if (!err.ok()) {
			std::cerr << "Reindexer.Upsert error: " << err.what() << std::endl;
			return -6;
		}
	}

	// Build & execute query
	auto query = Query("mytable").Where("id", CondEq, 0);
	QueryResults results;
	err = db.Select(query, results);
	if (!err.ok()) {
		std::cerr << "Select error" << err.what() << std::endl;
		return -7;
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

	query = Query("mytable").Aggregate(AggType::AggMin, {"id"}).Aggregate(AggType::AggMax, {"id"}).Aggregate(AggType::AggAvg, {"id"});

	QueryResults aggResults;
	err = db.Select(query, aggResults);
	if (!err.ok()) {
		std::cerr << "Select error" << err.what() << std::endl;
		return -8;
	}

	std::cout << "Aggregations: " << std::endl;
	for (auto& agg : aggResults.GetAggregationResults()) {
		reindexer::WrSerializer ser;
		agg.GetJSON(ser);
		std::cout << ser.Slice() << std::endl;
	}
	return 0;
}
