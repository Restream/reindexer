#include "ttl_index.h"
#include <thread>

using namespace reindexer;

void TtlIndexFixture::RegisterAllCases() {
	Register("ItemsSimpleVanishing", &TtlIndexFixture::ItemsSimpleVanishing, this);
	Register("ItemsVanishingAfterInsertRemove", &TtlIndexFixture::ItemsVanishingAfterInsertRemove, this);
}

Error TtlIndexFixture::Initialize() {
	assert(db_);
	Error err = db_->AddNamespace(nsdef_);
	if (!err.ok()) return err;
	addDataToNs(20000);
	return 0;
}

void TtlIndexFixture::addDataToNs(size_t count) {
	for (size_t i = 0; i < count; ++i) {
		Item item = MakeItem();
		db_->Upsert(nsdef_.name, item);
	}

	Error err = db_->Commit(nsdef_.name);
	if (!err.ok()) std::cerr << err.what() << std::endl;
}

void TtlIndexFixture::removeAll() {
	QueryResults qr;
	db_->Delete(Query(nsdef_.name), qr);
}

void TtlIndexFixture::removeItems(int idFirst, int idLast) {
	if (idFirst < idLast) {
		QueryResults qr;
		db_->Delete(Query(nsdef_.name).Where("id", CondGe, Variant(idFirst)).Where("id", CondLe, Variant(idLast)), qr);
	}
}

Item TtlIndexFixture::MakeItem() {
	Item item = db_->NewItem(nsdef_.name);
	if (item.Status().ok()) {
		item["id"] = id_seq_->Next();
		item["data"] = random<int>(0, 5000);
		item["date"] = static_cast<int64_t>(time(nullptr));
	}
	return item;
}

size_t TtlIndexFixture::getItemsCount() {
	QueryResults qr;
	Error err = db_->Select(Query(nsdef_.name), qr);
	if (!err.ok()) std::cerr << err.what() << std::endl;
	return qr.Count();
}

void printTimestamp(const time_t rawtime) {
	struct tm* timeinfo;
	timeinfo = localtime(&rawtime);

	char buffer[80];
	strftime(buffer, 80, "%I:%M:%S%p", timeinfo);
	puts(buffer);
}

int TtlIndexFixture::waitForVanishing() {
	size_t count = getItemsCount();
	if (count > 0) {
		for (size_t i = 0; i < 10; ++i) {
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
			count = getItemsCount();
			if (count == 0) break;
		}
	}
	return count;
}

void TtlIndexFixture::ItemsSimpleVanishing(State& state) {
	std::this_thread::sleep_for(std::chrono::milliseconds(3000));
	size_t count = waitForVanishing();
	if (count != 0) {
		auto e = Error(errConflict, "Ns should be empty after timeout: %d", count);
		state.SkipWithError(e.what().c_str());
	}

	addDataToNs(5000);

	std::this_thread::sleep_for(std::chrono::milliseconds(3000));

	count = waitForVanishing();
	if (count != 0) {
		auto e = Error(errConflict, "Ttl of items doesn't work properly: %d", count);
		state.SkipWithError(e.what().c_str());
	}
}

void TtlIndexFixture::selectData() {
	for (size_t i = 0; i < 10; ++i) {
		int from = random<int>(1, 500);
		int till = random<int>(from + 1, 5000);
		QueryResults qr;
		db_->Select(Query(nsdef_.name).Where("id", CondGe, Variant(from)).Where("id", CondLe, Variant(till)), qr);
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	}
}

void TtlIndexFixture::removeItemsSlowly() {
	size_t idFirst = 0, idLast = 100;
	for (size_t i = 0; i < 10; ++i) {
		removeItems(idFirst, idLast);

		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		size_t offset = random<size_t>(1, 300);
		idFirst += offset;
		idLast += offset;
	}
}

void TtlIndexFixture::insertItemsSlowly() {
	for (size_t i = 0; i < 15; ++i) {
		addDataToNs(random<int>(10, 300));
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}

void TtlIndexFixture::ItemsVanishingAfterInsertRemove(State& state) {
	std::this_thread::sleep_for(std::chrono::milliseconds(3000));
	size_t count = waitForVanishing();
	if (count != 0) {
		auto e = Error(errConflict, "Ns should be empty after timeout: ", count);
		state.SkipWithError(e.what().c_str());
	}

	addDataToNs(3000);

	std::vector<std::thread> threads;
	threads.reserve(3);

	threads.emplace_back(std::thread(&TtlIndexFixture::insertItemsSlowly, this));
	threads.emplace_back(std::thread(&TtlIndexFixture::removeItemsSlowly, this));
	threads.emplace_back(std::thread(&TtlIndexFixture::selectData, this));

	for (size_t i = 0; i < threads.size(); ++i) threads[i].join();

	std::this_thread::sleep_for(std::chrono::milliseconds(3000));
	count = waitForVanishing();
	if (count != 0) {
		auto e = Error(errConflict, "There should be 0 items after ttl expired: %d", count);
		state.SkipWithError(e.what().c_str());
	}
}
