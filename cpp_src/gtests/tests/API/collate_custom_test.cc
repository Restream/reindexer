#include "collate_custom_mode_api.h"

const std::vector<std::string_view> sourceTable = {
	reinterpret_cast<const char*>(u8"Вася"),	 reinterpret_cast<const char*>(u8"Johny"),	  reinterpret_cast<const char*>(u8"Mary"),
	reinterpret_cast<const char*>(u8"Иван"),	 reinterpret_cast<const char*>(u8"Петр"),	  reinterpret_cast<const char*>(u8"Emmarose"),
	reinterpret_cast<const char*>(u8"Gabriela"), reinterpret_cast<const char*>(u8"Антон"),	  reinterpret_cast<const char*>(u8"1й Петр"),
	reinterpret_cast<const char*>(u8"2й Петр"),	 reinterpret_cast<const char*>(u8"3й Петр"),  reinterpret_cast<const char*>(u8"Maxwell"),
	reinterpret_cast<const char*>(u8"Anthony"),	 reinterpret_cast<const char*>(u8"1й Павел"), reinterpret_cast<const char*>(u8"Jane"),
	reinterpret_cast<const char*>(u8"2й Павел"), reinterpret_cast<const char*>(u8"3й Павел")};

// clang-format off
const std::vector<std::string_view> cyrillicNames = {
   reinterpret_cast<const char*>(u8"Антон"),
	reinterpret_cast<const char*>(u8"Вася"),
	reinterpret_cast<const char*>(u8"Иван"),
	reinterpret_cast<const char*>(u8"Петр"),
};

const std::vector<std::string_view> numericNames = {
	reinterpret_cast<const char*>(u8"1й Павел"),
	reinterpret_cast<const char*>(u8"1й Петр"),
	reinterpret_cast<const char*>(u8"2й Павел"),
	reinterpret_cast<const char*>(u8"2й Петр"),
	reinterpret_cast<const char*>(u8"3й Павел"),
	reinterpret_cast<const char*>(u8"3й Петр"),
};

const std::vector<std::string_view> ansiNames = {
	reinterpret_cast<const char*>(u8"Anthony"),
	reinterpret_cast<const char*>(u8"Emmarose"),
	reinterpret_cast<const char*>(u8"Gabriela"),
	reinterpret_cast<const char*>(u8"Jane"),
	reinterpret_cast<const char*>(u8"Johny"),
	reinterpret_cast<const char*>(u8"Mary"),
	reinterpret_cast<const char*>(u8"Maxwell"),
};
// clang-format on

TEST_F(CollateCustomModeAPI, CollateCustomTest1) {
	PrepareNs(rt.reindexer, default_namespace, reinterpret_cast<const char*>(u8"А-Я0-9A-Z"), sourceTable);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	std::vector<std::string_view> sortedTable;
	sortedTable.insert(sortedTable.end(), cyrillicNames.begin(), cyrillicNames.end());
	sortedTable.insert(sortedTable.end(), numericNames.begin(), numericNames.end());
	sortedTable.insert(sortedTable.end(), ansiNames.begin(), ansiNames.end());

	CompareResults(qr, sortedTable);
}

TEST_F(CollateCustomModeAPI, CollateCustomTest2) {
	PrepareNs(rt.reindexer, default_namespace, reinterpret_cast<const char*>(u8"A-ZА-Я0-9"), sourceTable);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	std::vector<std::string_view> sortedTable;
	sortedTable.insert(sortedTable.end(), ansiNames.begin(), ansiNames.end());
	sortedTable.insert(sortedTable.end(), cyrillicNames.begin(), cyrillicNames.end());
	sortedTable.insert(sortedTable.end(), numericNames.begin(), numericNames.end());

	CompareResults(qr, sortedTable);
}

TEST_F(CollateCustomModeAPI, CollateCustomTest3) {
	PrepareNs(rt.reindexer, default_namespace, reinterpret_cast<const char*>(u8"0-9A-ZА-Я"), sourceTable);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	std::vector<std::string_view> sortedTable;
	sortedTable.insert(sortedTable.end(), numericNames.begin(), numericNames.end());
	sortedTable.insert(sortedTable.end(), ansiNames.begin(), ansiNames.end());
	sortedTable.insert(sortedTable.end(), cyrillicNames.begin(), cyrillicNames.end());

	CompareResults(qr, sortedTable);
}

TEST_F(CollateCustomModeAPI, CollateCustomTest4) {
	const std::vector<std::string_view> sourceData = {reinterpret_cast<const char*>(u8"вампир"),
													  reinterpret_cast<const char*>(u8"Johny"),
													  reinterpret_cast<const char*>(u8"яблоко"),
													  reinterpret_cast<const char*>(u8"Carrick Michael"),
													  reinterpret_cast<const char*>(u8"Валенсия"),
													  reinterpret_cast<const char*>(u8"Петрозаводск"),
													  reinterpret_cast<const char*>(u8"jumper"),
													  reinterpret_cast<const char*>(u8"carry on, please"),
													  reinterpret_cast<const char*>(u8"петля"),
													  reinterpret_cast<const char*>(u8"1й Петр"),
													  reinterpret_cast<const char*>(u8"2й Петр"),
													  reinterpret_cast<const char*>(u8"электричка"),
													  reinterpret_cast<const char*>(u8"йод"),
													  reinterpret_cast<const char*>(u8"3й Петр"),
													  reinterpret_cast<const char*>(u8"Minnesota Timberwolves"),
													  reinterpret_cast<const char*>(u8"1й Павел"),
													  reinterpret_cast<const char*>(u8"mindblowing shit!"),
													  reinterpret_cast<const char*>(u8"Арсенал Лондон"),
													  reinterpret_cast<const char*>(u8"Houston Rockets"),
													  reinterpret_cast<const char*>(u8"ананас"),
													  reinterpret_cast<const char*>(u8"ёжик"),
													  reinterpret_cast<const char*>(u8"Ёрохин"),
													  reinterpret_cast<const char*>(u8"2й Павел"),
													  reinterpret_cast<const char*>(u8"чоткий парень"),
													  reinterpret_cast<const char*>(u8"3й Павел"),
													  reinterpret_cast<const char*>(u8"Элтон Джон"),
													  reinterpret_cast<const char*>(u8"humble person"),
													  reinterpret_cast<const char*>(u8"Чебоксары"),
													  reinterpret_cast<const char*>(u8"Я даже и не знаю, почему это все работает")};

	const std::vector<std::string_view> correctlyOrderedData = {
		reinterpret_cast<const char*>(u8"Арсенал Лондон"),
		reinterpret_cast<const char*>(u8"ананас"),
		reinterpret_cast<const char*>(u8"Валенсия"),
		reinterpret_cast<const char*>(u8"вампир"),
		reinterpret_cast<const char*>(u8"Петрозаводск"),
		reinterpret_cast<const char*>(u8"петля"),
		reinterpret_cast<const char*>(u8"Чебоксары"),
		reinterpret_cast<const char*>(u8"чоткий парень"),
		reinterpret_cast<const char*>(u8"Элтон Джон"),
		reinterpret_cast<const char*>(u8"Я даже и не знаю, почему это все работает"),
		reinterpret_cast<const char*>(u8"Carrick Michael"),
		reinterpret_cast<const char*>(u8"carry on, please"),
		reinterpret_cast<const char*>(u8"Houston Rockets"),
		reinterpret_cast<const char*>(u8"humble person"),
		reinterpret_cast<const char*>(u8"Johny"),
		reinterpret_cast<const char*>(u8"jumper"),
		reinterpret_cast<const char*>(u8"Minnesota Timberwolves"),
		reinterpret_cast<const char*>(u8"mindblowing shit!"),
		reinterpret_cast<const char*>(u8"1й Павел"),
		reinterpret_cast<const char*>(u8"1й Петр"),
		reinterpret_cast<const char*>(u8"2й Павел"),
		reinterpret_cast<const char*>(u8"2й Петр"),
		reinterpret_cast<const char*>(u8"3й Павел"),
		reinterpret_cast<const char*>(u8"3й Петр"),
		reinterpret_cast<const char*>(u8"Ёрохин"),
		reinterpret_cast<const char*>(u8"ёжик"),
		reinterpret_cast<const char*>(u8"йод"),
		reinterpret_cast<const char*>(u8"электричка"),
		reinterpret_cast<const char*>(u8"яблоко")};

	PrepareNs(
		rt.reindexer, default_namespace,
		reinterpret_cast<const char*>(
			u8"АаБбВвГгДдЕеЖжЗзИиКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭ-ЯAaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0-9ЁёЙйэ-я"),
		sourceData);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	CompareResults(qr, correctlyOrderedData);
}
