#include "collate_custom_mode_api.h"
#include "core/indexopts.h"

const std::vector<std::string> sourceTable = {u8"Вася",		u8"Johny",	  u8"Mary",	   u8"Иван",	 u8"Петр",	  u8"Emmarose",
											  u8"Gabriela", u8"Антон",	  u8"1й Петр", u8"2й Петр",	 u8"3й Петр", u8"Maxwell",
											  u8"Anthony",	u8"1й Павел", u8"Jane",	   u8"2й Павел", u8"3й Павел"};

// clang-format off
const std::vector<std::string> cyrillicNames = {
    u8"Антон",
    u8"Вася",
    u8"Иван",
    u8"Петр",
};

const std::vector<std::string> numericNames = {
    u8"1й Павел",
    u8"1й Петр",
    u8"2й Павел",
    u8"2й Петр",
    u8"3й Павел",
    u8"3й Петр",
};

const std::vector<std::string> ansiNames = {
    u8"Anthony",
    u8"Emmarose",
    u8"Gabriela",
    u8"Jane",
    u8"Johny",
    u8"Mary",
    u8"Maxwell",
};
// clang-format on

TEST_F(CollateCustomModeAPI, CollateCustomTest1) {
	PrepareNs(rt.reindexer, default_namespace, u8"А-Я0-9A-Z", sourceTable);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	std::vector<std::string> sortedTable;
	sortedTable.insert(sortedTable.end(), cyrillicNames.begin(), cyrillicNames.end());
	sortedTable.insert(sortedTable.end(), numericNames.begin(), numericNames.end());
	sortedTable.insert(sortedTable.end(), ansiNames.begin(), ansiNames.end());

	CompareResults(qr, sortedTable);
}

TEST_F(CollateCustomModeAPI, CollateCustomTest2) {
	PrepareNs(rt.reindexer, default_namespace, u8"A-ZА-Я0-9", sourceTable);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	std::vector<std::string> sortedTable;
	sortedTable.insert(sortedTable.end(), ansiNames.begin(), ansiNames.end());
	sortedTable.insert(sortedTable.end(), cyrillicNames.begin(), cyrillicNames.end());
	sortedTable.insert(sortedTable.end(), numericNames.begin(), numericNames.end());

	CompareResults(qr, sortedTable);
}

TEST_F(CollateCustomModeAPI, CollateCustomTest3) {
	PrepareNs(rt.reindexer, default_namespace, u8"0-9A-ZА-Я", sourceTable);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	std::vector<std::string> sortedTable;
	sortedTable.insert(sortedTable.end(), numericNames.begin(), numericNames.end());
	sortedTable.insert(sortedTable.end(), ansiNames.begin(), ansiNames.end());
	sortedTable.insert(sortedTable.end(), cyrillicNames.begin(), cyrillicNames.end());

	CompareResults(qr, sortedTable);
}

TEST_F(CollateCustomModeAPI, CollateCustomTest4) {
	const std::vector<std::string> sourceData = {u8"вампир",
												 u8"Johny",
												 u8"яблоко",
												 u8"Carrick Michael",
												 u8"Валенсия",
												 u8"Петрозаводск",
												 u8"jumper",
												 u8"carry on, please",
												 u8"петля",
												 u8"1й Петр",
												 u8"2й Петр",
												 u8"электричка",
												 u8"йод",
												 u8"3й Петр",
												 u8"Minnesota Timberwolves",
												 u8"1й Павел",
												 u8"mindblowing shit!",
												 u8"Арсенал Лондон",
												 u8"Houston Rockets",
												 u8"ананас",
												 u8"ёжик",
												 u8"Ёрохин",
												 u8"2й Павел",
												 u8"чоткий парень",
												 u8"3й Павел",
												 u8"Элтон Джон",
												 u8"humble person",
												 u8"Чебоксары",
												 u8"Я даже и не знаю, почему это все работает"};

	const std::vector<std::string> correctlyOrderedData = {u8"Арсенал Лондон",
														   u8"ананас",
														   u8"Валенсия",
														   u8"вампир",
														   u8"Петрозаводск",
														   u8"петля",
														   u8"Чебоксары",
														   u8"чоткий парень",
														   u8"Элтон Джон",
														   u8"Я даже и не знаю, почему это все работает",
														   u8"Carrick Michael",
														   u8"carry on, please",
														   u8"Houston Rockets",
														   u8"humble person",
														   u8"Johny",
														   u8"jumper",
														   u8"Minnesota Timberwolves",
														   u8"mindblowing shit!",
														   u8"1й Павел",
														   u8"1й Петр",
														   u8"2й Павел",
														   u8"2й Петр",
														   u8"3й Павел",
														   u8"3й Петр",
														   u8"Ёрохин",
														   u8"ёжик",
														   u8"йод",
														   u8"электричка",
														   u8"яблоко"};

	PrepareNs(rt.reindexer, default_namespace,
			  u8"АаБбВвГгДдЕеЖжЗзИиКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭ-ЯAaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0-9ЁёЙйэ-я",
			  sourceData);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	CompareResults(qr, correctlyOrderedData);
}
