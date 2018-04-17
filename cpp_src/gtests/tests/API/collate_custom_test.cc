#include "collate_custom_mode_api.h"
#include "core/indexopts.h"

using reindexer::Reindexer;

const vector<string> sourceTable = {u8"Вася",	 u8"Johny",	u8"Mary",	u8"Иван",	 u8"Петр",	u8"Emmarose",
									u8"Gabriela", u8"Антон",	u8"1й Петр", u8"2й Петр",  u8"3й Петр", u8"Maxwell",
									u8"Anthony",  u8"1й Павел", u8"Jane",	u8"2й Павел", u8"3й Павел"};

// clang-format off
const vector<string> cyrillicNames = {
    u8"Антон",
    u8"Вася",
    u8"Иван",
    u8"Петр",
};

const vector<string> numericNames = {
    u8"1й Павел",
    u8"1й Петр",
    u8"2й Павел",
    u8"2й Петр",
    u8"3й Павел",
    u8"3й Петр",
};

const vector<string> ansiNames = {
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
	PrepareNs(reindexer, default_namespace, u8"А-Я0-9A-Z", sourceTable);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	vector<string> sortedTable;
	sortedTable.insert(sortedTable.end(), cyrillicNames.begin(), cyrillicNames.end());
	sortedTable.insert(sortedTable.end(), numericNames.begin(), numericNames.end());
	sortedTable.insert(sortedTable.end(), ansiNames.begin(), ansiNames.end());

	CompareResults(qr, sortedTable);
}

TEST_F(CollateCustomModeAPI, CollateCustomTest2) {
	PrepareNs(reindexer, default_namespace, u8"A-ZА-Я0-9", sourceTable);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	vector<string> sortedTable;
	sortedTable.insert(sortedTable.end(), ansiNames.begin(), ansiNames.end());
	sortedTable.insert(sortedTable.end(), cyrillicNames.begin(), cyrillicNames.end());
	sortedTable.insert(sortedTable.end(), numericNames.begin(), numericNames.end());

	CompareResults(qr, sortedTable);
}

TEST_F(CollateCustomModeAPI, CollateCustomTest3) {
	PrepareNs(reindexer, default_namespace, u8"0-9A-ZА-Я", sourceTable);

	QueryResults qr;
	SortByName(qr);
	PrintQueryResults(default_namespace, qr);

	vector<string> sortedTable;
	sortedTable.insert(sortedTable.end(), numericNames.begin(), numericNames.end());
	sortedTable.insert(sortedTable.end(), ansiNames.begin(), ansiNames.end());
	sortedTable.insert(sortedTable.end(), cyrillicNames.begin(), cyrillicNames.end());

	CompareResults(qr, sortedTable);
}
