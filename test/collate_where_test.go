package reindexer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/restream/reindexer"
)

type TestCollateWhereNumericItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"collate_where_numeric,-,collate_numeric"`
}

type TestCollateWhereAsciiItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"collate_where_ascii,-,collate_ascii"`
}

type TestCollateWhereUtfItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"collate_where_utf,-,collate_utf8"`
}

var testCollateWhereNumeric = "test_collate_where_numeric"
var testCollateWhereAscii = "test_collate_where_ascii"
var testCollateWhereUtf = "test_collate_where_utf8"

var testCollateWhereNumericData = []*TestCollateWhereNumericItem{
	{1, "-99Apple"},
	{2, "52яблоко"},
	{3, "44аНаНаС"},
	{4, "-2BanaNa"},
	{5, "100АнАнАс"},
	{6, "12cherry"},
	{7, "78яблоко"},
	{8, "-99bANana"},
	{9, "-5apple"},
	{10, "0CHERRY"},
}

var testCollateWhereUtfData = []*TestCollateWhereUtfItem{
	{1, "Яблоко"},
	{2, "бАнаН"},
	{3, "Ананас"},
	{4, "Банан"},
	{5, "ананас"},
	{6, "вишня"},
	{7, "яблоко"},
	{8, "аПелЬсин"},
	{9, "Апельсин"},
	{10, "Вишня"},
}

var testCollateWhereAsciiData = []*TestCollateWhereAsciiItem{
	{1, "Apple"},
	{2, "apRICot"},
	{3, "courgette"},
	{4, "apRicoT"},
	{5, "Courgette"},
	{6, "cherry"},
	{7, "ApRIcoT"},
	{8, "bANana"},
	{9, "apple"},
	{10, "Xerox"},
}

func init() {
	tnamespaces[testCollateWhereNumeric] = TestCollateWhereNumericItem{}
	tnamespaces[testCollateWhereAscii] = TestCollateWhereAsciiItem{}
	tnamespaces[testCollateWhereUtf] = TestCollateWhereUtfItem{}
}

func FillTestCollateWhereItems() {
	tx := newTestTx(DB, testCollateWhereNumeric)
	for _, item := range testCollateWhereNumericData {
		if err := tx.Insert(item); err != nil {
			panic(err)
		}
	}
	cnt := tx.MustCommit()
	if cnt != len(testCollateWhereNumericData) {
		panic(fmt.Errorf("Could not commit testCollateWhereNumericData"))
	}

	tx = newTestTx(DB, testCollateWhereAscii)
	for _, item := range testCollateWhereAsciiData {
		if err := tx.Insert(item); err != nil {
			panic(err)
		}
	}
	cnt = tx.MustCommit()
	if cnt != len(testCollateWhereAsciiData) {
		panic(fmt.Errorf("Could not commit testCollateWhereAscii"))
	}
	tx = newTestTx(DB, testCollateWhereUtf)
	for _, item := range testCollateWhereUtfData {
		if err := tx.Insert(item); err != nil {
			panic(err)
		}
	}
	cnt = tx.MustCommit()
	if cnt != len(testCollateWhereUtfData) {
		panic(fmt.Errorf("Could not commit testCollateWhereUtf"))
	}
}

func CollateEq() {
	// ASCII
	keyWord := "apRICot"
	results, err := DB.Query(testCollateWhereAscii).Where("COLLATE_WHERE_ASCII", reindexer.EQ, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic(fmt.Errorf("No results of '%s' are found", keyWord))
	}

	wordsCountWritten := 0
	for _, element := range testCollateWhereAsciiData {
		word := element.InsItem
		if strings.EqualFold(word, keyWord) {
			wordsCountWritten++
		}
	}

	wordsCountReceived := 0
	for i := 0; i < len(results); i++ {
		word := results[i].(*TestCollateWhereAsciiItem).InsItem
		if strings.EqualFold(word, keyWord) {
			wordsCountReceived++
		}
	}

	if wordsCountWritten != wordsCountReceived {
		panic(fmt.Errorf("ASCII EQ gives wrong result"))
	}

	// UTF
	keyWord = "ВИшнЯ"
	results, err = DB.Query(testCollateWhereUtf).Where("COLLATE_WHERE_UTF", reindexer.EQ, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic(fmt.Errorf("No results of '%s' are found", keyWord))
	}

	wordsCountWritten = 0
	for _, element := range testCollateWhereUtfData {
		word := element.InsItem
		if strings.EqualFold(word, keyWord) {
			wordsCountWritten++
		}
	}

	wordsCountReceived = 0
	for i := 0; i < len(results); i++ {
		word := results[i].(*TestCollateWhereUtfItem).InsItem
		if strings.EqualFold(word, keyWord) {
			wordsCountReceived++
		}
	}

	if wordsCountWritten != wordsCountReceived {
		panic(fmt.Errorf("UTF EQ gives wrong result"))
	}

	// Numeric
	keyWord = "12cherry"
	results, err = DB.Query(testCollateWhereNumeric).Where("COLLATE_WHERE_NUMERIC", reindexer.EQ, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic(fmt.Errorf("No results of '%s' are found", keyWord))
	}

	wordsCountWritten = 0
	for _, element := range testCollateWhereNumericData {
		word := element.InsItem
		if strings.EqualFold(word, keyWord) {
			wordsCountWritten++
		}
	}

	wordsCountReceived = 0
	for i := 0; i < len(results); i++ {
		word := results[i].(*TestCollateWhereNumericItem).InsItem
		if strings.EqualFold(word, keyWord) {
			wordsCountReceived++
		}
	}

	if wordsCountWritten != wordsCountReceived {
		panic(fmt.Errorf("UTF EQ gives wrong result"))
	}
}

func CollateLt() {
	// ASCII
	keyWord := "aPRIcot"
	results, err := DB.Query(testCollateWhereAscii).Where("collate_where_ascii", reindexer.LT, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}
	// two words apple
	if len(results) != 2 {
		panic(fmt.Errorf("ASCII LT gives wrong result %d, expected 2", len(results)))
	}

	// UTF
	keyWord = "АпельсиН"
	results, err = DB.Query(testCollateWhereUtf).Where("collate_where_utf", reindexer.LT, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// two words ананас
	if len(results) != 2 {
		panic(fmt.Errorf("UTF LT gives wrong result %d, expected 2", len(results)))
	}

	// Numeric
	keyWord = "-5apple"
	results, err = DB.Query(testCollateWhereNumeric).Where("collate_where_numeric", reindexer.LT, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// two words less than -5apple
	if len(results) != 2 {
		panic(fmt.Errorf("NUMERIC LT gives wrong result %d, expected 2", len(results)))
	}
}

func CollateLe() {
	// ASCII
	keyWord := "aPRIcot"
	results, err := DB.Query(testCollateWhereAscii).Where("collate_where_ascii", reindexer.LE, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// two words apple and three words apricot
	if len(results) != 5 {
		panic(fmt.Errorf("ASCII LE gives wrong result %d, expected 5", len(results)))
	}

	// UTF
	keyWord = "АпельсиН"
	results, err = DB.Query(testCollateWhereUtf).Where("collate_where_utf", reindexer.LE, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// two words ананас and two words апельсин
	if len(results) != 4 {
		panic(fmt.Errorf("UTF LE gives wrong result %d, expected 4", len(results)))
	}

	// Numeric
	keyWord = "-5apple"
	results, err = DB.Query(testCollateWhereNumeric).Where("collate_where_numeric", reindexer.LE, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// three words less or equal -5apple
	if len(results) != 3 {
		panic(fmt.Errorf("NUMERIC LE gives wrong result %d, expected 3", len(results)))
	}
}

func CollateGt() {
	// ASCII
	keyWord := "coURgette"
	results, err := DB.Query(testCollateWhereAscii).Where("collate_where_ascii", reindexer.GT, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// one word Xerox
	if len(results) != 1 {
		panic(fmt.Errorf("ASCII GT gives wrong result %d, expected 1", len(results)))
	}

	// UTF
	keyWord = "вишня"
	results, err = DB.Query(testCollateWhereUtf).Where("collate_where_utf", reindexer.GT, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// two words яблоко
	if len(results) != 2 {
		panic(fmt.Errorf("UTF GT gives wrong result %d, expected 2", len(results)))
	}

	// Numeric
	keyWord = "52яблоко"
	results, err = DB.Query(testCollateWhereNumeric).Where("collate_where_numeric", reindexer.GT, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// two words 100АнАнАс and 78яблоко greater than 52яблоко
	if len(results) != 2 {
		panic(fmt.Errorf("NUMERIC GT gives wrong result %d, expected 2", len(results)))
	}
}

func CollateGe() {
	// ASCII
	keyWord := "coURgette"
	results, err := DB.Query(testCollateWhereAscii).Where("collate_where_ascii", reindexer.GE, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// one word Xerox and two Courgette
	if len(results) != 3 {
		panic(fmt.Errorf("ASCII GT gives wrong result %d, expected 3", len(results)))
	}

	// UTF
	keyWord = "вишня"
	results, err = DB.Query(testCollateWhereUtf).Where("collate_where_utf", reindexer.GE, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// two words яблоко and two вишня
	if len(results) != 4 {
		panic(fmt.Errorf("UTF GT gives wrong result %d, expected 4", len(results)))
	}

	// Numeric
	keyWord = "52яблоко"
	results, err = DB.Query(testCollateWhereNumeric).Where("collate_where_numeric", reindexer.GE, keyWord).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// three words 52яблоко and 100АнАнАс and 78яблоко greater or equal 52яблоко
	if len(results) != 3 {
		panic(fmt.Errorf("NUMERIC GT gives wrong result %d, expected 3", len(results)))
	}
}

func TestCollateWhere(t *testing.T) {
	FillTestCollateWhereItems()

	CollateEq()
	CollateLt()
	CollateLe()
	CollateGt()
	CollateGe()
}
