package reindexer

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestSortModeNumericItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"item_numeric,tree,collate_numeric"`
}

type TestSortModeAsciiItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"item_ascii,tree,collate_ascii"`
}

type TestSortModeUtfItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"item_utf,tree,collate_utf8"`
}

type TestSortModeAsciiItemHash struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"item_ascii_hash,hash,collate_ascii"`
}

type TestSortModeCustomItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"item_custom,hash,collate_custom=А-ЯA-Z0-9"`
}

const (
	testSortDataNumericNs   = "test_sort_data_numeric"
	testSortDataAsciiNs     = "test_sort_data_ascii"
	testSortDataUtfNs       = "test_sort_data_utf8"
	testSortDataAsciiHashNs = "test_sort_data_ascii_hash"
	testSortDataCustomNs    = "test_sort_data_custom"
)

func init() {
	tnamespaces[testSortDataNumericNs] = TestSortModeNumericItem{}
	tnamespaces[testSortDataAsciiNs] = TestSortModeAsciiItem{}
	tnamespaces[testSortDataUtfNs] = TestSortModeUtfItem{}
	tnamespaces[testSortDataAsciiHashNs] = TestSortModeAsciiItemHash{}
	tnamespaces[testSortDataCustomNs] = TestSortModeCustomItem{}
}

var testSortModeDataNumeric = []*TestSortModeNumericItem{
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

var testSortModeDataUtf = []*TestSortModeUtfItem{
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

var testSortModeDataAscii = []*TestSortModeAsciiItem{
	{1, "Apple"},
	{2, "apRICot"},
	{3, "courgette"},
	{4, "BanaNa"},
	{5, "Courgette"},
	{6, "cherry"},
	{7, "ApRIcoT"},
	{8, "bANana"},
	{9, "apple"},
	{10, "CHERRY"},
}

var testSortModeDataAsciiHash = []*TestSortModeAsciiItemHash{
	{1, "Apple"},
	{2, "apRICot"},
	{3, "courgette"},
	{4, "BanaNa"},
	{5, "Courgette"},
	{6, "cherry"},
	{7, "ApRIcoT"},
	{8, "bANana"},
	{9, "apple"},
	{10, "CHERRY"},
}

var testSortModeDataCustomSource = []*TestSortModeCustomItem{
	{1, "Вася"},
	{2, "Johny"},
	{3, "Mary"},
	{4, "Иван"},
	{5, "Петр"},
	{6, "Emmarose"},
	{7, "Gabriela"},
	{8, "Антон"},
	{9, "1й Петр"},
	{10, "2й Петр"},
	{11, "3й Петр"},
	{12, "Maxwell"},
	{13, "Anthony"},
	{14, "1й Павел"},
	{15, "Jane"},
	{16, "2й Павел"},
	{17, "3й Павел"},
}

var testSortModeDataCustomSorted = []*TestSortModeCustomItem{
	{1, "Антон"},
	{2, "Вася"},
	{3, "Иван"},
	{4, "Петр"},
	{5, "Anthony"},
	{6, "Emmarose"},
	{7, "Gabriela"},
	{8, "Jane"},
	{9, "Johny"},
	{10, "Mary"},
	{11, "Maxwell"},
	{12, "1й Павел"},
	{13, "1й Петр"},
	{14, "2й Павел"},
	{15, "2й Петр"},
	{16, "3й Павел"},
	{17, "3й Петр"},
}

func StrToInt(s string) (v int, hasValue bool) {
	negative := false
	if s[0] == '-' {
		negative = true
		s = s[1:len(s)]
	}

	offset := strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' })

	if offset == -1 {
		offset = len(s)
	}
	if offset == 0 {
		hasValue = false
		return
	}

	v, err := strconv.Atoi(s[:offset])
	if err == nil {
		if negative {
			v = -1 * v
		}
		hasValue = true
	}

	return
}

func FillTestItemsWithInsensitiveIndex(t *testing.T) {
	tx := newTestTx(DB, testSortDataNumericNs)
	for _, item := range testSortModeDataNumeric {
		assert.NoError(t, tx.Insert(item))
	}
	assert.Equal(t, tx.MustCommit(), len(testSortModeDataNumeric), "Could not commit testSortModeDataNumeric")

	tx = newTestTx(DB, testSortDataAsciiNs)
	for _, item := range testSortModeDataAscii {
		assert.NoError(t, tx.Insert(item))
	}
	assert.Equal(t, tx.MustCommit(), len(testSortModeDataAscii), "Could not commit testSortModeDataAscii")

	tx = newTestTx(DB, testSortDataUtfNs)
	for _, item := range testSortModeDataUtf {
		assert.NoError(t, tx.Insert(item))
	}
	assert.Equal(t, tx.MustCommit(), len(testSortModeDataUtf), "Could not commit testSortModeDataUtf")

	tx = newTestTx(DB, testSortDataAsciiHashNs)
	for _, item := range testSortModeDataAsciiHash {
		assert.NoError(t, tx.Insert(item))
	}
	assert.Equal(t, tx.MustCommit(), len(testSortModeDataAsciiHash), "Could not commit testSortModeDataAsciiHash")

	tx = newTestTx(DB, testSortDataCustomNs)
	for _, item := range testSortModeDataCustomSource {
		assert.NoError(t, tx.Insert(item))
	}
	assert.Equal(t, tx.MustCommit(), len(testSortModeDataCustomSource), "Could not commit testSortModeDataCustomSource")
}

func TestSortDataIndexMode(t *testing.T) {
	FillTestItemsWithInsensitiveIndex(t)

	// Numeric
	results, err := DB.Query(testSortDataNumericNs).Sort("item_numeric", false).Exec(t).FetchAll()
	assert.NoError(t, err)

	// Test: Numeric words are sorted
	var nums []int
	for i := 0; i < len(results); i++ {
		word := results[i].(*TestSortModeNumericItem).InsItem
		num, _ := StrToInt(word)
		nums = append(nums, num)
	}

	for i := 0; i < len(nums); i += 2 {
		assert.LessOrEqual(t, nums[i], nums[i+1], "Numeric collate doesn't provide sorted results. Expected %d <= %d", nums[i], nums[i+1])
	}

	// ASCII
	results, err = DB.Query(testSortDataAsciiNs).Sort("item_ascii", false).Exec(t).FetchAll()
	assert.NoError(t, err)

	// Test: ASCII words are sorted
	for i := 0; i < len(results); i += 2 {
		lword := results[i].(*TestSortModeAsciiItem).InsItem
		rword := results[i+1].(*TestSortModeAsciiItem).InsItem
		assert.Equal(t, strings.ToLower(lword), strings.ToLower(rword), "Expected words %s and %s are the same", lword, rword)
	}

	// UTF8
	results, err = DB.Query(testSortDataUtfNs).Sort("item_utf", false).Exec(t).FetchAll()
	assert.NoError(t, err)

	// Test: UTF8 words are sorted
	for i := 0; i < len(results); i += 2 {
		lword := results[i].(*TestSortModeUtfItem).InsItem
		rword := results[i+1].(*TestSortModeUtfItem).InsItem
		if strings.ToLower(lword) != strings.ToLower(rword) {
			panic(fmt.Errorf("Expected words %s and %s are the same", lword, rword))
		}
	}

	// Custom
	results, err = DB.Query(testSortDataCustomNs).Sort("item_custom", false).Exec(t).FetchAll()
	assert.NoError(t, err)

	// Test: Custom mode words are sorted
	assert.Equal(t, len(results), len(testSortModeDataCustomSorted), "Custom mode containers have different sizes")
	for i := 0; i < len(results); i++ {
		lword := results[i].(*TestSortModeCustomItem).InsItem
		rword := testSortModeDataCustomSorted[i].InsItem
		assert.Equal(t, strings.ToLower(lword), strings.ToLower(rword), "Expected words %s and %s are the same", lword, rword)
	}
}
