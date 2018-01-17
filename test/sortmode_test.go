package reindexer

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
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

var testSortDataNumeric = "test_sort_data_numeric"
var testSortDataAscii = "test_sort_data_ascii"
var testSortDataUtf = "test_sort_data_utf8"
var testSortDataAsciiHash = "test_sort_data_ascii_hash"

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

func init() {
	tnamespaces[testSortDataNumeric] = TestSortModeNumericItem{}
	tnamespaces[testSortDataAscii] = TestSortModeAsciiItem{}
	tnamespaces[testSortDataUtf] = TestSortModeUtfItem{}
	tnamespaces[testSortDataAsciiHash] = TestSortModeAsciiItemHash{}
}

func FillTestItemsWithInsensitiveIndex() {
	tx := newTestTx(DB, testSortDataNumeric)
	for _, item := range testSortModeDataNumeric {
		if cnt, err := tx.Insert(item); err != nil {
			panic(err)
		} else if cnt == 0 {
			panic(fmt.Errorf("Could not insert item: %+v", *item))
		}
	}
	tx.MustCommit(nil)

	tx = newTestTx(DB, testSortDataAscii)
	for _, item := range testSortModeDataAscii {
		if cnt, err := tx.Insert(item); err != nil {
			panic(err)
		} else if cnt == 0 {
			panic(fmt.Errorf("Could not insert item: %+v", *item))
		}
	}
	tx.MustCommit(nil)

	tx = newTestTx(DB, testSortDataUtf)
	for _, item := range testSortModeDataUtf {
		if cnt, err := tx.Insert(item); err != nil {
			panic(err)
		} else if cnt == 0 {
			panic(fmt.Errorf("Could not insert item: %+v", *item))
		}
	}
	tx.MustCommit(nil)

	tx = newTestTx(DB, testSortDataAsciiHash)
	for _, item := range testSortModeDataAsciiHash {
		if cnt, err := tx.Insert(item); err != nil {
			panic(err)
		} else if cnt == 0 {
			panic(fmt.Errorf("Could not insert item: %+v", *item))
		}
	}
	tx.MustCommit(nil)
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

func TestSortDataIndexMode(t *testing.T) {
	FillTestItemsWithInsensitiveIndex()

	// Numeric
	results, err := DB.Query(testSortDataNumeric).Sort("item_numeric", false).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// Test: Numeric words are sorted
	var nums []int
	for i := 0; i < len(results); i++ {
		word := results[i].(*TestSortModeNumericItem).InsItem
		num, _ := StrToInt(word)
		nums = append(nums, num)
	}

	for i := 0; i < len(nums); i += 2 {
		if nums[i] > nums[i+1] {
			panic(fmt.Errorf("Numeric collate doesn't provide sorted results. Expected %i <= %i", nums[i], nums[i+1]))
		}
	}

	// ASCII
	results, err = DB.Query(testSortDataAscii).Sort("item_ascii", false).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// Test: ASCII words are sorted
	for i := 0; i < len(results); i += 2 {
		lword := results[i].(*TestSortModeAsciiItem).InsItem
		rword := results[i+1].(*TestSortModeAsciiItem).InsItem
		if strings.ToLower(lword) != strings.ToLower(rword) {
			panic(fmt.Errorf("Expected words %s and %s are the same", lword, rword))
		}
	}

	// UTF8
	results, err = DB.Query(testSortDataUtf).Sort("item_utf", false).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	// Test: UTF8 words are sorted
	for i := 0; i < len(results); i += 2 {
		lword := results[i].(*TestSortModeUtfItem).InsItem
		rword := results[i+1].(*TestSortModeUtfItem).InsItem
		if strings.ToLower(lword) != strings.ToLower(rword) {
			panic(fmt.Errorf("Expected words %s and %s are the same", lword, rword))
		}
	}
}
