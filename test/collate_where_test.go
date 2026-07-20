package reindexer

import (
	"strings"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCollateWhereNumericItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"collate_where_numeric,hash,collate_numeric"`
}

type TestCollateWhereAsciiItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"collate_where_ascii,tree,collate_ascii"`
}

type TestCollateWhereUtfItem struct {
	ID      int    `reindex:"id,,pk"`
	InsItem string `reindex:"collate_where_utf,-,collate_utf8"`
}

const (
	testCollateWhereNumericNs = "test_collate_where_numeric"
	testCollateWhereAsciiNs   = "test_collate_where_ascii"
	testCollateWhereUtfNs     = "test_collate_where_utf8"
)

func init() {
	tnamespaces[testCollateWhereNumericNs] = TestCollateWhereNumericItem{}
	tnamespaces[testCollateWhereAsciiNs] = TestCollateWhereAsciiItem{}
	tnamespaces[testCollateWhereUtfNs] = TestCollateWhereUtfItem{}
}

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
	{11, "12apple1"},
	{12, "  +12cherry"},
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

func fillTestCollateWhereItems(t *testing.T) {
	tx := newTestTx(DB, testCollateWhereNumericNs)
	for _, item := range testCollateWhereNumericData {
		if err := tx.Insert(item); err != nil {
			panic(err)
		}
	}
	cnt := tx.MustCommit()
	require.Equal(t, cnt, len(testCollateWhereNumericData))

	tx = newTestTx(DB, testCollateWhereAsciiNs)
	for _, item := range testCollateWhereAsciiData {
		if err := tx.Insert(item); err != nil {
			panic(err)
		}
	}
	cnt = tx.MustCommit()
	require.Equal(t, cnt, len(testCollateWhereAsciiData))

	tx = newTestTx(DB, testCollateWhereUtfNs)
	for _, item := range testCollateWhereUtfData {
		if err := tx.Insert(item); err != nil {
			panic(err)
		}
	}
	cnt = tx.MustCommit()
	require.Equal(t, cnt, len(testCollateWhereUtfData))
}

func CollateEq(t *testing.T) {
	t.Run("Equality with ASCII collate mode", func(t *testing.T) {
		keyWord := "apRICot"
		results, err := DB.Query(testCollateWhereAsciiNs).Where("COLLATE_WHERE_ASCII", reindexer.EQ, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)
		assert.Greater(t, len(results), 0, "No results of '%s' are found", keyWord)

		expectedItems := make(map[int]*TestCollateWhereAsciiItem)
		for _, element := range testCollateWhereAsciiData {
			word := element.InsItem
			if strings.EqualFold(word, keyWord) {
				expectedItems[element.ID] = element
			}
		}

		for i := 0; i < len(results); i++ {
			item := results[i].(*TestCollateWhereAsciiItem)
			expectedItem, ok := expectedItems[item.ID]
			assert.True(t, ok, "Unexpected item: %v", item)
			assert.Equal(t, expectedItem.InsItem, item.InsItem)
			delete(expectedItems, item.ID)
		}
		assert.Equal(t, len(expectedItems), 0, "Items were not found: %v", expectedItems)
	})

	t.Run("Equality with UTF collate mode", func(t *testing.T) {
		keyWord := "ВИшнЯ"
		results, err := DB.Query(testCollateWhereUtfNs).Where("COLLATE_WHERE_UTF", reindexer.EQ, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)
		assert.Greater(t, len(results), 0, "No results of '%s' are found", keyWord)

		expectedItems := make(map[int]*TestCollateWhereUtfItem)
		for _, element := range testCollateWhereUtfData {
			word := element.InsItem
			if strings.EqualFold(word, keyWord) {
				expectedItems[element.ID] = element
			}
		}

		for i := 0; i < len(results); i++ {
			item := results[i].(*TestCollateWhereUtfItem)
			expectedItem, ok := expectedItems[item.ID]
			assert.True(t, ok, "Unexpected item: %v", item)
			assert.Equal(t, expectedItem.InsItem, item.InsItem)
			delete(expectedItems, item.ID)
		}
		assert.Equal(t, len(expectedItems), 0, "Items were not found: %v", expectedItems)
	})

	t.Run("Equality with Numeric collate mode", func(t *testing.T) {
		keyWord := "12cherry"
		results, err := DB.Query(testCollateWhereNumericNs).Where("COLLATE_WHERE_NUMERIC", reindexer.EQ, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)
		assert.Greater(t, len(results), 0, "No results of '%s' are found", keyWord)

		expectedItems := make(map[int]*TestCollateWhereNumericItem)
		for _, element := range testCollateWhereNumericData {
			word := element.InsItem
			if word == "12cherry" || word == "  +12cherry" {
				expectedItems[element.ID] = element
			}
		}

		for i := 0; i < len(results); i++ {
			item := results[i].(*TestCollateWhereNumericItem)
			expectedItem, ok := expectedItems[item.ID]
			assert.True(t, ok, "Unexpected item: %v", item)
			assert.Equal(t, expectedItem.InsItem, item.InsItem)
			delete(expectedItems, item.ID)
		}
		assert.Equal(t, len(expectedItems), 0, "Items were not found: %v", expectedItems)
	})
}

func CollateLt(t *testing.T) {
	t.Run("Less with ASCII collate mode", func(t *testing.T) {
		keyWord := "aPRIcot"
		results, err := DB.Query(testCollateWhereAsciiNs).Where("collate_where_ascii", reindexer.LT, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// two words apple
		assert.Equal(t, len(results), 2)
	})

	t.Run("Less with UTF collate mode", func(t *testing.T) {
		keyWord := "АпельсиН"
		results, err := DB.Query(testCollateWhereUtfNs).Where("collate_where_utf", reindexer.LT, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// two words ананас
		assert.Equal(t, len(results), 2)
	})

	t.Run("Less with Numeric collate mode", func(t *testing.T) { // Numeric
		keyWord := "-5apple"
		results, err := DB.Query(testCollateWhereNumericNs).Where("collate_where_numeric", reindexer.LT, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// two words less than -5apple
		assert.Equal(t, len(results), 2)
	})
}

func CollateLe(t *testing.T) {
	t.Run("Less or equal with ASCII collate mode", func(t *testing.T) {
		keyWord := "aPRIcot"
		results, err := DB.Query(testCollateWhereAsciiNs).Where("collate_where_ascii", reindexer.LE, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// two words apple and three words apricot
		assert.Equal(t, len(results), 5)
	})

	t.Run("Less or equal with UTF collate mode", func(t *testing.T) {
		keyWord := "АпельсиН"
		results, err := DB.Query(testCollateWhereUtfNs).Where("collate_where_utf", reindexer.LE, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// two words ананас and two words апельсин
		assert.Equal(t, len(results), 4)
	})

	t.Run("Less or equal with Numeric collate mode", func(t *testing.T) {
		keyWord := "-5apple"
		results, err := DB.Query(testCollateWhereNumericNs).Where("collate_where_numeric", reindexer.LE, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// three words less or equal -5apple
		assert.Equal(t, len(results), 3)
	})
}

func CollateGt(t *testing.T) {
	t.Run("Greater with ASCII collate mode", func(t *testing.T) {
		keyWord := "coURgette"
		results, err := DB.Query(testCollateWhereAsciiNs).Where("collate_where_ascii", reindexer.GT, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// one word Xerox
		assert.Equal(t, len(results), 1)
	})

	t.Run("Greater with UTF collate mode", func(t *testing.T) {
		keyWord := "вишня"
		results, err := DB.Query(testCollateWhereUtfNs).Where("collate_where_utf", reindexer.GT, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// two words яблоко
		assert.Equal(t, len(results), 2)
	})

	t.Run("Greater with Numeric collate mode", func(t *testing.T) {
		keyWord := "52яблоко"
		results, err := DB.Query(testCollateWhereNumericNs).Where("collate_where_numeric", reindexer.GT, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// two words 100АнАнАс and 78яблоко greater than 52яблоко
		assert.Equal(t, len(results), 2)
	})
}

func CollateGe(t *testing.T) {
	t.Run("Greater or equal with ASCII collate mode", func(t *testing.T) {
		keyWord := "coURgette"
		results, err := DB.Query(testCollateWhereAsciiNs).Where("collate_where_ascii", reindexer.GE, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// one word Xerox and two Courgette
		assert.Equal(t, len(results), 3)
	})

	t.Run("Greater or equal with UTF collate mode", func(t *testing.T) {
		keyWord := "вишня"
		results, err := DB.Query(testCollateWhereUtfNs).Where("collate_where_utf", reindexer.GE, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// two words яблоко and two вишня
		assert.Equal(t, len(results), 4)
	})

	t.Run("Greater or equal with Numeric collate mode", func(t *testing.T) {
		keyWord := "52яблоко"
		results, err := DB.Query(testCollateWhereNumericNs).Where("collate_where_numeric", reindexer.GE, keyWord).Exec(t).FetchAll()
		require.NoError(t, err)

		// three words 52яблоко and 100АнАнАс and 78яблоко greater or equal 52яблоко
		assert.Equal(t, len(results), 3)
	})
}

func TestCollateWhere(t *testing.T) {
	fillTestCollateWhereItems(t)

	CollateEq(t)
	CollateLt(t)
	CollateLe(t)
	CollateGt(t)
	CollateGe(t)
}
