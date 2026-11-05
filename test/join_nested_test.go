package reindexer

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

type NestedJoinBook struct {
	BookID     int                 `reindex:"bookid,,pk"`
	Title      string              `reindex:"title"`
	Pages      int                 `reindex:"pages,tree"`
	Price      int                 `reindex:"price,tree"`
	AuthorIDFK int                 `reindex:"authorid_fk"`
	Authors    []*NestedJoinAuthor `reindex:"authors,,joined"`
}

type NestedJoinAuthor struct {
	AuthorID     int                   `reindex:"authorid,,pk"`
	Name         string                `reindex:"name"`
	Age          int                   `reindex:"age,tree"`
	LocationIDFK int                   `reindex:"locationid_fk"`
	Locations1   []*NestedJoinLocation `reindex:"locations1,,joined"`
	Locations2   []*NestedJoinLocation `reindex:"locations2,,joined"`
}

type NestedJoinLocation struct {
	LocationID  int                  `reindex:"locationid,,pk"`
	CountryIDFK int                  `reindex:"countryid_fk"`
	Code        int                  `reindex:"code"`
	City        string               `reindex:"city"`
	Countries   []*NestedJoinCountry `reindex:"countries,,joined"`
}

type NestedJoinCountry struct {
	CountryID   int    `reindex:"countryid,,pk"`
	CountryName string `reindex:"country_name"`
	CountryCode int    `reindex:"country_code"`
}

type NestedJoinSelfItem struct {
	ID            int                   `reindex:"id,,pk"`
	ParentID      int                   `reindex:"parent_id,tree"`
	Name          string                `reindex:"name"`
	Children      []*NestedJoinSelfItem `reindex:"children,,joined"`
	GrandChildren []*NestedJoinSelfItem `reindex:"grandchildren,,joined"`
}

type NestedJoinLeftInnerItem1 struct {
	ID        int                         `reindex:"id,,pk" json:"id"`
	Age       int                         `reindex:"age" json:"age"`
	JoinedNs2 []*NestedJoinLeftInnerItem2 `reindex:"joined_ns2,,joined" json:"joined_ns2"`
}

type NestedJoinLeftInnerItem2 struct {
	ID        int                         `reindex:"id,,pk" json:"id"`
	ParentID  int                         `reindex:"parent_id,tree" json:"parent_id"`
	RefID     int                         `reindex:"ref_id,tree" json:"ref_id"`
	Age       int                         `reindex:"age" json:"age"`
	JoinedNs3 []*NestedJoinLeftInnerItem3 `reindex:"joined_ns3,,joined" json:"joined_ns3"`
}

type NestedJoinLeftInnerItem3 struct {
	ID  int `reindex:"id,,pk" json:"id"`
	Age int `reindex:"age" json:"age"`
}

const (
	nestedJoinBooksNs     = "nested_join_books"
	nestedJoinBooksNs2    = "nested_join_books_2"
	nestedJoinAuthorsNs   = "nested_join_authors"
	nestedJoinLocationsNs = "nested_join_locations"
	nestedJoinCountriesNs = "nested_join_countries"
	nestedJoinSelfNs      = "nested_join_self"

	nestedJoinLeftInnerNs1 = "nested_join_left_inner_1"
	nestedJoinLeftInnerNs2 = "nested_join_left_inner_2"
	nestedJoinLeftInnerNs3 = "nested_join_left_inner_3"
)

func init() {
	tnamespaces[nestedJoinBooksNs] = NestedJoinBook{}
	tnamespaces[nestedJoinBooksNs2] = NestedJoinBook{}
	tnamespaces[nestedJoinAuthorsNs] = NestedJoinAuthor{}
	tnamespaces[nestedJoinLocationsNs] = NestedJoinLocation{}
	tnamespaces[nestedJoinCountriesNs] = NestedJoinCountry{}
	tnamespaces[nestedJoinSelfNs] = NestedJoinSelfItem{}
	tnamespaces[nestedJoinLeftInnerNs1] = NestedJoinLeftInnerItem1{}
	tnamespaces[nestedJoinLeftInnerNs2] = NestedJoinLeftInnerItem2{}
	tnamespaces[nestedJoinLeftInnerNs3] = NestedJoinLeftInnerItem3{}
}

var (
	nestedJoinCities = []string{
		"Moscow", "Tambov", "Kazan", "Ulyanovsk", "Krasnodar",
		"Uryupinsk", "Minsk", "Kiev", "Berdiansk", "Novocherkassk",
		"Grozny", "Amsterdam", "Paris", "Berlin", "New York",
	}
	nestedJoinCountryNames = []string{
		"Northern Country", "Southern Country", "Western Country", "Eastern Country",
	}
	nestedJoinSetupOnce sync.Once
)

func FillNestedJoinCountries() {
	tx := newTestTx(DB, nestedJoinCountriesNs)
	for i, name := range nestedJoinCountryNames {
		err := tx.Upsert(&NestedJoinCountry{
			CountryID:   i,
			CountryName: name,
			CountryCode: i * 100,
		})
		if err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func FillNestedJoinLocations(count int) {
	tx := newTestTx(DB, nestedJoinLocationsNs)
	for i := 0; i < count; i++ {
		code := rand.Int() % 65536
		if i%7 == 3 {
			code = 13
		}
		err := tx.Upsert(&NestedJoinLocation{
			LocationID:  i,
			CountryIDFK: rand.Int() % len(nestedJoinCountryNames),
			Code:        code,
			City:        nestedJoinCities[rand.Int()%len(nestedJoinCities)],
		})
		if err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func FillNestedJoinAuthors(count int) {
	tx := newTestTx(DB, nestedJoinAuthorsNs)
	for i := 0; i < count; i++ {
		err := tx.Upsert(&NestedJoinAuthor{
			AuthorID:     i,
			Name:         "author_" + randString(),
			Age:          rand.Int()%80 + 20,
			LocationIDFK: rand.Int() % nestedJoinLocationsCount,
		})
		if err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func FillNestedJoinBooks(namespace string, namePrefix string, count int) {
	tx := newTestTx(DB, namespace)
	for i := 0; i < count; i++ {
		err := tx.Upsert(&NestedJoinBook{
			BookID:     i,
			Title:      fmt.Sprintf("%s_%s", namePrefix, randString()),
			Pages:      rand.Int() % 10000,
			Price:      rand.Int() % 10000,
			AuthorIDFK: rand.Int() % nestedJoinAuthorsCount,
		})
		if err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func FillNestedJoinSelf(count int) {
	tx := newTestTx(DB, nestedJoinSelfNs)
	for i := 0; i < count; i++ {
		parentID := -1
		if i > 0 {
			parentID = (i - 1) / 2
		}
		err := tx.Upsert(&NestedJoinSelfItem{
			ID:       i,
			ParentID: parentID,
			Name:     fmt.Sprintf("self_%d", i),
		})
		if err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func FillNestedJoinLeftInnerItems(t *testing.T) {
	t.Helper()
	require.NoError(t, DB.TruncateNamespace(nestedJoinLeftInnerNs1))
	require.NoError(t, DB.TruncateNamespace(nestedJoinLeftInnerNs2))
	require.NoError(t, DB.TruncateNamespace(nestedJoinLeftInnerNs3))

	for i := 0; i < 4; i++ {
		require.NoError(t, DB.Upsert(nestedJoinLeftInnerNs1, &NestedJoinLeftInnerItem1{ID: i, Age: i}))
	}
	for _, item := range []*NestedJoinLeftInnerItem2{
		{ID: 10, ParentID: 0, RefID: 100, Age: 10},
		{ID: 11, ParentID: 1, RefID: 101, Age: 11},
		{ID: 12, ParentID: 1, RefID: 102, Age: 12},
		{ID: 13, ParentID: 2, RefID: 103, Age: 13},
		{ID: 14, ParentID: 3, RefID: 104, Age: 11},
	} {
		require.NoError(t, DB.Upsert(nestedJoinLeftInnerNs2, item))
	}
	for _, item := range []*NestedJoinLeftInnerItem3{
		{ID: 101, Age: 101},
		{ID: 103, Age: 103},
	} {
		require.NoError(t, DB.Upsert(nestedJoinLeftInnerNs3, item))
	}
}

func FillNamespaces() {
	nestedJoinSetupOnce.Do(func() {
		if err := DB.OpenNamespace(nestedJoinCountriesNs, reindexer.DefaultNamespaceOptions().NoStorage(), NestedJoinCountry{}); err != nil {
			panic(err)
		}
		if err := DB.OpenNamespace(nestedJoinLocationsNs, reindexer.DefaultNamespaceOptions().NoStorage(), NestedJoinLocation{}); err != nil {
			panic(err)
		}
		if err := DB.OpenNamespace(nestedJoinAuthorsNs, reindexer.DefaultNamespaceOptions().NoStorage(), NestedJoinAuthor{}); err != nil {
			panic(err)
		}
		if err := DB.OpenNamespace(nestedJoinBooksNs, reindexer.DefaultNamespaceOptions().NoStorage(), NestedJoinBook{}); err != nil {
			panic(err)
		}
		if err := DB.OpenNamespace(nestedJoinBooksNs2, reindexer.DefaultNamespaceOptions().NoStorage(), NestedJoinBook{}); err != nil {
			panic(err)
		}
		if err := DB.OpenNamespace(nestedJoinSelfNs, reindexer.DefaultNamespaceOptions().NoStorage(), NestedJoinSelfItem{}); err != nil {
			panic(err)
		}

		FillNestedJoinCountries()
		FillNestedJoinLocations(nestedJoinLocationsCount)
		FillNestedJoinAuthors(nestedJoinAuthorsCount)
		FillNestedJoinBooks(nestedJoinBooksNs, "book", nestedJoinBooksCount)
		FillNestedJoinBooks(nestedJoinBooksNs2, "book2", nestedJoinBooksCount/2)
		FillNestedJoinSelf(nestedJoinSelfCount)
	})
}

const (
	nestedJoinCountriesCount = 4
	nestedJoinLocationsCount = 20
	nestedJoinAuthorsCount   = 10
	nestedJoinBooksCount     = 100
	nestedJoinSelfCount      = 50
)

func VerifyNestedJoinResults(t *testing.T, iter *reindexer.Iterator, expectedMinCount int, strictCountCheck bool) {
	t.Helper()

	if strictCountCheck {
		require.Equal(t, expectedMinCount, iter.Count(), "query result count mismatch")
	} else {
		require.GreaterOrEqual(t, iter.Count(), expectedMinCount,
			"query returned fewer results than expected minimum")
	}

	for iter.Next() {
		verifyNestedJoinBook(t, iter.Object().(*NestedJoinBook))
	}
}

func VerifyNestedJoinResultsNoCount(t *testing.T, iter *reindexer.Iterator) {
	t.Helper()
	for iter.Next() {
		verifyNestedJoinBook(t, iter.Object().(*NestedJoinBook))
	}
}

func verifyNestedJoinBook(t *testing.T, item *NestedJoinBook) {
	t.Helper()

	require.GreaterOrEqual(t, item.Price, 2, "book price must be >= 2")
	require.NotEmpty(t, item.Authors, "expected at least 1 author per book")

	for _, author := range item.Authors {
		require.Equal(t, item.AuthorIDFK, author.AuthorID,
			"author join condition failed: authorid_fk != authorid")

		require.NotEmpty(t, author.Locations1,
			"expected at least 1 location in first locations join")
		for _, loc := range author.Locations1 {
			require.Equal(t, author.LocationIDFK, loc.LocationID,
				"location join condition (1st) failed: locationid_fk != locationid")
			require.NotEqual(t, 13, loc.Code,
				"first locations join should exclude code=13 (NOT filter)")

			require.NotEmpty(t, loc.Countries,
				"expected at least 1 country in nested countries join")
			for _, cntry := range loc.Countries {
				require.Equal(t, loc.CountryIDFK, cntry.CountryID,
					"country join condition failed: countryid_fk != countryid")
				require.NotEmpty(t, cntry.CountryName, "country name should not be empty")
			}
		}

		require.NotEmpty(t, author.Locations2,
			"expected at least 1 location in second locations join")
		for _, loc := range author.Locations2 {
			require.Equal(t, author.LocationIDFK, loc.LocationID,
				"location join condition (2nd) failed: locationid_fk != locationid")
			require.GreaterOrEqual(t, loc.Code, 1,
				"second locations join should have code >= 1")
		}
	}
}

func VerifyJson(t *testing.T, jsonData []byte) {
	t.Helper()

	var result map[string]interface{}
	err := json.Unmarshal(jsonData, &result)
	require.NoError(t, err, "Response must be a valid JSON")

	require.Contains(t, result, "BookID")
	require.Contains(t, result, "Title")

	authors := mustJoinArrayByName(t, result, nestedJoinAuthorsNs)
	require.NotEmpty(t, authors)

	for _, a := range authors {
		author, ok := a.(map[string]interface{})
		require.True(t, ok)
		require.Contains(t, author, "AuthorID")
		require.Contains(t, author, "Name")

		locs1 := mustJoinArrayByNameAndAlias(t, author, nestedJoinLocationsNs, "joined_1")
		for _, l := range locs1 {
			loc, ok := l.(map[string]interface{})
			require.True(t, ok)
			require.Contains(t, loc, "LocationID")
			require.Contains(t, loc, "City")

			cntries := mustJoinArrayByName(t, loc, nestedJoinCountriesNs)
			require.NotEmpty(t, cntries)
			for _, c := range cntries {
				cntry, ok := c.(map[string]interface{})
				require.True(t, ok)
				require.Contains(t, cntry, "CountryID")
				require.Contains(t, cntry, "CountryName")
			}
		}

		locs2 := mustJoinArrayByNameAndAlias(t, author, nestedJoinLocationsNs, "joined_2")
		for _, l := range locs2 {
			loc, ok := l.(map[string]interface{})
			require.True(t, ok)
			require.Contains(t, loc, "LocationID")
			require.Contains(t, loc, "City")
		}
	}
}

func VerifyNestedLeftJoinResults(t *testing.T, iter *reindexer.Iterator) {
	t.Helper()
	for iter.Next() {
		item := iter.Object().(*NestedJoinBook)
		require.GreaterOrEqual(t, item.Price, 0)
		verifyLeftJoinedAuthors(t, item)
	}
}

func verifyLeftJoinedAuthors(t *testing.T, item *NestedJoinBook) {
	t.Helper()
	if len(item.Authors) == 0 {
		return
	}
	for _, author := range item.Authors {
		verifyLeftJoinedAuthorData(t, item.AuthorIDFK, author)
	}
}

func verifyLeftJoinedAuthorData(t *testing.T, bookAuthorIDFK int, author *NestedJoinAuthor) {
	t.Helper()
	require.NotNil(t, author)
	require.Equal(t, bookAuthorIDFK, author.AuthorID)
	require.Len(t, author.Locations2, 0)
	require.NotEmpty(t, author.Locations1)
	for _, loc := range author.Locations1 {
		require.NotNil(t, loc)
		require.Equal(t, author.LocationIDFK, loc.LocationID)
		require.GreaterOrEqual(t, loc.Code, 1)
		require.NotEmpty(t, loc.Countries)
		for _, cntry := range loc.Countries {
			require.NotNil(t, cntry)
			require.Equal(t, loc.CountryIDFK, cntry.CountryID)
			require.NotEmpty(t, cntry.CountryName)
		}
	}
}

func verifyInnerJoinedAuthorsWithEmptyLeftJoin(t *testing.T, item *NestedJoinBook) {
	t.Helper()
	require.NotNil(t, item)
	require.NotEmpty(t, item.Title)
	require.NotEmpty(t, item.Authors)
	for _, author := range item.Authors {
		require.NotNil(t, author)
		require.Equal(t, item.AuthorIDFK, author.AuthorID)
		require.NotEmpty(t, author.Name)
		require.Len(t, author.Locations1, 0)
		require.Len(t, author.Locations2, 0)
	}
}

func verifyNestedLeftWithInnerJoinJSON(t *testing.T, iter *reindexer.JSONIterator, expected map[int][]int) {
	t.Helper()
	jsonData, err := iter.FetchAll()
	require.NoError(t, err)

	var response map[string][]map[string]interface{}
	require.NoError(t, json.Unmarshal(jsonData, &response))
	items := response[nestedJoinLeftInnerNs1]
	require.Len(t, items, len(expected), string(jsonData))

	seen := make(map[int]struct{}, len(expected))
	for _, item := range items {
		id := toInt(t, item["id"])
		seen[id] = struct{}{}
		expectedNs2IDs, ok := expected[id]
		require.True(t, ok, "unexpected item id: %d", id)
		joinedNs2, _ := findJoinedArrayByNamespace(t, item, nestedJoinLeftInnerNs2)
		require.Len(t, joinedNs2, len(expectedNs2IDs), "unexpected joined_ns2 count for item id: %d", id)
		for i, rawNs2 := range joinedNs2 {
			ns2, ok := rawNs2.(map[string]interface{})
			require.True(t, ok)
			require.Equal(t, id, toInt(t, ns2["parent_id"]))
			require.Equal(t, expectedNs2IDs[i], toInt(t, ns2["id"]))
			refID := toInt(t, ns2["ref_id"])
			joinedNs3, _ := findJoinedArrayByNamespace(t, ns2, nestedJoinLeftInnerNs3)
			if refID == 104 {
				require.Empty(t, joinedNs3)
				continue
			}
			require.Len(t, joinedNs3, 1)
			ns3, ok := joinedNs3[0].(map[string]interface{})
			require.True(t, ok)
			require.Equal(t, refID, toInt(t, ns3["id"]))
		}
	}
	require.Len(t, seen, len(expected))
}

func VerifyLeftJoinJSONFields(t *testing.T, jsonData []byte) {
	t.Helper()

	var result map[string]interface{}
	err := json.Unmarshal(jsonData, &result)
	require.NoError(t, err, "Response must be valid JSON")

	require.Contains(t, result, "BookID")
	require.Contains(t, result, "Title")
	require.Contains(t, result, "AuthorIDFK")
	bookAuthorIDFK := toInt(t, result["AuthorIDFK"])

	authors, ok := findJoinedArrayByNamespace(t, result, nestedJoinAuthorsNs)
	require.True(t, ok, "expected joined authors field in JSON")

	for _, a := range authors {
		author, ok := a.(map[string]interface{})
		require.True(t, ok)
		require.Contains(t, author, "AuthorID")
		require.Contains(t, author, "Name")
		require.Equal(t, bookAuthorIDFK, toInt(t, author["AuthorID"]))

		locs1 := mustFindJoinedLocations1Array(t, author)
		require.Contains(t, author, "LocationIDFK")
		authorLocationIDFK := toInt(t, author["LocationIDFK"])
		for _, l := range locs1 {
			loc, ok := l.(map[string]interface{})
			require.True(t, ok)
			require.Contains(t, loc, "LocationID")
			require.Equal(t, authorLocationIDFK, toInt(t, loc["LocationID"]))
			require.Contains(t, loc, "Code")
			require.GreaterOrEqual(t, toInt(t, loc["Code"]), 1)
			require.Contains(t, loc, "CountryIDFK")
			locCountryIDFK := toInt(t, loc["CountryIDFK"])

			cntries := mustJoinArrayByName(t, loc, nestedJoinCountriesNs)
			require.NotEmpty(t, cntries)
			for _, c := range cntries {
				cntry, ok := c.(map[string]interface{})
				require.True(t, ok)
				require.Contains(t, cntry, "CountryID")
				require.Contains(t, cntry, "CountryName")
				require.Equal(t, locCountryIDFK, toInt(t, cntry["CountryID"]))
			}
		}
	}
}

func findJoinedArrayByNamespace(t *testing.T, m map[string]interface{}, nsName string) ([]interface{}, bool) {
	t.Helper()
	for key, value := range m {
		lowerKey := strings.ToLower(key)
		if strings.Contains(lowerKey, strings.ToLower(nsName)) {
			arr, ok := value.([]interface{})
			if !ok {
				return nil, false
			}
			return arr, true
		}
	}
	return nil, false
}

func mustFindJoinedLocations1Array(t *testing.T, author map[string]interface{}) []interface{} {
	t.Helper()
	keys := make([]string, 0, len(author))
	for key, value := range author {
		keys = append(keys, key)
		lowerKey := strings.ToLower(key)
		if strings.Contains(lowerKey, "locations1") || strings.Contains(lowerKey, "joined_1") || strings.Contains(lowerKey, strings.ToLower(nestedJoinLocationsNs)) {
			arr, ok := value.([]interface{})
			require.True(t, ok, "joined field %q should be an array", key)
			return arr
		}
	}
	require.FailNow(t, "joined locations1 field not found", "keys: %v", keys)
	return nil
}

func toInt(t *testing.T, v interface{}) int {
	t.Helper()
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	case json.Number:
		i, err := x.Int64()
		require.NoError(t, err)
		return int(i)
	default:
		require.FailNow(t, "expected numeric value")
		return 0
	}
}

func mustJoinArrayByName(t *testing.T, m map[string]interface{}, nsName string) []interface{} {
	t.Helper()
	for key, value := range m {
		if strings.Contains(strings.ToLower(key), strings.ToLower(nsName)) {
			arr, ok := value.([]interface{})
			require.True(t, ok, "joined field %q should be an array", key)
			return arr
		}
	}
	require.FailNow(t, "joined field not found", "namespace: %s", nsName)
	return nil
}

func mustJoinArrayByNameAndAlias(t *testing.T, m map[string]interface{}, nsName string, alias string) []interface{} {
	t.Helper()
	for key, value := range m {
		lowerKey := strings.ToLower(key)
		if strings.Contains(lowerKey, strings.ToLower(nsName)) && strings.Contains(lowerKey, strings.ToLower(alias)) {
			arr, ok := value.([]interface{})
			require.True(t, ok, "joined field %q should be an array", key)
			return arr
		}
	}
	require.FailNow(t, "joined field not found", "namespace: %s alias: %s", nsName, alias)
	return nil
}

func TestNestedJoinQueries(t *testing.T) {
	FillNamespaces()

	t.Run("inner join with nested join", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations1 := DB.Query(nestedJoinLocationsNs).
			Not().Where("code", reindexer.EQ, 13)
		qLocations1.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qLocations2 := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations1, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")
		qAuthors.InnerJoin(qLocations2, "locations2").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 2).
			Limit(50)
		qBooks.InnerJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		iter := qBooks.MustExec(t)
		defer iter.Close()
		VerifyNestedJoinResults(t, iter, 1, false)
	})

	t.Run("join handler verification", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations1 := DB.Query(nestedJoinLocationsNs).
			Not().Where("code", reindexer.EQ, 13)
		qLocations1.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qLocations2 := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations1, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")
		qAuthors.InnerJoin(qLocations2, "locations2").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 2).
			Limit(50)
		qBooks.InnerJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		handlerCalled := 0
		qBooks.JoinHandler("authors", func(field string, item interface{}, subitems []interface{}) bool {
			handlerCalled++
			book := item.(*NestedJoinBook)
			for _, subitem := range subitems {
				author := subitem.(*NestedJoinAuthor)
				require.Equal(t, book.AuthorIDFK, author.AuthorID)
				require.NotEmpty(t, author.Locations1)
				require.NotEmpty(t, author.Locations2)
				for _, loc := range author.Locations1 {
					require.Equal(t, author.LocationIDFK, loc.LocationID)
					require.NotEqual(t, 13, loc.Code)
					require.NotEmpty(t, loc.Countries)
				}
				for _, loc := range author.Locations2 {
					require.Equal(t, author.LocationIDFK, loc.LocationID)
					require.GreaterOrEqual(t, loc.Code, 1)
				}
			}
			return true
		})

		iter := qBooks.MustExec(t)
		defer iter.Close()
		VerifyNestedJoinResults(t, iter, 1, false)
		require.Greater(t, handlerCalled, 0)
	})

	t.Run("joined objects access", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations1 := DB.Query(nestedJoinLocationsNs).
			Not().Where("code", reindexer.EQ, 13)
		qLocations1.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qLocations2 := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations1, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")
		qAuthors.InnerJoin(qLocations2, "locations2").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 2).
			Limit(50)
		qBooks.InnerJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		iter := qBooks.MustExec(t)
		defer iter.Close()

		for iter.Next() {
			item := iter.Object().(*NestedJoinBook)

			authorsSubitems, err := iter.JoinedObjects("authors")
			require.NoError(t, err)
			require.NotEmpty(t, authorsSubitems)
			require.Equal(t, len(item.Authors), len(authorsSubitems))

			for i, s := range authorsSubitems {
				author := s.(*NestedJoinAuthor)
				require.Equal(t, item.Authors[i].AuthorID, author.AuthorID)
				require.NotEmpty(t, author.Locations1)
				require.NotEmpty(t, author.Locations2)
				for _, loc := range author.Locations1 {
					require.Equal(t, author.LocationIDFK, loc.LocationID)
					require.NotEqual(t, 13, loc.Code)
					require.NotEmpty(t, loc.Countries)
				}
				for _, loc := range author.Locations2 {
					require.Equal(t, author.LocationIDFK, loc.LocationID)
					require.GreaterOrEqual(t, loc.Code, 1)
				}
			}
		}
	})

	t.Run("nested join queries with JSON output", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations1 := DB.Query(nestedJoinLocationsNs).
			Not().Where("code", reindexer.EQ, 13)
		qLocations1.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qLocations2 := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations1, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")
		qAuthors.InnerJoin(qLocations2, "locations2").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 2).
			Limit(50)
		qBooks.InnerJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		jIter := qBooks.ExecToJson()
		defer jIter.Close()
		require.NoError(t, jIter.Error())
		require.Greater(t, jIter.Count(), 0)

		for jIter.Next() {
			VerifyJson(t, jIter.JSON())
		}
	})

	t.Run("nested join queries with merge queries", func(t *testing.T) {
		qCountries1 := DB.Query(nestedJoinCountriesNs)
		qLocations1a := DB.Query(nestedJoinLocationsNs).
			Not().Where("code", reindexer.EQ, 13)
		qLocations1a.InnerJoin(qCountries1, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")
		qLocations1b := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)
		qAuthors1 := DB.Query(nestedJoinAuthorsNs)
		qAuthors1.InnerJoin(qLocations1a, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")
		qAuthors1.InnerJoin(qLocations1b, "locations2").
			On("locationid_fk", reindexer.EQ, "locationid")
		qBooks1 := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 2).
			Limit(30)
		qBooks1.InnerJoin(qAuthors1, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		qCountries2 := DB.Query(nestedJoinCountriesNs)
		qLocations2a := DB.Query(nestedJoinLocationsNs).
			Not().Where("code", reindexer.EQ, 13)
		qLocations2a.InnerJoin(qCountries2, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")
		qAuthors2 := DB.Query(nestedJoinAuthorsNs)
		qAuthors2.InnerJoin(qLocations2a, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")
		qBooks2 := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 5000).
			Limit(20)
		qBooks2.InnerJoin(qAuthors2, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		mergeIter := qBooks1.Merge(qBooks2).MustExec(t)
		defer mergeIter.Close()
		VerifyNestedJoinResultsNoCount(t, mergeIter)
		mergeCount := 0
		for mergeIter.Next() {
			mergeCount++
		}
		require.GreaterOrEqual(t, mergeCount, 0)
	})

}

func TestNestedJoinWithLeftJoin(t *testing.T) {
	FillNamespaces()

	t.Run("book left join with nested inner join", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)
		qLocations.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 0).
			Limit(100)
		qBooks.LeftJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		iter := qBooks.MustExec(t)
		defer iter.Close()
		require.Equal(t, nestedJoinBooksCount, iter.Count())
		joinedSeen := false
		for iter.Next() {
			item := iter.Object().(*NestedJoinBook)
			if len(item.Authors) > 0 {
				joinedSeen = true
			}
			require.GreaterOrEqual(t, item.Price, 0)
			verifyLeftJoinedAuthors(t, item)
		}
		require.True(t, joinedSeen, "expected at least one book with joined authors")
	})

	t.Run("left join with JoinHandler access", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)
		qLocations.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 0).
			Limit(100)
		qBooks.LeftJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		handlerCalled := 0
		qBooks.JoinHandler("authors", func(field string, item interface{}, subitems []interface{}) bool {
			book := item.(*NestedJoinBook)
			for _, subitem := range subitems {
				author := subitem.(*NestedJoinAuthor)
				require.Equal(t, book.AuthorIDFK, author.AuthorID)
				require.NotEmpty(t, author.Locations1)
				require.Len(t, author.Locations2, 0)
				for _, loc := range author.Locations1 {
					require.Equal(t, author.LocationIDFK, loc.LocationID)
					require.GreaterOrEqual(t, loc.Code, 1)
					require.NotEmpty(t, loc.Countries)
					for _, cntry := range loc.Countries {
						require.Equal(t, loc.CountryIDFK, cntry.CountryID)
						require.NotEmpty(t, cntry.CountryName)
					}
				}
			}
			handlerCalled++
			return true
		})

		iter := qBooks.MustExec(t)
		defer iter.Close()
		VerifyNestedLeftJoinResults(t, iter)
		require.Greater(t, handlerCalled, 0)
	})

	t.Run("left join with JoinedObjects access", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)
		qLocations.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 0).
			Limit(100)
		qBooks.LeftJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		iter := qBooks.MustExec(t)
		defer iter.Close()
		VerifyNestedLeftJoinResults(t, iter)

		for iter.Next() {
			item := iter.Object().(*NestedJoinBook)
			authorsSubitems, err := iter.JoinedObjects("authors")
			require.NoError(t, err)
			require.Equal(t, len(item.Authors), len(authorsSubitems))
			for i, s := range authorsSubitems {
				author := s.(*NestedJoinAuthor)
				require.Equal(t, item.Authors[i].AuthorID, author.AuthorID)
				verifyLeftJoinedAuthorData(t, item.AuthorIDFK, author)
			}
		}
	})

	t.Run("left join JSON output", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)
		qLocations.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 0).
			Limit(100)
		qBooks.LeftJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		jIter := qBooks.ExecToJson()
		defer jIter.Close()
		require.NoError(t, jIter.Error())
		require.Greater(t, jIter.Count(), 0)

		seen := false
		for jIter.Next() {
			VerifyLeftJoinJSONFields(t, jIter.JSON())
			var result map[string]interface{}
			require.NoError(t, json.Unmarshal(jIter.JSON(), &result))
			authors, ok := findJoinedArrayByNamespace(t, result, nestedJoinAuthorsNs)
			require.True(t, ok)
			if len(authors) > 0 {
				seen = true
			}
		}
		require.True(t, seen, "expected at least one JSON record with joined authors")
	})
}

func TestNestedJoinInnerWithNestedLeftJoinEmpty(t *testing.T) {
	FillNamespaces()

	qAuthors := DB.Query(nestedJoinAuthorsNs)
	qAuthors.LeftJoin(
		DB.Query(nestedJoinLocationsNs).Where("code", reindexer.EQ, 99999),
		"locations1",
	).On("locationid_fk", reindexer.EQ, "locationid")

	iter := DB.Query(nestedJoinBooksNs).Limit(1).
		InnerJoin(qAuthors, "authors").
		On("authorid_fk", reindexer.EQ, "authorid").
		MustExec(t)
	defer iter.Close()

	require.Equal(t, 1, iter.Count())
	require.True(t, iter.Next())
	verifyInnerJoinedAuthorsWithEmptyLeftJoin(t, iter.Object().(*NestedJoinBook))
	require.False(t, iter.Next())
}

func TestNestedLeftJoinWithNestedInnerJoin(t *testing.T) {
	FillNestedJoinLeftInnerItems(t)

	t.Run("inner join filters nested left join items", func(t *testing.T) {
		qNs2 := DB.Query(nestedJoinLeftInnerNs2).Sort("id", false)
		qNs2.InnerJoin(DB.Query(nestedJoinLeftInnerNs3), "joined_ns3").
			On("ref_id", reindexer.EQ, "id")

		iter := DB.Query(nestedJoinLeftInnerNs1).Sort("id", false).
			LeftJoin(qNs2, "joined_ns2").
			On("id", reindexer.EQ, "parent_id").
			ExecToJson()

		verifyNestedLeftWithInnerJoinJSON(t, iter, map[int][]int{
			0: {},
			1: {11},
			2: {13},
			3: {},
		})
	})

	t.Run("or inner join keeps rows matched by another branch", func(t *testing.T) {
		qNs2 := DB.Query(nestedJoinLeftInnerNs2).
			Where("age", reindexer.EQ, 11).
			Or()
		qNs2.InnerJoin(DB.Query(nestedJoinLeftInnerNs3), "joined_ns3").
			On("ref_id", reindexer.EQ, "id").
			Sort("id", false)
		qNs2.Sort("id", false)

		iter := DB.Query(nestedJoinLeftInnerNs1).Sort("id", false).
			LeftJoin(qNs2, "joined_ns2").
			On("id", reindexer.EQ, "parent_id").
			ExecToJson()

		verifyNestedLeftWithInnerJoinJSON(t, iter, map[int][]int{
			0: {},
			1: {11},
			2: {13},
			3: {14},
		})
	})
}

func TestNestedJoinSelfJoin(t *testing.T) {
	FillNestedJoinSelf(nestedJoinSelfCount)

	t.Run("nested inner join with self join", func(t *testing.T) {
		qGrandChildren := DB.Query(nestedJoinSelfNs).
			Where("parent_id", reindexer.GE, 0)

		qChildren := DB.Query(nestedJoinSelfNs).
			Where("parent_id", reindexer.GE, 0)
		qChildren.InnerJoin(qGrandChildren, "grandchildren").
			On("id", reindexer.EQ, "parent_id")

		qParent := DB.Query(nestedJoinSelfNs).
			Where("id", reindexer.EQ, 0)
		qParent.InnerJoin(qChildren, "children").
			On("id", reindexer.EQ, "parent_id")

		iter := qParent.MustExec(t)
		defer iter.Close()
		require.Greater(t, iter.Count(), 0)

		for iter.Next() {
			item := iter.Object().(*NestedJoinSelfItem)
			require.Equal(t, -1, item.ParentID)
			require.NotEmpty(t, item.Children)
			for _, child := range item.Children {
				require.Equal(t, item.ID, child.ParentID)
				require.NotEmpty(t, child.GrandChildren)
				for _, grandChild := range child.GrandChildren {
					require.Equal(t, child.ID, grandChild.ParentID)
				}
			}
		}
	})

	t.Run("nested inner join with self join via JoinedObjects access", func(t *testing.T) {
		qGrandChildren := DB.Query(nestedJoinSelfNs).
			Where("parent_id", reindexer.GE, 0)

		qChildren := DB.Query(nestedJoinSelfNs).
			Where("parent_id", reindexer.GE, 0)
		qChildren.InnerJoin(qGrandChildren, "grandchildren").
			On("id", reindexer.EQ, "parent_id")

		qParent := DB.Query(nestedJoinSelfNs).
			Where("id", reindexer.EQ, 0)
		qParent.InnerJoin(qChildren, "children").
			On("id", reindexer.EQ, "parent_id")

		iter := qParent.MustExec(t)
		defer iter.Close()

		for iter.Next() {
			item := iter.Object().(*NestedJoinSelfItem)

			childrenSubitems, err := iter.JoinedObjects("children")
			require.NoError(t, err)
			require.NotEmpty(t, childrenSubitems)
			require.Equal(t, len(item.Children), len(childrenSubitems))

			for i, s := range childrenSubitems {
				child := s.(*NestedJoinSelfItem)
				require.Equal(t, item.Children[i].ID, child.ID)
				require.Equal(t, item.ID, child.ParentID)
				require.NotEmpty(t, child.GrandChildren)
				for _, grandChild := range child.GrandChildren {
					require.Equal(t, child.ID, grandChild.ParentID)
				}
			}
		}
	})
}
func TestNestedJoinModifyQueries(t *testing.T) {
	FillNamespaces()

	t.Run("update query with nested joins", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations := DB.Query(nestedJoinLocationsNs).
			Not().Where("code", reindexer.EQ, 13)
		qLocations.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 2)
		qBooks.InnerJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")
		qBooks.Limit(1)

		before, err := qBooks.MustExec(t).FetchAll()
		require.NoError(t, err)
		require.Greater(t, len(before), 0)
		bookID := before[0].(*NestedJoinBook).BookID

		qBooksUpdate := DB.Query(nestedJoinBooksNs).
			Where("bookid", reindexer.EQ, bookID)
		qBooksUpdate.Set("title", "nested_join_updated")

		updIter := qBooksUpdate.Update()
		defer updIter.Close()
		require.NoError(t, updIter.Error())

		checkQ := DB.Query(nestedJoinBooksNs).
			Where("bookid", reindexer.EQ, bookID)
		after, err := checkQ.MustExec(t).FetchAll()
		require.NoError(t, err)
		require.Equal(t, len(before), len(after))
	})

	t.Run("delete query with nested joins", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs)
		qLocations := DB.Query(nestedJoinLocationsNs).
			Not().Where("code", reindexer.EQ, 13)
		qLocations.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 8000)
		qBooks.InnerJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")
		qBooks.Limit(1)

		before, err := qBooks.MustExec(t).FetchAll()
		require.NoError(t, err)

		if len(before) > 0 {
			bookID := before[0].(*NestedJoinBook).BookID
			qBooksDel := DB.Query(nestedJoinBooksNs).
				Where("bookid", reindexer.EQ, bookID)

			delCount, err := qBooksDel.Delete()
			require.NoError(t, err)
			require.Equal(t, len(before), delCount)
		}
	})
}

func TestNestedJoinEdgeCases(t *testing.T) {
	FillNamespaces()

	t.Run("nested join with no matching countries", func(t *testing.T) {
		qCountries := DB.Query(nestedJoinCountriesNs).
			Where("countryid", reindexer.EQ, 9999)

		qLocations := DB.Query(nestedJoinLocationsNs).
			Not().Where("code", reindexer.EQ, 13)
		qLocations.InnerJoin(qCountries, "countries").
			On("countryid_fk", reindexer.EQ, "countryid")

		qAuthors := DB.Query(nestedJoinAuthorsNs)
		qAuthors.InnerJoin(qLocations, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 2).
			Limit(10)
		qBooks.InnerJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		iter := qBooks.MustExec(t)
		defer iter.Close()
		require.Equal(t, 0, iter.Count())
	})

	t.Run("nested join with multiple ON conditions", func(t *testing.T) {
		qAuthors := DB.Query(nestedJoinAuthorsNs)

		qLocations := DB.Query(nestedJoinLocationsNs).
			Where("code", reindexer.GE, 1)

		qAuthors.InnerJoin(qLocations, "locations1").
			On("locationid_fk", reindexer.EQ, "locationid").
			On("age", reindexer.LE, "code")

		qBooks := DB.Query(nestedJoinBooksNs).
			Where("price", reindexer.GE, 2).
			Limit(30)
		qBooks.InnerJoin(qAuthors, "authors").
			On("authorid_fk", reindexer.EQ, "authorid")

		iter := qBooks.MustExec(t)
		defer iter.Close()
		require.NoError(t, iter.Error())
	})
}
