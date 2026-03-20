package reindexer

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItemEqualPosition struct {
	ID         string                        `reindex:"id,,pk"`
	Name       string                        `reindex:"name"`
	SecondName string                        `reindex:"second_name"`
	TestFlag   bool                          `reindex:"test_flag"`
	ItemsArray []*TestArrayItemEqualPosition `reindex:"items_array,-"`
	ValueArray []int                         `reindex:"value_array,-"`
	_          struct{}                      `reindex:"name+second_name=searching,text,composite"`
}

type TestEqualPosPackageItem struct {
	ID        int64 `json:"id"`
	StartDate int64 `json:"start_date" reindex:"start_date"`
	EndDate   int64 `json:"end_date" reindex:"end_date"`
}

type TestEqualPosServiceItem struct {
	ID   int    `json:"id" reindex:"id,,pk"`
	Name string `json:"name"`
}

type TestEqualPosMediaItem struct {
	ID       int                       `json:"id" reindex:"id,,pk"`
	Name     string                    `json:"name"`
	Packages []TestEqualPosPackageItem `json:"packages" reindex:"packages"`

	ServicesJoined []*TestEqualPosServiceItem `reindex:"services,,joined"`
	SelfJoined     []*TestEqualPosMediaItem   `reindex:"self,,joined"`
}

const (
	testItemsEqualPositionNs = "test_items_eqaul_position"
	testEqualPosMediaItemsNs = "test_equal_position_media_items"
	testEqualPosServicesNs   = "test_equal_position_services"
)

func init() {
	tnamespaces[testEqualPosMediaItemsNs] = TestEqualPosMediaItem{}
	tnamespaces[testEqualPosServicesNs] = TestEqualPosServiceItem{}
	tnamespaces[testItemsEqualPositionNs] = TestItemEqualPosition{}
}

func newTestItemEqualPosition(id int, arrSize int) *TestItemEqualPosition {
	av := make([]*TestArrayItemEqualPosition, id%arrSize)
	av2 := make([]int, id%arrSize)
	for j := range av {
		av[j] = &TestArrayItemEqualPosition{
			SpaceId: "space_" + strconv.Itoa(j),
			Value:   id % 2,
		}
		av2[j] = (id + j) % 2
	}
	return &TestItemEqualPosition{
		ID:         strconv.Itoa(id),
		Name:       "Name_" + strconv.Itoa(id),
		SecondName: "Second_name_" + strconv.Itoa(id),
		TestFlag:   id%4 > 2,
		ItemsArray: av,
		ValueArray: av2,
	}
}

func TestEqualPosition(t *testing.T) {
	const ns = testItemsEqualPositionNs

	tx := newTestTx(DB, ns)
	for i := 0; i < 20; i++ {
		tx.Upsert(newTestItemEqualPosition(i, 3))
	}
	tx.MustCommit()

	t.Run("simple equal position", func(t *testing.T) {
		expectedIds := map[string]bool{
			"2":  true,
			"4":  true,
			"8":  true,
			"10": true,
			"14": true,
			"16": true,
		}
		it := newTestQuery(DB, ns).
			Match("searching", "name Name*").
			Where("items_array.space_id", reindexer.EQ, "space_0").
			Where("items_array.value", reindexer.EQ, 0).
			EqualPosition("items_array.space_id", "items_array.value").
			MustExec(t)
		defer it.Close()
		assert.NoError(t, it.Error())
		assert.Equal(t, len(expectedIds), it.Count())
		for it.Next() {
			assert.True(t, expectedIds[it.Object().(*TestItemEqualPosition).ID])
		}
	})

	t.Run("equal position with additional conditions", func(t *testing.T) {
		expectedIds := map[string]bool{
			"5":  true,
			"17": true,
		}
		it := newTestQuery(DB, ns).
			Match("searching", "name Name*").
			Where("items_array.space_id", reindexer.EQ, "space_1").
			Where("items_array.value", reindexer.EQ, 1).
			WhereBool("test_flag", reindexer.EQ, false).
			EqualPosition("items_array.space_id", "items_array.value").
			MustExec(t)
		defer it.Close()
		assert.NoError(t, it.Error())
		assert.Equal(t, len(expectedIds), it.Count())
		for it.Next() {
			assert.True(t, expectedIds[it.Object().(*TestItemEqualPosition).ID])
		}
	})

	t.Run("equal position in brackets", func(t *testing.T) {
		expectedIds := map[string]bool{
			"2":  true,
			"3":  true,
			"4":  true,
			"7":  true,
			"8":  true,
			"10": true,
			"11": true,
			"14": true,
			"15": true,
			"16": true,
			"19": true,
		}
		it := newTestQuery(DB, ns).
			OpenBracket().
			Where("items_array.space_id", reindexer.EQ, "space_0").
			Where("items_array.value", reindexer.EQ, 0).
			Where("value_array", reindexer.EQ, 0).
			EqualPosition("items_array.space_id", "items_array.value", "value_array").
			CloseBracket().
			Or().
			WhereBool("test_flag", reindexer.EQ, true).
			MustExec(t)
		defer it.Close()
		assert.NoError(t, it.Error())
		assert.Equal(t, len(expectedIds), it.Count())
		for it.Next() {
			fmt.Println(it.Object().(*TestItemEqualPosition).ID, expectedIds[it.Object().(*TestItemEqualPosition).ID])
			assert.True(t, expectedIds[it.Object().(*TestItemEqualPosition).ID])
		}
	})
	t.Run("equal position array mark", func(t *testing.T) {
		expectedIds := map[string]bool{
			"1": true,
		}
		it := newTestQuery(DB, ns).
			Where("ID", reindexer.EQ, 1).
			Where("items_array.space_id", reindexer.EQ, "space_0").
			Where("items_array.value", reindexer.EQ, 1).
			EqualPosition("ItemsArray[#].SpaceId", "ItemsArray[#].Value").
			MustExec(t)
		defer it.Close()
		assert.NoError(t, it.Error())
		assert.Equal(t, len(expectedIds), it.Count())
		for it.Next() {
			fmt.Println(it.Object().(*TestItemEqualPosition).ID, expectedIds[it.Object().(*TestItemEqualPosition).ID])
			assert.True(t, expectedIds[it.Object().(*TestItemEqualPosition).ID])
		}
	})

}

func fillTestEqualPosMediaItems(t *testing.T, count int, ns string) {
	tx := newTestTx(DB, ns)

	now := time.Now().Unix()
	for i := 0; i < count; i++ {
		err := tx.Upsert(&TestEqualPosMediaItem{
			ID:   i,
			Name: "some media item name",
			Packages: []TestEqualPosPackageItem{
				{
					ID:        int64(i),
					StartDate: now - 10 - rand.Int63n(10),
					EndDate:   now + 10 + rand.Int63n(10),
				},
				{
					ID:        int64(i + 100),
					StartDate: now + 100 + rand.Int63n(10),
					EndDate:   now + 500 + rand.Int63n(10),
				},
				{
					ID:        int64(i + 1000),
					StartDate: now + 1000 + rand.Int63n(10),
					EndDate:   now + 2000 + rand.Int63n(10),
				},
			}})
		require.NoError(t, err)
	}
	tx.MustCommit()
}

func fillTestEqualPosServices(t *testing.T, start int, count int, ns string, serviceName string) {
	tx := newTestTx(DB, ns)

	for i := 0; i < count; i++ {
		err := tx.Upsert(&TestEqualPosServiceItem{ID: start + i, Name: serviceName})
		require.NoError(t, err)
	}
	tx.MustCommit()
}

func TestEqualPostionWithJoin(t *testing.T) {

	fillTestEqualPosMediaItems(t, 5, testEqualPosMediaItemsNs)
	fillTestEqualPosServices(t, 1, 3, testEqualPosServicesNs, "some service")

	query := DB.Query(testEqualPosMediaItemsNs)

	servicesQuery := DB.Query(testEqualPosServicesNs).WhereString("name", reindexer.EQ, "some service")
	query.InnerJoin(servicesQuery, "services").On("packages.id", reindexer.SET, "id")
	query.LeftJoin(DB.Query(testEqualPosMediaItemsNs), "self").On("id", reindexer.EQ, "id")

	now := time.Now().Unix()
	query.OpenBracket()
	query.WhereInt64("packages.id", reindexer.SET, 1, 1002, 1003)
	query.WhereInt64("packages.start_date", reindexer.LT, now)
	query.WhereInt64("packages.end_date", reindexer.GT, now)
	query.EqualPosition("packages.id", "packages.start_date", "packages.end_date")
	query.CloseBracket()
	query.Or()
	query.OpenBracket()
	query.WhereInt64("packages.id", reindexer.SET, 103)
	query.WhereInt64("packages.start_date", reindexer.LT, now+200)
	query.WhereInt64("packages.end_date", reindexer.GT, now+200)
	query.EqualPosition("packages.id", "packages.start_date", "packages.end_date")
	query.CloseBracket()
	query.Sort("id", false)

	expectedIDs := []int{1, 3}
	expectedJoinedServicesIDs := []int{1, 3}

	it := query.MustExec(t)
	require.NoError(t, it.Error())
	defer it.Close()

	assert.Equal(t, len(expectedIDs), it.Count())
	i := 0
	for it.Next() {
		elem := it.Object().(*TestEqualPosMediaItem)
		assert.Equal(t, expectedIDs[i], elem.ID)
		require.Equal(t, 1, len(elem.SelfJoined))
		assert.Equal(t, elem.ID, elem.SelfJoined[0].ID)
		require.Equal(t, 1, len(elem.ServicesJoined))
		assert.Equal(t, expectedJoinedServicesIDs[i], elem.ServicesJoined[0].ID)
		i++
	}
	require.NoError(t, it.Error())
}
