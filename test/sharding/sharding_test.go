//go:build sharding_test
// +build sharding_test

package sharding

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/cproto"
	"github.com/restream/reindexer/v5/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItemShardingJoined struct {
	ID int `reindex:"id,,pk" json:"id"`
}

type TestItemSharding struct {
	ID        int                       `reindex:"id,hash" json:"id"`
	Data      string                    `json:"data"`
	Location  string                    `reindex:"location,hash" json:"location"`
	Timestamp int64                     `reindex:"timestamp,-"`
	Joined    []*TestItemShardingJoined `reindex:"joined,,joined"`
	_         struct{}                  `reindex:"id+location,,composite,pk"`
}

var dsn = flag.String("dsn", "builtin://", "reindex db dsn")

func CreateTestItemSharding(ID int, Location string) TestItemSharding {
	return TestItemSharding{ID: ID, Data: "data", Location: Location}
}

func TestMain(m *testing.M) {
	flag.Parse()
	retCode := m.Run()
	os.Exit(retCode)

}

func TestShardingIDs(t *testing.T) {

	rx, err := reindexer.NewReindex(*dsn, reindexer.WithCreateDBIfMissing())
	require.NoError(t, err)
	defer rx.Close()

	const testNamespace = "ns"

	err = rx.OpenNamespace(testNamespace, reindexer.DefaultNamespaceOptions(), TestItemSharding{})
	assert.NoError(t, err, "Can't open namespace \"%s\" what=", testNamespace, err)

	index := 0
	itemsOnShard := 3
	shardCount := 3

	idSetBaseAll := map[int]TestItemSharding{}
	idSetBaseShard := make([]map[int]TestItemSharding, shardCount)

	check := func() {
		for m := 0; m < 2; m++ {
			{
				q := rx.Query(testNamespace)
				it := q.Exec()
				defer it.Close()
				it.AllowUnsafe(true)
				testData, err := it.FetchAll()
				assert.NoError(t, err, "Can't get data from namespace")
				idSetBaseOneShard := map[int]TestItemSharding{}
				for _, v := range testData {
					item, _ := v.(*TestItemSharding)
					idSetBaseOneShard[item.ID] = *item
				}
				eq := reflect.DeepEqual(idSetBaseAll, idSetBaseOneShard)
				assert.True(t, eq)
			}

			for i := 0; i < shardCount; i++ {
				q := rx.Query(testNamespace).WhereString("location", reindexer.EQ, "key"+strconv.Itoa(i))
				it := q.Exec()
				defer it.Close()
				it.AllowUnsafe(true)
				testData, err := it.FetchAll()
				assert.NoError(t, err, "Can't get data from namespace")
				idSetBaseOneShard := map[int]TestItemSharding{}
				for _, v := range testData {
					item, _ := v.(*TestItemSharding)
					idSetBaseOneShard[item.ID] = *item
				}
				eq := reflect.DeepEqual(idSetBaseShard[i], idSetBaseOneShard)
				assert.True(t, eq)
			}
		}
	}

	t.Run("check shard IDs after upserts", func(t *testing.T) {
		for k := 0; k < shardCount; k++ {
			idSetBaseShard[k] = map[int]TestItemSharding{}
			for ; index < itemsOnShard+k*itemsOnShard; index++ {
				item := TestItemSharding{ID: index, Data: "data", Location: "key" + strconv.Itoa(k)}
				err = rx.Upsert(testNamespace, item)
				assert.NoError(t, err, "Can't Upsert data to namespace")
				idSetBaseAll[index] = item
				idSetBaseShard[k][index] = item
			}
		}
		check()
	})

	t.Run("check shard IDs after updates", func(t *testing.T) {
		for k := 0; k < shardCount; k++ {
			itemToChange := 2 + k*itemsOnShard
			item := TestItemSharding{ID: itemToChange, Data: "datanew", Location: "key" + strconv.Itoa(k)}
			count, err := rx.Update(testNamespace, item)
			assert.NoError(t, err, "Can't Update data in namespace")
			assert.True(t, count == 1)
			idSetBaseAll[itemToChange] = item
			idSetBaseShard[k][itemToChange] = item

		}
		check()
	})

	t.Run("check shard IDs after deletes and reinserts", func(t *testing.T) {
		for k := 0; k < shardCount; k++ {
			itemToChange := 2 + k*itemsOnShard
			itemDel := TestItemSharding{ID: itemToChange, Data: "datanew", Location: "key" + strconv.Itoa(k)}
			err := rx.Delete(testNamespace, itemDel)
			assert.NoError(t, err, "Can't Delete data from namespace")
			itemIns := TestItemSharding{ID: itemToChange, Data: "datanew2", Location: "key" + strconv.Itoa(k)}
			count, err := rx.Insert(testNamespace, itemIns)
			assert.NoError(t, err, "Can't Insert data to namespace")
			assert.True(t, count == 1)
			idSetBaseAll[itemToChange] = itemIns
			idSetBaseShard[k][itemToChange] = itemIns
		}
		check()
	})

}

func TestShardingBuiltin(t *testing.T) {
	shardedNsName := "sharded_ns"
	joinedNsName := "joined_local_ns"
	initialData := "data"
	updatedData := "new_data"
	keys := []string{"key0", "key1"}
	var shardingCfg helpers.TestShardingConfig
	shardingCfg.Namespaces = []helpers.TestShardedNamespaceConfig{{Name: shardedNsName, DefaultShard: 0, Index: "location", Keys: []helpers.TestShardKeyConfig{{ShardID: 0, Values: []string{"key0"}}, {ShardID: 1, Values: []string{"key1"}}}}}
	shardingCfg.Shards = []*helpers.TestServer{
		{T: t, RpcPort: "6734", HttpPort: "9188", DbName: "sharded_db", SrvType: helpers.ServerTypeBuiltin},
		{T: t, RpcPort: "6735", HttpPort: "9189", DbName: "sharded_db", SrvType: helpers.ServerTypeBuiltin},
	}
	defer shardingCfg.Shards[0].Clean()
	defer shardingCfg.Shards[1].Clean()

	helpers.WriteShardingConfig(t, &shardingCfg)
	require.NoError(t, shardingCfg.Shards[0].Run())
	require.NoError(t, shardingCfg.Shards[1].Run())
	shards := []*reindexer.Reindexer{shardingCfg.Shards[0].DB(), shardingCfg.Shards[1].DB()}
	err := shards[0].OpenNamespace(shardedNsName, reindexer.DefaultNamespaceOptions(), TestItemSharding{})
	require.NoError(t, err)
	err = shards[1].RegisterNamespace(shardedNsName, reindexer.DefaultNamespaceOptions(), TestItemSharding{})
	require.NoError(t, err)

	check := func(dataPerShard int, dataValue string) {
		for shardID, shard := range shards {
			log.Println("Checking queries via shard", shardID)
			for keyID, keyValue := range keys {
				log.Printf("Checking query to shard%d\n", keyID)
				it := shard.Query(shardedNsName).Where("location", reindexer.EQ, keyValue).Sort("id", false).Exec()
				require.NoError(t, it.Error())
				require.Equal(t, it.Count(), dataPerShard)
				for j := 0; j < dataPerShard; j++ {
					require.True(t, it.Next())
					require.NoError(t, it.Error())
					item, ok := it.Object().(*TestItemSharding)
					require.True(t, ok)
					assert.Equal(t, item.Location, keyValue)
					assert.Equal(t, item.ID, j)
					assert.Equal(t, item.Data, dataValue)
				}
			}

			log.Println("Checking distributed query")
			it := shard.Query(shardedNsName).Sort("id", false).Exec()
			require.NoError(t, it.Error())
			require.Equal(t, it.Count(), dataPerShard*len(shards))
			for j := 0; j < dataPerShard; j++ {
				for i, _ := range shards {
					require.True(t, it.Next())
					require.NoError(t, it.Error())
					item, ok := it.Object().(*TestItemSharding)
					require.True(t, ok)
					assert.Equal(t, item.Location, "key"+strconv.Itoa(i))
					assert.Equal(t, item.ID, j)
					assert.Equal(t, item.Data, dataValue)
				}
			}
		}
	}

	currentDocsCount := 0
	const initialDocsPerShard = 10
	const txDocsPerShard = 10
	t.Run("check upserts", func(t *testing.T) {
		for i, s := range shards {
			for j := 0; j < initialDocsPerShard; j++ {
				err = s.Upsert(shardedNsName, CreateTestItemSharding(j, "key"+strconv.Itoa(i)))
				require.NoError(t, err)
			}
		}

		check(initialDocsPerShard, initialData)
		currentDocsCount += initialDocsPerShard
	})

	t.Run("check json selects", func(t *testing.T) {
		for shardID, shard := range shards {
			log.Println("Checking queries via shard", shardID)
			for keyID, keyValue := range keys {
				log.Printf("Checking query to shard%d\n", keyID)
				it := shard.Query(shardedNsName).Where("location", reindexer.EQ, keyValue).Sort("id", false).ExecToJson()
				require.NoError(t, it.Error())
				require.Equal(t, it.Count(), currentDocsCount)
				for j := 0; j < currentDocsCount; j++ {
					require.True(t, it.Next())
					require.NoError(t, it.Error())
					var item TestItemSharding
					err := json.Unmarshal(it.JSON(), &item)
					require.NoError(t, err)
					assert.Equal(t, item.Location, keyValue)
					assert.Equal(t, item.ID, j)
					assert.Equal(t, item.Data, initialData)
				}
			}

			log.Println("Checking distributed query")
			it := shard.Query(shardedNsName).Sort("id", false).ExecToJson()
			require.NoError(t, it.Error())
			require.Equal(t, it.Count(), currentDocsCount*len(shards))
			for j := 0; j < currentDocsCount; j++ {
				for i, _ := range shards {
					require.True(t, it.Next())
					require.NoError(t, it.Error())
					var item TestItemSharding
					err := json.Unmarshal(it.JSON(), &item)
					require.NoError(t, err)
					assert.Equal(t, item.Location, "key"+strconv.Itoa(i))
					assert.Equal(t, item.ID, j)
					assert.Equal(t, item.Data, initialData)
				}
			}
		}
	})

	t.Run("check joins", func(t *testing.T) {
		const joinDocsTotal = 10
		err := shards[0].OpenNamespace(joinedNsName, reindexer.DefaultNamespaceOptions(), TestItemShardingJoined{})
		require.NoError(t, err)
		err = shards[1].OpenNamespace(joinedNsName, reindexer.DefaultNamespaceOptions(), TestItemShardingJoined{})
		require.NoError(t, err)
		for j := 0; j < joinDocsTotal; j++ {
			shardIdx := 0
			if j >= 5 {
				shardIdx = 1
			}
			err = shards[shardIdx].Upsert(joinedNsName, TestItemShardingJoined{ID: j})
			require.NoError(t, err)
		}

		for shardID, shard := range shards {
			log.Println("Checking queries via shard", shardID)
			for keyID, keyValue := range keys {
				log.Printf("Checking query to shard%d", keyID)
				jq := shard.Query(joinedNsName)
				it := shard.Query(shardedNsName).Where("location", reindexer.EQ, keyValue).InnerJoin(jq, "joined").On("id", reindexer.EQ, "id").Sort("id", false).Exec()
				require.NoError(t, it.Error())
				require.Equal(t, it.Count(), joinDocsTotal/2)
				for j := 0; j < joinDocsTotal/2; j++ {
					require.True(t, it.Next())
					require.NoError(t, it.Error())
					item, ok := it.Object().(*TestItemSharding)
					require.True(t, ok)
					assert.Equal(t, item.Location, keyValue)
					if keyID == 0 {
						assert.Equal(t, item.ID, j)
					} else {
						assert.Equal(t, item.ID, j+joinDocsTotal/2)
					}
					assert.Equal(t, item.Data, initialData)
					assert.Equal(t, len(item.Joined), 1)
					assert.Equal(t, item.Joined[0].ID, item.ID)
				}
			}
		}
	})

	t.Run("check transactions", func(t *testing.T) {
		for i, s := range shards {
			tx, err := s.BeginTx(shardedNsName)
			require.NoError(t, err)
			for j := 0; j < txDocsPerShard; j++ {
				err = tx.Upsert(CreateTestItemSharding(currentDocsCount+j, "key"+strconv.Itoa(i)))
				require.NoError(t, err)
			}
			cnt, err := tx.CommitWithCount()
			require.NoError(t, err)
			require.Equal(t, cnt, txDocsPerShard)
		}

		check(currentDocsCount+txDocsPerShard, initialData)
		currentDocsCount += txDocsPerShard
	})

	t.Run("check delete queries", func(t *testing.T) {
		for i, shard := range shards {
			count, err := shard.Query(shardedNsName).Where("location", reindexer.EQ, "key"+strconv.Itoa(i)).Where("id", reindexer.GE, initialDocsPerShard+txDocsPerShard/2).Delete()
			require.NoError(t, err)
			require.Equal(t, count, txDocsPerShard/2)
		}
		check(initialDocsPerShard+txDocsPerShard/2, initialData)
		currentDocsCount = initialDocsPerShard + txDocsPerShard/2

		for i, shard := range shards {
			count, err := shard.Query(shardedNsName).Where("location", reindexer.EQ, "key"+strconv.Itoa(i)).Where("id", reindexer.GE, initialDocsPerShard).Delete()
			require.NoError(t, err)
			require.Equal(t, count, txDocsPerShard/2)
		}
		check(initialDocsPerShard, initialData)
		currentDocsCount = initialDocsPerShard
	})

	t.Run("check update queries", func(t *testing.T) {
		for i, shard := range shards {
			res, err := shard.Query(shardedNsName).Set("data", updatedData).Where("location", reindexer.EQ, "key"+strconv.Itoa(i)).Where("id", reindexer.GE, currentDocsCount/2).Update().FetchAll()
			require.NoError(t, err)
			require.Equal(t, len(res), currentDocsCount/2)
			for _, r := range res {
				item, ok := r.(*TestItemSharding)
				require.True(t, ok)
				assert.Equal(t, item.Location, "key"+strconv.Itoa(i))
				assert.GreaterOrEqual(t, item.ID, currentDocsCount/2)
				assert.Equal(t, item.Data, updatedData)
			}
		}

		for i, shard := range shards {
			res, err := shard.Query(shardedNsName).Set("data", updatedData).Where("location", reindexer.EQ, "key"+strconv.Itoa(i)).Where("id", reindexer.LT, currentDocsCount/2).Update().FetchAll()
			require.NoError(t, err)
			require.Equal(t, len(res), currentDocsCount/2)
			for _, r := range res {
				item, ok := r.(*TestItemSharding)
				require.True(t, ok)
				assert.Equal(t, item.Location, "key"+strconv.Itoa(i))
				assert.Less(t, item.ID, currentDocsCount/2)
				assert.Equal(t, item.Data, updatedData)
			}
		}
		check(currentDocsCount, updatedData)
	})

	t.Run("check upserts with precepts", func(t *testing.T) {
		count, err := shards[0].Query(shardedNsName).Where("location", reindexer.EQ, "key0").Delete()
		require.NoError(t, err)
		require.Equal(t, count, currentDocsCount)
		count, err = shards[0].Query(shardedNsName).Where("location", reindexer.EQ, "key1").Delete()
		require.NoError(t, err)
		require.Equal(t, count, currentDocsCount)

		for i, s := range shards {
			for j := 0; j < initialDocsPerShard; j++ {
				item := CreateTestItemSharding(j, "key"+strconv.Itoa(i))
				beg := time.Now().Unix()
				err = s.Upsert(shardedNsName, &item, "timestamp=now(sec)")
				require.NoError(t, err)
				assert.GreaterOrEqual(t, item.Timestamp, beg)
				assert.LessOrEqual(t, item.Timestamp, time.Now().Unix())
			}
		}

		check(initialDocsPerShard, initialData)
		currentDocsCount = initialDocsPerShard
	})

	require.NoError(t, shardingCfg.Shards[0].Stop())
	require.NoError(t, shardingCfg.Shards[1].Stop())
}
