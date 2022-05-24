package reindexer

import (
	"math/rand"
	"testing"
	"time"

	"github.com/restream/reindexer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItemAutogen struct {
	ID               int   `reindex:"id,,pk"`
	Genre            int64 `reindex:"genre,tree"`
	Age              int   `reindex:"age,hash"`
	UpdatedTime      int64 `reindex:"updated_time,-"`
	UpdatedTimeNano  int64 `reindex:"updated_time_nano,-"`
	UpdatedTimeMicro int64 `reindex:"updated_time_micro,-"`
	UpdatedTimeMilli int64 `reindex:"updated_time_milli,-"`
}

var ns = "test_items_autogen"

func init() {
	tnamespaces["test_items_autogen"] = TestItemAutogen{}
}

func TestAutogen(t *testing.T) {
	t.Run("field should be updated with current timestamp using NOW() function", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()"}
		item := TestItemAutogen{}
		err := DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())

		it, _ := DB.Query(ns).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchAll()
		assert.Equal(t, item.UpdatedTime, it[0].(*TestItemAutogen).UpdatedTime)
	})

	t.Run("field should contain different count of digits after 'nsec, usec, msec, sec' params usage", func(t *testing.T) {
		precepts := []string{"updated_time=NOW(sec)", "updated_time_milli=NOW(MSEC)", "updated_time_micro=now(usec)", "updated_time_nano=now(NSEC)"}
		item := TestItemAutogen{}
		now := time.Now().Unix()
		err := DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)

		assert.GreaterOrEqual(t, item.UpdatedTimeMilli, now*1000)
		assert.Greater(t, item.UpdatedTimeMicro, now*1000000)
		assert.Greater(t, item.UpdatedTimeNano, now*1000000000)

		it, _ := DB.Query(ns).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchAll()
		assert.Equal(t, item.UpdatedTimeMilli, it[0].(*TestItemAutogen).UpdatedTimeMilli)
		assert.Equal(t, item.UpdatedTimeMicro, it[0].(*TestItemAutogen).UpdatedTimeMicro)
		assert.Equal(t, item.UpdatedTimeNano, it[0].(*TestItemAutogen).UpdatedTimeNano)
	})

	t.Run("serial field shoud be increased by one", func(t *testing.T) {
		precepts := []string{"genre=SERIAL()", "age=serial()"}
		item := TestItemAutogen{}
		err := DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)

		assert.Equal(t, 1, item.Age)
		assert.Equal(t, int64(1), item.Genre)

		it, _ := DB.Query(ns).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchAll()
		assert.Equal(t, 1, it[0].(*TestItemAutogen).Age)
		assert.Equal(t, int64(1), it[0].(*TestItemAutogen).Genre)

		t.Run("serial field should be increased by 5 after 5 iterations (must be equal 6 after previous test)", func(t *testing.T) {
			precepts := []string{"genre=SERIAL()", "age=serial()"}
			item := TestItemAutogen{}
			for i := 0; i < 5; i++ {
				err := DB.Upsert(ns, &item, precepts...)
				require.NoError(t, err)
			}

			assert.Equal(t, 6, item.Age)
			assert.Equal(t, int64(6), item.Genre)

			it, _ := DB.Query(ns).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchAll()
			assert.Equal(t, 6, it[0].(*TestItemAutogen).Age)
			assert.Equal(t, int64(6), it[0].(*TestItemAutogen).Genre)
		})
		t.Run("serial field should be increased by 5 after 5 iterations in transaction (must be equal 11 after previous tests)", func(t *testing.T) {
			precepts := []string{"genre=SERIAL()", "age=serial()"}
			tx := newTestTx(DB, ns)
			var lastID int
			for i := 0; i < 5; i++ {
				item := TestItemAutogen{}
				lastID = item.ID
				tx.UpsertAsync(&item, func(err error) { assert.NoError(t, err) }, precepts...)
			}
			tx.MustCommit()

			it, _ := DB.Query(ns).Where("id", reindexer.EQ, lastID).MustExec(t).FetchAll()
			assert.Equal(t, 11, it[0].(*TestItemAutogen).Age)
			assert.Equal(t, int64(11), it[0].(*TestItemAutogen).Genre)
		})
	})

	t.Run("fill on insert, update, upsert", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()"}

		item := TestItemAutogen{ID: rand.Intn(100000000)}
		_, err := DB.Insert(ns, &item, precepts...)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		it, _ := DB.Query(ns).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchAll()
		assert.Equal(t, item.UpdatedTime, it[0].(*TestItemAutogen).UpdatedTime)

		item = TestItemAutogen{}
		err = DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		it, _ = DB.Query(ns).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchAll()
		assert.Equal(t, item.UpdatedTime, it[0].(*TestItemAutogen).UpdatedTime)

		item = TestItemAutogen{}
		_, err = DB.Update(ns, &item, precepts...)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		it, _ = DB.Query(ns).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchAll()
		assert.Equal(t, item.UpdatedTime, it[0].(*TestItemAutogen).UpdatedTime)
	})

	t.Run("fill on upsert not exist item", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()"}
		item := TestItemAutogen{ID: rand.Intn(100000000)}

		err := DB.Upsert(ns, &item, precepts...)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		it, _ := DB.Query(ns).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchAll()
		assert.Equal(t, item.UpdatedTime, it[0].(*TestItemAutogen).UpdatedTime)
	})

	t.Run("not fill on update not exist item", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()"}
		item := TestItemAutogen{ID: rand.Intn(100000000)}

		count, err := DB.Update(ns, &item, precepts...)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, int64(0), item.UpdatedTime)
	})

	t.Run("not fill on insert exist item", func(t *testing.T) {
		precepts := []string{"updated_time=NOW()"}
		id := rand.Intn(100000000)
		item := TestItemAutogen{ID: id}

		count, err := DB.Insert(ns, &item, precepts...)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.GreaterOrEqual(t, item.UpdatedTime, time.Now().Unix()-1)
		assert.LessOrEqual(t, item.UpdatedTime, time.Now().Unix())
		it, _ := DB.Query(ns).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchAll()
		assert.Equal(t, item.UpdatedTime, it[0].(*TestItemAutogen).UpdatedTime)

		item = TestItemAutogen{ID: id}
		count, err = DB.Insert(ns, &item, precepts...)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, int64(0), item.UpdatedTime)
	})
}
