package reindexer

import (
	"fmt"
	"testing"
	"time"
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

func init() {
	tnamespaces["test_items_autogen"] = TestItemAutogen{}
}

func TestAutogen(t *testing.T) {
	precepts := []string{"updated_time=NOW()"}

	defaultTestItem := TestItemAutogen{
		ID:               1,
		Genre:            0,
		Age:              0,
		UpdatedTime:      0,
		UpdatedTimeNano:  0,
		UpdatedTimeMicro: 0,
		UpdatedTimeMilli: 0,
	}

	// Test1: A field should be updated with current timestamp using NOW() function
	DB.Upsert("test_items_autogen", defaultTestItem, precepts...)

	results, err := DB.ExecSQL("SELECT * FROM test_items_autogen WHERE id=1").FetchAll()
	if err != nil {
		panic(err)
	}

	result := results[0]
	res, ok := result.(*TestItemAutogen)
	if !ok {
		panic(fmt.Sprintf("wait %T, got %T", TestItemAutogen{}, result))
	}

	if res.UpdatedTime == defaultTestItem.UpdatedTime {
		panic(fmt.Errorf("NOW function doesn't work. The same value received."))
	}

	// Test2: A field should be updated with current timestamp using NOW() function not later than 1 second
	DB.Upsert("test_items_autogen", defaultTestItem, precepts...)

	results, err = DB.ExecSQL("SELECT * FROM test_items_autogen WHERE id=1").FetchAll()
	if err != nil {
		panic(err)
	}

	result = results[0]
	res, ok = result.(*TestItemAutogen)
	if !ok {
		panic(fmt.Sprintf("wait %T, got %T", TestItemAutogen{}, result))
	}

	if time.Now().UTC().Unix()-res.UpdatedTime > 1 {
		panic(fmt.Errorf("NOW function doesn't work properly. It took more than 2 seconds for value update."))
	}

	// Test3: A field should contain different count of digits after "nsec, usec, msec, sec" params usage
	precepts = []string{"updated_time=NOW(sec)", "updated_time_milli=NOW(MSEC)", "updated_time_micro=now(usec)", "updated_time_nano=now(NSEC)"}

	DB.Upsert("test_items_autogen", defaultTestItem, precepts...)

	results, err = DB.ExecSQL("SELECT * FROM test_items_autogen WHERE id=1").FetchAll()
	if err != nil {
		panic(err)
	}

	result = results[0]
	res, ok = result.(*TestItemAutogen)
	if !ok {
		panic(fmt.Sprintf("wait %T, got %T", TestItemAutogen{}, result))
	}

	if res.UpdatedTimeMilli-1000*res.UpdatedTime > 1000 {
		panic(fmt.Errorf("NOW(msec) gave an incorrect value."))
	}

	if res.UpdatedTimeMicro-1000000*res.UpdatedTime > 1000000 {
		panic(fmt.Errorf("NOW(usec) gave an incorrect value."))
	}

	if res.UpdatedTimeNano-1000000000*res.UpdatedTime > 1000000000 {
		panic(fmt.Errorf("NOW(nsec) gave an incorrect value."))
	}

	// Test4: A serial field shoud be increased by one
	precepts = []string{"genre=SERIAL()", "age=serial()"}

	DB.Upsert("test_items_autogen", defaultTestItem, precepts...)

	results, err = DB.ExecSQL("SELECT * FROM test_items_autogen WHERE id=1").FetchAll()
	if err != nil {
		panic(err)
	}

	result = results[0]
	res, ok = result.(*TestItemAutogen)
	if !ok {
		panic(fmt.Sprintf("wait %T, got %T", TestItemAutogen{}, result))
	}

	if res.Age != defaultTestItem.Age+1 {
		panic(fmt.Errorf("SERIAL function doesn't work. Int field was not incremented."))
	}

	if res.Genre != defaultTestItem.Genre+1 {
		panic(fmt.Errorf("SERIAL function doesn't work. Int64 field was not incremented."))
	}

	// Test5: A serial field should be increased by 5 after 5 iterations (must be equal 6 after Test4)
	for i := 0; i < 5; i++ {
		DB.Upsert("test_items_autogen", defaultTestItem, precepts...)
	}

	results, err = DB.ExecSQL("SELECT * FROM test_items_autogen WHERE id=1").FetchAll()
	if err != nil {
		panic(err)
	}

	result = results[0]
	res, ok = result.(*TestItemAutogen)
	if !ok {
		panic(fmt.Sprintf("wait %T, got %T", TestItemAutogen{}, result))
	}

	if res.Age != 6 {
		panic(fmt.Errorf("SERIAL function doesn't work. Int field was not incremented."))
	}

	if res.Genre != 6 {
		panic(fmt.Errorf("SERIAL function doesn't work. Int64 field was not incremented."))
	}
}
