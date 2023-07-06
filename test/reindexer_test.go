package reindexer

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/restream/reindexer/v3"
	_ "github.com/restream/reindexer/v3/bindings/builtin"
	_ "github.com/restream/reindexer/v3/bindings/cproto"
	// _ "github.com/restream/reindexer/v3/pprof"
)

var DB *ReindexerWrapper
var DBD *reindexer.Reindexer

var tnamespaces map[string]interface{} = make(map[string]interface{}, 100)

var dsn = flag.String("dsn", "builtin:///tmp/reindex_test/", "reindex db dsn")
var dsnSlave = flag.String("dsnslave", "", "reindex slave db dsn")
var slaveCount = flag.Int("slavecount", 1, "reindex slave db count")

var benchmarkSeedCount = flag.Int("seedcount", 500000, "count of items for benchmark seed")
var benchmarkSeedCPU = flag.Int("seedcpu", 1, "number threads of for seeding")
var benchmarkSeed = flag.Int64("seed", time.Now().Unix(), "seed number for random")

func TestMain(m *testing.M) {

	flag.Parse()

	udsn, err := url.Parse(*dsn)
	if err != nil {
		panic(err)
	}

	opts := []interface{}{}
	if udsn.Scheme == "builtin" {
		os.RemoveAll("/tmp/reindex_test/")
	} else if udsn.Scheme == "cproto" {
		opts = []interface{}{reindexer.WithCreateDBIfMissing(), reindexer.WithNetCompression(), reindexer.WithAppName("RxTestInstance")}
	}

	DB = NewReindexWrapper(*dsn, opts...)
	DBD = &DB.Reindexer
	if err = DB.Status().Err; err != nil {
		panic(err)
	}

	if *dsnSlave != "" {
		DB.AddSlave(*dsnSlave, *slaveCount, reindexer.WithCreateDBIfMissing())
	}

	if testing.Verbose() {
		DB.SetLogger(&TestLogger{})
	}
	for k, v := range tnamespaces {
		DB.DropNamespace(k)

		if err := DB.OpenNamespace(k, reindexer.DefaultNamespaceOptions(), v); err != nil {
			panic(fmt.Sprintf("Namespace: %s, err: %s", k, err.Error()))
		}
	}

	if err = DB.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type: "profiling",
		Profiling: &reindexer.DBProfilingConfig{
			QueriesThresholdUS: 10,
			MemStats:           true,
			PerfStats:          true,
			QueriesPerfStats:   true,
			ActivityStats:      true,
			LongQueryLogging: &reindexer.LongQueryLoggingConfig{
				SelectItem: reindexer.LongQueryLoggingItem{
					ThresholdUS: -1,
					Normalized:  false,
				},
				UpdDelItem: reindexer.LongQueryLoggingItem{
					ThresholdUS: -1,
					Normalized:  false,
				},
				TxItem: reindexer.LongTxLoggingItem{
					ThresholdUS:              1000,
					AverageTxStepThresholdUs: 50,
				},
			}},
	}); err != nil {
		panic(err)
	}

	retCode := m.Run()

	DB.Close()
	os.Exit(retCode)
}

func mkID(i int) int {
	return i*17 + 8000000
}

type TestLogger struct {
}

func (TestLogger) Printf(level int, format string, msg ...interface{}) {
	if level <= reindexer.TRACE {
		log.Printf(format, msg...)
	}
}

func randString() string {
	return adjectives[rand.Int()%len(adjectives)] + "_" + names[rand.Int()%len(names)]
}
func randLangString() string {
	return adjectives[rand.Int()%len(adjectives)] + "_" + cyrillic[rand.Int()%len(cyrillic)]
}
func randSearchString() string {
	if rand.Int()%2 == 0 {
		return adjectives[rand.Int()%len(adjectives)]
	}
	return names[rand.Int()%len(names)]

}

func randStringArr(cnt int) []string {
	if cnt < 1 {
		return nil
	}
	arr := make([]string, 0, cnt)
	for i := 0; i < cnt; i++ {
		arr = append(arr, randString())
	}
	return arr
}

func randDevice() string {
	return devices[rand.Int()%len(devices)]
}

func randPostalCode() int {
	return rand.Int() % 999999
}

func randLocation() string {
	return locations[rand.Int()%len(locations)]
}

func randFloat(min int, max int) float64 {
	divider := (1 << rand.Intn(10))
	min *= divider
	max *= divider
	return float64(rand.Intn(max-min)+min) / float64(divider)
}

func randPoint() reindexer.Point {
	return reindexer.Point{randFloat(-10, 10), randFloat(-10, 10)}
}

func randIntArr(cnt int, start int, rng int) (arr []int) {
	if cnt == 0 {
		return nil
	}
	arr = make([]int, 0, cnt)
	for j := 0; j < cnt; j++ {
		arr = append(arr, start+rand.Int()%rng)
	}
	return arr
}
func randInt32Arr(cnt int, start int, rng int) (arr []int32) {
	if cnt == 0 {
		return nil
	}
	arr = make([]int32, 0, cnt)
	for j := 0; j < cnt; j++ {
		arr = append(arr, int32(start+rand.Int()%rng))
	}
	return arr
}

const nilUuid = "00000000-0000-0000-0000-000000000000"
const hexChars = "0123456789abcdef"

func randUuid() string {
	if rand.Int()%1000 == 0 {
		return nilUuid
	}
	var b strings.Builder
	b.Grow(36)
	for i := 0; i < 36; i++ {
		switch i {
		case 8, 13, 18, 23:
			b.WriteByte('-')
		case 19:
			b.WriteByte(hexChars[8+(rand.Int()%(len(hexChars)-8))])
		default:
			b.WriteByte(hexChars[rand.Int()%len(hexChars)])
		}
	}
	return b.String()
}

func randUuidArray(cnt int) []string {
	if cnt < 1 {
		return nil
	}
	arr := make([]string, 0, cnt)
	for i := 0; i < cnt; i++ {
		arr = append(arr, randUuid())
	}
	return arr
}

var (
	adjectives = [...]string{"able", "above", "absolute", "balanced", "becoming", "beloved", "calm", "capable", "capital", "destined", "devoted", "direct", "enabled", "enabling", "endless", "factual", "fair", "faithful", "grand", "grateful", "great", "humane", "humble", "humorous", "ideal", "immense", "immortal", "joint", "just", "keen", "key", "kind", "logical", "loved", "loving", "mint", "model", "modern", "nice", "noble", "normal", "one", "open", "optimal", "polite", "popular", "positive", "quality", "quick", "quiet", "rapid", "rare", "rational", "sacred", "safe", "saved", "tight", "together", "tolerant", "unbiased", "uncommon", "unified", "valid", "valued", "vast", "wealthy", "welcome"}

	names = [...]string{"ox", "ant", "ape", "asp", "bat", "bee", "boa", "bug", "cat", "cod", "cow", "cub", "doe", "dog", "eel", "eft", "elf", "elk", "emu", "ewe", "fly", "fox", "gar", "gnu", "hen", "hog", "imp", "jay", "kid", "kit", "koi", "lab", "man", "owl", "pig", "pug", "pup", "ram", "rat", "ray", "yak", "bass", "bear", "bird", "boar", "buck", "bull", "calf", "chow", "clam", "colt", "crab", "crow", "dane", "deer", "dodo", "dory", "dove", "drum", "duck", "fawn", "fish", "flea", "foal", "fowl", "frog", "gnat", "goat", "grub", "gull", "hare", "hawk", "ibex", "joey", "kite", "kiwi", "lamb", "lark", "lion", "loon", "lynx", "mako", "mink", "mite", "mole", "moth", "mule", "mutt", "newt", "orca", "oryx", "pika", "pony", "puma", "seal", "shad", "slug", "sole", "stag", "stud", "swan", "tahr", "teal", "tick", "toad", "tuna", "wasp", "wolf", "worm", "wren", "yeti"}

	cyrillic = [...]string{"жук", "сон", "кофе", "кирилица", "етти", "мышь", "кот"}

	devices   = [...]string{"iphone", "android", "smarttv", "stb", "ottstb"}
	locations = [...]string{"mos", "ct", "dv", "sth", "vlg", "sib", "ural"}
)
