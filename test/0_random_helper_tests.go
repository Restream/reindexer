package reindexer

import (
	"math/rand"
	"strings"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/builtin"
	_ "github.com/restream/reindexer/v5/bindings/cproto"
)

const (
	charset  = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	nilUuid  = "00000000-0000-0000-0000-000000000000"
	hexChars = "0123456789abcdef"
)

var (
	adjectives = [...]string{"able", "above", "absolute", "balanced", "becoming", "beloved", "calm", "capable", "capital", "destined", "devoted", "direct", "enabled", "enabling", "endless", "factual", "fair", "faithful", "grand", "grateful", "great", "humane", "humble", "humorous", "ideal", "immense", "immortal", "joint", "just", "keen", "key", "kind", "logical", "loved", "loving", "mint", "model", "modern", "nice", "noble", "normal", "one", "open", "optimal", "polite", "popular", "positive", "quality", "quick", "quiet", "rapid", "rare", "rational", "sacred", "safe", "saved", "tight", "together", "tolerant", "unbiased", "uncommon", "unified", "valid", "valued", "vast", "wealthy", "welcome"}
	names      = [...]string{"ox", "ant", "ape", "asp", "bat", "bee", "boa", "bug", "cat", "cod", "cow", "cub", "doe", "dog", "eel", "eft", "elf", "elk", "emu", "ewe", "fly", "fox", "gar", "gnu", "hen", "hog", "imp", "jay", "kid", "kit", "koi", "lab", "man", "owl", "pig", "pug", "pup", "ram", "rat", "ray", "yak", "bass", "bear", "bird", "boar", "buck", "bull", "calf", "chow", "clam", "colt", "crab", "crow", "dane", "deer", "dodo", "dory", "dove", "drum", "duck", "fawn", "fish", "flea", "foal", "fowl", "frog", "gnat", "goat", "grub", "gull", "hare", "hawk", "ibex", "joey", "kite", "kiwi", "lamb", "lark", "lion", "loon", "lynx", "mako", "mink", "mite", "mole", "moth", "mule", "mutt", "newt", "orca", "oryx", "pika", "pony", "puma", "seal", "shad", "slug", "sole", "stag", "stud", "swan", "tahr", "teal", "tick", "toad", "tuna", "wasp", "wolf", "worm", "wren", "yeti"}
	cyrillic   = [...]string{"жук", "сон", "кофе", "кирилица", "етти", "мышь", "кот"}
	devices    = [...]string{"iphone", "android", "smarttv", "stb", "ottstb"}
	locations  = [...]string{"mos", "ct", "dv", "sth", "vlg", "sib", "ural"}
)

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

func trueRandWord(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
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

func randVect(dimension int) []float32 {
	result := make([]float32, dimension)
	for i := 0; i < dimension; i++ {
		result[i] = rand.Float32()
	}
	return result
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
