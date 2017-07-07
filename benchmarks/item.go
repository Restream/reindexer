package benchmarks

import "math/rand"

type Item struct {
	ID   int64  `reindex:"id,,pk" json:"id" db:"id"`
	Name string `reindex:"name" json:"name"  db:"name"`
	Year int    `reindex:"year,tree" json:"year" db:"year"`
}

func newItem(id int) *Item {
	return &Item{
		ID:   int64(id),
		Name: randString(),
		Year: 2000 + rand.Int()%50,
	}
}

func randString() string {
	return adjectives[rand.Int()%len(adjectives)] + " " + names[rand.Int()%len(names)]
}

var (
	adjectives = [...]string{"able", "above", "absolute", "balanced", "becoming", "beloved", "calm", "capable", "capital", "destined", "devoted", "direct", "enabled", "enabling", "endless", "factual", "fair", "faithful", "grand", "grateful", "great", "humane", "humble", "humorous", "ideal", "immense", "immortal", "joint", "just", "keen", "key", "kind", "logical", "loved", "loving", "mint", "model", "modern", "nice", "noble", "normal", "one", "open", "optimal", "polite", "popular", "positive", "quality", "quick", "quiet", "rapid", "rare", "rational", "sacred", "safe", "saved", "tight", "together", "tolerant", "unbiased", "uncommon", "unified", "valid", "valued", "vast", "wealthy", "welcome"}

	names = [...]string{"ox", "ant", "ape", "asp", "bat", "bee", "boa", "bug", "cat", "cod", "cow", "cub", "doe", "dog", "eel", "eft", "elf", "elk", "emu", "ewe", "fly", "fox", "gar", "gnu", "hen", "hog", "imp", "jay", "kid", "kit", "koi", "lab", "man", "owl", "pig", "pug", "pup", "ram", "rat", "ray", "yak", "bass", "bear", "bird", "boar", "buck", "bull", "calf", "chow", "clam", "colt", "crab", "crow", "dane", "deer", "dodo", "dory", "dove", "drum", "duck", "fawn", "fish", "flea", "foal", "fowl", "frog", "gnat", "goat", "grub", "gull", "hare", "hawk", "ibex", "joey", "kite", "kiwi", "lamb", "lark", "lion", "loon", "lynx", "mako", "mink", "mite", "mole", "moth", "mule", "mutt", "newt", "orca", "oryx", "pika", "pony", "puma", "seal", "shad", "slug", "sole", "stag", "stud", "swan", "tahr", "teal", "tick", "toad", "tuna", "wasp", "wolf", "worm", "wren", "yeti"}
)
