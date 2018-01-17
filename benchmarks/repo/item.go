package repo

import (
	"math/rand"
)

type Item struct {
	ID          int64  `reindex:"id,,pk" json:"id" db:"id"`
	Name        string `reindex:"name" json:"name"  db:"name"`
	Description string `reindex:"description,text" json:"description"  db:"description"`
	Year        int    `reindex:"year,tree" json:"year" db:"year"`
}

func newItem(id int) *Item {
	return &Item{
		ID:          int64(id),
		Name:        randString(),
		Description: randTextString(),
		Year:        2000 + rand.Int()%50,
	}
}
