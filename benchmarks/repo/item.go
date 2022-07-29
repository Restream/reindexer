package repo

import (
	"math/rand"
)

type Item struct {
	ID          int64         `reindex:"id,,pk" json:"id" db:"id"`
	Name        string        `reindex:"name" json:"name"  db:"name"`
	Description string        `reindex:"description,text" json:"description"  db:"description"`
	Year        int           `reindex:"year,tree" json:"year" db:"year"`
	Joined      []*JoinedItem `reindex:"joined,,joined" json:"joined"`
}

func newItem(id int) *Item {
	return &Item{
		ID:          int64(id),
		Name:        randString(),
		Description: randTextString(),
		Year:        2000 + rand.Int()%50,
	}
}

type JoinedItem struct {
	ID          int64  `reindex:"id,hash,pk" json:"id" db:"id"`
	Description string `json:"description"  db:"description"`
}

func newJoinedItem(id int) *JoinedItem {
	return &JoinedItem{
		ID:          int64(id),
		Description: randString(),
	}
}

func (item *Item) Join(field string, subitems []interface{}, context interface{}) {
	// switch field {
	// case "joined":
	// 	for _, joinItem := range subitems {
	// 		item.Joined = append(item.Joined, joinItem.(*JoinedItem))
	// 	}
	// }
}
