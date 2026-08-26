package reindexer

import (
	"bufio"
	"embed"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestItemFTStress struct {
	ID       int    `reindex:"id,,pk"`
	Name     string `reindex:"name,text"`
	Location string `reindex:"location"`
	Amount   int    `reindex:"amount,tree"`
}

const maxId = 6000

const testFtStressNs = "test_items_ft_stress"

func init() {
	tnamespaces[testFtStressNs] = TestItemFTStress{}
}

//go:embed ft/dict.txt
var ftDict embed.FS
var dictWords []string

func readDict() ([]string, error) {
	f, err := ftDict.Open("ft/dict.txt")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func dictRandWord() string {
	return dictWords[rand.Intn(len(dictWords))]
}

func randFtString() string {
	wordsCount := 1 + rand.Intn(8)
	var result strings.Builder
	for i := 0; i < wordsCount; i++ {
		var nextWord string
		if rand.Intn(10) == 0 {
			nextWord = trueRandWord(rand.Intn(15) + 1)
		} else {
			nextWord = dictRandWord()
		}
		if result.Len() > 0 {
			result.WriteString(" ")
		}
		result.WriteString(nextWord)
	}
	return result.String()
}

func FTStressTest(t *testing.T) {
	t.Parallel()

	const ns = testFtStressNs

	var err error
	dictWords, err = readDict()
	require.NoError(t, err)

	var wg sync.WaitGroup
	endCh := make(chan struct{})

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	for_loop:
		for {
			DB.Query(ns).Match("name", dictRandWord()).Sort("rank()", true).MustExec(t).Close()
			select {
			case <-endCh:
				break for_loop
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	for_loop:
		for {
			DB.Query(ns).Match("name", randFtString()).MustExec(t).Close()
			select {
			case <-endCh:
				break for_loop
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	for_loop:
		for {
			DB.Query(ns).Match("name", trueRandWord(rand.Intn(5)+1)).Sort("id", false).MustExec(t).Close()
			select {
			case <-endCh:
				break for_loop
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	for_loop:
		for {
			_, err := DB.Query(ns).Match("name", dictRandWord()).Delete()
			require.NoError(t, err)
			select {
			case <-endCh:
				break for_loop
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	for_loop:
		for {
			it := DB.Query(ns).Match("name", dictRandWord()).Set("name", randFtString()).Update()
			require.NoError(t, it.Error())
			it.Close()
			select {
			case <-endCh:
				break for_loop
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < 3000; i++ {
			item := TestItemFTStress{
				ID:       rand.Intn(maxId),
				Name:     randFtString(),
				Location: randLocation(),
				Amount:   rand.Intn(maxId),
			}
			err := DB.Upsert(ns, item)
			require.NoError(t, err)
			time.Sleep(1 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
		close(endCh)
	}(&wg)

	wg.Wait()
}
