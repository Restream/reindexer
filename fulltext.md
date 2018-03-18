# Full text search with Reindexer

Reindexer has builtin full text search engine. This document describes usage of full text search.

- [Define full text index fields](#define-full-text-index-fields)
- [Query to full text index](#query-to-full-text-index)
- [Text query format](#text-query-format)
	- [Patterns](#patterns)
	- [Field selection](#field-selection)
	- [Binary operators](#binary-operators)
- [Examples of text queris](#examples-of-text-queris)
- [Natural language processing](#natural-language-processing)
- [Merging queries results](#merging-queries-results)
- [Using select fucntions](#using-select-functions)
- [Performance and memory usage](#performance-and-memory-usage)
- [Configuration](#configuration)
	- [Limitations and know issues](#limitations-and-know-issues)


## Define full text index fields

Full text search is performed in fields marked with `text` tag:

```go
type Item struct {
	ID          int64  `reindex:"id,,pk"`
	Description string `reindex:"description,text"`
}
```

Full text search is also available for multiple fields of composite index marked with `text` tag:

```go
type Item struct {
	ID          int64  `reindex:"id,,pk"`
	Name        string `reindex:"name,-"`
	Description string `reindex:"description,-"`
	_ struct{}         `reindex:"name+description=text_search,text,composite`
}
```
In this example full text index will include fields `name` and `description`,`text_search` is short alias of composite index name for using in Queries.

Full text index is case insensitive. The source text is tokenized to set of words. Word is sequence of any utf8 letters, digits or `+`, `-` or `/` symbols. First symbol of word is letter or digit.

## Query to full text index

Queries to full text index are constructed by usual query interface

```go
	query := db.Query ("items").
		Match ("name+description","text query","<stemmers>")
```
Or equivalent query using name alias:

```go
	query := db.Query ("items").
		Match ("text_search","text query","<stemmers>")
```

Queries to full text index can be combined with conditions on another fields. e.g:

```go
	query := db.Query ("items").
		Match ("description","text query").
		WhereInt("year",reindexer.GT,2010)
```

Each result of query contains rank of match. Rank is integer from 0 to 100. 0 - lowest relevancy, 100 - best relevancy. The query Iterator has method `Rank()`, which returns rank of current result


## Text query format

The format of query is:

`query := [@field[^boost][,field2[^boost]]]    [=][*]term1[*][~][^boost] [+|-][*]term2[*][~][@][^boost] ...`

### Patterns
- `*` - match any symbols. For example, `termina*` matches words `terminator` and `terminal`. `*` Can be at word start or at word end, but can not be at word middle. Minimum prefix len is 2 symbols.
- `~` - fuzzy match misspeled word by typos dictionary. For example, `black~` watches words `block`, `blck`, or `blask`. `~`. The typos dictionary is contains all words in index with 1 possible mistake. If `~` combined with `*`, then it means mistake match or prefix match, but not prefix with mistake. For example, `turmin*~` is matches `turminals`, `termin`, but not `terminal`
- `^x` - boost matches term by x. default boost value is 1.

### Field selection
- `@` - comma separated list of fields to search
- `*` - search in all fields
- `^x` - boost matches in field by x. default boost value is 1.

### Binary operators
- `+` - next pattern must present in found document
- `-` - next pattern must not present in found document

## Examples of text queris

`termina* -genesis` - find documents contains words begins with `termina`, exclude documents contains word `genesis`  
`black~` - find documents contains word black with 1 possible mistake. e.g `block`, `blck`, or `blask`  
`tom jerry cruz^2` - find documents contains at least one of word `tom`, `cruz` `jerry`. relevancy of documents, which contains `tom cruz` will be greater, than `tom jerry`  
`fox +fast` - find documents contains both words: `fox` and `fast`  
`"one two"` - find documents with phrase `one two`  
`"one two"~5` - find documents with words `one` and `two` with distance beetwen terms < 5  
`@name rush` - find docuemnts with word `rush` only in `name` field  
`@name^1.5,* rush` - find documents with word `rush`, and boost 1.5 results from `name` field  
`=windows` - find documents with exact term `windows` without language specific term variants (stemmers/translit/wrong kb layout)  


## Natural language processing

There are built in stemmers support in full text search. It enables natural language search of words with same stem. For example, query `users` will also match `user`. Stemmer is language specific, so it is neccessary to specify language of used stemmer.


## Merging queries results

It is possible to merge multiple queries results and sort final result by relevancy.

```go
	query := db.Query ("items").
		Match ("description","text query1")
	q2 := db.Query ("another_items").
		Match ("description","text query2")
	query.Merge (q2)
    iterator = query.Exec ()
	// Check the error
	if err := iterator.Error(); err != nil {
		panic(err)
	}
	// Iterate over results
	for iterator.Next() {
		// Get the next document and cast it to a pointer
		switch elem := iterator.Ptr().(type) {
			case Item:
				fmt.Printf ("%v,rank=%d\n",*elem,iterator.Rank())
			case AnotherItem:
				fmt.Printf ("%v,rank=%d\n",*elem,iterator.Rank())
		}
	}
```
## Using select fucntions
It is possible to use select functions to process result data.
For now you can use snippet and highlight
You can not put [,)\0] symbols in functions params

highlight - only highlights text area that was found
it has two arguments -
- `first` string that will be inserted before found text area
- `second` string that will be inserted after found text area

Example:
word: "some text"

```go
b.Query("items").Match("text", query).Limit(limit).Offset(offset).Functions("text.highlight(<b>,</b>)")
```
result: "some <b>text</b>"

snippet -  highlights text area and erase other text
it has six arguments - last two is default
- `first` string that will be inserted before found text area
- `second` string that will be inserted after found text area
- `third`  number of symbols that will be placed before area
- `fourth`  number of symbols that will be placed after area
- `fifth` delimiter before snippets ,default  space
- `sixth` delimiter after snippets ,default  nothing

Example:
word: "some text"

```go
b.Query("items").Match("text", query).Limit(limit).Offset(offset).Functions("text.snippet(<b>,</b>,2,0)")
```
result: "e <b>text</b>"

## Performance and memory usage

Internally reindexer uses enhanced suffix array of unique words, and compresed reverse index of documents. Typically size of index is about 30%-80% of source text. But can vary in corner cases.

The `Upsert` operation does not perform actual indexing, but just stores text. There are lazy indexing is implemented. So actually, full text index is building on first Query on fulltext field. The indexing is uses several threads, so it is efficently utilizes resources of modern multi core CPU. Therefore the indexing speed is very high. On modern hardware indexing speed is about ~50MB/sec

But on huge text size lazy indexing can seriously slow down first Query to text index. To avoid this side-effect it is possible to warmup text index: just by dummy Query after last `Upsert`

## Configuration

Several parameters of full text search engine can be configured from application side. To setup configration use `db.ConfigureIndex` method:

```go
...
	ftconfig := reindexer.DefaultFtFastConfig()
	// Setup configuration
	ftconfig.LogLevel = reindexer.TRACE
	// Setup another parameters
	return db.ConfigureIndex("items", "description", ftconfig)

```

Here is list of available parameters:


|   | Parameter name |   Type   |                                                                                                                        Description                                                                                                                        | Default value |
|---|:--------------:|:--------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:-------------:|
|   | Bm25Boost      |   float  | Boost of bm25 ranking                                                                                                                                                                                                                                     |       1       |
|   | Bm25Weight     |   float  | Weight of bm25 rank in final rank 0: bm25 will not change final rank. 1: bm25 will affect to finl rank in 0 - 100% range.                                                                                                                                 | 0.5           |
|   | DistanceBoost  |   float  | Boost of search query term distance in found document.                                                                                                                                                                                                    |       1       |
|   | DistanceWeight |   float  | Weight of search query terms distance in found document in final rank 0: distance will not change final rank. 1: distance will affect to final rank in 0 - 100% range.                                                                                    |      0.5      |
|   | TermLenBoost   |   float  | Boost of search query term length                                                                                                                                                                                                                         |       1       |
|   | TermLenWeight  |   float  | Weight of search query term length in final rank. 0: term length will not change final rank. 1: term length will affect to final rank in 0 - 100% range                                                                                                   |      0.3      |
|   | MinRelevancy   |   float  | Minimum rank of found documents. 0: all found documents will be returned 1: only documents with relevancy >= 100% will be returned                                                                                                                        |      0.05     |
|   | MaxTyposInWord |    int   | Maximum possible typos in word. 0: typos is disabled, words with typos will not match. N: words with N possible typos will match. It is not recommended to set more than 1 possible typo -It will seriously increase RAM usage, and decrease search speed |       1       |
|   | MaxTypoLen     |    int   | Maximum word length for building and matching variants with typos.                                                                                                                                                                                        |       15      |
|   | MergeLimit     |    int   | Maximum documents count which will be processed in merge query results.  Increasing this value may refine ranking of queries with high frequency words, but will decrease search speed                                                                    |     20000     |
|   | Stemmers       | []string | List of stemmers to use                                                                                                                                                                                                                                   | "en","ru"     |
|   | EnableTranslit |   bool   | Enable russian translit variants processing. e.g. term "luntik" will match word "лунтик"                                                                                                                                                                  |      true     |
|   | EnableKbLayout |   bool   | Enable wrong keyboard layout variants processing. e.g. term "keynbr" will match word "лунтик"                                                                                                                                                             |      true     |
|   | StopWords      | []string | List of stop words. Words from this list will be ignored in documents and queries                                                                                                                                                                         |               |
|   | LogLevel       |    int   | Log level of full text search engine                                                                                                                                                                                                                      |       0       |

### Limitations and know issues

- Results of full text search is always sorted by relevancy.
- It is not possible to combine 2 Match in single Query. (but actually this can be done by composite indexes or by Merge functional)
