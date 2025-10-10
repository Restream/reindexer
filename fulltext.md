# Full text search with Reindexer

Reindexer has builtin full text search engine. This document describes usage of full text search.

- [LIKE](#like)
- [Define full text index fields](#define-full-text-index-fields)
- [Query to full text index](#query-to-full-text-index)
- [Text query format](#text-query-format)
    - [Patterns](#patterns)
    - [Field selection](#field-selection)
    - [Binary operators](#binary-operators)
    - [Escape character](#escape-character)
    - [Phrase search](#phrase-search)
- [Examples of text queries](#examples-of-text-queries)
- [Natural language processing](#natural-language-processing)
- [Merging queries results](#merging-queries-results)
- [Using select functions](#using-select-functions)
    - [Highlight](#highlight)
    - [Snippet](#snippet)
    - [Snippet_n](#snippet_n)
    - [Debug_rank](#debug_rank)
- [Typos algorithm](#typos-algorithm)
    - [Typos handling details](#typos-handling-details)
    - [More examples](#more-examples)
- [Performance and memory usage](#performance-and-memory-usage)
- [Configuration](#configuration)
    - [Base config parameters](#base-config-parameters)
    - [Stopwords details](#stopwords-details)
    - [Detailed typos config](#detailed-typos-config)
    - [Base ranking config](#base-ranking-config)
    - [Text splitters](#text-splitters)
    - [Basic document ranking algorithms](#basic-document-ranking-algorithms)
    - [Limitations and know issues](#limitations-and-know-issues)


## LIKE

For simple search in text can be used operator `LIKE`. It searches strings which match a pattern. In the pattern `_` means any char and `%` means any sequence of chars.

```
    In Go:
    query := db.Query("items").
        Where("field", reindexer.LIKE, "pattern")

    In SQL:
    SELECT * FROM items WHERE fields LIKE 'pattern'
```

```
    'me_t' corresponds to 'meet', 'meat', 'melt' and so on
    '%tion' corresponds to 'tion', 'condition', 'creation' and so on
```


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

Full text index is case-insensitive. The source text is tokenized to set of words. Word is sequence of any UTF-8 letters, digits or `+`, `-` or `/` symbols. First symbol of word is letter or digit.

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

Each result of query contains rank of match. Rank is integer from 0 to 255. 0 - lowest relevancy, 255 - best relevancy. The query Iterator has method `Rank()`, which returns rank of current result

> **Note**: `<stemmers>` see more [Stemming](https://en.wikipedia.org/wiki/Stemming).

## Text query format

The format of query is:

`query := [@[+]field[^boost][,field2[^boost]]]    [=][*]term1[*][~][^boost] [+|-][*]term2[*][~][@][^boost] ...`

### Patterns
- `*` - match any symbols. For example, `termina*` matches words `terminator` and `terminal`. `*` Can be at word start or at word end, but can not be at word middle.
- `~` - fuzzy match misspelled word by typos' dictionary. For example, `black~` watches words `block`, `blck`, or `blask`. `~`. The typos dictionary is contains all words in index with 1 possible mistake. If `~` combined with `*`, then it means mistake match or prefix match, but not prefix with mistake. For example, `turmin*~` is matches `turminals`, `termin`, but not `terminal`
- `^x` - boost matches term by x. default boost value is 1.

### Field selection
- `@` - comma separated list of fields to search
- `*` - search in all fields
- `^x` - boost matches in field by x. default boost value is 1.
- `+` - when a term matches in several fields final rank for this term will be maximum rank among fields. If some fields are marked with `+` ranks of matches in these fields will be added with ratio specified in config as `SumRanksByFieldsRatio`. All not skipped rank will be sorted and summed as `Rmax + K*R1 + K*K*R2 + ... + K^n * Rn` where `+` is `SumRanksByFieldsRatio`. For example, value of `SumRanksByFieldsRatio` is `K`, query is `@f1,+f2,f3,+f4 term`, if ranks of matches in the fields are `R1 < R2 < R3 < R4` then `R = R4 + K*R2` (`R1` and `R3` are skipped as fields `f1` and `f3` are not marked with `+`), if `R2 < R3 < R4 < R1` then `R = R1 + K*R4 + K*K*R2` (`R1` is not skipped as it is maximum).

### Binary operators
- `+` - next pattern must present in found document
- `-` - next pattern must not present in found document

### Escape character
`\` character allows to include any special character (`+`,`-`,`@`,`*`,`^`,`~`) to a word to be searched:
`\*crisis` means to search literally `*crisis` and not all words that end with 'crisis'.

### Phrase search
The DSL's operand can be a phrase:
`"word1 word2 ..."[~N]`. 
In this case `word1`, `word2`, ... are the words that make up the phrase in the given sequence. 
Words order is matter, i.e. sequence `word2 word1` will not be found with `"word1 word2"` DSL.

`~N` - max distance (in words) between adjacent word positions. This argument is optional, default value for `N` is 1.

Synonyms of multiple words are not supported in the phrase.

## Examples of text queries

- `termina* -genesis` - find documents contains words begins with `termina`, exclude documents contains word `genesis`
- `black~` - find documents contains word black with 1 possible mistake. e.g `block`, `blck`, or `blask`
- `tom jerry cruz^2` - find documents contains at least one of word `tom`, `cruz` `jerry`. relevancy of documents, which
  contains `tom cruz` will be greater, than `tom jerry`
- `fox +fast` - find documents contains both words: `fox` and `fast`
- `"one two"` - find documents with phrase `one two`
- `"one two"~5` - find documents with words `one` and `two` with distance between terms < 5. Order of terms is matter.
  If you need to search those terms in any order, all the required permutations must be explicitly specified in
  DSL: `"one two"~5 "two one"~5`
- `@name rush` - find documents with word `rush` only in `name` field
- `@name^1.5,* rush` - find documents with word `rush`, and boost 1.5 results from `name` field
- `=windows` - find documents with exact term `windows` without language specific term variants (stemmers/translit/wrong
  kb layout)
- `one -"phrase example"` - find documents containing the word `one` and not containing the phrase `"phrase example"`
- `one "phrase example"` - find documents containing the word `one` or the phrase `"phrase example"` or both
- `+one +"phrase example"` - find documents containing the word `one` and the phrase `"phrase example"`
- `one "phrase example"~3` - find documents containing the word `one` or the phrase `"phrase example"` (with max
  distance equals of 3 word between `phrase` and `example`) or both

## Natural language processing

There are built in stemmers support in full text search. It enables natural language search of words with same stem. For example, query `users` will also match `user`. Stemmer is language specific, so it is necessary to specify language of used stemmer.

All the available stemmers are in this [directory].(cpp_src/vendor/libstemmer/src_c)


## Merging queries results

It is possible to merge multiple queries results and sort final result by relevancy.

```go
    query := db.Query("items").
        Match("description","text query1")
    q2 := db.Query("another_items").
        Match("description","text query2")
    query.Merge(q2)
    iterator = query.Exec()
    // Check the error
    if err := iterator.Error(); err != nil {
        panic(err)
    }
    defer iterator.Close()
    // Iterate over results
    for iterator.Next() {
        // Get the next document and cast it to a pointer
        switch elem := iterator.Object().(type) {
            case Item:
                fmt.Printf ("%v,rank=%d\n",*elem,iterator.Rank())
            case AnotherItem:
                fmt.Printf ("%v,rank=%d\n",*elem,iterator.Rank())
        }
    }
```
## Using select functions
It is possible to use select functions to process result data.
For now, you can use snippet, snippet_n and highlight, debug_rank. For composite indexes the result of the function will be written in to corresponding subfields.
You can not put [,)\0] symbols in functions params. If the value contains special characters, it must be enclosed
in single quotes.

Notice: although text indexes may be created over numeric fields, select functions can not be applied to any non-string field.

For all the functions there are two types of supported syntax with the same behavior: `field.func_name(...)` and `field = func_name(...)`.

### Highlight
This functions just highlights text area that was found.
It has two arguments -
- `first` string that will be inserted before found text area
- `second` string that will be inserted after found text area

Example:
word: "some text"

```go
b.Query("items").Match("text", query).Limit(limit).Offset(offset).Functions("text.highlight(<b>,</b>)")
```
result: "some <b>text</b>"

### Snippet
Snippet highlights text area and erase other text.
It has six arguments - last two is default
- `first` string that will be inserted before found text area
- `second` string that will be inserted after found text area
- `third`  number of symbols that will be placed before area
- `fourth`  number of symbols that will be placed after area
- `fifth` delimiter before snippets, default nothing
- `sixth` delimiter after snippets, default space

Example:
word: "some text"

```go
b.Query("items").Match("text", query).Limit(limit).Offset(offset).Functions("text.snippet(<b>,</b>,2,0)")
```

result: "e <b>text</b>"

### Snippet_n

Snippet_n highlights text area and erase other text. More flexible version of the snippet function.
It has 4 position arguments and 5 named arguments. The named arguments are optional and can be passed in any order.

- `first` String that will be inserted before found text area
- `second` String that will be inserted after found text area
- `third`  Number of symbols that will be placed before area
- `fourth`  Number of symbols that will be placed after area
- `pre_delim` Named argument. Delimiter before snippets, default nothing
- `post_delim` Named argument. Delimiter after snippets, default space
- `with_area` Named argument. Takes the value 0 or 1. Print the beginning and end of the fragment relative to the
  beginning of the document in characters (UTF-8) in the format [B,E] after `pre_delim`
- `left_bound` Named argument. UTF-8 character string. The beginning of the fragment is the character from the
  string `left_bound` if it occurs before `third`
- `right_bound` Named argument. UTF-8 character string. The end of the fragment is a character from the
  string `right_bound` if it occurs before `fourth`

String values must be enclosed into single quotes.

Parameters' names may be specified without quotes or in double quotes.

Numbers may be passed without quotes or in single quotes.

Examples:
word: "some text string"

```go
b.Query("items").Match("text", query).Limit(limit).Offset(offset).Functions("text.snippet_n('<b>','</b>',2,2,pre_delim='{',post_delim='}',with_area=1)")
```

result: "{[3,11]e <b>text</b> s}"

```go
b.Query("items").Match("text", query).Limit(limit).Offset(offset).Functions("text.snippet_n('<b>','</b>',5,5,pre_delim='{',post_delim='}',left_bound='o',right_bound='i')")
```

result: "{me <b>text</b> str}"

### Debug_rank

This function outputs additional information about ranking of the found word in the text in the key-value format. Returned format and content may vary depending on reindexer's version. Works with `text`-index only.

## Typos algorithm

Reindexer finds misspelled words by matching terms with deleted symbols.
`MaxTypos` in fulltext index configuration limits how many symbols could be deleted in two words (in query and searching
document).
In each word could be deleted up to `MaxTypos / 2` with rounding to a large value.

### Typos handling details

Typos handling algorithm is language independent and simply checks possible word permutations. There are a bunch of parameters to tune it:

- MaxTypos - configures possible number of typos. Available values: [0, 4]. Larger values allow to handle more permutations, however will require much more RAM and will decrease search speed. Recommended (and default) value is 2.
  Behavior, depending on MaxTypos value:
  - 0 - typos are disabled.
  - 1 - allows to find words with 1 missing or 1 extra symbol. For example, query `world~` will match to `world`, `word` and `worlds`.
  - 2 - same as '1', but also allows to find words with 1 changed symbol. For example, query `sward~` will match to `sward`, `sword`, `ward` and `swards`.
  - 3 - same as '2', but also allows to find words with 1 changed symbol AND 1 extra/missing symbol at the same time. For example, query `sward~` will match to `sward`, `sword`, `swords`, `ward`, `wards`, `war` and `swards`.
  - 4 - same as '3', but also allows to find words with 2 changed symbols at the same time. For example, query `sward~` will match to `dword` (in addition to all the results, which it would match to with `MaxTypos == 3`).
- MaxTypoLen - Maximum word length for building and matching variants with typos. Larger values will also require more RAM for typos mappings.
- TyposDetailedConfig - those configs are used to fine-tune typo correction algorithm.
  - MaxMissingLetters - allows to configure maximum number of symbols, which may be removed from the initial term to transform it into the result word. Possible values are: [-1, 2]. Values, larger than `(MaxTypos / 2) + (MaxTypos % 2)` will be reduced to this value.
  - MaxExtraLetters - allows to configure maximum number of symbols, which may be added to the initial term to transform it into the result word. Possible values are: [-1, 2]. Values, larger than `(MaxTypos / 2) + (MaxTypos % 2)` will be reduced to this value.
  - MaxTypoDistance - allows to configure maximum distance between removed and added symbol in case of the symbols switch (requires MaxTypos >= 2).
    For example, with `MaxTypoDistance = -1` (means 'no limitations') and `MaxTypos == 2` query `dword~` will match to both `sword` and `words` (i.e. initial symbol may not only be changed, but also may be moved to any distance).
    And with `MaxTypoDistance = 0` (default) and `MaxTypos == 2` positions of the initial and result symbol must be the same. I.e. query `dword~` will match to `sword`, but will not match to `words`.
    Possible values for MaxTypoDistance: [-1,100].
  - MaxSymbolPermutationDistance - by design this parameter goes in pair with MaxTypoDistance. It allows to relax restrictions from MaxTypoDistance to be able to handle repositioning of the same symbol.
    For example, with `MaxSymbolPermutationDistance = 0`, `MaxTypoDistance = 0` and `MaxTypos == 2` query `wsord~` will not match to the word `sword`, because it requires to either switch 2 letters with each other or change 2 letters on the same positions.
    With `MaxSymbolPermutationDistance = 1`, this symbol permutation will be handled independent from `MaxTypoDistance = 0` and query `wsord~` will be able to receive `sword` as a result.

### More examples

`MaxTypos = 1` - 1 symbol could be deleted. `black` and `blaack` matches if delete excess `a` in the second
word. `black` and `block` do not match.

`MaxTypos = 2` - by 1 symbol could be deleted in each word (up to 2 symbols in sum). `black` and `blaack` match if delete excess `a` in the second word. `black` and `block` match if delete `a` in the first word and `o` in the second word. `black` and `blok` do not match.

`MaxTypos = 3` - 1 symbol in one word and 2 symbols in another one could be deleted (up to 3 symbols in sum). `black` and `blok` match if delete `ac` in the first word and `o` in the second word.

## Performance and memory usage

Internally reindexer uses enhanced suffix array of unique words, and compressed reverse index of documents. Typically size of index is about 30%-80% of source text. But can vary in corner cases.

The `Upsert` operation does not perform actual indexing, but just stores text. There are lazy indexing is implemented. So actually, full text index is building on first Query on fulltext field. The indexing is uses several threads, so it is efficiently utilizes resources of modern multicore CPU. Therefore, the indexing speed is very high. On modern hardware indexing speed is about ~50MB/sec

But on huge text size lazy indexing can seriously slow down first Query to text index. To avoid this side effect it is possible to warmup text index: just by dummy Query after last `Upsert`

## Configuration

Several parameters of full text search engine can be configured from application side. To set up configuration use `db.AddIndex` or `db.UpdateIndex` methods:

```go
...
    ftconfig := reindexer.DefaultFtFastConfig()
    // Setup configuration
    ftconfig.LogLevel = reindexer.TRACE
    // Setup another parameters
    // ...
    // Create index definition
    indexDef := reindexer.IndexDef {
        Name: "description",
        JSONPaths: []string{"description"},
        IndexType: "text",
        FieldType: "string",
        Config: ftconfig,
    }
    // Add index with configuration
    return db.AddIndex ("items",indexDef)

```

### Base config parameters

|   |    Parameter name     |   Type   |                                                                                                                                                            Description                                                                                                                                                            | Default value |
|---|:---------------------:|:--------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:-------------:|
|   |       Bm25Boost       |   float  | Boost of bm25 ranking                                                                                                                                                                                                                                                                                                             |       1       |
|   |      Bm25Weight       |   float  | Weight of bm25 rank in final rank 0: bm25 will not change final rank. 1: bm25 will affect to fin l rank in 0 - 100% range.                                                                                                                                                                                                        |      0.1      |
|   |     DistanceBoost     |   float  | Boost of search query term distance in found document.                                                                                                                                                                                                                                                                            |       1       |
|   |    DistanceWeight     |   float  | Weight of search query terms distance in found document in final rank 0: distance will not change final rank. 1: distance will affect to final rank in 0 - 100% range.                                                                                                                                                            |      0.5      |
|   |     TermLenBoost      |   float  | Boost of search query term length                                                                                                                                                                                                                                                                                                 |       1       |
|   |     TermLenWeight     |   float  | Weight of search query term length in final rank. 0: term length will not change final rank. 1: term length will affect to final rank in 0 - 100% range                                                                                                                                                                           |      0.3      |
|   |     PositionBoost     |   float  | Boost of search query term position                                                                                                                                                                                                                                                                                               |      1.0      |
|   |    PositionWeight     |   float  | Weight of search query term position in final rank. 0: term position will not change final rank. 1: term position will affect to final rank in 0 - 100% range                                                                                                                                                                     |      0.1      |
|   |    FullMatchBoost     |   float  | Boost of full match of search phrase with doc                                                                                                                                                                                                                                                                                     |      1.1      |
|   | PartialMatchDecrease  |    int   | Decrease of relevancy in case of partial match by value: partial_match_decrease * (non matched symbols) / (matched symbols)                                                                                                                                                                                                       |      15       |
|   |     MinRelevancy      |   float  | Minimum rank of found documents. 0: all found documents will be returned 1: only documents with relevancy >= 100% will be returned                                                                                                                                                                                                |     0.05      |
|   |       MaxTypos        |    int   | Maximum possible typos in word. 0: typos are disabled, words with typos will not match. N: words with N possible typos will match. Check [typos handling](#typos-handling-details) section for detailed description.                                                                                                              |       2       |
|   |    MaxTyposInWord     |    int   | Deprecated, use MaxTypos instead of this. Cannot be used with MaxTypos. Maximum possible typos in word. 0: typos is disabled, words with typos will not match. N: words with N possible typos will match. It is not recommended to set more than 1 possible typo -It will seriously increase RAM usage, and decrease search speed |       -       |
|   |      MaxTypoLen       |    int   | Maximum word length for building and matching variants with typos.                                                                                                                                                                                                                                                                |      15       |
|   | FtTyposDetailedConfig |  struct  | Config for more precise typos algorithm tuning                                                                                                                                                                                                                                                                                    |               | 
|   |    MaxRebuildSteps    |    int   | Maximum steps without full rebuild of ft - more steps faster commit slower select - optimal about 15.                                                                                                                                                                                                                             |      50       |
|   |      MaxStepSize      |    int   | Maximum unique words to step                                                                                                                                                                                                                                                                                                      |     4000      |
|   |      MergeLimit       |    int   | Maximum documents count which will be processed in merge query results. Increasing this value may refine ranking of queries with high frequency words, but will decrease search speed                                                                                                                                             |     20000     |
|   |       Stemmers        | []string | List of stemmers to use. Available values: "en", "ru", "nl", "fin", "de", "da", "fr", "it", "hu", "no", "pt", "ro", "es", "sv", "tr"                                                                                                                                                                                              |   "en","ru"   |
|   |    EnableTranslit     |   bool   | Enable russian translit variants processing. e.g. term "luntik" will match word "лунтик"                                                                                                                                                                                                                                          |     true      |
|   |    EnableKbLayout     |   bool   | Enable wrong keyboard layout variants processing. e.g. term "keynbr" will match word "лунтик"                                                                                                                                                                                                                                     |     true      |
|   |       StopWords       | []struct | List of objects of stopwords. Words from this list will be ignored when building indexes, but may be used in fulltext queries (such as 'word*', 'word~' etc) and produce non-empty search results. [More...](#stopwords-details)                                                                                                  |               |
|   | SumRanksByFieldsRatio |   float  | Ratio of summation of ranks of match one term in several fields                                                                                                                                                                                                                                                                   |      0.0      |
|   |       LogLevel        |    int   | Log level of full text search engine                                                                                                                                                                                                                                                                                              |       0       |
|   |       FieldsCfg       | []struct | Configs for certain fields. Overlaps parameters from main config. Contains parameters: FieldName, Bm25Boost, Bm25Weight, TermLenBoost, TermLenWeight, PositionBoost, PositionWeight.                                                                                                                                              |     empty     |
|   |   ExtraWordSymbols    |  string  | Extra symbols, which will be treated as parts of word to addition to letters and digits                                                                                                                                                                                                                                          |     "-/+_`'"     |
|   |     MaxAreasInDoc     |    int   | Max number of highlighted areas for each field in each document (for snippet() and highlight()). '-1' means unlimited                                                                                                                                                                                                             |       5       |
|   | MaxTotalAreasToCache  |    int   | Max total number of highlighted areas in ft result, when result still remains cacheable. '-1' means unlimited                                                                                                                                                                                                                     |      -1       |
|   |     Optimization      |  string  | Optimize the index by 'memory' or by 'cpu'                                                                                                                                                                                                                                                                                        |   "memory"    |
|   |     FtBaseRanking     |  struct  | Relevance of the word in different forms                                                                                                                                                                                                                                                                                          |               |
|   |      Bm25Config       |  struct  | Document ranking function parameters  [More...](#basic-document-ranking-algorithms)                                                                                                                                                                                                                                               |               |
|   |     SplitterType      |  string  | Text breakdown algorithm. Available values: 'mmseg_cn' and 'fast'                                                                                                                                                                                                                                                                    |    "fast"     |
|   |  WordPartDelimiters   |  string  | Symbols, which will be treated as word part delimiters     |    "-/+_`'"     |
|   |   MinWordPartSize     |    int   | Min word part size for indexing and searching     |      3       |

### Stopwords details
The list item can be either a string or a structure containing a string (the stopword) and a bool attribute (`is_morpheme`) indicating whether the stopword can be part of a word that can be shown in query-results.
If the stopword is set as a string, then the `is_morpheme` attribute is `false` by default and following entries are equivalent:
```json
"stop_words":[
    {
        "word": "some_word",
        "is_morpheme": false
    },
    ///...
]
``` 
,
```json
"stop_words":[
    "some_word",
    ///...
]
```

#### Example:
If the list of stopwords looks like this:
```json
"stop_words":[
    {
        "word": "under",
        "is_morpheme": true
    },
    ///...
]
```
and there are a pair of documents containing this word: `{"...under the roof ..."}, {"... to understand and forgive..."}`. Then for the query 'under*' we will get as a result only document `{"... to understand and forgive..."}` and for the query 'under'  we will get nothing as a result.

If the "StopWords" section is not specified in the config, then the [default_en](./cpp_src/core/ft/stopwords/stop_en.cc) and [default_ru](./cpp_src/core/ft/stopwords/stop_ru.cc) stopwords list will be used, and if it is explicitly specified empty, it means that there are no stopwords.

### Detailed typos config

FtTyposDetailedConfig: config for more precise typos algorithm tuning.

|   |       Parameter name         |   Type   |                                                                                                                        Description                                                                                                                        | Default value |
|---|:----------------------------:|:--------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:-------------:|
|   | MaxTypoDistance              |    int   | Maximum distance between symbols in initial and target words to perform substitution. Check [typos handling](#typos-handling-details) section for detailed description.                                                                                   |       0       |
|   | MaxSymbolPermutationDistance |    int   | aximum distance between same symbols in initial and target words to perform substitution (to handle cases, when two symbols were switched with each other). Check [typos handling](#typos-handling-details) section for detailed description.            |       1       |
|   | MaxMissingLetters            |    int   | Maximum number of symbols, which may be removed from the initial term to transform it into the result word. Check [typos handling](#typos-handling-details) section for detailed description.                                                             |       2       |
|   | MaxExtraLetters              |    int   | Maximum number of symbols, which may be added to the initial term to transform it into the result word. Check [typos handling](#typos-handling-details) section for detailed description.                                                                 |       2       |

### Base ranking config

FtBaseRanking: config for the base relevancy of the word in different forms.

|   |       Parameter name         |   Type   |                                                                                                                        Description                                                                                                                        | Default value |
|---|:----------------------------:|:--------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:-------------:|
|   | FullMatch                    |    int   | Relevancy of full word match                                                                                                                                                                                                                              |      100      |   
|   | PrefixMin                    |    int   | Minimum relevancy of prefix word match.                                                                                                                                                                                                                   |       50      |           
|   | SuffixMin                    |    int   | Minimum relevancy of suffix word match.                                                                                                                                                                                                                   |       10      | 
|   | Typo                         |    int   | Base relevancy of typo match                                                                                                                                                                                                                              |       85      |
|   | TypoPenalty                  |    int   | Extra penalty for each word's permutation (addition/deletion of the symbol) in typo algorithm. The minimum rank after applying penalties will be at least 1.                                                                                              |       15      |
|   | StemmerPenalty               |    int   | Penalty for the variants, created by stemming. The minimum rank after applying penalties will be at least 1.                                                                                                                                              |       15      |
|   | Kblayout                     |    int   | Relevancy of the match in incorrect kblayout                                                                                                                                                                                                              |       90      |
|   | Translit                     |    int   | Relevancy of the match in translit                                                                                                                                                                                                                        |       90      |
|   | Synonyms                     |    int   | Relevancy of synonyms match                                                                                                                                                                                                                               |       95      |
|   | Delimited                    |    int   |  Relevancy of the delimited word part match     |        80      |

### Text splitters
Reindexer supports two algorithms to break texts into words: `fast` and `mmseg_cn`.

Default `fast` algorithm is based on the definition of a word in the form of an alpha (from supported Unicode subset), number and an extended character, everything else (whitespaces, special characters, unsupported Unicode subsets, etc.) will be treated as a delimiters.

Reindexer supports the following unicode block codes / extra symbols:

- `Basic Latin`
- `Latin-1 Supplement`
- `Latin Extended-A`
- `Latin Extended-B`
- `Latin Extended Additional`
- `IPA Extensions`
- `Greek and Coptic`
- `Cyrillic`
- `Armenian`
- `Hebrew`
- `Arabic`
- `Devanagari`
- `Gujarati`
- `Georgian`
- `Hangul Jamo`
- `Greek Extended`
- `Enclosed Alphanumerics`
- `Hiragana`
- `Katakana`
- `Hangul Compatibility Jamo`
- `CJK Unified Ideographs`
- `Hangul Jamo Extended-A`
- `Hangul Syllables`
- `Hangul Jamo Extended-B`
- `Fullwidth Latin Forms` 
- Digits: '0'-'9'
- Extended characters: defined in the `ExtraWordSymbols` field of the text index config.

This algorithm is simple and provides high performance, but it can not handle texts without delimiters (for example, in Chinese, spaces between words are not required, so `fast`-splitter will not be able to index it properly). 

Alternative `mmseg_cn`-splitter is based on [friso](https://github.com/lionsoul2014/friso) implementation of `mmseg` algorithm and uses dictionaries for tokenization. Currently, this splitter supports only Chinese and English languages.


### Basic document ranking algorithms

For basic document ranking, one of the following algorithms can be used:

- `bm25`
- `bm25_rx`
- `word_count`

Calculation formula for `bm25`:

```
R = (log(totalDocCount / (matchedDocCount + 1)) + 1) * termCountInDoc / wordsInDoc * (k1 + 1.0) / (termCountInDoc / wordsInDoc + k1 * (1.0 - b_ + b_ * wordsInDoc / avgDocLen))
```

Calculation formula for `bm25_rx`:

```
R = (log(totalDocCount / (matchedDocCount + 1)) + 1) * termCountInDoc * (k1 + 1.0) / (termCountInDoc + k1 * (1.0 - b_ + b_ * wordsInDoc / avgDocLen))
```

Calculation formula for `word_count`:

```
R = termCountInDoc
```

- `totalDocCount` - total number of documents
- `matchedDocCount` - number of documents in which a subform of the word was found
- `termCountInDoc` - number of words found in the document for the query word subform
- `wordsInDoc` - number of words in document
- `k1` - free coefficient (Coefficient that sets the saturation threshold for the frequency of the term. The higher the coefficient, the higher the threshold and the lower the saturation rate.)
- `b` - free coefficient (If b is bigger, the effects of the length of the document compared to the average length are more amplified.) 

|   |    Parameter name     |   Type   |                             Description                                                 | Default value |
|---|:---------------------:|:--------:|:---------------------------------------------------------------------------------------:|:-------------:|
|   |   Bm25k1              |   float  | Coefficient k1 in the formula for calculating bm25 (used only for rx_bm25, bm25).       |      2.0      |                                          	
|   |   Bm25b               |   float  | Coefficient b in the formula for calculating bm25 (used only for rx_bm25, bm25).        |     0.75      |                     
|   |  Bm25Type             |  string  | Formula for calculating document relevance (rx_bm25, bm25, word_count)                  |   "rx_bm25"   |                                   



### Limitations and know issues

- Results of full text search is always sorted by relevancy.
- It is not possible to combine 2 Match in single Query. (but actually this can be done by composite indexes or by Merge functional)
