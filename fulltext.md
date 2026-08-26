# Full-text search with Reindexer

Reindexer has builtin full text search engine. This document describes usage of full text search.

- [Define full text index fields](#define-full-text-index-fields)
- [Query to full text index](#query-to-full-text-index)
- [Text query format](#text-query-format)
    - [Terms and subterms](#terms-and-subterms)
    - [Patterns](#patterns)
    - [Field selection](#field-selection)
    - [Binary operators](#binary-operators)
    - [Escape character](#escape-character)
    - [Phrase search](#phrase-search)
- [Examples of text queries](#examples-of-text-queries)
- [Using select functions](#using-select-functions)
    - [Highlight](#highlight)
    - [Snippet](#snippet)
    - [Snippet_n](#snippet_n)
    - [Debug_rank](#debug_rank)
        - [What debug_rank fields mean](#what-debug_rank-fields-mean)
- [Merging queries results](#merging-queries-results)
- [Natural language processing](#natural-language-processing)
- [Typos algorithm](#typos-algorithm)
    - [Typos handling details](#typos-handling-details)
    - [More examples](#more-examples)
- [Configuration](#configuration)
    - [Base config parameters](#base-config-parameters)
    - [Wrong keyboard layout](#wrong-keyboard-layout)
    - [Text splitters](#text-splitters)
    - [Stopwords details](#stopwords-details)
    - [Detailed typos config](#detailed-typos-config)
    - [Base ranking config](#base-ranking-config)
    - [Ranking pipeline](./cpp_src/core/ft/ft_fast/fulltext_ranking.md)
    - [Basic document ranking algorithms](#basic-document-ranking-algorithms)
        - [bm25 and rx_bm25](#bm25-and-rx_bm25)
        - [word_count](#word_count)
    - [Limitations and known issues](#limitations-and-known-issues)
- [Performance and memory usage](#performance-and-memory-usage)
- [LIKE](#like)

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
    _ struct{}         `reindex:"name+description=text_search,text,composite"`
}
```
In this example the full-text index will include fields `name` and `description`; `text_search` is a short alias of the composite index name for use in Queries.

The full-text index is case-insensitive. With the default `fast` splitter, text is tokenized into words: a maximal sequence of letters from a supported Unicode subset, digits `0`–`9`, and symbols from `ExtraWordSymbols` (default: ``-/+_`'` ``). All other characters are delimiters. Word boundaries and extra symbols are configurable; see [Text splitters](#text-splitters).

## Query to full text index

Queries to the full-text index are constructed using the standard query interface.

```go
    query := db.Query ("items").
        Match ("name+description","text query")
```
Or equivalent query using name alias:

```go
    query := db.Query ("items").
        Match ("text_search","text query")
```

Queries to the full-text index can be combined with conditions on other fields, e.g.:

```go
    query := db.Query ("items").
        Match ("description","text query").
        WhereInt("year",reindexer.GT,2010)
```

Each query result contains a match rank. The rank is an integer from 0 to 255, where 0 is the lowest relevancy and 255 is the highest. The query Iterator has a `Rank()` method, which returns the rank of the current result.

## Text query format

### Terms and subterms

- **Term** — one token of the DSL query (`fast`, `black~`, `termina*`, a word inside a phrase, …).
- **Subterm** — one concrete word variant generated for a term and looked up in the index (exact form, typo, stem, translit, synonym, split/concat part, …). Ranking scores subterm occurrences; see [Terms and subterms](./cpp_src/core/ft/ft_fast/fulltext_ranking.md#terms-and-subterms).

The format of query is:

`query := [@[+]field[^boost][,field2[^boost]]]    [=][*]term1[*][~][^boost] [+|-][*]term2[*][~][^boost] ...`

### Patterns
- `*` - match any symbols. For example, `termina*` matches words `terminator` and `terminal`. `*` Can be at word start or at word end, but can not be at word middle.
- `~` - fuzzy match misspelled word by typos' dictionary. For example, `black~` watches words `block`, `blck`, or `blask`. `~`. The typos dictionary is contains all words in index with 1 possible mistake. If `~` combined with `*`, then it means mistake match or prefix match, but not prefix with mistake. For example, `turmin*~` is matches `turminals`, `termin`, but not `terminal`
- `^x` - boost matches term by x. default boost value is 1.

### Field selection

A query may start with a field selector:

`@field[,field2...] term`

- `@` - starts a comma-separated list of full-text fields to search.
- `*` - selects all fields that were not specified explicitly.
- `^x` - applies field boost `x` to matches in this field. Default boost is 1.
- `+field` - marks this field for rank summation. This `+` is different from the `+term` binary operator.

By default, when the same query term matches several fields, the term rank is the maximum rank among those fields.

If `SumRanksByFieldsRatio = K > 0` and some fields are marked with `+`, ranks from the marked fields are added to the winner with decreasing weights:

`R = Rmax + K*R1 + K*K*R2 + ...`

where `R1`, `R2`, ... are ranks from `+` fields sorted from highest to lowest. If the winner field is also marked with `+`, it is not added twice.

Example: for `@f1,+f2,f3,+f4 term`, if ranks are `R1 < R2 < R3 < R4`, then `R = R4 + K*R2` (`f4` is the winner and `f2` is the remaining `+` field). If `R2 < R3 < R4 < R1`, then `R = R1 + K*R4 + K*K*R2` (`f1` is the winner, while `f4` and `f2` are marked with `+`).

### Binary operators

Terms without an explicit operator are optional and are combined with OR: a document may match any of them.

- `+term` - the term or phrase must be present in the found document.
- `-term` - the term or phrase must not be present in the found document.

For example, `fox +fast -slow` finds documents that must contain `fast`, must not contain `slow`, and may also contain `fox`.

### Escape character

Use `\` to search for a DSL special character as a literal part of a term. This is useful for characters such as `+`, `-`, `@`, `*`, `^`, `~`, `=`, quotes, or `\` itself.

For example, `\*crisis` searches for the literal word `*crisis`, not for all words ending with `crisis`.

The escaped character must be listed in `ExtraWordSymbols`; otherwise it is still treated as a word separator and will not become part of the indexed term.

### Phrase search

A DSL operand may be a phrase enclosed in double or single quotes:

`"word1 word2 ..."[~N]`

or

`'word1 word2 ...'[~N]`

The words inside the phrase must appear in the same order. For example, `"word1 word2"` will not match a document containing only `word2 word1`.

`~N` sets the maximum distance, in word positions, between adjacent phrase terms. This argument is optional; the default value is `N = 1`.

To search for the same phrase words in any order, specify all required permutations explicitly, for example: `"word1 word2"~5 "word2 word1"~5`.

Synonyms of multiple words are not supported in the phrase.

## Examples of text queries

- `termina* -genesis` - find documents that contain words beginning with `termina`, and exclude documents that contain the word `genesis`
- `black~` - find documents that contain the word `black` with one possible typo, e.g. `block`, `blck`, or `blask`
- `tom jerry cruz^2` - find documents that contain at least one of the words `tom`, `jerry`, or `cruz`. Documents that contain `tom cruz` will have higher relevancy than documents that contain `tom jerry`
- `fox +fast` - find documents that must contain `fast` and may also contain `fox`; documents containing both terms rank higher
- `"one two"` - find documents with the phrase `one two`
- `"one two"~5` - find documents with the words `one` and `two` at a distance of at most 5 words. Word order matters.
  If you need to search those terms in any order, all the required permutations must be explicitly specified in
  the DSL: `"one two"~5 "two one"~5`
- `@name rush` - find documents with the word `rush` only in the `name` field
- `@name^1.5,* rush` - find documents with the word `rush`, and boost matches in the `name` field by 1.5 compared to all other full-text fields (`*`)
- `=windows` - find documents with the exact term `windows` without language-specific term variants (stemmers, translit, wrong keyboard layout)
- `one -"phrase example"` - find documents containing the word `one` but not containing the phrase `"phrase example"`
- `one "phrase example"` - find documents containing the word `one`, the phrase `"phrase example"`, or both
- `+one +"phrase example"` - find documents that must contain both the word `one` and the phrase `"phrase example"`
- `one "phrase example"~3` - find documents containing the word `one`, the phrase `"phrase example"` (with at most 3 words between `phrase` and `example`), or both
- `\*crisis` - find documents containing the literal word `*crisis`. The escaped character must be present in `ExtraWordSymbols` for correct searching
- `*level*` - find documents containing words with `level` as a substring, e.g. `sslevel1`, `level2`, or `level`
- `'long nose'~3` - same as `"long nose"~3`: find documents with the words `long` and `nose` at a distance of at most 3 words

## Using select functions
It is possible to use select functions to process result data.
For now, you can use snippet, snippet_n and highlight, debug_rank. For composite indexes the result of the function will be written in to corresponding subfields.
You can not put [,)\0] symbols in functions params. If the value contains special characters, it must be enclosed
in single quotes.

Notice: although text indexes may be created over numeric fields, select functions can not be applied to any non-string field.

For all the functions there are two types of supported syntax with the same behavior: `field.func_name(...)` and `field = func_name(...)`.

### Highlight
This function highlights the text area that was found.
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

More flexible version of `snippet`. It has 4 position arguments and 5 named arguments. The named arguments are optional and can be passed in any order.

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

Example:

```go
b.Query("items").Match("text", "masha").Functions("text.debug_rank()")
```

result: `{term_rank:97, term:маша, pattern:маша, bm25_norm:0.98, term_len_boost:1, position_rank:1, norm_dist:0, proc:100, full_match_boost:0} Маша ела кашу.`

#### What debug_rank fields mean

| Field | Description |
|-------|-------------|
| `term_rank` | Score of this subterm occurrence after BM25, position, term length, query boost, and field boost (`occurrenceScore` in [Score of one subterm occurrence](./cpp_src/core/ft/ft_fast/fulltext_ranking.md#score-of-one-subterm-occurrence)). |
| `term` | Original query term (DSL token) that produced the match. |
| `pattern` | Actual indexed word or subterm variant that matched (e.g. stemmed form). |
| `bm25_norm` | Normalized BM25 score for this match: `(1 - bm25Weight) + bm25 * bm25Boost * bm25Weight`. |
| `term_len_boost` | Boost from query term length, blended with per-field `termLenWeight` and `termLenBoost`. |
| `position_rank` | Boost from word position in the field (earlier positions score higher). |
| `norm_dist` | Distance factor between this term and the previous query term in a multi-term or phrase query. Zero for the first term. |
| `proc` | Base relevancy (`subterm.Proc()`) of the matched word variant before BM25 and position adjustments. See [How term variants are scored](./cpp_src/core/ft/ft_fast/fulltext_ranking.md#how-term-variants-are-scored). |
| `full_match_boost` | Extra multiplier when the document contains a full match of the entire query (see [How document rank is built](./cpp_src/core/ft/ft_fast/fulltext_ranking.md#how-document-rank-is-built)). |

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

## Natural language processing

Built-in stemmer support is available in full-text search. It enables natural language search of words with same stem. For example, query `users` will also match `user`. Stemmer is language specific, so it is necessary to specify language of used stemmer.

All the available stemmers are in this [directory](cpp_src/vendor/libstemmer/src_c).

## Typos algorithm

Reindexer handles typos with a language-independent deletion-based algorithm: indexed words and query terms are matched via variants produced by removing characters. Substitutions and extra/missing letters are modeled by comparing these deletion skeletons. `MaxTypos` limits the total number of deletions counted across the query term and the matched indexed word. For each word, up to `MaxTyposInWord = (MaxTypos / 2) + (MaxTypos % 2)` characters may be removed when building variants.

Typos apply only to query terms with the `~` suffix. Words shorter than 3 characters are not indexed for typos; words longer than `MaxTypoLen` (default: 15) are excluded.

### Typos handling details

Parameters to tune the algorithm:

- MaxTypos - configures the typo budget. Available values: [0, 4]. Larger values allow more permutations, but require more RAM and decrease search speed. Recommended (and default) value is 2.
  Behavior, depending on MaxTypos value:
  - 0 - typos are disabled.
  - 1 - allows finding words with 1 missing or 1 extra symbol. For example, the query `world~` will match `world`, `word`, and `worlds`.
  - 2 - same as '1', but also allows finding words with 1 changed symbol. For example, the query `sward~` will match `sward`, `sword`, `ward`, and `swards`.
  - 3 - same as '2', but also allows finding words with 1 changed symbol AND 1 extra/missing symbol at the same time. For example, the query `sward~` will match `sward`, `sword`, `swords`, `ward`, `wards`, `war`, and `swards`.
  - 4 - same as '3', but also allows finding words with 2 changed symbols at the same time. For example, the query `sward~` will match `dword` (in addition to all the results that it would match with `MaxTypos == 3`).
- MaxTypoLen - maximum word length for building and matching typo variants (default: 15). Larger values also require more RAM for typo mappings.
- TyposDetailedConfig - those configs are used to fine-tune typo correction algorithm.
  - MaxMissingLetters - maximum allowed difference in deletion count when the indexed word has more deletions than the query typo variant. Possible values: [-1, 2]. `-1` means no additional limit beyond `MaxTypos`.
  - MaxExtraLetters - maximum allowed difference in deletion count when the query typo variant has more deletions than the indexed word. Possible values: [-1, 2]. `-1` means no additional limit beyond `MaxTypos`.
  - MaxTypoDistance - configures the maximum distance between the removed and the added symbol in case of a symbol switch (requires MaxTypos >= 2).
    For example, with `MaxTypoDistance = -1` ('no limitations') and `MaxTypos == 2`, the query `dword~` will match both `sword` and `words` (i.e. the initial symbol may not only be changed, but also moved to any distance).
    With `MaxTypoDistance = 0` (default) and `MaxTypos == 2`, the positions of the initial and the resulting symbol must be the same. I.e. the query `dword~` will match `sword`, but will not match `words`.
    Possible values for MaxTypoDistance: [-1,100].
  - MaxSymbolPermutationDistance - by design, this parameter goes in pair with MaxTypoDistance. It relaxes restrictions from MaxTypoDistance to handle the repositioning of the same symbol.
    For example, with `MaxSymbolPermutationDistance = 0`, `MaxTypoDistance = 0` and `MaxTypos == 2`, the query `wsord~` will not match the word `sword`, because it requires either switching 2 letters with each other or changing 2 letters at the same positions.
    With `MaxSymbolPermutationDistance = 1`, this symbol permutation will be handled independently from `MaxTypoDistance = 0`, and the query `wsord~` will match `sword`.

Typo matches use base relevancy from `Typo` and `TypoPenalty` in [Base ranking config](#base-ranking-config); see [How term variants are scored](./cpp_src/core/ft/ft_fast/fulltext_ranking.md#how-term-variants-are-scored).

### More examples

`MaxTypos = 1` - one symbol may be deleted. `black` and `blaack` match if the excess `a` is deleted in the second
word. `black` and `block` do not match.

`MaxTypos = 2` - up to one symbol may be deleted in each word (2 symbols in total). `black` and `blaack` match if the excess `a` is deleted in the second word. `black` and `block` match if `a` is deleted in the first word and `o` in the second word. `black` and `blok` do not match.

`MaxTypos = 3` - up to 2 symbols may be deleted in one word and 1 in the other (3 symbols in total). `black` and `blok` match if `ac` is deleted in the first word and `o` in the second word.

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
|   |       Bm25Boost       |   float  | Boost of BM25 ranking                                                                                                                                                                                                                                                                                                             |       1       |
|   |      Bm25Weight       |   float  | Weight of BM25 rank in final rank. 0: BM25 will not change final rank. 1: BM25 will affect final rank in the 0 - 100% range.                                                                                                                                                                                                      |      0.1      |
|   |     DistanceBoost     |   float  | Boost of search query term distance in the found document.                                                                                                                                                                                                                                                                        |       1       |
|   |    DistanceWeight     |   float  | Weight of search query term distance in final rank. 0: distance will not change final rank. 1: distance will affect final rank in the 0 - 100% range.                                                                                                                                                                             |      0.5      |
|   |     TermLenBoost      |   float  | Boost of search query term length                                                                                                                                                                                                                                                                                                 |       1       |
|   |     TermLenWeight     |   float  | Weight of search query term length in final rank. 0: term length will not change final rank. 1: term length will affect final rank in the 0 - 100% range                                                                                                                                                                          |      0.3      |
|   |     PositionBoost     |   float  | Boost of search query term position                                                                                                                                                                                                                                                                                               |      1.0      |
|   |    PositionWeight     |   float  | Weight of search query term position in final rank. 0: term position will not change final rank. 1: term position will affect final rank in the 0 - 100% range                                                                                                                                                                    |      0.1      |
|   |    FullMatchBoost     |   float  | Boost for documents containing a full match of the search phrase                                                                                                                                                                                                                                                                  |      1.1      |
|   | PartialMatchDecrease  |    int   | Penalty for prefix/suffix partial matches: `partial_match_decrease * non_matched_symbols / max(matched_symbols, 3)`, limited by `PrefixMin` / `SuffixMin` from [Base ranking config](#base-ranking-config)                                                                                                                       |      15       |
|   |     MinRelevancy      |   float  | **Deprecated**. Use `MinRank` instead. Minimum rank of found documents. 0: all found documents will be returned; 1: only documents with relevancy >= 100% will be returned                                                                                                                                                                                                |     0.05      |
|   |       MinRank         |    int   | Minimum rank of found documents. 0: all found documents will be returned; 255: only documents with relevancy == 255 will be returned                                                                                                                                                                                                |      5        |
|   |       MaxTypos        |    int   | Maximum typo budget. 0: typos are disabled, words with typos will not match. N: words with N possible typos will match. Check [typos handling](#typos-handling-details) section for detailed description.                                                                                                                        |       2       |
|   |    MaxTyposInWord     |    int   | **Deprecated**. Use `MaxTypos` instead. Cannot be used together with `MaxTypos`. It is not recommended to set more than 1 possible typo per word: it will seriously increase RAM usage and decrease search speed                                                     |       -       |
|   |      MaxTypoLen       |    int   | Maximum word length for building and matching variants with typos.                                                                                                                                                                                                                                                                |      15       |
|   | FtTyposDetailedConfig |  struct  | Config for more precise typos algorithm tuning                                                                                                                                                                                                                                                                                    |               | 
|   |      MergeLimit       |    int   | Maximum documents count which will be processed in merge query results. Increasing this value may refine ranking of queries with high frequency words, but will decrease search speed. For a single-term prefix/suffix query the engine may also stop collecting matches after about `2 × MergeLimit` document hits |     20000     |
|   |       Stemmers        | []string | List of stemmers to use. More about [stemming](https://en.wikipedia.org/wiki/Stemming). Available values: "en", "ru", "nl", "fin", "de", "da", "fr", "it", "hu", "no", "pt", "ro", "es", "sv", "tr"                                                                                                                                                                                              |   "en","ru"   |
|   |    EnableTermsConcat  |   bool   | Enable concatenated terms processing. e.g. terms "di caprio" will match word "dicaprio"                                                                                                                                                                                                                                          |     true      |
|   |    EnableTermsSplit   |   bool   | Enable splitting query terms. e.g. term "dicaprio" will match words "di" and "caprio". Also splits a number at the beginning or end of a term, e.g. "season1" produces the "season 1" variant. Terms with '-' and '+' prefixes will not be split                                                                                  |     true      |
|   |    EnableTranslit     |   bool   | Enable russian translit variants processing. e.g. term "luntik" will match word "лунтик"                                                                                                                                                                                                                                          |     true      |
|   |    EnableKbLayout     |  string  | Wrong keyboard layout variants: `"disable"`, `"enable"`, or `"heuristic"` (default). See [Wrong keyboard layout](#wrong-keyboard-layout)                                                                                                                                                                                          |  "heuristic"  |
|   | EnableNumbersSearch   |   bool   | Enable number variants processing. e.g. term "100" may match words "one hundred"                                                                                                                                                                                                                                                  |     false     |
|   |       StopWords       | []struct | List of stop words. At index time, stop words are not indexed as standalone terms. At query time, behavior depends on `is_morpheme`; see [Stopwords details](#stopwords-details).                                                                                    |               |
|   | SumRanksByFieldsRatio |   float  | Ratio used to add ranks when the same term matches several fields selected with `+` in the field selector                                                                                                                                                                                                                         |      0.0      |
|   |       LogLevel        |    int   | Log level of full text search engine                                                                                                                                                                                                                                                                                              |       0       |
|   |       FieldsCfg       | []struct | Configs for certain fields in composite full-text indexes. Overrides parameters from the main config. Contains parameters: FieldName, Bm25Boost, Bm25Weight, TermLenBoost, TermLenWeight, PositionBoost, PositionWeight.                                                                                                         |     empty     |
|   |   ExtraWordSymbols    |  string  | Extra symbols that will be treated as word parts in addition to letters and digits. `WordPartDelimiters` are automatically added to this set. See [Text splitters](#text-splitters)                                                                                                                                                |  ``-/+_`'``   |
|   |   KeepDiacritics      | []string | List of symbol types for which diacritics should be kept. Supported values: `acc` / `accent`, `ara` / `arabic`, `heb` / `hebrew`, `cyr` / `cyrillic`                                                                                                                                                                            |     empty     |
|   |     Synonyms          | []struct | A list of synonyms to be used in full text search                                                                                                                                                                                                                                                                                 |               |
|   |     TermsBoost        | []struct | A list of terms boosts to be used in full text search. The relevance of each term will be increased proportionally to the specified boost factor. For each term, all possible word forms generated using all the stemmers listed in `Stemmers` are boosted                                                                         |               |
|   |     MaxAreasInDoc     |    int   | Max number of highlighted areas for each field in each document (for snippet() and highlight()). '-1' means unlimited                                                                                                                                                                                                             |       5       |
|   | MaxTotalAreasToCache  |    int   | Max total number of highlighted areas in ft result, when result still remains cacheable. '-1' means unlimited                                                                                                                                                                                                                     |      -1       |
|   |     Optimization      |  string  | Optimize the index by `memory` or by `cpu`. `memory` uses compressed document id vectors and less RAM; `cpu` uses uncompressed vectors and may provide faster selection                                                                                                                                                           |   "memory"    |
|   |     FtBaseRanking     |  struct  | Base relevancy of term variants before BM25 and field boosts. See [Base ranking config](#base-ranking-config) and [How term variants are scored](./cpp_src/core/ft/ft_fast/fulltext_ranking.md#how-term-variants-are-scored)                                                                                                                              |               |
|   |      Bm25Config       |  struct  | Document ranking function parameters. See [Basic document ranking algorithms](#basic-document-ranking-algorithms)                                                                                                                                                                                                                 |               |
|   |     SplitterType      |  string  | Text breakdown algorithm. Available values: 'mmseg_cn' and 'fast'                                                                                                                                                                                                                                                                    |    "fast"     |
|   |  WordPartDelimiters   |  string  | Symbols that will be treated as delimiters inside words. Delimited parts with at least `MinWordPartSize` symbols are indexed and searched separately. See [Text splitters](#text-splitters)                                                                          |  ``-/+_`'``   |
|   |   MinWordPartSize     |    int   | Minimum word part size for indexing and searching delimited word parts                                                                                                                                                                                                                                                            |      3       |
|   | EnablePreselectBeforeFt |  bool  | If true, then non-fulltext filtering conditions will be executed before fulltext index selection     |    false     |

### Wrong keyboard layout

`EnableKbLayout` controls generation of wrong-keyboard-layout variants (for example query `keynbr` matching indexed `лунтик`).

Available values:

- `"disable"` — never generate kb-layout variants
- `"enable"` — always generate kb-layout variants (when the pattern is eligible)
- `"heuristic"` (default) — always generate for exact terms; for prefix/suffix terms skip kb-layout when the query already matches too broadly in the dictionary

Additional rules:

- Prefix/suffix patterns of **3 characters or fewer** never get kb-layout correction, even when the mode is `"enable"` or `"heuristic"`.
- Use `"enable"` if you need kb-layout variants for every eligible prefix/suffix query; `"heuristic"` may skip them for broad matches.

### Text splitters

Reindexer supports two algorithms to break texts into words: `fast` and `mmseg_cn`.

With the default `fast` splitter, a word is a maximal run of letters from the supported Unicode subset below, digits `0`–`9`, and symbols from `ExtraWordSymbols`. Everything else (whitespace, punctuation, unsupported Unicode subsets, and so on) is treated as a delimiter.

Reindexer supports the following Unicode blocks and extra symbols:

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
- Digits: `0`–`9`
- Extra symbols: defined in the `ExtraWordSymbols` field of the text index config (`WordPartDelimiters` are added to this set automatically).

Symbols from `WordPartDelimiters` split a word into parts during indexing and search. Each part with at least `MinWordPartSize` characters is indexed separately, and the word without delimiters is indexed as well. For example, with default delimiters ``-/+_`'`` and `MinWordPartSize = 3`, the text `foo-bar` is indexed as `foo`, `bar`, and `foobar`.

This algorithm is simple and provides high performance, but it can not handle texts without delimiters (for example, in Chinese, spaces between words are not required, so `fast`-splitter will not be able to index it properly). 

Alternative `mmseg_cn`-splitter is based on [friso](https://github.com/lionsoul2014/friso) implementation of `mmseg` algorithm and uses dictionaries for tokenization. Currently, this splitter supports only Chinese and English languages.

### Stopwords details

Stop words reduce noise from very frequent words. Behavior differs between indexing and querying:

- **Index time.** Any configured stop word is skipped as a standalone token when building the main full-text index. This applies to both `is_morpheme: false` and `is_morpheme: true`.
- **Query time.** Behavior depends on `is_morpheme`:
  - `is_morpheme: false` — the term is removed from the query entirely, including forms with `*`, `~`, or `+`.
  - `is_morpheme: true` — the term stays in the query, but an exact match usually finds nothing because the standalone token is not indexed. Prefix/suffix (`word*`), typo (`word~`), and similar non-exact modes can still match longer words that contain the stop word.

Each list item can be either a string or an object with `word` and `is_morpheme`. If the stop word is set as a string, `is_morpheme` defaults to `false`. The following entries are equivalent:

```json
"stop_words": [
    {
        "word": "some_word",
        "is_morpheme": false
    }
]
```

```json
"stop_words": [
    "some_word"
]
```

Stop words are normalized on load (lowercased, diacritics removed). Spaces are not allowed. Duplicate entries with different `is_morpheme` values are rejected.

#### Example

If the stop words list is:

```json
"stop_words": [
    {
        "word": "under",
        "is_morpheme": true
    }
]
```

and there are two documents: `"...under the roof..."` and `"...to understand and forgive..."`, then:

- `under*` returns only `"...to understand and forgive..."`;
- `under` returns nothing.

With `is_morpheme: false`, both `under` and `under*` are removed from the query and do not participate in search.

If `stop_words` is omitted from the config, the [default_en](./cpp_src/core/ft/stopwords/stop_en.cc) and [default_ru](./cpp_src/core/ft/stopwords/stop_ru.cc) lists are used. All words in these default lists have `is_morpheme: true`. An explicitly empty `stop_words` array disables stop words completely.

### Detailed typos config

FtTyposDetailedConfig: config for fine-tuning typo correction. These parameters do not increase the total `MaxTypos` budget; they only add more restrictions to specific typo shapes. For all fields, `-1` means "no additional limit". `MaxTypoDistance` and `MaxSymbolPermutationDistance` matter mainly when `MaxTypos >= 2`. See [typos handling](#typos-handling-details) for examples.

|   |       Parameter name         |   Type   |                                                                                                                        Description                                                                                                                        | Default value |
|---|:----------------------------:|:--------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:-------------:|
|   | MaxTypoDistance              |    int   | Maximum allowed positional shift when a changed symbol in the query variant and the matched indexed word occupy different positions within the deletion-based typo model. Range: [-1, 100]. `-1` means no distance limit. See [typos handling](#typos-handling-details). |       0       |
|   | MaxSymbolPermutationDistance |    int   | Maximum allowed positional shift for the same symbol when a typo match is explained by moving one letter to another position (for example `wsord` → `sword`). Range: [-1, 100]. `-1` means no distance limit. See [typos handling](#typos-handling-details). |       1       |
|   | MaxMissingLetters            |    int   | Maximum allowed difference in deletion count when the indexed word has more deletions than the query typo variant, within the total `MaxTypos` budget. Range: [-1, 2]. `-1` means no additional limit beyond `MaxTypos`.                                  |       2       |
|   | MaxExtraLetters              |    int   | Maximum allowed difference in deletion count when the query typo variant has more deletions than the indexed word, within the total `MaxTypos` budget. Range: [-1, 2]. `-1` means no additional limit beyond `MaxTypos`.                                    |       2       |

### Base ranking config

FtBaseRanking configures the base relevancy (`proc`) of generated query term variants before BM25, field boosts, position rank, and distance merging are applied. See [How term variants are scored](./cpp_src/core/ft/ft_fast/fulltext_ranking.md#how-term-variants-are-scored) and [How document rank is built](./cpp_src/core/ft/ft_fast/fulltext_ranking.md#how-document-rank-is-built) for how these values are applied.

All values are integers in the range [0, 500]. Parameters fall into three groups:

- **Absolute `proc` values:** `FullMatch`, `ConcatProc`, `PrefixMin`, `SuffixMin`
- **Coefficients** (`configValue / FullMatch`, capped at 1.0): `Typo`, `Kblayout`, `Translit`, `Synonyms`, `SplitProc`, `Delimited`
- **Penalties** (subtracted from `proc`, minimum result is 1): `TypoPenalty`, `StemmerPenalty`

Values above `FullMatch` are accepted, but coefficient-based variants are capped at 1.0 and therefore cannot become more relevant than the source variant.

|   |       Parameter name         |   Type   | Description                                                                                                                                         | Default value |
|---|:----------------------------:|:--------:|:---------------------------------------------------------------------------------------------------------------------------------------------------:|:-------------:|
|   | FullMatch                    |    int   | Base `proc` for an exact full word match; normally the baseline for other `proc` values                                                              |      100      |
|   | ConcatProc                   |    int   | Absolute base `proc` for concatenated terms, e.g. query `di caprio` matching indexed word `dicaprio`                                                |      90       |
|   | SplitProc                    |    int   | Relevancy coefficient for terms split by `EnableTermsSplit`, e.g. query `dicaprio` matching indexed words `di` and `caprio`. Each split part gets `(parent_proc / 2) * (SplitProc / FullMatch)` |      90       |
|   | PrefixMin                    |    int   | Minimum `proc` for prefix partial matches after `PartialMatchDecrease` is applied                                                                    |       20      |
|   | SuffixMin                    |    int   | Minimum `proc` for suffix partial matches after `PartialMatchDecrease` is applied                                                                    |       10      |
|   | Typo                         |    int   | Base relevancy coefficient for typo variants (`Typo / FullMatch`, capped at 1.0) before `TypoPenalty` is applied                                     |       85      |
|   | TypoPenalty                  |    int   | Penalty applied per typo operation. The penalty is scaled down for longer words; final `proc` is at least 1                                           |       15      |
|   | StemmerPenalty               |    int   | Penalty for variants created by stemming. Final `proc` is at least 1                                                                                 |       15      |
|   | Kblayout                     |    int   | Base relevancy coefficient for variants generated by wrong keyboard layout correction                                                                |       90      |
|   | Translit                     |    int   | Base relevancy coefficient for transliterated variants                                                                                               |       90      |
|   | Synonyms                     |    int   | Base relevancy coefficient for synonym variants                                                                                                      |       95      |
|   | Delimited                    |    int   | Relevancy coefficient for query terms split by `WordPartDelimiters`, e.g. `foo-bar` → `foo` and `bar` (`proc * Delimited / FullMatch`)              |        80     |

For the full ranking pipeline — how variant `proc` is computed, how term ranks are merged, and how the final 0–255 document score is produced — see [fulltext_ranking.md](./cpp_src/core/ft/ft_fast/fulltext_ranking.md).

### Basic document ranking algorithms

Document-level term frequency scoring uses one of the algorithms selected by `Bm25Type`:

- `rx_bm25` (default)
- `bm25`
- `word_count`

|   |    Parameter name     |   Type   |                             Description                                                 | Default value |
|---|:---------------------:|:--------:|:---------------------------------------------------------------------------------------:|:-------------:|
|   |   Bm25k1              |   float  | `k1` saturation coefficient (`bm25` and `rx_bm25` only)                                 |      2.0      |
|   |   Bm25b               |   float  | `b` length-normalization coefficient (`bm25` and `rx_bm25` only)                        |     0.75      |
|   |  Bm25Type             |  string  | Scoring algorithm: `rx_bm25`, `bm25`, or `word_count`                                   |   "rx_bm25"   |

#### bm25 and rx_bm25

Both algorithms use the same general form:

```
R = IDF * tf * (k1 + 1) / (tf + k1 * (1 - b + b * L / avgL))
```

The fraction is the BM25 term-frequency component. It grows as `tf` increases, but with diminishing returns (it saturates toward `k1 + 1`). Field-length normalization is applied only through the `b * L / avgL` part of the denominator — not by dividing `tf` by `L`.

Formula members:

- `tf` — term frequency in the document field: `termCountInDoc` (raw number of occurrences of the subterm in the field). The same for `bm25` and `rx_bm25`. More occurrences increase `R`, but each additional occurrence adds less than the previous one.
- `k1` — saturation coefficient (`Bm25k1` in config); higher values slow down score growth as `tf` increases. Default: `2.0`. Allowed range: `>= 0` (no upper limit is enforced)
- `k1 + 1` — saturation ceiling for the term-frequency component
- `L` — total number of words in the document field
- `avgL` — average field length across the index
- `L / avgL` — relative field length: current field length divided by the average field length in the index
- `b` — length-normalization weight; `0` ignores document length, `1` applies full normalization
- `1 - b + b * L / avgL` — length-normalization factor blended between `1` (no normalization) and `L / avgL` (full normalization)
- `k1 * (1 - b + b * L / avgL)` — denominator offset; together with `tf` it controls how quickly the term-frequency component saturates for longer fields

`IDF` (inverse document frequency) measures how rare the matched subterm is across the index. It increases the score for terms that appear in fewer documents and lowers the weight of very common terms:

- `N` — total number of documents in the index (internally `totalNumDocs - 1`, because document id 0 is reserved)
- `df` — number of documents that contain the matched subterm

The two BM25 variants differ only in the `IDF` formula (`termCountInDoc` — number of occurrences of the subterm in the document field; `tf = termCountInDoc` for both):

| `Bm25Type` | `IDF` | `tf` |
|------------|-------|------|
| `bm25` | `log(N / (df + 1)) + 1` | `termCountInDoc` |
| `rx_bm25` | `max(log((N - df + 1) / df) / log(1 + N), 0.2)` | `termCountInDoc` |

`rx_bm25` is the default. Its `IDF` is normalized by `log(1 + N)` and floored at `0.2`, so very common terms do not collapse toward an extremely low weight.

When to use which:

- **`rx_bm25`** — default choice for most indexes; the `IDF` floor keeps ranking stable when many documents share the same subterm.
- **`bm25`** — classic `IDF` without a lower bound; may give a stronger contrast between rare and frequent terms.

Length effects are controlled by `Bm25b` for both types the same way. The practical difference between `bm25` and `rx_bm25` is usually noticeable mainly through `IDF`, not through term-frequency scaling.

#### word_count

The simplest scoring mode: no `IDF`, no saturation, no length normalization.

```
R = termCountInDoc
```

`termCountInDoc` — number of occurrences of the matched subterm in the document field.

See [How document rank is built](./cpp_src/core/ft/ft_fast/fulltext_ranking.md#how-document-rank-is-built) for how BM25 scores are combined with `proc`, field boosts, distance, and other factors into the final document rank.

### Limitations and known issues

- Results of full text search is always sorted by relevancy.
- A single query cannot contain multiple full-text `Match()` conditions on different text indexes. For example, `Query("items").Match("title", "foo").Match("description", "bar")` is not supported as two independent full-text searches in one query.
- To search several fields together, create a composite full-text index (for example, `title+description=text_search`) and call `Match()` on that composite index. To combine results of separate full-text queries, use `Merge()`.
- For a query that consists of a single prefix and/or suffix term (and is not an exact or phrase query), the engine may stop collecting matches after about `2 × MergeLimit` document hits. Raise `MergeLimit` if such queries return fewer hits than expected.
- Obsolete config keys `max_rebuild_steps` and `max_step_size` are no longer supported and are ignored if present.

## Performance and memory usage

Internally reindexer uses enhanced suffix array of unique words, and compressed reverse index of documents. Typically size of index is about 30%-80% of source text. But can vary in corner cases.

`Upsert` only stores text; the full-text index is built lazily on the first query to the text field. Indexing uses several threads and is usually fast on multicore CPUs.

After the first build, newly upserted documents are indexed incrementally on the next full-text query. There is no longer a “rebuild steps” setting that trades commit speed for select speed.

On very large texts, lazy indexing can slow down that first query. To avoid this, warm up the index with a dummy query after the last `Upsert`.

## LIKE

`LIKE` is not a full-text index query. It is a simpler pattern-matching operator for basic text searches: it matches strings against a pattern where `_` means any single character and `%` means any sequence of characters.

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
