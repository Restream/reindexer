# Full-text ranking in Reindexer

This document describes how Reindexer computes term variant relevancy (`proc`) and the final document rank (0–255). For full-text index configuration, query syntax, and BM25 parameters, see [fulltext.md](../../../../fulltext.md).

- [Terms and subterms](#terms-and-subterms)
- [How term variants are scored](#how-term-variants-are-scored)
- [How document rank is built](#how-document-rank-is-built)

## Terms and subterms

In the ranking pipeline two related notions are used:

- **Term** — one token of the full-text DSL query after parsing (for example `fast`, `black~`, `termina*`, or one word inside a phrase `"one two"`). A term is what the user wrote; options such as `+` / `-`, `^boost`, `*`, and `~` belong to that term.
- **Subterm** — one concrete word variant that the engine generates for a term and then looks up in the index: the original form, a typo correction, a transliteration, a stem, a synonym, a split or concatenated part, and so on. Each subterm has its own base relevancy `proc` (see below) and its own list of document occurrences (positions in fields).

One query term typically expands into several subterms. Ranking first scores individual subterm occurrences in a document, then aggregates those scores into the document rank.

## How term variants are scored

Before BM25 and field-level boosts are applied, the search engine assigns a base relevancy (`proc`) to each subterm. This value is stored as `subterm.Proc()` and later multiplies the score of every occurrence of that subterm.

The scoring pipeline works as follows:

1. **Starting value.** Each DSL term begins with `proc = FullMatch` (default 100). Concatenated adjacent terms (e.g. `di caprio` → `dicaprio`) start from `ConcatProc` (default 90).

2. **Derived variants.** When the engine generates alternative forms of a term, it adjusts `proc` using coefficients from [Base ranking config](../../../../fulltext.md#base-ranking-config). Each coefficient is effectively `configValue / FullMatch`, capped at 1.0:
   - keyboard layout correction: `proc *= KbLayoutCoeff()` (whether kb-layout variants are generated depends on `EnableKbLayout`, including heuristic skip for heavy prefix/suffix queries; see [Wrong keyboard layout](../../../../fulltext.md#wrong-keyboard-layout))
   - translit: `proc *= TranslitCoeff()`
   - synonym: `proc *= SynonymsCoeff()`
   - term split by `EnableTermsSplit`: each part gets `(proc / 2) * SplitCoeff()`
   - term split by `WordPartDelimiters`: `proc *= DelimitedCoeff()`

3. **Penalty-based variants.**
   - stemming: `proc = max(proc - StemmerPenalty, 1)`
   - typo: `proc = max(patternProc * TypoCoeff() - typoPenalty, 1)`, where the penalty grows with the number of character changes and shrinks for longer words (see [Typos algorithm](../../../../fulltext.md#typos-algorithm))

4. **Partial prefix/suffix match.** When a variant matches only part of an indexed word, an extra penalty is applied:
   ```
   penalty = PartialMatchDecrease * non_matched_symbols / max(matched_symbols, 3)
   proc = min(max(variantProc - penalty, PrefixMin or SuffixMin), variantProc)
   ```
   Optional per-word `terms_boost` from config may further multiply `proc`.

5. **Best variant per word.** If several variants of the same query term match the same indexed word, the highest `proc` is kept.

The resulting `subterm.Proc()` is then used when scoring subterm occurrences (see [Basic document ranking algorithms](../../../../fulltext.md#basic-document-ranking-algorithms) and [How document rank is built](#how-document-rank-is-built)).

## How document rank is built

`iterator.Rank()` returns an integer from 0 to 255 for each found document. That value is not computed in one step:

1. While merging matches, the engine accumulates a **raw floating-point score** for the document (BM25, `proc`, boosts, distance, and so on).
2. After all terms are merged, documents with a raw score below `MinRank` are dropped.
3. Remaining raw scores are **scaled into 0–255**: if the best raw score in the result set is greater than 255, every score is multiplied by `255 / maxRawScore`; otherwise scores are left as-is and then stored as `uint8`.
4. Results are sorted by this final integer rank (unless another sort expression overrides ordering).

So the float arithmetic happens first; the 0–255 conversion is only a final packaging step for the API.

### Score of one subterm occurrence

A subterm may appear in several fields of the same document (and several times inside a field). For **one occurrence** (one document hit of one subterm, with positions across fields), the engine:

1. Computes a score **separately for each field** where that occurrence has positions (fields with zero field boost are skipped).
2. Takes the **maximum** among those per-field scores as the winner.
3. If `SumRanksByFieldsRatio = K > 0` and some fields are marked with `+` in the `@field` selector, additional field scores are added to the winner with decreasing weights `K`, `K²`, … — the same rule as in [Field selection](../../../../fulltext.md#field-selection).

The score of that single occurrence is:

```
occurrenceScore = queryBoost * subtermProc * fieldBoost * bm25Norm * termLenBoost * positionRank
```

(with optional summation over `+` fields on top of the winning field score).

This is **not** “the rank of the whole query term”. It is the score of **one subterm hit in one document**. The same term can produce many such hits (different subterms, different documents, different positions). `debug_rank()` reports this value as `term_rank`.

Where:
- `queryBoost` — `^boost` from the DSL term (default 1)
- `subtermProc` — variant relevancy from [How term variants are scored](#how-term-variants-are-scored)
- `fieldBoost` — `^boost` from `@field` selection for the winning field (default 1)
- `bm25Norm` — `(1 - bm25Weight) + bm25 * bm25Boost * bm25Weight`
- `termLenBoost` — length factor blended with `termLenWeight`
- `positionRank` — earlier word positions in the field score higher (`positionWeight`, `positionBoost`)

### Single-term queries

If the query contains only one term, the document's raw score is the maximum `occurrenceScore` among all matching subterms of that term in the document.

### Multi-term and phrase queries

For queries with two or more terms (including phrases):

1. The first term contributes its best `occurrenceScore` with no distance factor.
2. Each subsequent term contributes `occurrenceScore * normDist`, where:
   ```
   normDist = (1 - distanceWeight) + distanceBoost / max(distance, 1) * distanceWeight
   ```

   **`distance`** is measured in **word positions inside the same field**: how many steps separate the current match from the match chosen for the previous query term. Adjacent words have distance `1`; one word between them means distance `2`, and so on. If the two matches are not in the same field, they do not form a usable pair for this step (distance is treated as absent / zero contribution path).

   Closer matches get a higher `normDist` (and therefore a higher contribution). `distanceWeight` / `distanceBoost` control how strongly that gap affects the score.

   For a phrase `"word1 word2"~N`, the same distance formula is used, but only positions of `word2` that lie **within at most `N` word positions after** a kept position of `word1` (same order, same field) are accepted. Positions outside that window are ignored for the phrase.

3. **One best hit per query-term step.** While processing the next term, a document may contain many candidate occurrences (several subterms, several positions). The engine does **not** sum all of them. It keeps only the candidate with the highest `occurrenceScore * normDist` relative to the positions already chosen for the previous term. That contribution **replaces** the previous best for the current term step (it is not added on top of other candidates of the same term).

4. The raw document score is the sum of:
   - the first term's best `occurrenceScore`, and
   - each following term's best distance-adjusted contribution.

**Phrases.** Phrase matching additionally requires that word positions can be chained left-to-right across all phrase terms within the `~N` distance limit. Documents that fail this phrase alignment receive zero rank from the phrase.

### Full-match boost

If a document contains all required query terms and the number of words in the matching field equals the query length, the raw score is multiplied by `FullMatchBoost` (default 1.1).

### Normalization and filtering

Before results are returned:

1. Documents with raw score below `MinRank` (default 5) are removed.
2. Scores are scaled to 0–255 as described above.
3. Results are sorted by rank descending (unless another sort expression overrides ordering).

The value exposed as `iterator.Rank()` is this final normalized integer.
