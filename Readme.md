# VIF – search engine for prehistoric-wildlife.com

Run all commands from the project root.

## Setup

```bash
python3 -m venv .venv                  # create the virtual environment (only once)
source .venv/bin/activate              # switch to the venv
pip install -r requirements.txt        # install dependencies (requests, pyspark); PyLucene runs in docker, see 6.
```

Spark needs a HotSpot JDK 17+. `sparkUtils.py` uses the Homebrew OpenJDK (`brew install openjdk`) when it exists,
because the IBM OpenJ9 JVM crashes while decompressing the bz2 wiki dump.

## 1. Crawl

Settings (iterations, delay, allowed domain, headers, link regex) are in `config.py`.

```bash
python resetSites.py                   # empty frontier-queue.csv and visited-sites.csv to start a fresh crawl
python main.py                         # crawl the site, save pages to data/ (set SKIP_SOURCE = True in config.py to resume from the queue)
```

## 2. Extract

```bash
python extractorSpecies.py             # species pages -> extracted/species/speciesData.json
python extractorArticles.py            # article pages -> extracted/articles/articlesData.json
```

### 2b. Extract with Spark

The same regex extraction, run in parallel by Spark (local mode, all cores). Outputs are JSON lines directories.

```bash
python sparkExtractor.py               # species + article pages -> extracted-spark/species, extracted-spark/articles
```

## 3. Wikipedia dump

Download a dump into `data/wiki/` from https://dumps.wikimedia.org/enwiki/latest/ — one small part for development
(`enwiki-latest-pages-articles1.xml-p1p41242.bz2`, ~300 MB) or the whole `enwiki-latest-pages-articles.xml.bz2` (~26 GB).

```bash
python wikiExtractor.py                                         # default: the small part in data/wiki/
python wikiExtractor.py data/wiki/enwiki-latest-pages-articles.xml.bz2   # any dump file (or glob)
```

The dump is split into pages at `</page>` and every page is parsed with regexes. Kept are articles with a taxobox
containing `fossil_range` (-> `extracted-spark/wiki/pages`) and all redirects (-> `extracted-spark/wiki/redirects`).

## 4. Join species with Wikipedia

```bash
python joiner.py                       # -> extracted-spark/joined (species + wiki columns), extracted-spark/joinStats.json
```

Match keys, best first: our cleaned title = wiki title / taxobox taxon, our title = a redirect to a fossil taxon page,
binomial (genus + epithet from `species`) = wiki title / redirect / type species.

## 5. Old index and search (own JSON inverted index)

```bash
python indexer.py build                                             # build extracted/index.json from both JSON files
python indexer.py search "carnivore cretaceous china"                # search with the default metric (cosine), top 10
python indexer.py search "carnivore cretaceous china" --metric bm25 --top 5   # choose metric: cosine, maxtf or bm25; --top = number of results
```

## 6. Lucene index (PyLucene)

`luceneIndexer.py` indexes the joined data (`extracted-spark/joined`, species + Wikipedia) and the articles
(`extracted-spark/articles`) into `extracted/luceneIndex`. PyLucene is not on pip (it is built from source with JCC),
so it runs in the prebuilt `coady/pylucene` docker image with the project folder mounted:

```bash
docker pull coady/pylucene
```

```bash
docker run --rm -v "$PWD":/app -w /app coady/pylucene python luceneIndexer.py build
```

```bash
docker run --rm -v "$PWD":/app -w /app coady/pylucene python luceneIndexer.py search '+diet:carnivore +period:cretaceous +location:china'
```

```bash
docker run --rm -v "$PWD":/app -w /app coady/pylucene python luceneIndexer.py compare --wiki logs/compare.wiki
```

Query syntax (Lucene classic query parser + numeric ranges):

| Query type | Example |
|---|---|
| words, searched in all text fields with boosts | `feathered dinosaur china` |
| Boolean (`+` must, `-` must not, `AND`, `OR`, `NOT`) | `+diet:carnivore +period:cretaceous -location:china` |
| Phrase | `location:"hell creek formation"` |
| Fuzzy (typos) | `tyranosaurus~` (also used automatically when plain words find nothing) |
| Prefix / Wildcard | `taxon:pachy*`, `title:*raptor` |
| Range, lived in millions of years ago (overlap) | `lived_ma:[66 TO 72]` |
| Range, body length in meters | `size_m:[9 TO *]` |
| Range, year of naming | `named_year:[2020 TO *]` |

Options: `--top N`, `--diet carnivore` (filter), `--type species|article`, `--explain` (score of the first hit).
Fields: `title`, `taxon`, `classification`, `author`, `in_depth`, `wiki_lead`, `period`, `location`, `formation`,
`category`, `remains`, `diet`, `doc_type`; numeric `lived_ma`, `size_m`, `named_year`.
