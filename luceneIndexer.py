# Lucene index of the joined data (our species + Wikipedia) and the articles, built with PyLucene.
#
# python luceneIndexer.py build
# python luceneIndexer.py search '+diet:carnivore +period:cretaceous +location:china'
# python luceneIndexer.py search 'diet:carnivore lived_ma:[66 TO 72] size_m:[9 TO *]' --top 5
# python luceneIndexer.py compare                     # the same queries on the old (indexer.py) and the new index
#
# PyLucene is not on pip, run it in docker (see Readme):
# docker run --rm -v "$PWD":/app -w /app coady/pylucene python luceneIndexer.py build

# Run interactive bash in the docker image to test queries:
# docker run --rm -it -v "$PWD":/app -w /app coady/pylucene bash
    # example: python luceneIndexer.py search '+diet:carnivore +period:cretaceous +location:china' --top 5

import argparse
import json
import logging
import re
import shutil
import time
from collections import Counter
from pathlib import Path

try:
    import lucene
    from java.lang import Float
    from java.nio.file import Paths
    from java.util import HashMap
    from org.apache.lucene.analysis.custom import CustomAnalyzer
    from org.apache.lucene.analysis.en import EnglishAnalyzer
    from org.apache.lucene.analysis.miscellaneous import PerFieldAnalyzerWrapper
    from org.apache.lucene.document import Document, DoublePoint, Field, IntPoint, StoredField, StringField, TextField
    from org.apache.lucene.index import DirectoryReader, IndexWriter, IndexWriterConfig, Term
    from org.apache.lucene.queryparser.classic import MultiFieldQueryParser, QueryParser, QueryParserBase
    from org.apache.lucene.search import BooleanClause, BooleanQuery, IndexSearcher, MatchAllDocsQuery, TermQuery
    from org.apache.lucene.store import FSDirectory
except ImportError:  # the record preparation below works without PyLucene, building and searching does not
    lucene = None

log = logging.getLogger(__name__)

JOINED_INPUT = "extracted-spark/joined"
ARTICLES_INPUT = "extracted-spark/articles"
INDEX_DIR = "extracted/luceneIndex"

# fields of the index

# names: lowercase + no accents, no stemming 
NAME_FIELDS = ["title", "taxon", "classification", "author"]
# english text: lowercase, stop words removed, stemmed (feathered -> feather)
ENGLISH_FIELDS = ["in_depth", "wiki_lead", "period", "location", "formation", "category", "remains"]
# exact values for filtering (diet:carnivore)
KEYWORD_FIELDS = ["diet", "doc_type"]
# numbers for range queries: age in millions of years (lived_ma = lived_from_ma + lived_to_ma), length in meters, year named
NUMERIC_FIELDS = ["lived_ma", "size_m", "named_year"]

# fields searched when the query names no field, and how much a match in each one counts
SEARCH_BOOSTS = {
    "title": 5.0, "taxon": 4.0, "classification": 2.0, "diet": 2.0, "period": 2.0, "location": 2.0,
    "formation": 1.5, "category": 1.5, "wiki_lead": 1.5, "author": 1.0, "in_depth": 1.0, "remains": 0.5,
}


# preparing records (plain python)

YEAR_REGEX = re.compile(r"\b(1[789]\d\d|20[0-2]\d)\b")
# controlled diet vocabulary, the source text has typos and variants ("Herbviore", "pisicvore", "omnivorous")
DIET_PATTERNS = {
    "carnivore": r"carniv", "herbivore": r"herb[iv]", "piscivore": r"pis?i?c[iv]*vor|picivor",
    "insectivore": r"insectiv", "omnivore": r"omn?i[nv]+or", "filter-feeder": r"filter",
    "detritivore": r"detrit", "durophage": r"durophag|shellfish|mollus", "frugivore": r"frugiv|fruit",
    "granivore": r"seed", "scavenger": r"scaveng", "planktivore": r"plankt",
}
# "Up to 2.5 meters long", "120-200 centimetres long", "between 4.5 and 7 meterslong", "body length about 5 centimetres"
SIZE_NUMBER = r"(\d+(?:\.\d+)?)(?:\s*(?:-|–|to|and)\s*(\d+(?:\.\d+)?))?\s*"
SIZE_UNIT = r"(millimet(?:er|re)s?|mm|centimet(?:er|re)s?|cm|met(?:ere|er|re)s?|m)"
SIZE_REGEXES = [
    re.compile(SIZE_NUMBER + SIZE_UNIT + r"(?=\s*(?:long|in length|in total length|(?:in )?body length))", re.I),
    re.compile(r"\blength\b\D{0,25}?" + SIZE_NUMBER + SIZE_UNIT + r"(?![a-z])", re.I),
]

# helpers, run during build

def readJsonLines(directory):
    for path in sorted(Path(directory).glob("part-*.json")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)

def clean(text):
    return (text or "").strip().rstrip(".").strip()

# "Carnivore/Piscivore?" -> ["carnivore", "piscivore"], "Filter feeder." -> ["filter-feeder"], "Unknown." -> []
def diets(text):
    text = (text or "").lower()
    return [diet for diet, pattern in DIET_PATTERNS.items() if re.search(pattern, text)]

def unitToMeters(unit):
    unit = unit.lower()
    return 0.001 if unit.startswith("mi") or unit == "mm" else 0.01 if unit.startswith("c") else 1.0

# the longest body length in meters, None if the text has none ("Unavailable.", only a wingspan, height or weight)
def sizeMeters(text):
    values = [float(high or low) * unitToMeters(unit)
              for regex in SIZE_REGEXES for low, high, unit in regex.findall(text or "")]
    return round(max(values), 3) if values else None

# wikipedia authority year, otherwise the last year in our "named_by"
def namedYear(record):
    if record.get("authority_year"):
        return int(record["authority_year"])
    years = YEAR_REGEX.findall(record.get("named_by") or "")
    return int(years[-1]) if years else None

# (oldest, youngest) in millions of years; one value only -> both the same, swapped values are put in order
def livedRange(record):
    values = [v for v in (record.get("range_start_ma"), record.get("range_end_ma")) if v is not None]
    return (max(values), min(values)) if values else None

# one joined species record -> field name: list of values (+ numbers), the same shape for articles
def prepareSpecies(r):
    return {
        "url": r["url"],
        "doc_type": "species",
        "title": [r.get("title")],
        "taxon": [r.get("taxon"), r.get("wiki_title"), clean(r.get("name")), r.get("species"), r.get("type_species")],
        "classification": [r.get("classification")],
        "author": [r.get("named_by"), r.get("authority")],
        "in_depth": [r.get("in_depth")],
        "wiki_lead": [r.get("short_description"), r.get("lead")],
        "period": [r.get("time_period"), r.get("fossil_range")],
        "location": [r.get("known_locations")] + (r.get("formations") or []),
        "formation": r.get("formations") or [],
        "category": r.get("categories") or [],
        "remains": [r.get("fossil_representation")],
        "diet": diets(r.get("diet")),
        "diet_text": clean(r.get("diet")),
        "lived": livedRange(r),
        "size_m": sizeMeters(r.get("size")),
        "named_year": namedYear(r),
    }

def prepareArticle(r):
    return {"url": r["url"], "doc_type": "article", "title": [r.get("title")], "in_depth": [r.get("in_depth")],
            "diet": [], "lived": None, "size_m": None, "named_year": None}

def prepareAll():
    return [prepareSpecies(r) for r in readJsonLines(JOINED_INPUT)] + \
           [prepareArticle(r) for r in readJsonLines(ARTICLES_INPUT)]

# ======
# lucene
# ======

def initLucene():
    if lucene is None:
        raise SystemExit("PyLucene is not installed - run this script in the pylucene docker image (see Readme)")
    lucene.initVM(vmargs=["-Djava.awt.headless=true"])

def customAnalyzer(tokenizer, *filters):
    builder = CustomAnalyzer.builder().withTokenizer(tokenizer, HashMap())
    for name in filters:
        builder = builder.addTokenFilter(name, HashMap())
    return builder.build()

# every field gets its own analyser, the same one is used for indexing and for parsing the query
def createAnalyzer():
    names = customAnalyzer("standard", "lowercase", "asciiFolding")
    keyword = customAnalyzer("keyword", "lowercase")
    perField = HashMap()
    for field in NAME_FIELDS:
        perField.put(field, names)
    for field in KEYWORD_FIELDS:
        perField.put(field, keyword)
    return PerFieldAnalyzerWrapper(EnglishAnalyzer(), perField)

STORED = {"title", "period", "wiki_lead"}

def toDocument(p):
    doc = Document()
    doc.add(StringField("url", p["url"], Field.Store.YES))
    doc.add(StringField("doc_type", p["doc_type"], Field.Store.YES))
    for field in NAME_FIELDS + ENGLISH_FIELDS:
        values = [v for v in dict.fromkeys(p.get(field) or []) if v and str(v).strip()]
        for i, value in enumerate(values):
            store = Field.Store.YES if field in STORED and i == 0 else Field.Store.NO
            doc.add(TextField(field, str(value), store))
    for value in p["diet"]:
        doc.add(StringField("diet", value, Field.Store.NO))
    if p.get("diet_text"):
        doc.add(StoredField("diet_text", p["diet_text"]))
    if p["lived"]:
        oldest, youngest = p["lived"]
        doc.add(DoublePoint("lived_from_ma", float(oldest)))
        doc.add(DoublePoint("lived_to_ma", float(youngest)))
        doc.add(StoredField("lived_from_ma", float(oldest)))
        doc.add(StoredField("lived_to_ma", float(youngest)))
    if p["size_m"] is not None:
        doc.add(DoublePoint("size_m", float(p["size_m"])))
        doc.add(StoredField("size_m", float(p["size_m"])))
    if p["named_year"] is not None:
        doc.add(IntPoint("named_year", int(p["named_year"])))
        doc.add(StoredField("named_year", int(p["named_year"])))
    return doc

def directorySize(path):
    return sum(f.stat().st_size for f in Path(path).iterdir() if f.is_file())

def buildIndex(path=INDEX_DIR):
    start = time.perf_counter()
    records = prepareAll()
    shutil.rmtree(path, ignore_errors=True)
    config = IndexWriterConfig(createAnalyzer())
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(FSDirectory.open(Paths.get(path)), config)
    for record in records:
        writer.addDocument(toDocument(record))
    writer.commit()
    writer.close()
    stats = {
        "documents": len(records),
        "species": sum(r["doc_type"] == "species" for r in records),
        "articles": sum(r["doc_type"] == "article" for r in records),
        "with_lived_ma": sum(r["lived"] is not None for r in records),
        "with_size_m": sum(r["size_m"] is not None for r in records),
        "with_named_year": sum(r["named_year"] is not None for r in records),
        "with_diet": sum(bool(r["diet"]) for r in records),
        "diet_values": dict(Counter(d for r in records for d in r["diet"]).most_common(12)),
        "index_size_mb": round(directorySize(path) / 2 ** 20, 2),
        "seconds": round(time.perf_counter() - start, 2),
    }
    log.info("lucene index written to %s:\n%s", path, json.dumps(stats, indent=2))
    return stats

# =======
# queries
# =======

# numeric ranges in the query text: lived_ma:[66 TO 72], size_m:[9 TO *], named_year:[2015 TO 2020]
# the classic query parser only knows text ranges, so these parts are cut out and turned into point queries
RANGE_REGEX = re.compile(r"\b(lived_ma|size_m|named_year):\[\s*(\S+)\s+TO\s+(\S+?)\s*\]")
INT_MIN, INT_MAX = -2 ** 31, 2 ** 31 - 1

def bound(value, default):
    return default if value == "*" else float(value)

# lived_ma:[a TO b] = the animal lived at some time between a and b million years ago,
# its range [youngest, oldest] overlaps [a, b]: oldest >= a and youngest <= b
def rangeQuery(field, low, high):
    if field == "lived_ma":
        both = BooleanQuery.Builder()
        both.add(DoublePoint.newRangeQuery("lived_from_ma", bound(low, float("-inf")), float("inf")), BooleanClause.Occur.FILTER)
        both.add(DoublePoint.newRangeQuery("lived_to_ma", float("-inf"), bound(high, float("inf"))), BooleanClause.Occur.FILTER)
        return both.build()
    if field == "size_m":
        return DoublePoint.newRangeQuery("size_m", bound(low, float("-inf")), bound(high, float("inf")))
    return IntPoint.newRangeQuery("named_year", int(bound(low, INT_MIN)), int(bound(high, INT_MAX)))

def createParser(analyzer):
    boosts = HashMap()
    for field, boost in SEARCH_BOOSTS.items():
        boosts.put(field, Float.valueOf(float(boost)))
    parser = MultiFieldQueryParser(list(SEARCH_BOOSTS), analyzer, boosts)
    parser.setDefaultOperator(QueryParser.Operator.OR)
    # one word at a time, otherwise the keyword analyser of "diet" gets the whole text as one token
    parser.setSplitOnWhitespace(True)
    parser.setAllowLeadingWildcard(True)  # title:*raptor
    return parser

# text part -> query parser (Boolean +/-/AND/OR/NOT, field:, "phrase", word~ fuzzy, prefix*, wildcard?),
# numeric ranges and --diet / --type -> FILTER clauses (must match, do not change the score)
def buildQuery(parser, text, diet=None, docType=None):
    filters = [rangeQuery(*m) for m in RANGE_REGEX.findall(text)]
    text = RANGE_REGEX.sub(" ", text).strip()
    if diet:
        filters.append(TermQuery(Term("diet", diet.lower())))
    if docType:
        filters.append(TermQuery(Term("doc_type", docType)))
    builder = BooleanQuery.Builder()
    # MultiFieldQueryParser has static parse() methods that hide the instance one in PyLucene
    builder.add(QueryParserBase.parse(parser, text) if text else MatchAllDocsQuery(), BooleanClause.Occur.MUST)
    for f in filters:
        builder.add(f, BooleanClause.Occur.FILTER)
    return builder.build()

# plain words without query syntax -> every word fuzzy, used when the exact query finds nothing
def fuzzyRewrite(text):
    if re.search(r'[:"~*?+\-\[\]()^]|\b(?:AND|OR|NOT)\b', text):
        return None
    words = text.split()
    return " ".join(w + "~" for w in words) if words else None

class Searcher:
    def __init__(self, path=INDEX_DIR):
        self.reader = DirectoryReader.open(FSDirectory.open(Paths.get(path)))
        self.searcher = IndexSearcher(self.reader)  # default similarity = BM25
        self.parser = createParser(createAnalyzer())

    # returns (query, hit count, [(title, url, score, document, lucene doc id)])
    def search(self, text, top=10, diet=None, docType=None, fuzzyFallback=True):
        query = buildQuery(self.parser, text, diet, docType)
        total = self.searcher.count(query)
        if total == 0 and fuzzyFallback and fuzzyRewrite(text):
            query = buildQuery(self.parser, fuzzyRewrite(text), diet, docType)
            total = self.searcher.count(query)
        stored = self.searcher.storedFields()
        hits = []
        for sd in self.searcher.search(query, top).scoreDocs:
            doc = stored.document(sd.doc)
            hits.append((doc.get("title"), doc.get("url"), round(float(sd.score), 3), doc, sd.doc))
        return query, total, hits

    def explain(self, query, docId):
        return self.searcher.explain(query, docId).toString()

    def close(self):
        self.reader.close()

def formatHit(title, url, score, doc):
    info = []
    if doc.get("diet_text"):
        info.append(doc.get("diet_text").lower())
    if doc.get("lived_from_ma"):
        oldest, youngest = float(doc.get("lived_from_ma")), float(doc.get("lived_to_ma"))
        info.append(f"{oldest:g} Ma" if oldest == youngest else f"{oldest:g}-{youngest:g} Ma")
    if doc.get("size_m"):
        info.append(f"{float(doc.get('size_m')):g} m")
    if doc.get("named_year"):
        info.append(f"named {doc.get('named_year')}")
    return f"{score:8.3f}  {title}  [{', '.join(info)}]  {url}"


# old vs new index: (what it shows, query for the old index, query for the new index)
COMPARISON = [
    ("keyword", "tyrannosaurus", "tyrannosaurus"),
    ("typo (Fuzzy)", "tyranosaurus", "tyranosaurus~"),
    ("stemming", "feathered dinosaur china", "feathered dinosaur china"),
    ("Phrase", "hell creek formation", 'location:"hell creek formation"'),
    ("Boolean AND", "carnivore cretaceous china", "+diet:carnivore +period:cretaceous +location:china"),
    ("Boolean NOT", "sauropoda", "+classification:sauropoda -location:china"),
    ("Range Ma + size", "large carnivore maastrichtian", "+diet:carnivore lived_ma:[66 TO 72] size_m:[9 TO *]"),
    ("Range year", "pterosaur 2020", "classification:pterosauria named_year:[2020 TO *]"),
    ("Wildcard", "raptor", "title:*raptor"),
]

def compare(top=5, wikiPath=None):
    import indexer  # the old json index (pure python)
    oldIndex = indexer.loadIndex()
    new = Searcher()
    rows = []
    for name, oldText, newText in COMPARISON:
        start = time.perf_counter()
        # old index with its cosine metric, the new one with BM25 (lucene default)
        oldScores = {u: s for u, s in indexer.scoreCosine(oldIndex, indexer.tokenize(oldText)).items() if s > 0}
        oldHits = indexer.search(oldIndex, oldText, "cosine", top)
        oldMs = (time.perf_counter() - start) * 1000
        start = time.perf_counter()
        query, total, newHits = new.search(newText, top, fuzzyFallback=False)
        newMs = (time.perf_counter() - start) * 1000
        overlap = len({u for _, u, _ in oldHits} & {h[1] for h in newHits})
        rows.append((name, oldText, newText, len(oldScores), total, oldMs, newMs, overlap,
                     [t for t, _, _ in oldHits], [h[0] for h in newHits]))
        print(f"\n=== {name} ===")
        print(f"old  {oldText!r}: {len(oldScores)} hits, {oldMs:.1f} ms")
        for t in rows[-1][8]:
            print(f"       {t}")
        print(f"new  {newText!r}: {total} hits, {newMs:.1f} ms, overlap of top {top}: {overlap}")
        print(f"     lucene: {query}")
        for title, url, score, doc, _ in newHits:
            print("   " + formatHit(title, url, score, doc))
    new.close()
    if wikiPath:
        lines = ["^ Čo testuje ^ Starý index (dopyt) ^ Zásahy ^ Nový index (dopyt) ^ Zásahy ^ Spoločné v top %d ^ Top 3 starý ^ Top 3 nový ^" % top]
        for name, oldText, newText, oldN, newN, _, _, overlap, oldTop, newTop in rows:
            # %%...%% so DokuWiki does not turn quotes and -- into typographic characters
            lines.append(f"| {name} | ''%%{oldText}%%'' | {oldN} | ''%%{newText}%%'' | {newN} | {overlap} | "
                         f"{', '.join(oldTop[:3]) or '–'} | {', '.join(newTop[:3]) or '–'} |")
        Path(wikiPath).write_text("\n".join(lines) + "\n", encoding="utf-8")
        log.info("comparison table written to %s", wikiPath)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("build")
    p = sub.add_parser("search")
    p.add_argument("query", nargs="?", default="")
    p.add_argument("--top", type=int, default=10)
    p.add_argument("--diet", help="only this diet, e.g. carnivore, herbivore, piscivore, filter-feeder")
    p.add_argument("--type", choices=["species", "article"])
    p.add_argument("--explain", action="store_true", help="print why the first hit got its score")
    c = sub.add_parser("compare")
    c.add_argument("--top", type=int, default=5)
    c.add_argument("--wiki", help="also write the comparison as a DokuWiki table to this file")
    args = parser.parse_args()

    initLucene()
    if args.command == "build":
        buildIndex()
    elif args.command == "compare":
        compare(args.top, args.wiki)
    else:
        searcher = Searcher()
        query, total, hits = searcher.search(args.query, args.top, args.diet, args.type)
        print(f"lucene query: {query}\n{total} hits")
        for title, url, score, doc, _ in hits:
            print(formatHit(title, url, score, doc))
        if args.explain and hits:
            print("\n" + searcher.explain(query, hits[0][4]))
        searcher.close()
