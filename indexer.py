import argparse
import json
import logging
import math
import re
from collections import Counter
from pathlib import Path

log = logging.getLogger(__name__)

# input: records from the extractors (species and articles), output: one inverted index of both
SOURCE_FILES = ["extracted/species/speciesData.json", "extracted/articles/articlesData.json"]
INDEX_FILE = "extracted/index.json"

# common english words that are not indexed and are ignored in queries
STOP_WORDS = set(
    "a about all also an and any are as at be been but by can could did do does for from had has have he her his how "
    "i if in into is it its may more most no not of on or other our she should so some such than that the their them "
    "then there these they this those to up was we were what when where which while who will with would you".split()
)

# lowercase text split into words and numbers, stop words removed
# used for both documents and queries
def tokenize(text):
    return [t for t in re.findall(r"[a-z0-9]+", text.lower()) if t not in STOP_WORDS]

# reads the records of all source files into one list
def loadRecords(paths=SOURCE_FILES):
    records = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            records += json.load(f)
    return records

# joins all fields of a record (except url) into one text, list fields are joined by spaces
def documentText(record):
    parts = []
    for key, value in record.items():
        if key != "url" and value:
            parts.append(value if isinstance(value, str) else " ".join(value))
    return " ".join(parts)

# builds the inverted index: for every term the documents containing it and the term frequency (tf)
# also stores per document the length, the log-tf vector length (cosine) and the highest tf (max-tf)
def buildIndex(records):
    postings = {}  # term -> {url: tf}
    titles = {}
    doc_len, doc_norm, doc_max_tf = {}, {}, {}
    for record in records:
        url = record["url"]
        titles[url] = record["title"]
        counts = Counter(tokenize(documentText(record)))
        doc_len[url] = sum(counts.values())
        doc_max_tf[url] = max(counts.values(), default=1)
        # turn each word count into 1 + log10(tf), square, sum and sqrt
        # used for cosine metric => scores[url] += query_weight * (1 + math.log10(tf)) / index["doc_norm"][url]
        doc_norm[url] = math.sqrt(sum((1 + math.log10(tf)) ** 2 for tf in counts.values())) or 1.0
        for term, tf in counts.items():
            postings.setdefault(term, {})[url] = tf
    log.info("indexed %d documents, %d terms", len(titles), len(postings))
    return {
        "N": len(titles),
        "avgdl": sum(doc_len.values()) / len(doc_len),
        "titles": titles,
        "doc_len": doc_len,
        "doc_norm": doc_norm,
        "doc_max_tf": doc_max_tf,
        "postings": postings,
    }

# writes the index to a json file
def saveIndex(index, path=INDEX_FILE):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False)

# reads the index from a json file
def loadIndex(path=INDEX_FILE):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

# bm25: k1 tf saturation, b length normalisation, epsilon lowest idf, max-tf: a weight of the term in short documents
K1, B, EPSILON, MAX_TF_A = 1.5, 0.75, 0.01, 0.5

# three idf variants, df = number of documents containing the term
# "rareness" computation, the more documents is the word in, the lower value => 0
def idfClassic(n, df):
    return math.log10(n / df)

# always positive, even for terms in every document
def idfSmooth(n, df):
    return math.log10(1 + n / df)

# idf of Okapi BM25, floored because it turns negative for terms in more than half of the documents
def idfBm25(n, df):
    return max(math.log((n - df + 0.5) / (df + 0.5)), EPSILON)

# metric 1: cosine tf-idf
# document weight = (1 + log tf) / euclidean length of the document vector, query weight = (1 + log tf) * idf
# query weight = normalise input + weights based on df
# score = input weight * document weight = normalised tf / doc_norm
def scoreCosine(index, query_terms):
    scores = Counter()
    for term, query_tf in Counter(query_terms).items():
        postings = index["postings"].get(term, {})
        if not postings:
            continue
        # calculate importance for every word in the query
        query_weight = (1 + math.log10(query_tf)) * idfClassic(index["N"], len(postings))
        for url, tf in postings.items():
            scores[url] += query_weight * (1 + math.log10(tf)) / index["doc_norm"][url]
    return scores

# metric 2: tf-idf with max-tf normalisation, score = sum of ntf * idf
# ntf = a + (1 - a) * tf / (highest tf in the document), so long documents do not win by repeating words
# ignores how many times a word occurs in the query, for each query word,
# it takes the words frequency in the document relative to
# the documents most frequent word, multiplies it by the idf, and sums the results
def scoreMaxTf(index, query_terms):
    scores = Counter()
    for term in set(query_terms):
        postings = index["postings"].get(term, {})
        idf = idfSmooth(index["N"], len(postings)) if postings else 0.0
        for url, tf in postings.items():
            scores[url] += (MAX_TF_A + (1 - MAX_TF_A) * tf / index["doc_max_tf"][url]) * idf
    return scores

# metric 3: Okapi BM25, length normalisation through b and the average document length
def scoreBm25(index, query_terms):
    scores = Counter()
    for term in set(query_terms):
        postings = index["postings"].get(term, {})
        idf = idfBm25(index["N"], len(postings)) if postings else 0.0
        for url, tf in postings.items():
            length = 1 - B + B * index["doc_len"][url] / index["avgdl"]
            scores[url] += idf * tf * (K1 + 1) / (tf + K1 * length)
    return scores

# metric name -> scoring function, all return {url: score}
METRICS = {"cosine": scoreCosine, "maxtf": scoreMaxTf, "bm25": scoreBm25}

# scores all documents containing a query word with the chosen metric
# returns the best documents as (title, url, score), ties sorted by url
def search(index, query, metric="cosine", top=10):
    scores = METRICS[metric](index, tokenize(query))
    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    return [(index["titles"][url], url, round(score, 3)) for url, score in ranked[:top] if score > 0]


# python indexer.py build
# python indexer.py search "carnivore cretaceous china" --metric bm25 --top 10
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("build")
    p = sub.add_parser("search")
    p.add_argument("query")
    p.add_argument("--metric", choices=METRICS, default="cosine")
    p.add_argument("--top", type=int, default=10)
    args = parser.parse_args()
    if args.command == "build":
        saveIndex(buildIndex(loadRecords()))
    else:
        for title, url, score in search(loadIndex(), args.query, args.metric, args.top):
            print(f"{score:8.3f}  {title}  {url}")
