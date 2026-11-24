import json
import logging
import re
from html import unescape
from pathlib import Path

log = logging.getLogger(__name__)

SPECIES_DIR = "data/prehistoric-wildlife/species"
OUTPUT_FILE = "extracted/species/speciesData.json"

# <span class="gb-headline-text">LABEL</span></p> <p class="wp-block-paragraph" style="font-size:15px">VALUE</p>
FACT_REGEX = re.compile(
    r'<span class="gb-headline-text">([^<]+)</span></p>\s*'
    r'<p class="wp-block-paragraph" style="font-size:15px">(.*?)</p>',
    re.S,
)
URL_REGEX = re.compile(r'<link rel="canonical" href="([^"]+)"')
TITLE_REGEX = re.compile(r'<h1[^>]*class="[^"]*\bgb-headline\b[^"]*"[^>]*>(.*?)</h1>', re.S)
# article body: from the dynamic-entry-content div up to the start of the sidebar div
BODY_REGEX = re.compile(r'class="[^"]*dynamic-entry-content[^"]*">(.*?)(?:<div[^>]*mv-sidebar|\Z)', re.S)
# the "Further Reading" h2 splits the body: paragraphs before it are the in depth text, paragraphs after it the references
FURTHER_READING_REGEX = re.compile(r"<h2[^>]*>\s*Further\s+Reading\s*</h2>", re.I)
PARAGRAPH_REGEX = re.compile(r'<p class="wp-block-paragraph"[^>]*>(.*?)</p>', re.S)

# convert html tags to readable text
def cleanText(html):
    text = unescape(re.sub(r"<[^>]+>", "", html))
    text = re.sub(r"[\u200b\u202a-\u202e\ufeff]", "", text).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()

# returns all label/value pairs as dictionary
# labels and values are cleaned, labels lowercased with underscores for spaces
def extractFacts(html):
    return {cleanText(label).lower().replace(" ", "_"): cleanText(value) for label, value in FACT_REGEX.findall(html)}

# returns (paragraphs before the "Further Reading" h2, paragraphs after it) from the article body
def splitBody(html):
    body = BODY_REGEX.search(html)
    if not body:
        return [], []
    parts = FURTHER_READING_REGEX.split(body.group(1), maxsplit=1)
    in_depth = PARAGRAPH_REGEX.findall(parts[0])
    reading = PARAGRAPH_REGEX.findall(parts[1]) if len(parts) > 1 else []
    return in_depth, reading

# in depth text as one string, one paragraph per line
def extractInDepth(paragraphs):
    return "\n".join(p for p in (cleanText(p) for p in paragraphs) if p)

# further reading as a list of references, a paragraph holds one reference per line starting with a dash
def extractFurtherReading(paragraphs):
    references = (ref for p in paragraphs for ref in re.split(r"\n\s*[-–]", "\n" + p))
    return [r for r in (cleanText(ref).lstrip("-– ") for ref in references) if r]

# builds one json record for page
def extractPage(html):
    facts = extractFacts(html)
    if not facts:
        return None
    url = URL_REGEX.search(html)
    title = TITLE_REGEX.search(html)
    in_depth, reading = splitBody(html)
    return {
        "url": url.group(1) if url else None,
        "title": cleanText(title.group(1)) if title else None,
        **facts,
        "in_depth": extractInDepth(in_depth),
        "further_reading": extractFurtherReading(reading),
    }

# main function to run everything
# creates json file and pushes records
def extractAll(root=SPECIES_DIR, output=OUTPUT_FILE):
    records, seen = [], set()
    for path in sorted(Path(root).glob("*.html")):
        record = extractPage(path.read_text(encoding="utf-8", errors="replace"))
        if record is None or record["url"] in seen:
            log.info("skipping %s", path)
            continue
        seen.add(record["url"])
        records.append(record)
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    log.info("wrote %d records to %s", len(records), output)
    return records


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    extractAll()
