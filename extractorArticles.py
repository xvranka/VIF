import json
import logging
import re
from pathlib import Path

from extractorSpecies import URL_REGEX, TITLE_REGEX, cleanText, splitBody, extractInDepth, extractFurtherReading

log = logging.getLogger(__name__)

ARTICLES_DIR = "data/prehistoric-wildlife/articles"
OUTPUT_FILE = "extracted/articles/articlesData.json"

# the species page layout without the fact table, the body is read with the same functions as for species
# builds one json record for page
def extractPage(html):
    url = URL_REGEX.search(html)
    title = TITLE_REGEX.search(html)
    in_depth, reading = splitBody(html)
    # the hub articles list species separated by <br>, a space keeps the names apart
    in_depth = [re.sub(r"<br\s*/?>", " ", p) for p in in_depth]
    return {
        "url": url.group(1) if url else None,
        "title": cleanText(title.group(1)) if title else None,
        "in_depth": extractInDepth(in_depth),
        "further_reading": extractFurtherReading(reading),
    }

# main function to run everything
# creates json file and pushes records, the same article saved twice is written once
def extractAll(root=ARTICLES_DIR, output=OUTPUT_FILE):
    records, seen = [], set()
    for path in sorted(Path(root).glob("*.html*")):
        record = extractPage(path.read_text(encoding="utf-8", errors="replace"))
        if record["url"] is None or record["url"] in seen:
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
