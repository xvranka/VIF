import csv
import logging
import time
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import urldefrag, urljoin, urlparse

import requests

import config

log = logging.getLogger(__name__)

class _LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)

def fetchURLs(file_path, base_url):
    parser = _LinkParser()
    parser.feed(Path(file_path).read_text(encoding="utf-8"))
    urls = []
    for href in parser.links:
        url, _ = urldefrag(urljoin(base_url, href.strip()))
        if urlparse(url).scheme in ("http", "https") and url not in urls:
            urls.append(url)
    log.info("found %d unique urls in %s", len(urls), file_path)
    return urls

def scrapeSite(url, out_dir="data"):
    parsed = urlparse(url)
    host = parsed.netloc.removeprefix("www.").removesuffix(".com")
    path = parsed.path.strip("/")
    name = f"{host}/{path}" if path else host
    target = Path(out_dir, name + ".html")
    if Path(out_dir).resolve() not in target.resolve().parents:
        raise ValueError(f"unsafe path in url: {url}")
    log.info("scraping %s", url)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(response.text, encoding="utf-8")
    log.info("saved %s (%d bytes)", target, len(response.content))
    return target

def pushToQueue(values, path="sites/frontier-queue.csv"):
    try:
        with open(path, newline="", encoding="utf-8") as f:
            existing = {row[0].strip() for row in csv.reader(f) if row}
    except FileNotFoundError:
        existing = set()
    new = []
    for v in values:
        if v not in existing:
            existing.add(v)
            new.append(v)
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([v] for v in new)
    log.info("pushed %d new urls to %s (%d already present, skipped)", len(new), path, len(values) - len(new))

def loadSource(path="sites/source-sites.csv"):
    with open(path, newline="", encoding="utf-8") as f:
        urls = [row[0].strip() for row in csv.reader(f) if row and row[0].strip()]
    log.info("loaded %d source urls from %s", len(urls), path)
    return urls

def loadFromQueue(path="sites/frontier-queue.csv"):
    with open(path, newline="", encoding="utf-8") as f:
        rows = [row for row in csv.reader(f) if row and row[0].strip()]
    url = rows[0][0].strip() if rows else None
    log.info("next in queue: %s (%d queued)", url, len(rows))
    return url

def markVisited(url, queue_path="sites/frontier-queue.csv", visited_path="sites/visited-sites.csv"):
    with open(queue_path, newline="", encoding="utf-8") as f:
        rows = [row for row in csv.reader(f) if row and row[0].strip() != url]
    with open(queue_path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)
    log.info("removed %s from %s", url, queue_path)
    pushToQueue([url], visited_path)
    log.info("marked visited: %s", url)

def loadVisited(path="sites/visited-sites.csv"):
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return {row[0].strip() for row in csv.reader(f) if row}
    except FileNotFoundError:
        return set()

def isAllowed(url):
    if not config.ALLOWED_DOMAINS:
        return True
    host = (urlparse(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in config.ALLOWED_DOMAINS)

def filterAllowed(urls):
    allowed = [u for u in urls if isAllowed(u)]
    if len(allowed) < len(urls):
        log.info("dropped %d urls outside allowed domains", len(urls) - len(allowed))
    return allowed

def crawlerBegin(iterations=None, delay=1, skip_source=False):
    if skip_source:
        log.info("skipping source loading, using existing queue")
    else:
        pushToQueue(filterAllowed([u for u in loadSource() if u not in loadVisited()]))
    count = 0
    while iterations is None or count < iterations:
        currentsite = loadFromQueue()
        if currentsite is None:
            log.info("queue is empty, stopping")
            break
        if not isAllowed(currentsite):
            log.info("outside allowed domains, removing from queue: %s", currentsite)
            markVisited(currentsite)
            continue
        if count:
            log.info("waiting %ss", delay)
            time.sleep(delay)
        count += 1
        log.info("--- iteration %d%s: %s", count, f"/{iterations}" if iterations else "", currentsite)
        try:
            scrapedfile = scrapeSite(currentsite)
            visited = loadVisited()
            pushToQueue(filterAllowed([u for u in fetchURLs(scrapedfile, currentsite) if u not in visited]))
        except (requests.RequestException, ValueError) as e:
            log.warning("skipping %s: %s", currentsite, e)
        markVisited(currentsite)
    log.info("done after %d iteration(s)", count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    crawlerBegin(config.ITERATIONS, config.DELAY, config.SKIP_SOURCE)
