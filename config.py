# Crawler settings. Edit, then run:
# python main.py.

# Max pages to scrape in one run. None = run until the frontier queue is empty.
ITERATIONS = None

# Seconds to wait between requests.
DELAY = 3

# True = don't load sites/source-sites.csv, continue from the existing queue.
SKIP_SOURCE = False

# Scrape only from this domain.
ALLOWED_DOMAINS = ["prehistoric-wildlife.com"]

# Custom headers
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0 "
        "school project; contact: xvranka@stuba.sk"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en,sk;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Regex used to collect URLs from a downloaded page (case-insensitive).
# Group 1 (if present) is taken as the URL; otherwise the whole match.
# Relative URLs are resolved against the page URL, #fragments are dropped,
# and only http/https results are kept.
URL_REGEX = r"""<a\s[^>]*?href\s*=\s*["']([^"']+)["']"""
