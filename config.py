# Crawler settings. Edit here, then run `python main.py`.

# Max pages to scrape in one run. None = run until the frontier queue is empty.
ITERATIONS = 400

# Seconds to wait between requests.
DELAY = 3

# True = don't load sites/source-sites.csv, continue from the existing queue.
SKIP_SOURCE = True

# Only URLs on these domains (and their subdomains) are queued and scraped.
# Empty list = no restriction.
ALLOWED_DOMAINS = ["prehistoric-wildlife.com"]
