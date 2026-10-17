from pathlib import Path

FILES = ["sites/frontier-queue.csv", "sites/visited-sites.csv"]

def reset():
    for path in FILES:
        Path(path).write_text("", encoding="utf-8")

if __name__ == "__main__":
    reset()
