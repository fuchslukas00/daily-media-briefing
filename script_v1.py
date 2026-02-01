#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from jinja2 import Environment, FileSystemLoader, select_autoescape

import feedparser
import requests
import yaml


DB_PATH = Path("briefing.db")
FEEDS_YML = Path("feeds.yml")
OUT_MD = Path("site/briefing.md")
OUT_HTML = Path("site/briefing.html")

# How many recent items per topic in the markdown output
MAX_ITEMS_PER_TOPIC = 12

USER_AGENT = "daily-briefing-bot/0.1 (+https://example.com)"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def stable_id(source_name: str, link: str, title: str) -> str:
    """
    Create a stable-ish unique key for de-duplication.
    Uses source + link (if present) + title as fallback.
    """
    base = f"{source_name}||{link or ''}||{title or ''}".strip()
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            uid TEXT PRIMARY KEY,
            topic TEXT NOT NULL,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            link TEXT,
            published TEXT,
            summary TEXT,
            language TEXT,
            paywall TEXT,
            fetched_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_topic ON items(topic)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_fetched ON items(fetched_at)")
    conn.commit()


def load_feeds() -> Dict[str, List[Dict[str, Any]]]:
    if not FEEDS_YML.exists():
        raise FileNotFoundError(f"Missing {FEEDS_YML}. Create it first.")
    with FEEDS_YML.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError("feeds.yml must be a mapping: topic -> list of sources")

    # Basic validation
    for topic, sources in data.items():
        if not isinstance(sources, list):
            raise ValueError(f"Topic '{topic}' must contain a list of sources.")
        for s in sources:
            if "name" not in s or "url" not in s:
                raise ValueError(f"Each source under '{topic}' needs 'name' and 'url'.")
    return data


def fetch_feed(url: str) -> bytes:
    r = requests.get(url, timeout=20, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.content


def parse_entries(feed_bytes: bytes) -> feedparser.FeedParserDict:
    return feedparser.parse(feed_bytes)


def upsert_items(
    conn: sqlite3.Connection,
    topic: str,
    source: Dict[str, Any],
    entries: List[feedparser.FeedParserDict],
) -> int:
    inserted = 0
    fetched_at = utc_now_iso()

    source_name = source.get("name", "").strip()
    language = str(source.get("language", "")).strip()
    paywall = str(source.get("paywall", "")).strip()


    for e in entries:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        summary = (e.get("summary") or e.get("description") or "").strip()

        # published can be in various fields
        published = (e.get("published") or e.get("updated") or "").strip()

        if not title:
            continue

        uid = stable_id(source_name, link, title)

        try:
            conn.execute(
                """
                INSERT INTO items (uid, topic, source, title, link, published, summary, language, paywall, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (uid, topic, source_name, title, link, published, summary, language, paywall, fetched_at),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            # already exists (duplicate)
            continue

    conn.commit()
    return inserted


def get_latest_items_by_topic(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    """
    Grab recent items per topic. Since RSS dates are messy, we sort by fetched_at (reliable).
    """
    topics = [row[0] for row in conn.execute("SELECT DISTINCT topic FROM items").fetchall()]
    out: Dict[str, List[Dict[str, Any]]] = {}

    for topic in sorted(topics):
        rows = conn.execute(
            """
            SELECT source, title, link, published, summary, language, paywall, fetched_at
            FROM items
            WHERE topic = ?
            ORDER BY fetched_at DESC
            LIMIT ?
            """,
            (topic, MAX_ITEMS_PER_TOPIC),
        ).fetchall()

        out[topic] = [
            {
                "source": r[0],
                "title": r[1],
                "link": r[2],
                "published": r[3],
                "summary": r[4],
                "language": r[5],
                "paywall": r[6],
                "fetched_at": r[7],
            }
            for r in rows
        ]
    return out


def write_briefing_md(items_by_topic: Dict[str, List[Dict[str, Any]]]) -> None:
    lines: List[str] = []
    lines.append(f"# Daily Briefing")
    lines.append("")
    lines.append(f"_Generated: {utc_now_iso()}_")
    lines.append("")
    lines.append("This is an RSS-based briefing (v1). Summaries are feed snippets, not full-article text.")
    lines.append("")

    for topic, items in items_by_topic.items():
        lines.append(f"## {topic}")
        lines.append("")
        if not items:
            lines.append("_No items yet._")
            lines.append("")
            continue

        for it in items:
            src = it["source"]
            title = it["title"]
            link = it["link"]
            paywall = it["paywall"] or "unknown"
            published = it["published"] or it["fetched_at"]

            # markdown line with link if available
            if link:
                lines.append(f"- **{src}** — [{title}]({link})  \n  _{published}_ · paywall: `{paywall}`")
            else:
                lines.append(f"- **{src}** — {title}  \n  _{published}_ · paywall: `{paywall}`")

        lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


def render_md_to_html(md_path: Path, html_path: Path, page_title: str = "Daily Media Briefing") -> None:
    import markdown
    from datetime import datetime, timezone

    md_text = md_path.read_text(encoding="utf-8")
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    body = markdown.markdown(
        md_text,
        extensions=["extra", "tables", "toc"]
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{page_title}</title>

  <!-- CSS -->
  <link rel="stylesheet" href="assets/style.css" />
</head>
<body>

<header class="header">
  <div class="container">
    <div class="nav">
      <strong>Daily Briefing</strong>
      <a href="./">Home</a>
      <a href="briefing.html">Latest</a>
      <a href="briefing.md">Markdown</a>
    </div>
  </div>
</header>

<main class="main">
  <div class="meta">
    Last updated: {generated_at}
  </div>

  {body}
</main>

</body>
</html>
"""

    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html, encoding="utf-8")


def write_index_html() -> None:
    from datetime import datetime, timezone
    from pathlib import Path

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Daily Media Briefing</title>
  <style>
    body {{
      font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
      max-width: 900px;
      margin: 2rem auto;
      padding: 0 1rem;
      line-height: 1.6;
    }}
    .meta {{ color: #666; font-size: 0.9em; margin-bottom: 1.5rem; }}
    a {{ color: #0366d6; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <h1>Daily Media Briefing</h1>
  <div class="meta">Last updated: {generated_at}</div>

  <ul>
    <li><a href="briefing.html">Open latest briefing (HTML)</a></li>
    <li><a href="briefing.md">Open latest briefing (Markdown)</a></li>
  </ul>

</body>
</html>
"""
    site_dir = Path("site")
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "index.html").write_text(html, encoding="utf-8")

def build_env() -> Environment:
    return Environment(
        loader=FileSystemLoader("templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )

def render_template(env: Environment, template_name: str, out_path: Path, **ctx) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tpl = env.get_template(template_name)
    out_path.write_text(tpl.render(**ctx), encoding="utf-8")


from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def _build_story_text(item: dict) -> str:
    """Text used for similarity: title + short summary/snippet."""
    title = (item.get("title") or "").strip()
    summary = (item.get("summary") or "").strip()
    return f"{title}. {summary}".strip()


def _shorten(text: str, max_len: int = 180) -> str:
    text = " ".join((text or "").split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


class _UnionFind:
    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


def cluster_items(items: list[dict], *, threshold: float, stop_words=None) -> list[list[dict]]:
    """
    Clusters items by cosine similarity of TF-IDF vectors.
    threshold: similarity cut-off to connect items into the same story component.
    """
    if not items:
        return []

    texts = [_build_story_text(it) for it in items]
    # Edge case: if everything is empty
    if all(not t for t in texts):
        return [[it] for it in items]

    vec = TfidfVectorizer(
        lowercase=True,
        stop_words=stop_words,     # 'english' for international, None for german v1
        ngram_range=(1, 2),
        max_df=0.90,
        min_df=1,
    )
    X = vec.fit_transform(texts)
    sim = cosine_similarity(X)

    n = len(items)
    uf = _UnionFind(n)
    for i in range(n):
        for j in range(i + 1, n):
            if sim[i, j] >= threshold:
                uf.union(i, j)

    # group indices by root
    groups: dict[int, list[int]] = {}
    for i in range(n):
        r = uf.find(i)
        groups.setdefault(r, []).append(i)

    # return clusters (largest first)
    clusters = [[items[i] for i in idxs] for idxs in groups.values()]
    clusters.sort(key=len, reverse=True)
    return clusters


def story_title_for_cluster(cluster: list[dict]) -> str:
    """
    v1 heuristic: choose the 'best' title among cluster items.
    Prefer a title that is descriptive (not too short) and from a major source doesn't matter.
    """
    # If only one item, return its title.
    if len(cluster) == 1:
        return cluster[0].get("title") or "Untitled story"

    # Choose longest reasonable title (often most descriptive)
    titles = [(it.get("title") or "").strip() for it in cluster]
    titles = [t for t in titles if t]
    if not titles:
        return "Untitled story"

    # Avoid extremely long titles if present
    titles_sorted = sorted(titles, key=lambda t: (min(len(t), 120), len(t)), reverse=True)
    return titles_sorted[0]


def story_summary_for_cluster(cluster: list[dict], max_sentences: int = 3) -> str:
    """
    v1 heuristic: build 2–3 short sentences from distinct sources' snippets if possible.
    """
    picked = []
    seen_sources = set()

    for it in cluster:
        src = (it.get("source") or "").strip()
        snip = (it.get("summary") or "").strip()
        if not snip:
            continue
        if src and src in seen_sources:
            continue
        seen_sources.add(src)
        picked.append(_shorten(snip, 180))
        if len(picked) >= max_sentences:
            break

    # fallback: use titles if no snippets exist
    if not picked:
        for it in cluster[:max_sentences]:
            picked.append(_shorten((it.get("title") or ""), 160))

    # Ensure 2–3 sentences (join with spaces)
    return " ".join(picked).strip()


def build_stories_for_topic(topic: str, items: list[dict]) -> list[dict]:
    """
    Returns list of story dicts:
      {title, summary, articles:[{source,title,link,published,paywall}]}
    """
    # Tunable thresholds: international tends to be more consistent in wording
    if topic == "international":
        threshold = 0.40
        stop_words = "english"
    else:
        # german titles vary more; start slightly lower
        threshold = 0.34
        stop_words = None

    clusters = cluster_items(items, threshold=threshold, stop_words=stop_words)

    stories = []
    for cluster in clusters:
        # sort articles in cluster by published desc if you have it; else keep as-is
        articles = []
        for it in cluster:
            articles.append({
                "source": it.get("source"),
                "title": it.get("title"),
                "link": it.get("link"),
                "published": it.get("published"),
                "paywall": it.get("paywall"),
            })

        stories.append({
            "title": story_title_for_cluster(cluster),
            "summary": story_summary_for_cluster(cluster, max_sentences=3),
            "articles": articles,
            "n_articles": len(articles),
        })

    return stories


def build_stories(items_by_topic: dict[str, list[dict]]) -> dict[str, list[dict]]:
    return {topic: build_stories_for_topic(topic, items) for topic, items in items_by_topic.items()}



def main() -> None:
    feeds = load_feeds()

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)

        total_inserted = 0
        for topic, sources in feeds.items():
            for source in sources:
                url = source["url"]
                try:
                    raw = fetch_feed(url)
                    parsed = parse_entries(raw)
                    entries = list(parsed.entries) if parsed and parsed.entries else []
                    inserted = upsert_items(conn, topic, source, entries)
                    total_inserted += inserted
                    print(f"[OK] {topic} / {source['name']}: {len(entries)} entries, {inserted} new")
                except Exception as ex:
                    print(f"[ERR] {topic} / {source.get('name')} ({url}): {ex}")

        # --- collect data for rendering ---
        items_by_topic = get_latest_items_by_topic(conn)
        stories_by_topic = build_stories(items_by_topic)

        # --- reorder stories: multi-source stories first ---
        for topic in list(stories_by_topic.keys()):
            stories = stories_by_topic[topic]
            multi = [s for s in stories if s["n_articles"] >= 2]
            single = [s for s in stories if s["n_articles"] == 1]
            stories_by_topic[topic] = multi + single

        # --- markdown + html briefing ---
        write_briefing_md(items_by_topic)
        render_md_to_html(OUT_MD, OUT_HTML)

        # --- render templated pages ---
        env = build_env()
        generated_at = utc_now_iso()

        # Home page
        render_template(
            env,
            "index.html",
            Path("site/index.html"),
            title="Daily Media Briefing",
            generated_at=generated_at,
            topics=sorted(items_by_topic.keys()),
            base_path="./",
        )

        # Topics overview
        render_template(
            env,
            "topics_index.html",
            Path("site/topics/index.html"),
            title="Topics",
            generated_at=generated_at,
            topics=sorted(items_by_topic.keys()),
            base_path="../",
        )

        # About page
        render_template(
            env,
            "about.html",
            Path("site/about.html"),
            title="About",
            generated_at=generated_at,
            base_path="./",
        )

        # Individual topic pages
        # Individual topic pages (with collapsible single-article stories)
        MAX_MULTI = 25
        MAX_SINGLE = 50

        for topic, stories in stories_by_topic.items():
            stories_multi = [s for s in stories if s["n_articles"] >= 2][:MAX_MULTI]
            stories_single = [s for s in stories if s["n_articles"] == 1][:MAX_SINGLE]

            render_template(
                env,
                "topic.html",
                Path(f"site/topics/{topic}.html"),
                title=f"Topic: {topic}",
                generated_at=generated_at,
                topic=topic,
                stories_multi=stories_multi,
                stories_single=stories_single,
                base_path="../",
            )

        print(f"\nDone. New items inserted: {total_inserted}")
        print(f"DB: {DB_PATH.resolve()}")
        print(f"Site output: {Path('site').resolve()}")

    finally:
        conn.close()




if __name__ == "__main__":
    main()
