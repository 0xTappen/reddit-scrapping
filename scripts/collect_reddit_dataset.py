#!/usr/bin/env python3
"""
Script pengumpulan data komentar Reddit (CSV + JSON) untuk penelitian:
- Counter-Strike 2 (r/GlobalOffensive)
- VALORANT (r/VALORANT)

Target: 5.000 komentar per game/subreddit.
Metode:
1. Mengambil komentar real-time feed melalui /subreddits/comments (pagination cursor).
2. Jika feed real-time mencapai batas upstream (~950 komentar), script otomatis
   mengambil daftar posts populer & hot (/subreddits/posts) lalu mengekstrak komentar
   lengkapnya melalui /posts/comments/export secara merata lintas post hingga mencapai 5.000.
3. Menyimpan format CSV dan JSON indent=2 (UTF-8).
"""

import csv
from datetime import datetime, timezone
import json
import os
import sys
import time
import urllib.parse
import urllib.request

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")

API_BASE_URL = "http://127.0.0.1:8000"

SUBREDDIT_CONFIG = [
    {
        "game": "CS2",
        "subreddit": "GlobalOffensive",
        "csv_filename": "cs2_comments_raw.csv",
        "json_filename": "cs2_comments_raw.json",
    },
    {
        "game": "VALORANT",
        "subreddit": "VALORANT",
        "csv_filename": "valorant_comments_raw.csv",
        "json_filename": "valorant_comments_raw.json",
    },
]

CSV_HEADERS = [
    "game",
    "subreddit",
    "post_id",
    "post_title",
    "post_url",
    "comment_id",
    "comment_text",
    "comment_score",
    "comment_created_at",
    "author",
    "scraped_at",
]


def is_deleted_or_removed(comment: dict) -> bool:
    flags = comment.get("flags") or {}
    if flags.get("is_deleted", False):
        return True

    text = (comment.get("text") or "").strip()
    if not text or text in ["[deleted]", "[removed]"]:
        return True

    return False


def extract_comment_records(comment: dict, game: str, subreddit: str, default_post: dict = None) -> list:
    records = []
    post_info = comment.get("post") or default_post or {}
    post_id = comment.get("post_id") or post_info.get("id") or ""
    post_title = post_info.get("title") or ""
    post_url = post_info.get("link") or ""

    author_info = comment.get("author") or {}
    author_username = author_info.get("username") or ""

    stats_info = comment.get("stats") or {}
    score = stats_info.get("score")
    if score is None:
        score = ""

    scraped_at = datetime.now(timezone.utc).isoformat()

    record = {
        "game": game,
        "subreddit": subreddit,
        "post_id": post_id,
        "post_title": post_title,
        "post_url": post_url,
        "comment_id": comment.get("id") or "",
        "comment_text": comment.get("text") or "",
        "comment_score": score,
        "comment_created_at": comment.get("created_at") or "",
        "author": author_username,
        "scraped_at": scraped_at,
        "_is_deleted": is_deleted_or_removed(comment),
    }
    records.append(record)

    replies = comment.get("replies") or []
    for reply in replies:
        if isinstance(reply, dict):
            records.extend(extract_comment_records(reply, game, subreddit, default_post=post_info))

    return records


def api_get(endpoint: str, params: dict, max_retries: int = 10):
    qs = urllib.parse.urlencode(params)
    url = f"{API_BASE_URL}{endpoint}?{qs}"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    )
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=40) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            error_body = ""
            try:
                error_body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            if "rate-limited" in error_body or e.code == 429 or e.code == 500:
                if max_retries <= 2:
                    raise e
                print(f"\n[RATE-LIMIT] Terkena rate-limit Reddit ({endpoint}). Istirahat 60 detik sebelum coba lagi (Percobaan {attempt+1}/{max_retries})...")
                time.sleep(60)
            else:
                raise e
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                raise e


def fetch_comments_stream(subreddit: str, game: str, target_count: int = 5000):
    collected_records = []
    seen_comment_ids = set()
    deleted_removed_count = 0
    duplicate_count = 0

    # Muat komentar yang sudah ada di disk agar langsung resume
    existing_json = os.path.join("data", "raw", f"{game.lower()}_comments_raw.json")
    if os.path.exists(existing_json):
        try:
            with open(existing_json, "r", encoding="utf-8") as f:
                saved = json.load(f)
            for r in saved:
                cid = r.get("comment_id")
                if cid and cid not in seen_comment_ids:
                    seen_comment_ids.add(cid)
                    collected_records.append(r)
            if collected_records:
                print(f"[RESUME] Melanjutkan dari {len(collected_records)} komentar yang sudah ada di disk.")
        except Exception:
            pass

    print(f"\n============================================================")
    print(f"Mulai scraping r/{subreddit} (Game: {game}) | Target: {target_count} komentar")
    print(f"============================================================")

    # FASE 1: Feed komentar terbaru (/subreddits/comments)
    cursor = None
    page = 1
    print("[FASE 1] Mengambil komentar dari feed aktif r/" + subreddit)
    while len(collected_records) < target_count:
        params = {"subreddit": subreddit, "limit": 100}
        if cursor:
            params["cursor"] = cursor

        try:
            data = api_get("/subreddits/comments", params)
        except Exception as e:
            print(f"  [WARN] Feed r/{subreddit} page {page}: {e}")
            break

        comments = data.get("comments", [])
        if not comments:
            break

        for item in comments:
            for rec in extract_comment_records(item, game, subreddit):
                c_id = rec["comment_id"]
                if rec["_is_deleted"]:
                    deleted_removed_count += 1
                    continue
                if not c_id:
                    continue
                if c_id in seen_comment_ids:
                    duplicate_count += 1
                    continue

                seen_comment_ids.add(c_id)
                clean_rec = {k: v for k, v in rec.items() if not k.startswith("_")}
                collected_records.append(clean_rec)
                if len(collected_records) >= target_count:
                    break
            if len(collected_records) >= target_count:
                break

        if page % 5 == 0 or len(collected_records) >= target_count:
            print(f"  Feed Page {page}: Terkumpul {len(collected_records)}/{target_count}")

        cursor = data.get("next_cursor")
        if not cursor or not data.get("has_more", True):
            print(f"  [INFO] Batas feed aktif r/{subreddit} tercapai di {len(collected_records)} komentar.")
            break

        page += 1
        time.sleep(0.3)

    # FASE 2: Jika belum sampai target, ambil postingan top/hot/new lalu export thread
    if len(collected_records) < target_count:
        print(f"\n[FASE 2] Menelusuri postingan thread r/{subreddit} untuk melengkapi hingga {target_count}...")
        sort_modes = [("hot", None), ("top", "all"), ("top", "year"), ("top", "month"), ("new", None)]
        seen_posts = set()
        posts_to_scan = []

        for sort, time_filter in sort_modes:
            if len(posts_to_scan) >= 120:
                break
            p_params = {"subreddit": subreddit, "sort": sort, "limit": 100}
            if time_filter:
                p_params["time"] = time_filter
            try:
                p_data = api_get("/subreddits/posts", p_params)
                for p in p_data.get("posts", []):
                    pid = p.get("id")
                    cc = p.get("stats", {}).get("comment_count", 0)
                    if pid and pid not in seen_posts and cc > 5:
                        seen_posts.add(pid)
                        posts_to_scan.append(p)
            except Exception as e:
                print(f"  [WARN] Gagal mengambil postingan ({sort}): {e}")

        print(f"  Ditemukan {len(posts_to_scan)} thread relevan untuk di-export komentarnya.")

        for idx, post in enumerate(posts_to_scan):
            if len(collected_records) >= target_count:
                break

            pid = post.get("id")
            post_title = post.get("title", "")
            post_link = post.get("link", "")
            default_post = {"id": pid, "title": post_title, "link": post_link}

            try:
                # Gunakan endpoint /posts/comments (1 request upstream) daripada /export (bisa 40 request per post)
                thread_data = api_get("/posts/comments", {"post": pid, "limit": 100, "depth": 4}, max_retries=2)
                t_comments = thread_data.get("comments", [])
            except Exception as e:
                time.sleep(1)
                continue

            added_this_post = 0
            for item in t_comments:
                for rec in extract_comment_records(item, game, subreddit, default_post=default_post):
                    c_id = rec["comment_id"]
                    if rec["_is_deleted"]:
                        deleted_removed_count += 1
                        continue
                    if not c_id:
                        continue
                    if c_id in seen_comment_ids:
                        duplicate_count += 1
                        continue

                    seen_comment_ids.add(c_id)
                    clean_rec = {k: v for k, v in rec.items() if not k.startswith("_")}
                    collected_records.append(clean_rec)
                    added_this_post += 1

                    if len(collected_records) >= target_count:
                        break
                if len(collected_records) >= target_count:
                    break

            if (idx + 1) % 5 == 0 or len(collected_records) >= target_count:
                print(
                    f"  Thread {idx+1}/{len(posts_to_scan)}: Post '{post_title[:30]}...' (+{added_this_post} komentar) "
                    f"-> Total: {len(collected_records)}/{target_count}"
                )
                # Simpan inkremental agar data langsung bisa dibaca di disk
                csv_path = os.path.join("data", "raw", f"{game.lower()}_comments_raw.csv")
                json_path = os.path.join("data", "raw", f"{game.lower()}_comments_raw.json")
                save_to_csv(csv_path, collected_records)
                save_to_json(json_path, collected_records)

            time.sleep(0.3)

    return collected_records, duplicate_count, deleted_removed_count


def save_to_csv(filepath: str, records: list):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for r in records:
            writer.writerow(r)


def save_to_json(filepath: str, records: list):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, mode="w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def main():
    target = 5000
    if len(sys.argv) > 1:
        try:
            target = int(sys.argv[1])
        except ValueError:
            pass

    all_records = []
    stats = {}

    for cfg in SUBREDDIT_CONFIG:
        game = cfg["game"]
        sub = cfg["subreddit"]
        csv_file = cfg["csv_filename"]
        json_file = cfg["json_filename"]
        json_path = os.path.join("data", "raw", json_file)

        # Jika data game sudah mencapai target 5000 di disk, gunakan yang sudah ada
        if os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                if len(existing) >= target:
                    print(f"[SKIP] {game} sudah memiliki {len(existing)} komentar lengkap di {json_path}. Melewati scraping {game}.")
                    stats[game] = {
                        "count": len(existing),
                        "duplicates": 0,
                        "deleted_removed": 0,
                    }
                    all_records.extend(existing)
                    continue
            except Exception:
                pass

        records, dup_cnt, del_cnt = fetch_comments_stream(
            subreddit=sub,
            game=game,
            target_count=target,
        )

        stats[game] = {
            "count": len(records),
            "duplicates": dup_cnt,
            "deleted_removed": del_cnt,
        }

        # Simpan CSV
        csv_path = os.path.join("data", "raw", csv_file)
        save_to_csv(csv_path, records)
        print(f"[SUCCESS] CSV {game} tersimpan: {csv_path} ({len(records)} baris)")

        # Simpan JSON
        json_path = os.path.join("data", "raw", json_file)
        save_to_json(json_path, records)
        print(f"[SUCCESS] JSON {game} tersimpan: {json_path}")

        all_records.extend(records)

    # Simpan Gabungan CSV & JSON
    comb_csv = os.path.join("data", "raw", "reddit_comments_raw.csv")
    comb_json = os.path.join("data", "raw", "reddit_comments_raw.json")
    save_to_csv(comb_csv, all_records)
    save_to_json(comb_json, all_records)

    print("\n" + "=" * 60)
    print("RINGKASAN FINAL PENGUMPULAN DATASET (CSV & JSON):")
    print("=" * 60)
    print(f"Total komentar CS2              : {stats['CS2']['count']}")
    print(f"Total komentar VALORANT         : {stats['VALORANT']['count']}")
    print(f"Total komentar gabungan         : {len(all_records)}")
    total_dups = sum(s["duplicates"] for s in stats.values())
    total_dels = sum(s["deleted_removed"] for s in stats.values())
    print(f"Total komentar duplikat dibuang : {total_dups}")
    print(f"Total komentar deleted dibuang  : {total_dels}")
    print(f"File JSON dihasilkan:")
    print(f" - data/raw/cs2_comments_raw.json")
    print(f" - data/raw/valorant_comments_raw.json")
    print(f" - data/raw/reddit_comments_raw.json")
    print("=" * 60)


if __name__ == "__main__":
    main()
