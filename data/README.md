# Dataset Reddit Comments: CS2 vs VALORANT

Dataset mentah ini dikumpulkan untuk penelitian:
**"Analisis Perbandingan Sentimen Komunitas Pemain Counter-Strike 2 dan VALORANT pada Reddit Menggunakan Algoritma Naive Bayes"**

## 1. Struktur Folder
```text
data/
└── raw/
    ├── cs2_comments_raw.csv         # Komentar mentah dari r/GlobalOffensive
    ├── valorant_comments_raw.csv    # Komentar mentah dari r/VALORANT
    └── reddit_comments_raw.csv      # Dataset gabungan mentah (CS2 + VALORANT)
```

## 2. Skema Data CSV
Setiap file CSV memiliki kolom-kolom berikut:
- `game`: Label game (`CS2` atau `VALORANT`)
- `subreddit`: Asal subreddit (`GlobalOffensive` atau `VALORANT`)
- `post_id`: ID postingan Reddit
- `post_title`: Judul thread/postingan Reddit
- `post_url`: URL postingan Reddit
- `comment_id`: ID unik komentar
- `comment_text`: Teks mentah komentar (tanpa lowercase, tanpa stopword removal, tanpa stemming, tanpa penghapusan tanda baca)
- `comment_score`: Skor upvote/downvote net komentar
- `comment_created_at`: Waktu pembuatan komentar (format ISO 8601 UTC)
- `author`: Username pembuat komentar (metadata)
- `scraped_at`: Timestamp proses scraping dijalankan (UTC)

## 3. Catatan Integritas Data
- Teks komentar dibiarkan asli apa adanya (raw) untuk keperluan tahapan preprocessing selanjutnya.
- Komentar `[deleted]` dan `[removed]` telah disaring otomatis saat pengumpulan data.
- Duplikasi `comment_id` telah disaring secara otomatis.
