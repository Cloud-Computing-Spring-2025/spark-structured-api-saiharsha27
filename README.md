# Music Streaming Analytics with Apache Spark

## 📘 Overview

This project performs large-scale data analysis on a simulated music streaming platform using **Apache Spark**. The analysis includes:
- Identifying user preferences (favorite genre)
- Calculating song popularity and play durations
- Recommending songs based on mood
- Scoring user loyalty to genres
- Detecting night owl listening behavior
- Highlighting top trending songs

All processing is done using **Spark DataFrames** and **PySpark**.

---

## 📂 Dataset Description

Two CSV files are used:

### `listening_logs.csv`
| Column       | Description                    |
|--------------|--------------------------------|
| user_id      | Unique ID of the user          |
| song_id      | ID of the song played          |
| duration_sec | Listening duration in seconds  |
| timestamp    | DateTime when the song played  |

### `songs_metadata.csv`
| Column    | Description                  |
|-----------|------------------------------|
| song_id   | Unique song ID               |
| title     | Title of the song            |
| artist    | Performing artist            |
| genre     | Song's musical genre         |
| mood      | Mood classification (Happy, Sad, etc.) |

---

## 🏁 How to Run the Script

### 🔧 Pre-requisites
- Python 3.8+
- Java 8 or Java 11 (not Java 17+)
- Apache Spark 3.x
- Install dependencies:
  ```bash
  pip install pyspark pandas findspark
  ```

### ▶️ Execution
Run the analysis script:
```bash
python spark_analysis.py
```

Or launch Spark shell:
```bash
pyspark
```

---

## 🖼️ Output & Screenshots

### 📁 Output Directory Structure
```
output/
├── enriched_logs/
├── user_favorite_genres/
├── avg_listen_time_per_song/
├── top_songs_this_week/
├── happy_recommendations/
├── genre_loyalty_scores/
└── night_owl_users/
```

### ✅ Sample Result: Night Owl Users
```csv
user_545,38
user_115,36
user_146,36
user_698,35
user_279,34
user_242,33
user_166,33
user_572,33
```

### ✅ Sample Result: Happy Song Recommendations
```csv
user_id,song_id,title,artist,genre,mood
user_321,SONG_12,Feel Good,Vibes Club,Pop,Happy
```

## ❌ Errors & Fixes

### 1. **Java Security Manager Error**
```
UnsupportedOperationException: getSubject is supported only if a security manager is allowed
```
✅ **Fix:** Downgraded to Java 11  
✅ **Added:** Environment config to disable security manager
```python
os.environ['_JAVA_OPTIONS'] = '-Djava.security.manager=allow'
```

---

### 2. **Empty Top Songs Output**
Cause: Timestamps were not in this week's date range.

✅ **Fix:** Casted string timestamp to real TimestampType and extended filter to last 30 days
```python
enriched_logs.withColumn("ts", F.to_timestamp("timestamp"))
```

---

## ✅ Conclusion

This project showcases how **PySpark** can handle realistic music streaming data analytics. It also emphasizes filtering, ranking, and recommendation logic using Spark SQL functions and windowing operations.

---
```