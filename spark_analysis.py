import os
import sys
import pandas as pd
from datetime import datetime, timedelta

# Set environment variables for macOS Spark compatibility
os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
os.environ['_JAVA_OPTIONS'] = '-Djava.security.manager=allow'

try:
    import findspark
    findspark.init()
except ImportError:
    print("Installing findspark...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "findspark"])
    import findspark
    findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Music Streaming Analytics") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark session created successfully!")

# Check dataset existence
if not os.path.exists("data/listening_logs.csv") or not os.path.exists("data/songs_metadata.csv"):
    print("Datasets not found. Please run data_generator.py first.")
    sys.exit(1)

# Load datasets
print("Loading datasets...")
listening_logs = spark.read.csv("data/listening_logs.csv", header=True, inferSchema=True)
songs_metadata = spark.read.csv("data/songs_metadata.csv", header=True, inferSchema=True)

print(f"Listening logs count: {listening_logs.count()}")
print(f"Songs metadata count: {songs_metadata.count()}")

# Enrich logs
print("Creating enriched logs...")
enriched_logs = listening_logs.join(songs_metadata, on="song_id", how="inner")
enriched_logs.cache()
enriched_logs.write.mode("overwrite").csv("output/enriched_logs/data")
print("Enriched logs saved")

# Task 1: User favorite genre
print("Finding each user's favorite genre...")
user_genre_counts = enriched_logs.groupBy("user_id", "genre").count()
window_spec = Window.partitionBy("user_id").orderBy(F.desc("count"))
user_favorite_genres = user_genre_counts.withColumn("rank", F.rank().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .select("user_id", "genre", "count") \
    .withColumnRenamed("genre", "favorite_genre") \
    .withColumnRenamed("count", "play_count")
user_favorite_genres.write.mode("overwrite").csv("output/user_favorite_genres/data")
print("User favorite genres saved")

# Task 2: Average listen time
print("Calculating average listen time per song...")
avg_listen_time = enriched_logs.groupBy("song_id", "title", "artist") \
    .agg(F.avg("duration_sec").alias("avg_duration_sec"),
         F.count("*").alias("play_count"))
avg_listen_time.write.mode("overwrite").csv("output/avg_listen_time_per_song/data")
print("Average listen time per song saved")

# Task 3: Top songs this week
print("Finding top songs this week...")
current_date = datetime.now()
week_start = current_date - timedelta(days=current_date.weekday())
logs_with_ts = enriched_logs.withColumn("ts", F.to_timestamp("timestamp"))
this_week_logs = logs_with_ts.filter(F.col("ts") >= F.lit(week_start))

if this_week_logs.count() == 0:
    print("No songs played this week.")
else:
    top_songs = this_week_logs.groupBy("song_id", "title", "artist", "genre") \
        .count() \
        .orderBy(F.desc("count")) \
        .limit(10)
    top_songs.write.mode("overwrite").csv("output/top_songs_this_week/data")
    print("Top songs this week saved")

# Task 4: Happy recommendations
print("Generating happy song recommendations...")
user_mood_counts = enriched_logs.groupBy("user_id", "mood").count()
window_spec = Window.partitionBy("user_id").orderBy(F.desc("count"))
user_primary_moods = user_mood_counts.withColumn("rank", F.rank().over(window_spec)) \
    .filter((F.col("rank") == 1) & (F.col("mood") == "Sad")) \
    .select("user_id")

if user_primary_moods.count() > 0:
    user_listened_songs = enriched_logs.filter(F.col("user_id").isin([row.user_id for row in user_primary_moods.collect()])) \
        .select("user_id", "song_id").distinct()
    happy_songs = songs_metadata.filter(F.col("mood") == "Happy")
    user_ids = user_primary_moods.select("user_id").distinct()
    potential_recommendations = user_ids.crossJoin(happy_songs)
    recommendations = potential_recommendations.join(
        user_listened_songs,
        (potential_recommendations.user_id == user_listened_songs.user_id) &
        (potential_recommendations.song_id == user_listened_songs.song_id),
        "left_anti"
    )
    window_spec = Window.partitionBy("user_id").orderBy(F.rand())
    final_recommendations = recommendations.withColumn("row", F.row_number().over(window_spec)) \
        .filter(F.col("row") <= 3) \
        .drop("row")
    final_recommendations.write.mode("overwrite").csv("output/happy_recommendations/data")
    print("Happy song recommendations saved")
else:
    print("No users primarily listen to sad songs. Skipping recommendations.")

# Task 5: Genre loyalty
print("Calculating genre loyalty scores...")
user_total_plays = enriched_logs.groupBy("user_id").count().withColumnRenamed("count", "total_plays")
user_genre_plays = enriched_logs.groupBy("user_id", "genre").count().withColumnRenamed("count", "genre_plays")
window_spec = Window.partitionBy("user_id").orderBy(F.desc("genre_plays"))
user_top_genre = user_genre_plays.withColumn("rank", F.rank().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .drop("rank")

loyalty_scores_unfiltered = user_top_genre.join(user_total_plays, on="user_id") \
    .withColumn("loyalty_score", F.col("genre_plays") / F.col("total_plays"))

loyalty_scores = loyalty_scores_unfiltered \
    .select("user_id", "genre", "loyalty_score") \
    .orderBy(F.desc("loyalty_score"))

if loyalty_scores.count() == 0:
    print("No loyalty scores found.")
else:
    loyalty_scores.write.mode("overwrite").csv("output/genre_loyalty_scores/data")
    print("Genre loyalty scores saved")

# Task 6: Night owl users
print("Finding night owl users...")
logs_with_hour = enriched_logs.withColumn("hour", F.hour(F.to_timestamp("timestamp")))
night_logs = logs_with_hour.filter((F.col("hour") >= 0) & (F.col("hour") < 5))
night_listen_counts = night_logs.groupBy("user_id").count().withColumnRenamed("count", "night_listens")
night_owl_users = night_listen_counts.filter(F.col("night_listens") >= 10).orderBy(F.desc("night_listens"))
night_owl_users.write.mode("overwrite").csv("output/night_owl_users/data")
print("Night owl users saved")

print("All tasks completed successfully!")
spark.stop()
