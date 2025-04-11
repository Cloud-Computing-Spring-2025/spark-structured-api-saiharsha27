import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
import os

# Set a random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Create directory for data if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

# Constants for dataset generation
NUM_USERS = 1000
NUM_SONGS = 5000
NUM_LOGS = 100000  # Number of listening logs
START_DATE = datetime(2025, 3, 1)
END_DATE = datetime(2025, 3, 31)

# Song titles and artists (fictional)
genres = ['Pop', 'Rock', 'Jazz', 'Hip-Hop', 'Classical', 'Electronic', 'R&B', 'Country', 'Metal', 'Folk']
moods = ['Happy', 'Sad', 'Energetic', 'Chill', 'Romantic', 'Angry', 'Nostalgic', 'Dreamy']

print("Generating songs metadata...")
# Generate song data
artists = [f"Artist{i+1}" for i in range(500)]
song_ids = [str(uuid.uuid4())[:8] for _ in range(NUM_SONGS)]
titles = [f"Song Title {i+1}" for i in range(NUM_SONGS)]

songs_data = {
    'song_id': song_ids,
    'title': titles,
    'artist': np.random.choice(artists, NUM_SONGS),
    'genre': np.random.choice(genres, NUM_SONGS),
    'mood': np.random.choice(moods, NUM_SONGS)
}

songs_df = pd.DataFrame(songs_data)
songs_df.to_csv('data/songs_metadata.csv', index=False)
print(f"Generated songs_metadata.csv with {NUM_SONGS} songs")

print("Generating listening logs...")
# Generate user IDs
user_ids = [f"user_{i+1}" for i in range(NUM_USERS)]

# Create listening logs
listening_data = []

# Create timestamp range
date_range = (END_DATE - START_DATE).days

for _ in range(NUM_LOGS):
    # Select random user and song
    user_id = np.random.choice(user_ids)
    song_row = songs_df.iloc[np.random.randint(0, len(songs_df))]
    song_id = song_row['song_id']
    
    # Generate random timestamp within the date range
    random_days = np.random.randint(0, date_range + 1)
    random_hours = np.random.randint(0, 24)
    random_minutes = np.random.randint(0, 60)
    timestamp = START_DATE + timedelta(days=random_days, hours=random_hours, minutes=random_minutes)
    
    # Generate duration (between 30 sec and full song length, typically 2-5 minutes)
    duration_sec = np.random.randint(30, 301)
    
    listening_data.append({
        'user_id': user_id,
        'song_id': song_id,
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'duration_sec': duration_sec
    })

logs_df = pd.DataFrame(listening_data)
logs_df.to_csv('data/listening_logs.csv', index=False)
print(f"Generated listening_logs.csv with {NUM_LOGS} logs")

# Create output directory structure
output_dirs = [
    'output',
    'output/user_favorite_genres',
    'output/avg_listen_time_per_song',
    'output/top_songs_this_week',
    'output/happy_recommendations',
    'output/genre_loyalty_scores',
    'output/night_owl_users',
    'output/enriched_logs'
]

for directory in output_dirs:
    if not os.path.exists(directory):
        os.makedirs(directory)

print("Created output directory structure")
print("Dataset generation complete!")