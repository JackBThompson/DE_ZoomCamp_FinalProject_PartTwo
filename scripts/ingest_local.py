##Objective: Standalone script to pull NBA data locally and upload to GCS, bypassing NBA.com cloud IP ban.##

import json
import os
import pandas as pd
from nba_api.stats.endpoints import LeagueGameFinder, PlayerGameLog
from nba_api.stats.static import players
from google.cloud import storage
from datetime import date
from time import sleep
 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-key.json'
 
# Read bucket from environment variable — set in .env or export before running
BUCKET = os.environ.get('GCS_BUCKET')
if not BUCKET:
    raise ValueError("GCS_BUCKET environment variable is not set. Run: export GCS_BUCKET=your-bucket-name")
execution_date = '2025-04-13'
 
def upload_to_gcs(bucket_name, destination, data):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_string(data, content_type='application/json')
    print(f'Uploaded to gs://{bucket_name}/{destination}')
 
# Fetch games — full 2024-25 season
print('Fetching games...')
games = LeagueGameFinder(
    season_nullable='2024-25',
    league_id_nullable='00',
    timeout=60
)
sleep(1)
df_games = games.get_data_frames()[0]
games_json = '\n'.join([json.dumps(r) for r in df_games.to_dict(orient='records')])
upload_to_gcs(BUCKET, f'raw/nba/{execution_date}/games.json', games_json)
print(f'Games uploaded: {len(df_games)} records')
 
# Fetch player stats — top NBA stars for 2024-25 season
print('Fetching player stats...')
star_player_ids = [
    2544,    # LeBron James
    1630178,  # Tyrese Maxey
    1629029, # Luka Doncic
    203507,  # Giannis Antetokounmpo
    1628369, # Jayson Tatum
    201939,  # Stephen Curry
    1629630, # Ja Morant
    203999,  # Nikola Jokic
    1641705, # Victor Wembanyama
    1628983, # Shai Gilgeous-Alexander
    ]
 
all_stats = []
 
for player_id in star_player_ids:
    player_log = PlayerGameLog(
        player_id=player_id,
        season='2024-25',
        timeout=60
    )
    sleep(1)
    df = player_log.get_data_frames()[0]
    df = df.fillna(0)
 
    # Look up player name from static list
    player_info = next((p for p in players.get_active_players() if p['id'] == player_id), None)
    df['player_name'] = player_info['full_name'] if player_info else str(player_id)
 
    all_stats.extend(df.to_dict(orient='records'))
    print(f"Fetched {df['player_name'].iloc[0] if len(df) > 0 else player_id}: {len(df)} records")
 
stats_json = '\n'.join([json.dumps(r) for r in all_stats])
upload_to_gcs(BUCKET, f'raw/nba/{execution_date}/player_stats.json', stats_json)
print(f'Player stats uploaded: {len(all_stats)} records')