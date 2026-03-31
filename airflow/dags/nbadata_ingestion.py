##Objective: Scheduled DAG that pulls NBA game data daily and lands raw JSON to GCS.##

import pandas as pd
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from nba_api.stats.endpoints import LeagueGameFinder, PlayerGameLog
from nba_api.stats.static import players
from datetime import datetime, timedelta
from time import sleep
 
dag = DAG(
    dag_id='nba_ingestion',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
)

def fetch_games(**context):
    execution_date = context['ds']
    bucket = os.environ.get('GCS_BUCKET')

    # Fetch full 2024-25 season
    games = LeagueGameFinder(
        season_nullable='2024-25',
        league_id_nullable='00',
        timeout=60
    )

    sleep(1)
    df = games.get_data_frames()[0]
    records = df.to_dict(orient='records')
    games_json = '\n'.join([json.dumps(r) for r in records])

    hook = GCSHook()
    hook.upload(
        bucket_name=bucket,
        object_name=f'raw/nba/{execution_date}/games.json',
        data=games_json,
        mime_type='application/json'
    )

def fetch_player_stats(**context):
    execution_date = context['ds']
    bucket = os.environ.get('GCS_BUCKET')

    current_season = '2024-25'

    star_player_ids = [
        2544,    # LeBron James
        1630178, # Tyrese Maxey
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
            season=current_season,
            timeout=60
        )

        sleep(1)

        df = player_log.get_data_frames()[0]
        df = df.fillna(0)

        player_info = next((p for p in players.get_active_players() if p['id'] == player_id), None)
        df['player_name'] = player_info['full_name'] if player_info else str(player_id)

        records = df.to_dict(orient='records')
        all_stats.extend(records)
        print(f"Fetched {df['player_name'].iloc[0] if len(df) > 0 else player_id}: {len(records)} records")

    # Outside the loop — upload after all players are fetched
    stats_json = '\n'.join([json.dumps(r) for r in all_stats])

    hook = GCSHook()
    hook.upload(
        bucket_name=bucket,
        object_name=f'raw/nba/{execution_date}/player_stats.json',
        data=stats_json,
        mime_type='application/json'
    )

trigger_spark = BashOperator(
    task_id='trigger_spark',
    bash_command='spark-submit \
      --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
      --jars ~/gcs-connector-hadoop3-latest.jar \
      --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
      --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
      --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
      --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile={{ var.value.gcp_keyfile_path }} \
      ~/DE_ZoomCamp_FinalProject/spark/transform.py {{ ds }}',
    dag=dag
)

fetch_games_task = PythonOperator(
    task_id='fetch_games',
    python_callable=fetch_games,
    dag=dag
)

fetch_player_stats_task = PythonOperator(
    task_id='fetch_player_stats',
    python_callable=fetch_player_stats,
    dag=dag
)

[fetch_games_task, fetch_player_stats_task] >> trigger_spark