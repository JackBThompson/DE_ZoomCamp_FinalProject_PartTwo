##Objective: Reads raw NBA JSON from GCS, cleans and transforms game and player stats, and writes to BigQuery.##
## Python holds it all together - PySpark handles reading/writing, and Spark SQL handles the transformation logic  ##

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import col, to_date, lit, expr
from datetime import datetime
import os
import sys

# [PYTHON] Read execution_date from command line args.  Airflow passes execution_date when it calls spark-submit

execution_date = sys.argv[1]

bucket = os.environ.get('GCS_BUCKET')
project = os.environ.get('GCP_PROJECT_ID')
dataset = os.environ.get('BIGQUERY_DATASET')
keyfile = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'gcp-key.json')


# [PYTHON] Create Spark session with BigQuery and GCS connectors
spark = SparkSession.builder \
    .appName('nba_transform') \
    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .config('spark.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
    .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
    .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', keyfile) \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', bucket)

# [PYSPARK] Read raw games JSON from GCS path
# [PYSPARK] Read raw player stats JSON from GCS path

df_games = spark.read.json(f'gs://{bucket}/raw/nba/{execution_date}/games.json')
df_stats = spark.read.json(f'gs://{bucket}/raw/nba/{execution_date}/player_stats.json')

# [PYSPARK] Clean GAME data:

df_games = df_games \
    .withColumn('GAME_DATE', to_date(col('GAME_DATE'), 'yyyy-MM-dd')) \
    .withColumn('PTS', col('PTS').cast(types.IntegerType())) \
    .withColumn('REB', col('REB').cast(types.IntegerType())) \
    .withColumn('AST', col('AST').cast(types.IntegerType())) \
    .withColumn('STL', col('STL').cast(types.IntegerType())) \
    .withColumn('BLK', col('BLK').cast(types.IntegerType())) \
    .withColumn('TOV', col('TOV').cast(types.IntegerType())) \
    .withColumn('TEAM_ID', col('TEAM_ID').cast(types.IntegerType())) \
    .withColumn('FG_PCT', col('FG_PCT').cast(types.FloatType())) \
    .withColumn('FG3_PCT', col('FG3_PCT').cast(types.FloatType())) \
    .withColumn('FT_PCT', col('FT_PCT').cast(types.FloatType())) \
    .withColumn('PLUS_MINUS', col('PLUS_MINUS').cast(types.FloatType())) \
    .filter(col('GAME_ID').isNotNull()) \
    .dropDuplicates(['GAME_ID', 'TEAM_ID'])

df_games = df_games.toDF(*[c.lower() for c in df_games.columns])

# [PYSPARK] Clean PLAYER data:

df_stats = df_stats \
    .withColumn('GAME_DATE', to_date(col('GAME_DATE'), 'MMM dd, yyyy')) \
    .withColumn('PTS', col('PTS').cast(types.IntegerType())) \
    .withColumn('REB', col('REB').cast(types.IntegerType())) \
    .withColumn('AST', col('AST').cast(types.IntegerType())) \
    .withColumn('STL', col('STL').cast(types.IntegerType())) \
    .withColumn('BLK', col('BLK').cast(types.IntegerType())) \
    .withColumn('TOV', col('TOV').cast(types.IntegerType())) \
    .withColumn('MIN', col('MIN').cast(types.IntegerType())) \
    .withColumn('FG_PCT', col('FG_PCT').cast(types.FloatType())) \
    .withColumn('FG3_PCT', col('FG3_PCT').cast(types.FloatType())) \
    .withColumn('FT_PCT', col('FT_PCT').cast(types.FloatType())) \
    .withColumn('PLUS_MINUS', col('PLUS_MINUS').cast(types.FloatType())) \
    .filter(col('Player_ID').isNotNull()) \
    .dropDuplicates(['Player_ID', 'Game_ID'])

df_stats = df_stats.toDF(*[c.lower() for c in df_stats.columns])

# [PYTHON] Add metadata column: ingestion_date = execution_date to both DataFrames

df_games = df_games.withColumn('ingestion_date', to_date(lit(execution_date), 'yyyy-MM-dd'))
df_stats = df_stats.withColumn('ingestion_date', to_date(lit(execution_date), 'yyyy-MM-dd'))

# [PYSPARK] Select only the columns defined in the BigQuery game_stats table

df_games = df_games.select(
    'season_id', 'team_id', 'team_abbreviation', 'team_name',
    'game_id', 'game_date', 'matchup', 'wl', 'min',
    'pts', 'fgm', 'fga', 'fg_pct',
    'fg3m', 'fg3a', 'fg3_pct',
    'ftm', 'fta', 'ft_pct',
    'oreb', 'dreb', 'reb',
    'ast', 'stl', 'blk', 'tov', 'pf',
    'plus_minus', 'ingestion_date'
)

# [PYSPARK] Select only the columns defined in the BigQuery player_stats table

df_stats = df_stats.select(
    'season_id', 'player_id', 'player_name', 'game_id', 'game_date',
    'matchup', 'wl', 'min',
    'pts', 'fgm', 'fga', 'fg_pct',
    'fg3m', 'fg3a', 'fg3_pct',
    'ftm', 'fta', 'ft_pct',
    'oreb', 'dreb', 'reb',
    'ast', 'stl', 'blk', 'tov', 'pf',
    'plus_minus', 'ingestion_date'
)

# [PYSPARK] Write games to GCS processed zone as Parquet partitioned by GAME_DATE
# [PYSPARK] Write player stats to GCS processed zone as Parquet partitioned by GAME_DATE

df_games.write.partitionBy('game_date') \
    .mode('append') \
    .parquet(f'gs://{bucket}/processed/nba/{execution_date}/games/')

df_stats.write.partitionBy('game_date') \
    .mode('append') \
    .parquet(f'gs://{bucket}/processed/nba/{execution_date}/player_stats/')

# [SPARK SQL] Load games Parquet to BigQuery table: nba_analytics.game_stats

df_games.write.format('bigquery') \
    .option('table', f'{project}.{dataset}.game_stats') \
    .option('partitionField', 'game_date') \
    .option('clusteredFields', 'team_abbreviation') \
    .option('writeMethod', 'direct') \
    .mode('append') \
    .save()

# [SPARK SQL] Load player stats Parquet to BigQuery table: nba_analytics.player_stats

df_stats.write.format('bigquery') \
    .option('table', f'{project}.{dataset}.player_stats') \
    .option('partitionField', 'game_date') \
    .option('clusteredFields', 'player_id') \
    .option('writeMethod', 'direct') \
    .mode('append') \
    .save()

spark.stop()