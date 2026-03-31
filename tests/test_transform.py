import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import col, to_date, lit
 
 
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test_nba_transform") \
        .master("local[1]") \
        .getOrCreate()
 
 
# Test 1 — GAME_DATE is cast to DateType correctly
def test_game_date_cast(spark):
    data = [("2025-01-15", "LBJ", 30)]
    df = spark.createDataFrame(data, ["GAME_DATE", "PLAYER_NAME", "PTS"])
    df = df.withColumn("GAME_DATE", to_date(col("GAME_DATE"), "yyyy-MM-dd"))
    assert df.schema["GAME_DATE"].dataType == types.DateType()
 
 
# Test 2 — Deduplication on PLAYER_ID and GAME_ID removes duplicate rows
def test_deduplication(spark):
    data = [
        (2544, "0022400001", "2025-01-15", 30),
        (2544, "0022400001", "2025-01-15", 30),  # duplicate
    ]
    df = spark.createDataFrame(data, ["PLAYER_ID", "GAME_ID", "GAME_DATE", "PTS"])
    df = df.dropDuplicates(["PLAYER_ID", "GAME_ID"])
    assert df.count() == 1
 
 
# Test 3 — Rows with null PLAYER_ID are dropped
def test_null_player_id_dropped(spark):
    data = [
        (2544, "0022400001", 30),
        (None, "0022400002", 25),  # should be dropped
    ]
    df = spark.createDataFrame(data, ["PLAYER_ID", "GAME_ID", "PTS"])
    df = df.filter(col("PLAYER_ID").isNotNull())
    assert df.count() == 1
 
 
# Test 4 — ingestion_date is cast to DateType, not left as string
def test_ingestion_date_is_date_type(spark):
    data = [(2544, 30)]
    df = spark.createDataFrame(data, ["PLAYER_ID", "PTS"])
    df = df.withColumn("ingestion_date", to_date(lit("2025-04-13"), "yyyy-MM-dd"))
    assert df.schema["ingestion_date"].dataType == types.DateType()
 
 
# Test 5 — Column names are lowercased after toDF()
def test_column_names_lowercased(spark):
    data = [(2544, 30)]
    df = spark.createDataFrame(data, ["PLAYER_ID", "PTS"])
    df = df.toDF(*[c.lower() for c in df.columns])
    assert "player_id" in df.columns
    assert "pts" in df.columns
    assert "PLAYER_ID" not in df.columns