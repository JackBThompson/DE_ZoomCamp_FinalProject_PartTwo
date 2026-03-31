-- Objective: Creates curated views and tables in BigQuery for downstream reporting and dashboards.


-- Partitioning — dashboard queries always filter by date range. Partitioning means BigQuery only scans the relevant days rather than the entire table, reducing query cost by up to 90%.
-- Clustering — most filters are team or player specific. Clustering lets BigQuery skip non-matching blocks entirely at no extra cost.


-- [SQL] CREATE TABLE nba_analytics.game_stats
-- Physically stores the data in BigQuery, rows written by Spark land here
-- Partitioned by GAME_DATE so queries only scan relevant days
-- Clustered by TEAM_ABBREVIATION so filtering by team hits minimal data
-- Columns: SEASON_ID, TEAM_ID, TEAM_ABBREVIATION, TEAM_NAME,
--            GAME_ID, GAME_DATE, MATCHUP, WL, MIN, PTS,
--            FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT,
--            FTM, FTA, FT_PCT, OREB, DREB, REB,
--            AST, STL, BLK, TOV, PF, PLUS_MINUS, ingestion_date

CREATE TABLE IF NOT EXISTS nba_analytics.game_stats (
    season_id         STRING,
    team_id           INT64,
    team_abbreviation STRING,
    team_name         STRING,
    game_id           STRING,
    game_date         DATE,
    matchup           STRING,
    wl                STRING,
    min               INT64,
    pts               INT64,
    fgm               INT64,
    fga               INT64,
    fg_pct            FLOAT64,
    fg3m              INT64,
    fg3a              INT64,
    fg3_pct           FLOAT64,
    ftm               INT64,
    fta               INT64,
    ft_pct            FLOAT64,
    oreb              INT64,
    dreb              INT64,
    reb               INT64,
    ast               INT64,
    stl               INT64,
    blk               INT64,
    tov               INT64,
    pf                INT64,
    plus_minus        FLOAT64,
    ingestion_date    DATE
)
PARTITION BY game_date
CLUSTER BY team_abbreviation;

-- [SQL] CREATE TABLE nba_analytics.player_stats
--Physically stores the data in BigQuery, rows written by Spark land here
--   Partition by GAME_DATE
--   Cluster by Player_ID for fast player-specific lookups
--   Columns: SEASON_ID, Player_ID, Game_ID, GAME_DATE,
--            MATCHUP, WL, MIN, PTS, FGM, FGA, FG_PCT,
--            FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT,
--            OREB, DREB, REB, AST, STL, BLK, TOV,
--            PF, PLUS_MINUS, ingestion_date

CREATE TABLE IF NOT EXISTS nba_analytics.player_stats (
    season_id      STRING,
    player_id      INT64,
    game_id        STRING,
    game_date      DATE,
    matchup        STRING,
    wl             STRING,
    min            INT64,
    pts            INT64,
    fgm            INT64,
    fga            INT64,
    fg_pct         FLOAT64,
    fg3m           INT64,
    fg3a           INT64,
    fg3_pct        FLOAT64,
    ftm            INT64,
    fta            INT64,
    ft_pct         FLOAT64,
    oreb           INT64,
    dreb           INT64,
    reb            INT64,
    ast            INT64,
    stl            INT64,
    blk            INT64,
    tov            INT64,
    pf             INT64,
    plus_minus     FLOAT64,
    ingestion_date DATE
)
PARTITION BY game_date
CLUSTER BY player_id;



-- [VIEW] Player Average Points for entire season
CREATE OR REPLACE VIEW nba_analytics.top_scorers AS
SELECT
    player_id,
    player_name,
    ROUND(AVG(pts), 1)        AS avg_pts,
    ROUND(AVG(reb), 1)        AS avg_reb,
    ROUND(AVG(ast), 1)        AS avg_ast,
    ROUND(AVG(plus_minus), 1) AS avg_plus_minus,
    COUNT(*) AS games_played
FROM nba_analytics.player_stats
WHERE game_date >= '2024-10-01'
GROUP BY player_id, player_name
ORDER BY avg_pts DESC
LIMIT 20;
 
-- [VIEW] Player Performance for entire season
 
CREATE OR REPLACE VIEW nba_analytics.player_performance_over_time AS
SELECT
    player_id,
    player_name,
    game_date,
    pts,
    reb,
    ast,
    plus_minus
FROM nba_analytics.player_stats
WHERE game_date >= '2024-10-01'
ORDER BY game_date;
 
 
-- [VIEW] win_loss_by_team - NOT USED IN DASHBOARD
 
CREATE OR REPLACE VIEW nba_analytics.win_loss_by_team AS
SELECT
    team_abbreviation,
    wl,
    COUNT(*) AS game_count
FROM nba_analytics.game_stats
WHERE game_date >= '2024-10-01'
GROUP BY team_abbreviation, wl
ORDER BY team_abbreviation;
 
 
-- [VIEW] pipeline_health_check - NOT USED IN DASHBOARD
 
CREATE OR REPLACE VIEW nba_analytics.pipeline_health_check AS
SELECT
    ingestion_date,
    COUNT(*) AS records_ingested
FROM nba_analytics.game_stats
WHERE ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;