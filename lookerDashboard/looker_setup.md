# Looker Studio Dashboard

## Live Dashboard
[NBA Analytics Dashboard '24-'25 Season](https://lookerstudio.google.com/u/0/reporting/787022c9-e531-44ce-9291-e9183fe5bf2e/page/dRZtF)

---

## Overview

The dashboard contains two interactive tiles built on top of BigQuery views. Both charts respond to clicks — selecting a player in either chart filters the view to show that player's stats across both tiles.

---

## Tile 1 — Top Scorers by Average Points (Bar Chart)

Shows average points per game for 10 NBA stars across the 2024-25 season, sorted descending.

**Setup:**
1. Add data source: `top_scorers`
2. Insert → Vertical bar chart
3. Dimension: `player_name`
4. Metric: `avg_pts` (aggregation: Average)
5. Sort: `avg_pts` descending
6. Title: "Top Scorers by Average Points ('24-'25 Season)"

**Interactivity:** Click any bar to filter the time series below to that player's season progression.

---

## Tile 2 — Player Performance Over Time (Time Series)

Shows average points per month per player across the 2024-25 regular season (Oct 2024 – Apr 2025).

**Setup:**
1. Add data source: `player_performance_over_time`
2. Insert → Time series chart
3. Dimension - X axis: `game_date` (granularity: Year Month)
4. Breakdown dimension: `player_name`
5. Metric - Y axis: `pts` (aggregation: Average)
6. Add filter: `game_date` Between `2024-10-01` and `2025-04-13`
7. Title: "Player Performance by Points ('24-'25 Season)"

**Interactivity:**
- Click any player name in the legend to isolate their line
- Use the **right arrow** on the legend to scroll and view the remaining 4 players not shown by default
- Hover over any point on the chart to see that player's exact average points for that month

---

## Connect to BigQuery

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com) → Create → Report
2. Add data → BigQuery → NBApipeline → nba_analytics
3. Select the relevant view for each tile as described above
