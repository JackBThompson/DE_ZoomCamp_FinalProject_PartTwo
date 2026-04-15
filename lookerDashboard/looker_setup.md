# Looker Studio Dashboard Setup

## Overview

The dashboard contains two tiles built on top of BigQuery views from the CRM migration pipeline. Both charts are interactive — clicking a segment in either tile filters the view across the report.

---

## Connect to BigQuery

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com) → Create → Report
2. Add data → BigQuery → your project → `crm_analytics`
3. Select the relevant view for each tile as described below

---

## Tile 1 — Deal Distribution by Stage (Bar Chart)

Shows deal count and total value broken down by pipeline stage (prospecting → closed won/lost).

**Data source:** `crm_analytics.deal_distribution_by_stage`

**Setup:**
1. Insert → Vertical bar chart
2. Dimension: `deal_stage`
3. Metric: `deal_count` (aggregation: Sum)
4. Sort: `deal_count` descending
5. Title: "Deal Distribution by Stage"

**Optional:** Add a second metric `total_value` as a line overlay to show deal value alongside count.

**Interactivity:** Click any bar to filter the signups tile below to customers in that deal stage.

---

## Tile 2 — Customer Signups Over Time (Time Series)

Shows new customers per month broken down by plan tier across the full dataset.

**Data source:** `crm_analytics.customer_signups_over_time`

**Setup:**
1. Insert → Time series chart
2. Dimension - X axis: `signup_month` (granularity: Year Month)
3. Breakdown dimension: `plan_tier`
4. Metric - Y axis: `new_customers` (aggregation: Sum)
5. Title: "Customer Signups Over Time by Plan Tier"

**Interactivity:**
- Click any plan tier in the legend to isolate that line
- Hover over any point to see exact signup count for that month and plan

---

## Optional: MRR by Plan Scorecard

A simple scorecard showing total MRR per plan tier.

**Data source:** `crm_analytics.mrr_by_plan`

**Setup:**
1. Insert → Scorecard or Table
2. Dimension: `plan`
3. Metric: `total_mrr`
4. Title: "MRR by Plan Tier"
