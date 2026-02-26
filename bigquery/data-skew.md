Here is the Markdown documentation for **Data Skew**, optimized for your GitHub repository. This guide explains how to identify and mitigate "hot keys" that cause uneven workload distribution in BigQuery.

---

# BigQuery Optimization: Handling Data Skew (Hot Keys) ⚖️

**Data Skew** occurs when data is partitioned or grouped by a key where one value (the "hot key") appears significantly more often than others. In BigQuery, this causes one "slot" or worker to do 90% of the work while others sit idle, leading to high **Max Time** vs. low **Avg Time** in your query plan.

## 1. Checking for Skew

You can identify skew by comparing execution timings. If your query plan shows that a few workers are taking much longer than the average, you likely have skewed data. Use `APPROX_TOP_COUNT` to see if a few keys dominate the dataset.

```sql
-- Checking: Use APPROX_TOP_COUNT to check for skew. 
-- The skew happens if AVG time is significantly lower than MAX time in the Query Plan.
SELECT
  wiki,
  APPROX_TOP_COUNT(title, 5) as top_titles,
  SUM(views) as total_views
FROM
  `bigquery-public-data.wikipedia.pageviews_2026`
WHERE
  datehour >= "2026-01-01"
GROUP BY
  wiki
ORDER BY
  total_views DESC;

```

---

## 2. Finding the "Hot Key"

A "Hot Key" is a specific value in your join or group-by column that appears with extreme frequency. For example, in Wikipedia data, the `'en'` (English) wiki usually has far more traffic than any other language.

```sql
-- Finding hot key: Look for a value that has a count significantly higher than the others
SELECT 
    APPROX_TOP_COUNT(wiki, 10) as top_wikis 
FROM 
    `bigquery-public-data.wikipedia.pageviews_2026`
WHERE 
    datehour >= "2026-01-01";

```

---

## 3. Mitigating Skew: Excluding Hot Keys

If your business logic allows, filtering out these hot keys early can dramatically rebalance the workload and speed up the query for the rest of the data.

```sql
-- Exclude hot key: Filter multiple hot keys early to fix max vs avg time imbalance
SELECT
  wiki,
  SUM(views) as total_views
FROM
  `bigquery-public-data.wikipedia.pageviews_2026`
WHERE
  datehour >= "2026-01-01"
  AND wiki NOT IN ('en', 'en.m') -- Removing the skewed English data
GROUP BY
  wiki
ORDER BY
  total_views DESC;

```

*Created for the BigQuery Performance Optimization Guide.*

Would you like me to add this to a **Table of Contents** for your main `README.md`?
