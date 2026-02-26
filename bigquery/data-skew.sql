-- 1. Checking: Use APPROX_TOP_COUNT to check for skew. The skew happens if AVG time is lower than MAX time.
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

-- 2. Finding hot key: Look for a value that has a count significantly higher than the others
SELECT 
    APPROX_TOP_COUNT(wiki, 10) as top_wikis 
FROM 
    `bigquery-public-data.wikipedia.pageviews_2026`
WHERE 
    datehour >= "2026-01-01";

-- 3. Exclude hot key:  Filter multiple hot keys early to fix max vs avg time
SELECT
  wiki,
  SUM(views) as total_views
FROM
  `bigquery-public-data.wikipedia.pageviews_2026`
WHERE
  datehour >= "2026-01-01"
  AND wiki NOT IN ('en', 'en.m') 
GROUP BY
  wiki
ORDER BY
  total_views DESC;
