SELECT
  language,
  title,
  SUM(views) AS views
FROM
  `cloud-training-demos.wikipedia_benchmark.Wiki100B`
WHERE
  title LIKE '%Google%'
GROUP BY
  language,
  title
ORDER BY
  views DESC;
