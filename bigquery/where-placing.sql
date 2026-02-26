-- BAD: This forces the CPU to calculate EVERYTHING before filtering
SELECT 
    name, 
    SUM(number) as total_births
FROM 
    `bigquery-public-data.usa_names.usa_1910_current`
GROUP BY 
    name
HAVING 
    name LIKE 'M%' -- Filtering happens AFTER the heavy math
ORDER BY 
    total_births DESC
LIMIT 5;


-- GOOD: This tells BigQuery to ignore 90% of the data immediately
SELECT 
    name, 
    SUM(number) as total_births
FROM 
    `bigquery-public-data.usa_names.usa_1910_current`
WHERE 
    name LIKE 'M%' -- Filtering happens BEFORE the heavy math
GROUP BY 
    name
ORDER BY 
    total_births DESC
LIMIT 5;
