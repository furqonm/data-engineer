-- This command changes the session settings to Batch
SET @@query_priority = 'BATCH';

-- Now run your actual query
SELECT 
    name, 
    SUM(number) as total 
FROM 
    `bigquery-public-data.usa_names.usa_1910_current`
GROUP BY 1 
LIMIT 10;
