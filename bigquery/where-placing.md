# BigQuery Optimization: `WHERE` vs. `HAVING` 🔍

A common mistake in SQL is placing filters in the `HAVING` clause that do not actually involve an aggregated result. In BigQuery, this forces the engine to perform heavy calculations on the entire dataset before throwing away the rows you didn't want in the first place.

## 1. The Core Difference

* **`WHERE`**: Filters data **before** the `GROUP BY` and aggregation occurs. It reduces the number of rows the CPU has to process.
* **`HAVING`**: Filters data **after** the aggregation. It should only be used for conditions involving aggregate functions like `SUM()`, `COUNT()`, or `AVG()`.

---

## 2. The "Bad" Way: Filtering Late

In this example, BigQuery is forced to group every single name in the United States since 1910 and calculate the sum of births for all of them, only to discard everything that doesn't start with 'M' at the very last second.

### ❌ Sub-optimal Query

```sql
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

```

---

## 3. The "Good" Way: Filtering Early

By moving the condition to the `WHERE` clause, BigQuery ignores roughly 90% of the table immediately. The `GROUP BY` and `SUM()` operations only run on names starting with 'M'.

### ✅ Optimized Query

```sql
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

```

---

## 💡 Performance Impact

* **Slot Usage**: The optimized query uses significantly fewer slot-seconds because the aggregation stage handles a much smaller volume of data.
* **Data Scanned**: While the amount of data read from disk might be the same, the **shuffle** and **compute** stages are drastically faster.
* **Cost**: Reduced computation time means you stay within your reservation limits or finish your "On-Demand" jobs faster.

## 🛠️ Pro-Tip

Only use `HAVING` when your filter looks like this: `HAVING SUM(number) > 1000`. If the column you are filtering on is part of your `SELECT` or `GROUP BY` list (like `name`), it almost always belongs in the `WHERE` clause.
