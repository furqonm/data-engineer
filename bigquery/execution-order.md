In SQL and BigQuery, the execution flow (Logical Query Processing) is different from the written order of the code. BigQuery processes your query in a specific sequence to optimize resource usage and data movement across its distributed workers.

### The Logical Execution Flow

| Sequence | Clause | Action |
| --- | --- | --- |
| **1** | **FROM** | BigQuery identifies the source tables in distributed storage. |
| **2** | **JOIN** | Combines tables. If optimized with **Clustering**, BigQuery filters data blocks *during* the read stage to save costs. |
| **3** | **WHERE** | Filters rows **before** any heavy math or aggregation occurs. |
| **4** | **GROUP BY** | Groups the remaining rows. Workers shuffle data to aggregate values. |
| **5** | **HAVING** | Filters the results **after** the aggregation is complete. |
| **6** | **SELECT** | Finalizes which columns to return and calculates any final expressions. |
| **7** | **ORDER BY** | Sorts the final dataset (usually done by a single worker at the end). |

---

### Why the Flow Matters for Optimization

#### 1. WHERE vs. HAVING

Filtering in the **WHERE** clause (Step 3) is much faster than filtering in **HAVING** (Step 5).

* **WHERE** reduces the number of rows that the **GROUP BY** stage has to process.
* **HAVING** forces the CPU to calculate the sum/count for everything before throwing data away.

#### 2. Expression Order in WHERE

Since BigQuery executes the `WHERE` clause early, the order of expressions inside it matters.

* BigQuery does not reorder your expressions.
* You should place the **most selective expression** (the one that removes the most data) first.
* **Example**: Filtering by a specific username (`user = 'anon'`) should happen before an expensive text search (`LIKE '%java%'`) so the search only runs on a tiny subset of data.

#### 3. Join Explosions

Because **JOIN** happens early in the flow (Step 2), a "Join Explosion" (Cartesian product) can ruin the entire pipeline.

* If you join on non-unique keys, the row count "explodes" before it even reaches the filter or aggregation stages.
* **Fix**: Use a subquery or CTE to **GROUP BY** (pre-aggregate) one side of the join so that the join key is unique.

#### 4. How BigQuery Aggregates

BigQuery does NOT use a single worker for `SUM` or `COUNT`. It uses a **Two-Stage Approach**:
1. **Distributed Stage**: Many workers calculate partial results from storage shards.
2. **Shuffle Stage**: Data is re-organized by key across the network.
3. **Final Stage**: Results are merged into the final output.

**Warning**: `COUNT(DISTINCT)` forces a heavy shuffle. If performance is slow, check your Query Plan for high "Shuffle" volume or use `APPROX_COUNT_DISTINCT`.
