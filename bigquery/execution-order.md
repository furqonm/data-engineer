# 🧠 BigQuery Logical Execution & Optimization

In BigQuery, the order you **write** SQL is not the order BigQuery **executes** it. Understanding this flow is the key to reducing slot usage and query costs.

## 🏗️ The Execution Pipeline

BigQuery processes your query in a specific sequence to minimize data movement across its distributed network.

| Sequence | Clause | Technical Action |
| --- | --- | --- |
| **1** | 📂 **FROM** | Identifies source tables in distributed storage. |
| **2** | 🔗 **JOIN** | Combines tables. If **Clustered**, it filters data blocks *during* the read stage. |
| **3** | 🧹 **WHERE** | Filters rows **before** any heavy math or aggregation occurs. |
| **4** | 🔢 **GROUP BY** | Groups rows. Workers **Shuffle** data to prepare for aggregation. |
| **5** | 🧪 **HAVING** | Filters results **after** the aggregation is complete. |
| **6** | 💎 **SELECT** | Finalizes columns and calculates remaining expressions/aliases. |
| **7** | 🏁 **ORDER BY** | Sorts the final dataset (typically the most expensive single-worker task). |
| **8** | 🛑 **LIMIT** | Discards all but the specified number of rows from the final result set. |

---

## ⚡ Critical Optimization Rules

### 1. `WHERE` vs. `HAVING`

> [!TIP]
> **Filter Early:** Always use `WHERE` instead of `HAVING` unless you are specifically filtering on an aggregated result (like `SUM(sales) > 100`).

* **WHERE**: Reduces the row count *before* the expensive `GROUP BY` stage.
* **HAVING**: Forces the CPU to aggregate *everything* first, only to discard it later.

### 2. Expression Order in `WHERE`

BigQuery assumes the user has provided the best order and **does not attempt to reorder expressions**.

* Place the **most selective expression** first.
* **Example**: `WHERE user_id = 123 AND text LIKE '%search%'` is faster because the `LIKE` only runs on one user's data.

### 3. Preventing "Join Explosions"

> [!WARNING]
> **Join Early, Explode Early:** Because `JOIN` happens at Step 2, non-unique keys can create a massive Cartesian product that slows down all subsequent steps.

* **Diagnosis**: Check the Query Plan for `Output Rows` being much higher than `Input Rows`.
* **The Fix**: Use a subquery or CTE to `GROUP BY` (pre-aggregate) your data before the join to ensure keys are unique.

### 4. The `LIMIT` Misconception

> [!CAUTION]
> **LIMIT does not reduce scan costs:** Because BigQuery uses columnar storage, a `LIMIT` clause does not stop the engine from scanning the entire column. It only reduces the amount of data returned to the UI/API.

* **When to use**: Use `LIMIT` to prevent the "Response too large" error or to sample data for preview.
* **Performance Tip**: When combined with `ORDER BY`, BigQuery uses a **Top-K algorithm**, which is much more efficient than a full sort because it only keeps the "top" N rows in memory.

---

## 🛰️ How Distributed Aggregation Works

BigQuery does **not** aggregate in a single worker. It uses a **Three-Step Distributed Approach**:

1. **Distributed Stage**: Many workers calculate partial results from storage shards simultaneously.
2. **Shuffle Stage**: Data is re-organized by key across the network to group related data.
3. **Final Stage**: Results are merged into the final output.

> [!IMPORTANT]
> **Avoid `COUNT(DISTINCT)` on massive data:** This forces a heavy shuffle because every single instance of a value must be moved to the same worker to verify uniqueness. Use `APPROX_COUNT_DISTINCT` for a 1% error margin with significantly lower costs.
