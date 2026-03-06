# 🛡️ BigQuery Cost Management

BigQuery provides several mechanisms to manage spending at the user, project, and individual query levels. Implementing these ensures that unoptimized queries (like **Join Explosions**) do not consume your entire budget.

## 1. Custom Cost Controls (Administrative Enforcement)

**Managed by: GCP Admins / Billing Admins**

These are "hard limits" set at the infrastructure level. A Data Analyst cannot change these settings. They are the primary defense against project-wide budget depletion.

* **Query usage per day per user**: Limits the total TiB scanned by a **specific identity** across 24 hours.
* **Query usage per day**: Limits the total aggregate scan volume for the **entire project**.

### How to Configure (Admin Only):

1. Navigate to **IAM & Admin** > **Quotas & System Limits**.
2. Filter by Service: **BigQuery API**.
3. Search for the Metric: `Query usage per day per user`.
4. Select the quota, click **Edit Quotas**, and enter the limit in **TiB** (e.g., `0.5` for ~500 GB).

---

## 2. Maximum Bytes Billed (Analyst Self-Control)

**Managed by: Individual Data Analysts**

This is a "safety fuse" that every Analyst should set before running a query. It ensures that if you accidentally write a query that would scan more data than expected (e.g., missing a join condition), BigQuery will fail the job **before** it starts, charging **$0**.

### Method A: Using the BigQuery UI

1. In the SQL Editor, click **More** > **Query settings**.
2. Expand **Advanced configurations**.
3. In the **Maximum bytes billed** field, enter your personal limit in **Bytes** (e.g., `1000000000` for 1 GB).
4. **Note:** This allows Analysts to experiment safely without fear of accidentally scanning the entire data warehouse.

---

## 3. Required Partition Filter (Proactive Governance)

**Managed by: Data Architects / Admins**

By enabling this on a partitioned table, you force all users to provide a `WHERE` clause on the partition column (e.g., date). This prevents accidental "Full Table Scans" that are often the leading cause of high costs.

### How to Enable:

* **UI**: In Table **Details**, click **Edit Details** and check **"Require partition filter"**.
* **SQL**:
```sql
ALTER TABLE my_dataset.my_table
SET OPTIONS (require_partition_filter = TRUE);

```



---

## 4. Decision Matrix: Who Controls What? 🚦

| Feature | Controlled By | Purpose |
| --- | --- | --- |
| **Custom Quotas** | **Admin** | Protects the company budget from total exhaustion. |
| **Max Bytes Billed** | **Analyst** | Protects the analyst from accidental "expensive" mistakes. |
| **Partition Filter** | **Admin/Architect** | Enforces query efficiency at the schema level. |
| **Slot Autoscaling** | **Admin** | Manages performance and capacity for the whole Org. |

> [!TIP]
> **Pro-Tip for Analysts:** Always look at the **"This query will process X GB when run"** validator in the top right of the BigQuery UI. If that number looks much higher than your **Maximum Bytes Billed** setting, your query will automatically stop before costing you anything.
