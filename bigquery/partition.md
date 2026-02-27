# 📅 BigQuery Table Partitioning: Management & Lifecycle

Partitioning is a data management technique that divides a large table into smaller segments based on a specific column or ingestion time. This allows BigQuery to perform **Partition Pruning**, where it only scans the specific segments needed for your query, saving costs and improving performance.

## 1. The 3 Ways to Partition a Table

BigQuery supports three distinct methods for segmenting your data:

| Method | Technical Implementation | Primary Use Case |
| --- | --- | --- |
| **Time-unit column** | Partitioned based on a `DATE` or `TIMESTAMP` column explicitly defined in your schema. | Sales data partitioned by a business-relevant `order_date`. |
| **Ingestion time** | BigQuery automatically creates partitions based on when data arrives using the **`_PARTITIONTIME`** pseudo-column. | High-velocity logs or streaming data where no internal date column is available. |
| **Integer range** | Partitioned based on a range of integers in a specific column. | Segmenting massive datasets by numeric IDs (e.g., `user_id` blocks). |

---

## 2. Creating Partitioned Tables via SQL (DDL)

### A. Partition by Ingestion Time (Pseudo-column)

When using Ingestion Time partitioning, BigQuery manages a hidden **pseudo-column** named `_PARTITIONTIME`. You do not need to define this column in your schema; BigQuery handles it automatically as data is loaded.

```sql
CREATE OR REPLACE TABLE `my_project.my_dataset.ingestion_logs`
(
  log_message STRING,
  severity STRING
)
-- Using ingestion time partitioning
PARTITION BY DATE(_PARTITIONTIME)
OPTIONS (
  partition_expiration_days = 3,
  require_partition_filter = TRUE
);

```

### B. Partition by Time-Unit Column

```sql
CREATE OR REPLACE TABLE `my_project.my_dataset.sales_data`
(
  order_id INT64,
  order_date DATE,
  amount FLOAT64
)
PARTITION BY order_date
OPTIONS (
  partition_expiration_days = 3,
  require_partition_filter = TRUE
);

```

### C. Partition by Integer Range

```sql
CREATE OR REPLACE TABLE `my_project.my_dataset.user_logs`
(
  user_id INT64,
  action STRING
)
PARTITION BY RANGE_BUCKET(user_id, GENERATE_ARRAY(0, 1000000, 100000))
OPTIONS (
  require_partition_filter = TRUE
);

```

---

## 3. Partition Expiration & Automated Deletion

To manage storage costs, you can set a lifecycle policy so that data is automatically deleted after a certain period.

### A. Setting Expiration via DDL

The `partition_expiration_days` option defines the age at which a partition is deleted based on its partition value.

```sql
CREATE OR REPLACE TABLE `my_project.my_dataset.temp_logs`
(
  log_time TIMESTAMP,
  entry STRING
)
PARTITION BY DATE(log_time)
OPTIONS (
  -- Automatically delete partitions older than 7 days
  partition_expiration_days = 7 
);

```

### B. Updating Expiration on Existing Tables

```sql
ALTER TABLE `my_project.my_dataset.temp_logs`
SET OPTIONS (partition_expiration_days = 30);

```

> [!NOTE]
> `partition_expiration_days` deletes **individual segments** based on time. This differs from `expiration_timestamp`, which deletes the **entire table**.
