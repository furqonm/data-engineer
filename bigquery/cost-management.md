# 🛡️ BigQuery Cost Management

BigQuery provides several mechanisms to manage spending at the user, project, and individual query levels. Implementing these ensures that unoptimized queries (like **Join Explosions**) do not consume your entire budget.

## 1. Custom Cost Controls (User & Project Level)

You can set daily limits on the amount of data processed to cap expenditures.

* **Project-level controls**: Limits the total aggregate bytes processed by all users within a specific project.
* **User-level controls**: Limits the bytes processed by a specific user within a project.

### How to Configure [Custom Quotas](https://docs.cloud.google.com/bigquery/docs/custom-quotas#set-custom-quotas):

1. Navigate to the **IAM & Admin** > **Quotas** page in the Google Cloud Console.
2. Search for **"Query usage per day"**.
3. Select the quota and click **Edit Quotas**.
4. Enter the limit in Terabytes (TB) or Gigabytes (GB).

---

## 2. Maximum Bytes Billed (Query Level)

This setting acts as a "safety fuse". If a query is estimated to scan more data than your limit, BigQuery will fail the job before it even starts, charging you $0.

### Method A: [Using the BigQuery UI](https://docs.cloud.google.com/bigquery/docs/best-practices-costs#restrict-bytes-billed)

1. In the SQL Editor, click on **More** > **Query settings**.
2. Expand **Advanced options**.
3. Check the box for **"Maximum bytes billed"**.
4. Enter the limit (e.g., `1000000000` for 1 GB).

### Method B: [Using the `bq` Command Line](https://www.google.com/search?q=%5Bhttps://docs.cloud.google.com/bigquery/docs/reference/bq-cli-reference%23flags_and_arguments_14%5D(https://docs.cloud.google.com/bigquery/docs/reference/bq-cli-reference%23flags_and_arguments_14))

You can enforce this limit using the `--maximum_bytes_billed` flag in the Cloud SDK:

```bash
# Example: Limit the query to 1 GB (1,073,741_824 bytes)
bq query --use_legacy_sql=false \
--maximum_bytes_billed=1073741824 \
'SELECT text FROM `my-project.my_dataset.my_table` WHERE user="anon"'

```

---

## 3. Slot Autoscaling: Flexible Capacity 📈

BigQuery slots act as virtual CPUs (containers) for query execution. [Autoscaling](https://docs.cloud.google.com/bigquery/docs/slots#slot-autoscaling) allows you to manage costs while handling workload spikes automatically.

* **Dynamic Adjustment**: BigQuery automatically scales the number of slots up or down based on current workload demand.
* **Baseline and Max Slots**: You define a **Baseline** (minimum capacity always available) and a **Max** (the ceiling for autoscaling).
* **Cost Efficiency**: You only pay for the slots used beyond the baseline during spikes.

---

## 4. Decision Matrix: When to Scale vs. Capping 🚦

When monitoring signals like **concurrency** or **slot utilization**, use these controls strategically:

| Scenario | Monitoring Signal | Recommended Action |
| --- | --- | --- |
| **Budget Protection** | Costs are spiking monthly. | Set **Project-level** daily usage quotas. |
| **"Runaway" Query** | A single user runs massive unoptimized joins. | Set **User-level** quotas or query-level **Max Bytes Billed**. |
| **High Contention** | Utilization is >90% and wait times are increasing. | Switch non-critical jobs to **BATCH priority** or increase **Max Autoscaling Slots**. |
| **Dashboard Lag** | Throughput is decreasing for critical users. | Reserve slots for **INTERACTIVE** jobs or increase **Baseline Slots**. |

> [!WARNING]
> Setting the "Maximum bytes billed" too low may cause valid, necessary queries to fail. Always check the **estimated bytes processed** in the UI before finalizing a limit.
