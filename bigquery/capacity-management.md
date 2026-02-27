# 🚦 BigQuery Capacity & Job Scheduling Management

Managing a high-performance BigQuery environment requires balancing real-time user needs against heavy background workloads. Understanding how BigQuery handles job transitions is key to maintaining a smooth experience.

## 1. Job Priority & Deferral Logic

BigQuery uses a "deferral" mechanism to control traffic and ensure that human users are not blocked by massive automated tasks.

| Priority | Deferral Behavior | Use Case |
| --- | --- | --- |
| **INTERACTIVE** ⚡ | **Never.** Jobs transition to `RUNNING` as soon as slots are available. | CEO dashboards, ad-hoc analysis, real-time debugging. |
| **BATCH** 🕒 | **Always deferred at least 1 minute.** Deferral lasts longer if quota is low or the server is at capacity. | Scheduled ETL, nightly reports, non-critical data syncs. |

> [!IMPORTANT]
> Because **BATCH** jobs always wait at least 1 minute, they are the ideal candidates for "Time-Shifting" to off-peak hours.

---

## 2. When to Add More Slots 📉

You should consider increasing your slot allocation if you observe the following persistent "signals" within a specific project or label group:

* **Concurrency**: Consistently increasing as more users are onboarded.
* **Throughput**: Consistently decreasing despite high activity.
* **Slot Utilization**: Remaining high (90%+) or hitting your reservation cap consistently.
* **Wait Times**: Average wait time is consistently increasing, indicating jobs are queuing.
* **Runtime**: Average query runtime is increasing, suggesting resource contention.

---

## 3. Decision & Monitoring Framework 🛠️

If you detect performance degradation, follow this decision matrix before scaling:

### A. Frequency Analysis

* Check if slot utilization spikes occur on a **regular frequency** (e.g., top of the hour).
* Identify which specific **labels or projects** are causing the spikes.

### B. Workload Time-Shifting

* Identify **non-critical workloads** that can be shifted to a different time of day.
* Moving these to **BATCH** priority during off-peak hours can flatten your utilization curve and save costs.

### C. Optimization Check

Before purchasing slots, ensure the queries are not suffering from common "wasteful" behaviors:

* **[Join Explosions](https://github.com/furqonm/data-engineer/blob/main/bigquery/cartesian-product.md)**: Check if output rows are exponentially higher than input rows.
* **[Expression Order](https://github.com/furqonm/data-engineer/blob/main/bigquery/where-placing.md)**: Ensure the most selective `WHERE` filters run first.
* **[Data Skew](https://github.com/furqonm/data-engineer/blob/main/bigquery/data-skew.md)**: Use the Query Plan to see if `Max worker time` is much higher than `Avg worker time`.
