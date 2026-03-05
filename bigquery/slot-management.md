# 📊 BigQuery Slot Management & Pricing Guide

A technical reference for optimizing compute power in Google BigQuery.

---

## 🔵 1. Idle Slot Sharing

BigQuery maximizes efficiency by letting reservations "borrow" power from each other.

* **The Mechanism:** If Reservation-A (e.g., Dashboarding) is quiet, its unused slots move to a "free pool."
* **The Benefit:** Reservation-B (e.g., Data Science) can automatically use those idle slots at **no extra cost**.
* **Preemption:** If Dashboarding suddenly needs its slots back, the Data Science queries are instantly throttled to return the capacity. ⚡

---

## 🟢 2. Editions vs. On-Demand

Google has shifted from charging per-terabyte to charging for compute-time (Editions).

| Feature | ⚡ Standard | 🏢 Enterprise | 💎 Enterprise Plus | 🖱️ On-Demand |
| --- | --- | --- | --- | --- |
| **Best For** | Ad-hoc / Development | Production workloads | Mission-critical / High security | Tiny workloads / Testing |
| **Scaling** | Autoscaling | Autoscaling | Autoscaling | Fixed (2,000 slots) |
| **Billing** | Per slot-hour | Per slot-hour | Per slot-hour | **Per TB scanned** |

> **⚠️ Warning:** On-Demand is "best effort." If the shared Google pool is busy, your query slows down. Editions provide **guaranteed** capacity.

---

## 🟡 3. Legacy Slots (The "Commitment" Era)

While largely replaced by Editions, many organizations still reference these legacy terms:

* **Flex Slots:** Short-term (60-second minimum). Great for massive one-time jobs.
* **Monthly/Annual:** Fixed-fee, fixed-capacity. These are now mapped to **Baseline Slots** in the Enterprise Edition.
* **Legacy Flat-Rate:** Required manual management. If you hit your limit, queries just queued until slots opened up. 🚦

---

## 🚀 4. Slot Autoscaling & Scaling Beyond Reservations

This is the most critical part of modern BigQuery architecture.

### The "Over-Reservation" Scenario

**Example:** You have a Reservation of **1,000 slots**, but a massive query needs **2,500 slots**.

1. **Baseline (1,000):** These are your "always-on" slots. You pay for these continuously (if committed) or they are your starting point.
2. **Max Reservation Size:** You must set a "Max Size" (e.g., 3,000).
3. **The Trigger:** BigQuery detects the 1,000 slots are 100% utilized and query "Demand" is still high.
4. **The Scale-Up:** It dynamically adds slots in increments (usually 100) until it hits the demand or your **Max Size** (3,000).
5. **The Bill:** You only pay for the extra 1,500 slots for the **exact duration** they were active (per-second billing).

### 💡 Pro-Tip: Setting the Ceiling

> Always set a `max_reservation_size`. If you leave it too high, a poorly written Cartesian-join query could scale to 10,000 slots and burn your budget in minutes. Set the ceiling based on your **concurrency needs** vs. **budget**.

---

### 🛠️ Quick Summary Table

| Goal | Strategy |
| --- | --- |
| **Cost Predictability** | Use a high **Baseline** with no autoscaling. |
| **Cost Savings** | Use a low **Baseline** (or 0) and high **Autoscaling**. |
| **Max Performance** | Use **Enterprise Plus** with a large Max Reservoir. |
