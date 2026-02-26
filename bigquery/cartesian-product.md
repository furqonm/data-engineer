# BigQuery Optimization: Avoiding Join Explosions 💥

When joining tables in BigQuery, a **Join Explosion** (Cartesian product) can occur if your join keys are not unique on both sides. This leads to incorrect data, inflated costs, and slow performance.

## 1. The Scenario

Imagine we are joining a **Sales** table with a **Promotions** table using `Product_ID` as the join key.

### Table A: Sales

| Product_ID | Sale_Amount |
| --- | --- |
| A101 | $50 |
| A101 | $70 |

### Table B: Promotions

| Product_ID | Promo_Name |
| --- | --- |
| A101 | 10% Off |
| A101 | Free Shipping |

---

## 2. The Problem: The "Bad Join"

In SQL relational algebra, joining non-unique keys results in every possible combination of matching rows. In this case, 2 rows on the left × 2 rows on the right = **4 output rows**.

### ❌ Sub-optimal Query

```sql
SELECT 
  s.Product_ID, 
  SUM(s.Sale_Amount) as total_revenue
FROM `Sales` s
JOIN `Promotions` p ON s.Product_ID = p.Product_ID
GROUP BY 1

```

### ⚠️ The Resulting Join (Cartesian Product)

| Product_ID | Sale_Amount | Promo_Name |
| --- | --- | --- |
| A101 | $50 | 10% Off |
| A101 | $50 | Free Shipping |
| A101 | $70 | 10% Off |
| A101 | $70 | Free Shipping |

> [!CAUTION]
> **Data Inflation:** Notice how the `Sale_Amount` is duplicated. Calculating `SUM(total_revenue)` here would return **$240**, while the actual revenue is only **$120**.

---

## 3. The Fix: Pre-Aggregation 🛠️

To solve this, we use a **Common Table Expression (CTE)** to ensure the "right" side of the join has unique keys before the join happens.

### ✅ Optimized Query

```sql
WITH unique_promos AS (
  SELECT 
    Product_ID, 
    -- Use ANY_VALUE to pick one or STRING_AGG to combine them
    ANY_VALUE(Promo_Name) as Promo_Name 
  FROM `Promotions`
  GROUP BY Product_ID
)

SELECT 
  s.Product_ID, 
  SUM(s.Sale_Amount) as total_revenue
FROM `Sales` s
JOIN unique_promos p ON s.Product_ID = p.Product_ID
GROUP BY 1

```
