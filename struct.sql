-- Single record per row (1:1 relationship)
CREATE TABLE `fruit_store.user_profiles` (
  user_id STRING,
  name STRING,
  -- RECORD tanpa repeated (hanya satu alamat per user)
  address STRUCT<
    street STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    country STRING
  >,
  created_date DATE
);


INSERT INTO `fruit_store.user_profiles` VALUES
('user1', 'John Doe', 
 STRUCT('123 Main St', 'New York', 'NY', '10001', 'USA'), 
 '2024-01-15'),

('user2', 'Jane Smith', 
 STRUCT('456 Oak Ave', 'Los Angeles', 'CA', '90210', 'USA'), 
 '2024-01-16');
--------------------------------------------------
-- Multiple records per row (1:Many relationship)
CREATE TABLE `fruit_store.user_profiles2` (
  user_id STRING,
  user_name STRING,
  -- RECORD dengan REPEATED (banyak order per user)
  orders ARRAY<STRUCT<
    order_id STRING,
    order_date DATE,
    product_name STRING,
    quantity INT64,
    price FLOAT64
  >>,
  total_orders INT64
);


INSERT INTO `fruit_store.user_profiles2` VALUES
('user1', 'John Doe', 
 [
   STRUCT('ord001' as order_id, DATE '2024-01-10' as order_date, 'Laptop ASUS' as product_name, 1 as quantity, 999.99 as price),
   STRUCT('ord002', DATE '2024-01-12', 'Mouse', 2, 25.50),
   STRUCT('ord003', DATE '2024-01-15', 'Keyboard', 1, 75.00)
 ], 
 3),

('user2', 'Jane Smith', 
 [
   STRUCT('ord004' as order_id, DATE '2024-01-11' as order_date, 'Tablet' as product_name, 1 as quantity, 299.99 as price),
   STRUCT('ord005', DATE '2024-01-14', 'Case', 1, 15.00)
 ],
 2),

('user3', 'Falcon', 
 [
   STRUCT('ord006', DATE '2024-01-10', 'Laptop Leonovo', 1, 899.99),
   STRUCT('ord007', DATE '2024-01-12', 'Mouse', 2, 25.50),
   STRUCT('ord008', DATE '2024-01-15', 'Keyboard', 1, 75.00)
 ], 
 3)
