-- SQL for MAP_ERP_ORDERS_TO_FACT_SALES as a VIEW (SQLITE COMPATIBLE)
-- Generated on: April 20, 2025

-- This SQL creates a VIEW that transforms data from the ERP Orders system
-- The view includes deduplication of orders, aggregation of order items,
-- and currency conversion to USD

-- First drop the existing view if it exists
DROP VIEW IF EXISTS V_MAP_ERP_ORDERS_TO_FACT_SALES;

-- Create the view with SQLite-compatible syntax
CREATE VIEW V_MAP_ERP_ORDERS_TO_FACT_SALES AS
SELECT 
    -- Primary key
    o.order_id AS Target_Order_ID,
    
    -- Financial measurements
    oi_agg.total_amount AS Target_Amount_Local,
    oi_agg.total_amount * COALESCE(ex.ExchangeRate, 1.0) AS Target_Amount_USD,
    
    -- Customer information
    c.customer_name AS Target_Customer_Name,
    
    -- Order details
    oi_agg.distinct_items AS Target_Distinct_Items,
    
    -- Status categorization - optimized by consolidating identical outcomes
    CASE 
        WHEN o.status IN ('Completed', 'Shipped') THEN 'Fulfilled'
        WHEN o.status = 'Cancelled' THEN 'Cancelled'
        ELSE 'Open' 
    END AS Target_Status_Category,
    
    -- ETL metadata
    CURRENT_TIMESTAMP AS ETL_Load_Timestamp
FROM 
    (
        -- Subquery 1: Deduplicated orders - Optimized with explicit column selection
        SELECT
            order_id, 
            customer_id, 
            order_date, 
            currency_code, 
            status,
            rn
        FROM (
            SELECT 
                order_id, 
                last_modified_date, 
                customer_id, 
                order_date, 
                currency_code, 
                status,
                ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY last_modified_date DESC) as rn
            FROM orders o_base
            WHERE is_active = 1
        ) 
        WHERE rn = 1  -- Pre-filter deduplication rows before joining
    ) o
    -- Join order optimization
    INNER JOIN (
        -- Subquery 2: Aggregate order items - Optimized with streamlined calculation
        SELECT 
            order_id, 
            SUM(quantity * unit_price) AS total_amount,
            COUNT(DISTINCT item_id) AS distinct_items
        FROM order_items oi_base
        GROUP BY order_id
    ) oi_agg ON o.order_id = oi_agg.order_id
    INNER JOIN customers c ON o.customer_id = c.customer_id
    -- Using a LEFT JOIN for optional currency conversion
    LEFT JOIN Dim_Exchange_Rates ex ON o.order_date = ex.RateDate 
                                   AND o.currency_code = ex.CurrencyCode;