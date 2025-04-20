-- Test cases for MAP_ERP_ORDERS_TO_FACT_SALES mapping

-- Test 1: Validate Target_Order_ID accuracy
INSERT INTO test_results (mapping_name, test_case_id, test_description, sql, result)
SELECT 
    'MAP_ERP_ORDERS_TO_FACT_SALES',
    'TEST-01',
    'Validate Target_Order_ID matches source orders',
    'Test order_id consistency between source and target',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM (
    SELECT o.order_id 
    FROM orders o
    WHERE o.is_active = true
    AND o.order_id IN (
        SELECT MAX(order_id) 
        FROM orders 
        WHERE is_active = true 
        GROUP BY order_id
    )
    EXCEPT
    SELECT Target_Order_ID 
    FROM Fact_Sales
) mismatch;

-- Test 2: Validate Target_Amount_Local calculation
INSERT INTO test_results (mapping_name, test_case_id, test_description, sql, result)
SELECT 
    'MAP_ERP_ORDERS_TO_FACT_SALES',
    'TEST-02',
    'Validate local amount calculation',
    'Test Target_Amount_Local equals sum of item prices × quantities',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM (
    SELECT 
        o.order_id,
        s.Target_Amount_Local,
        calc.total_amount
    FROM orders o
    JOIN (
        SELECT 
            order_id, 
            SUM(quantity * unit_price) AS total_amount
        FROM order_items
        GROUP BY order_id
    ) calc ON o.order_id = calc.order_id
    JOIN Fact_Sales s ON o.order_id = s.Target_Order_ID
    WHERE o.is_active = true
    AND ABS(s.Target_Amount_Local - calc.total_amount) > 0.001
) mismatch;

-- Test 3: Validate Target_Amount_USD calculation
INSERT INTO test_results (mapping_name, test_case_id, test_description, sql, result)
SELECT 
    'MAP_ERP_ORDERS_TO_FACT_SALES',
    'TEST-03',
    'Validate USD amount calculation',
    'Test Target_Amount_USD equals Target_Amount_Local × exchange rate',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM (
    SELECT 
        fs.Target_Order_ID,
        fs.Target_Amount_Local,
        fs.Target_Amount_USD,
        o.currency_code,
        o.order_date,
        er.ExchangeRate
    FROM Fact_Sales fs
    JOIN orders o ON fs.Target_Order_ID = o.order_id
    LEFT JOIN Dim_Exchange_Rates er ON o.order_date = er.RateDate AND o.currency_code = er.CurrencyCode
    WHERE o.is_active = true
    AND ABS(fs.Target_Amount_USD - (fs.Target_Amount_Local * COALESCE(er.ExchangeRate, 1.0))) > 0.001
) mismatch;

-- Test 4: Validate Target_Customer_Name mapping
INSERT INTO test_results (mapping_name, test_case_id, test_description, sql, result)
SELECT 
    'MAP_ERP_ORDERS_TO_FACT_SALES',
    'TEST-04',
    'Validate customer name mapping',
    'Test Target_Customer_Name matches source customer_name',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM (
    SELECT 
        fs.Target_Order_ID,
        fs.Target_Customer_Name,
        c.customer_name
    FROM Fact_Sales fs
    JOIN orders o ON fs.Target_Order_ID = o.order_id
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.is_active = true
    AND fs.Target_Customer_Name <> c.customer_name
) mismatch;

-- Test 5: Validate Target_Distinct_Items count
INSERT INTO test_results (mapping_name, test_case_id, test_description, sql, result)
SELECT 
    'MAP_ERP_ORDERS_TO_FACT_SALES',
    'TEST-05',
    'Validate distinct items count',
    'Test Target_Distinct_Items equals count of unique items per order',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM (
    SELECT 
        fs.Target_Order_ID,
        fs.Target_Distinct_Items,
        item_count.distinct_count
    FROM Fact_Sales fs
    JOIN (
        SELECT 
            order_id, 
            COUNT(DISTINCT item_id) AS distinct_count
        FROM order_items
        GROUP BY order_id
    ) item_count ON fs.Target_Order_ID = item_count.order_id
    WHERE fs.Target_Distinct_Items <> item_count.distinct_count
) mismatch;

-- Test 6: Validate Target_Status_Category mapping
INSERT INTO test_results (mapping_name, test_case_id, test_description, sql, result)
SELECT 
    'MAP_ERP_ORDERS_TO_FACT_SALES',
    'TEST-06',
    'Validate status categorization',
    'Test Target_Status_Category follows business rules for status classification',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM (
    SELECT 
        fs.Target_Order_ID,
        o.status,
        fs.Target_Status_Category,
        CASE 
            WHEN o.status = 'Completed' THEN 'Fulfilled' 
            WHEN o.status = 'Shipped' THEN 'Fulfilled' 
            WHEN o.status = 'Cancelled' THEN 'Cancelled' 
            ELSE 'Open' 
        END AS expected_status
    FROM Fact_Sales fs
    JOIN orders o ON fs.Target_Order_ID = o.order_id
    WHERE o.is_active = true
    AND fs.Target_Status_Category <> 
        CASE 
            WHEN o.status = 'Completed' THEN 'Fulfilled' 
            WHEN o.status = 'Shipped' THEN 'Fulfilled' 
            WHEN o.status = 'Cancelled' THEN 'Cancelled' 
            ELSE 'Open' 
        END
) mismatch;

-- Test 7: Check for duplicate target order IDs
INSERT INTO test_results (mapping_name, test_case_id, test_description, sql, result)
SELECT 
    'MAP_ERP_ORDERS_TO_FACT_SALES',
    'TEST-07',
    'Validate primary key uniqueness',
    'Test for duplicate Target_Order_ID values in Fact_Sales',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM (
    SELECT Target_Order_ID
    FROM Fact_Sales
    GROUP BY Target_Order_ID
    HAVING COUNT(*) > 1
) duplicates;

-- Test 8: Check ETL_Load_Timestamp is not null
INSERT INTO test_results (mapping_name, test_case_id, test_description, sql, result)
SELECT 
    'MAP_ERP_ORDERS_TO_FACT_SALES',
    'TEST-08',
    'Validate ETL_Load_Timestamp is populated',
    'Test ETL_Load_Timestamp is not null',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM Fact_Sales
WHERE ETL_Load_Timestamp IS NULL;
