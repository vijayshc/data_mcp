REPLACE VIEW Fact_Sales(
    Target_Order_ID,
    Target_Amount_Local,
    Target_Amount_USD,
    Target_Customer_Name,
    Target_Distinct_Items,
    Target_Status_Category,
    ETL_Load_Timestamp
)
LOCKING ROW FOR ACCESS
AS SELECT 
    o.order_id AS Target_Order_ID,
    oi_agg.total_amount AS Target_Amount_Local,
    oi_agg.total_amount * COALESCE(ex.ExchangeRate, 1.0) AS Target_Amount_USD,
    c.customer_name AS Target_Customer_Name,
    oi_agg.distinct_items AS Target_Distinct_Items,
    CASE 
        WHEN o.status = 'Completed' THEN 'Fulfilled' 
        WHEN o.status = 'Shipped' THEN 'Fulfilled' 
        WHEN o.status = 'Cancelled' THEN 'Cancelled' 
        ELSE 'Open' 
    END AS Target_Status_Category,
    CURRENT_TIMESTAMP() AS ETL_Load_Timestamp
FROM 
    (SELECT 
        order_id, 
        last_modified_date, 
        customer_id, 
        order_date, 
        currency_code, 
        status, 
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY last_modified_date DESC) as rn 
    FROM orders o_base 
    WHERE is_active = true
    ) o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN 
    (SELECT 
        order_id, 
        SUM(quantity * unit_price) AS total_amount, 
        COUNT(DISTINCT item_id) AS distinct_items 
    FROM order_items oi_base 
    GROUP BY order_id
    ) oi_agg ON o.order_id = oi_agg.order_id
LEFT JOIN Dim_Exchange_Rates ex ON o.order_date = ex.RateDate AND o.currency_code = ex.CurrencyCode
WHERE o.rn = 1;
