DELETE FROM rider_order_costs;


INSERT INTO rider_order_costs ( 
    order_id,
    order_name,
    rider_id,
    order_create_date,
    total_orders_in_payout_period,
    total_payout_in_payout_period,
    total_payout_rider_till_date,
    total_orders_rider_till_date,
    actual_order_cost,
    estimate_order_cost
)
WITH rider_totals AS (
    SELECT 
        dbp.driver_id_erp AS rider_id,
        SUM(dbp.total_payout) AS total_payout_rider_till_date,
        SUM(dbp.no_of_orders) AS total_orders_rider_till_date
    FROM driver_batch_payout_details dbp
    GROUP BY dbp.driver_id_erp
)
SELECT 
    so.id AS order_id,
    so.erp_order_name AS order_name,
    so.driver_uid AS rider_id,
    so.create_date AS order_create_date,  -- Use `create_date` as in Query 1
    COALESCE(SUM(dbp.no_of_orders), 0) AS total_orders_in_payout_period,
    COALESCE(SUM(dbp.total_payout), 0) AS total_payout_in_payout_period,
    COALESCE(rt.total_payout_rider_till_date, 0) AS total_payout_rider_till_date,
    COALESCE(rt.total_orders_rider_till_date, 0) AS total_orders_rider_till_date,
    ROUND(
        CASE 
            WHEN NULLIF(SUM(dbp.no_of_orders), 0) IS NOT NULL 
            THEN SUM(dbp.total_payout) / SUM(dbp.no_of_orders) 
            ELSE 0 
        END, 2
    ) AS actual_order_cost,
    ROUND(
        CASE 
            WHEN NULLIF(rt.total_orders_rider_till_date, 0) IS NOT NULL 
            THEN rt.total_payout_rider_till_date / rt.total_orders_rider_till_date 
            ELSE 0 
        END, 2
    ) AS estimate_order_cost
FROM sale_orders so
LEFT JOIN driver_batch_payout_details dbp 
    ON so.driver_uid = dbp.driver_id_erp 
    AND so.create_date BETWEEN dbp.payout_from_date AND dbp.payout_to_date  -- Match Query 1 logic
LEFT JOIN rider_totals rt 
    ON so.driver_uid = rt.rider_id
WHERE so.create_date BETWEEN '2025-02-01' AND '{{last_created_on}}'
GROUP BY 
    so.id, 
    so.erp_order_name, 
    so.driver_uid, 
    so.create_date, 
    rt.total_payout_rider_till_date, 
    rt.total_orders_rider_till_date
ORDER BY so.create_date;


