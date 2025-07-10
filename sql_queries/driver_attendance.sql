WITH relevant_orders AS (
    SELECT 
        driver_id, 
        DATE(date_order) AS date_order,
        COUNT(*) AS order_count
    FROM sale_order
    WHERE date_order BETWEEN '{{ last_created_on }}' AND '{{fetch_until_date}}'
    GROUP BY driver_id, DATE(date_order)
)

SELECT 
    HA.id,
    HA.date,
    TO_CHAR(HA.date, 'Day') AS day_name,
    HA.region_id,
    RG.name AS region,
    HA.employee_id,
    HA.employee_code,
    HE.name AS employee_name,
    HE.job_title,
    HE.employee_status,
    VC.id AS vehicle_category_id,
    VC.code AS vehicle_category_code,
    VC.name AS vehicle_category_name,
    HA.worked_hours,
    COALESCE(RO.order_count, 0) AS no_of_orders,  -- ✅ Number of orders
    CASE 
        WHEN RO.driver_id IS NULL THEN 'No Orders'
        ELSE 'Has Orders'
    END AS order_status
FROM hr_attendance HA
JOIN hr_employee HE ON HA.employee_id = HE.id
JOIN driver_vehicle_category VC ON HE.vehicle_category_id = VC.id
JOIN sales_region RG ON HA.region_id = RG.id
LEFT JOIN relevant_orders RO 
    ON HA.date = RO.date_order 
    AND HA.employee_id = RO.driver_id
WHERE 
    HA.date BETWEEN '{{ last_created_on }}' AND  '{{fetch_until_date}}'
    AND HE.job_title = 'Delivery Executive'
    AND HA.employee_code IS NOT NULL
    AND HE.employee_status = 'active'
    AND HA.region_id IS NOT NULL
ORDER BY HA.date, HA.region_id;
