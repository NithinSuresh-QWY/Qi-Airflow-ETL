
-- 05-06-2025
SELECT 
    rr.id,
    name AS rider_name,
    phone AS rider_phone,
    email AS rider_email,
    rider_type,
    vehicle_number,
    rider_status AS rider_reg_status,
    created_on,
    rider_availability_status,                             
    cash_in_hand,
    region_id,
    rider_key AS rider_id,
    due_amount,
    updated_on,
    CASE 
        WHEN vehicle_category = 1 THEN '2-wheeler'
        WHEN vehicle_category = 2 THEN '3-wheeler'
        WHEN vehicle_category = 3 THEN '4-wheeler'
        ELSE CAST(vehicle_category AS VARCHAR)
    END AS vehicle_category,    
    vehicle_category,
    updated_by,
    alternate_phone,
    blood_group,
    date_of_join,
    gender,
    referred_by,
    reporting_postal_code,
    vendor_name,
    address AS rider_address,
    date_of_birth,
    nominee_date_of_birth,
    driver_cash_limit,
    CASE 
        WHEN de_category = 1 THEN 'LIVE'
        WHEN de_category = 2 THEN 'DEDICATED'
        ELSE 'UNKNOWN'
    END AS de_category,
    vehicle_type,
    in_progress_orders_cash,
    plan_id,
    plan_name,
    de_availability_status,
    CASE 
        WHEN delivery_mode = 1 THEN 'HYPERLOCAL'
        WHEN delivery_mode = 2 THEN 'COURIER'
        ELSE 'UNKNOWN'
    END AS delivery_mode,
    NULL AS avg_cost_per_distance,
    NULL AS avg_cost_per_hr,
	orr.region_name
FROM public.rider_riderregistration rr
LEFT JOIN public.orders_regions orr
ON rr.region_id = orr.id
WHERE created_on > '{{ last_created_on }}';
