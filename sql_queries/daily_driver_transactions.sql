SELECT
    id,
    holiday_incentive,
    -- message_main_attachment_id,  Deleted
    employee_id,
    region_id,
    no_of_orders,
    vehicle_category_id,
    order_qty,
    batch_payout_id,
    company_id,
    create_uid,
    write_uid,
    stop_count_incentive,
    date,
    total_payout,
    create_date,
    write_date,
    minimum_wage,
    worked_hours,
    total_revenue,
    total_distance,
    total_estimated_distance,
    order_km_incentive,
    day_km_incentive,
    orders_incentive,
    hours_incentive,
    driver_uid,
    status as transaction_status
FROM
   public.driver_payout
   WHERE date > '{{ last_created_on }}' AND  
   date <= '{{fetch_until_date}}';