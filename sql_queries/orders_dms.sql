SELECT
    o.id,
    o.rider_commission,
    o.amount AS total_amount,
    o.distance,
    o.estimated_time,
    o.order_status AS order_status_id,
    o.created_on,
    o.customer_id,
    o.rider_id,
    o.weight_id,  
    o.region_id,
    o.discount_amount,
    o.discount_percentage,
    o.order_amount,
    o.draft_order,
    o.order_key,
    o.parent_order_id_id AS original_order_id,
    o.locality AS sender_locality,
    o.postal_code AS sender_postal_code,
    o.receiver_locality,
    o.receiver_postal_code,
    o.created_on_original,
    o.is_scheduled AS is_scheduled,
    o.processed_at AS moved_to_new_at,
    o.zone_id,
    o.fulfilled_by,
    o.order_type AS type_of_order,
    rg.region_name,
    z.name AS zone_name,
    CASE o.order_status
        WHEN 1 THEN 'New'
        WHEN 2 THEN 'Accepted'
        WHEN 3 THEN 'Picked up'
        WHEN 4 THEN 'Delivered'
        WHEN 5 THEN 'Cancelled'
        WHEN 6 THEN 'Undelivered'
        WHEN 7 THEN 'Returned to warehouse'
        WHEN 8 THEN 'Returned to sender'
        WHEN 9 THEN 'Returned to another address'
        WHEN 10 THEN 'Scheduled'
    END AS status,
    CASE o.payment_mode
        WHEN 1 THEN 'Cash on Pickup'
        WHEN 2 THEN 'Online'
        WHEN 3 THEN 'Cash on Delivery'
        WHEN 5 THEN 'Credit'
        WHEN 6 THEN 'Wallet'
        ELSE 'Unknown'
    END AS payment_mode,
FROM orders_order o
LEFT JOIN orders_regions rg ON o.region_id = rg.id
LEFT JOIN orders_zone z ON o.zone_id = z.id
WHERE
    o.created_on > '{{last_created_on}}'
    AND o.created_on <= '{{ fetch_until_date }}'
    AND o.region_id NOT IN (7)
    AND draft_order = false;
