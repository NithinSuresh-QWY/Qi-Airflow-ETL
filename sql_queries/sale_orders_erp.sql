
-- 05-06-2025
SELECT
	so.id,
	so.name AS erp_order_name,  -- Renaming field
	so.state AS sale_order_status,  -- Renaming field
	so.date_order,
	so.commitment_date,
	so.order_amount,
	so.discount_amount,
	so.total_amount,
	so.merchant_order_amount,
	so.order_status_id,
	os.name AS order_status,  -- Fetching order status name
	so.payment_status,
	so.partner_id AS customer_id,  
	rp.name AS customer_name,  -- Fetching partner name
	so.customer_segment_id,  
	cs.name AS customer_segment_name,  -- Fetching customer segment name
	so.customer_type,
	so.customer_rating,
	so.customer_feedback,
	so.customer_comment,
	so.customer_phone,
	so.delivery_status,
	so.weight,
	so.from_address,
	so.to_address,
	so.pickup_distance as till_pickup_distance,  -- Renamed from pickup_distance
	so.deliver_distance as order_distance,  -- Renamed from deliver_distance
	so.time_to_accept,
	so.time_to_pickup,
	so.time_to_deliver,
	so.overall_order_time,
	so.merchant_order_id,
	so.merchant_payment_mode_id AS merchant_payment_mode,  -- Renamed field
	so.merchant_discount_amount,
	so.merchant_total_amount,
	so.item_category_id,  
	ic.name AS item_category,  -- Fetching item category name
	so.product_line_id,
	so.total_product_qty,
	so.amount_untaxed,
	so.amount_tax,
	c.name AS currency_name,  -- Fetching currency name instead of ID
	so.pricing_plan AS driver_pricing_plan,  -- Renamed field
	pm.name AS payment_mode,  -- Fetching payment mode name instead of ID
	so.driver_uid,  
	so.driver_name,  -- Fetching driver name
	so.driver_rating,
	so.driver_comment,
	sp.name AS order_sales_person_name,  -- Fetching sales person name instead of ID
	so.region_id,  
	r.name AS region_name,  -- Fetching region name
	so.industry_id,
	so.promo_code,
	so.promo_desc,
	so.is_having_promocode, 
	so.order_source,  
	so.is_qshop_service,
	null as actual_order_cost,  -- Updated field name
	null AS estimated_order_cost,  -- Renaming field
	So.order_delivered_date,
	so.order_id,
    so.create_date,  
	pl.name AS product_line,
	-- rpi.name['en_US'] AS industry,
	rpi.name::jsonb ->> 'en_US' AS industry,
	rs.name AS state_name
FROM sale_order so
LEFT JOIN order_status os ON so.order_status_id = os.id  
LEFT JOIN res_partner rp ON so.partner_id = rp.id  
LEFT JOIN partner_segment cs ON so.customer_segment_id = cs.id  
LEFT JOIN item_category ic ON so.item_category_id = ic.id  
LEFT JOIN sales_region r ON so.region_id = r.id  
LEFT JOIN res_currency c ON so.currency_id = c.id  -- Fetching currency name
LEFT JOIN payment_mode pm ON so.payment_mode_id = pm.id  -- Fetching payment mode name
LEFT JOIN hr_employee sp ON so.order_sales_person = sp.id  -- Fetching sales person name
LEFT JOIN product_lines pl ON so.product_line_id = pl.id -- Fetching product line nam
LEFT JOIN res_partner_industry rpi ON so.industry_id = rpi.id -- Fetching customer industry name
LEFT JOIN res_country_state rs on (r.state_id = rs.id)
WHERE so.create_date < '{{fetch_until_date}}'
AND so.create_date > '{{last_created_on}}'::date - INTERVAL '5 days'
AND so.region_id NOT IN (150,274);    --To exclude demo region orders
