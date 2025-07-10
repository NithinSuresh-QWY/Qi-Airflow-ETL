SELECT 
    ore.id,
    ore.region_name,
    ore.active_status,
    s.state_name AS state_name,
    ore.gst_number,
    ore.region_code,
    ore.store_enabled AS qwqer_shop_enabled,
    ore.is_merchant_ondc_orders_accepted,
    ore.is_ondc_collection_amount_accepted,
    ore.is_ondc_enabled,
    ore.ondc_collection_amount_limit,
    ore.is_pincode_config_enabled
FROM public.orders_regions ore
LEFT JOIN public.orders_states s ON ore.state_id = s.id;