SELECT
    oz.id,
	oz.name AS zone_name,
    oz.coordinates,
    oz.region_id,
    orr.region_name,
	oz.created_at,
	oz.deleted
FROM
    public.orders_zone oz
LEFT JOIN public.orders_regions orr
    ON oz.region_id = orr.id
WHERE oz.created_at > '{{last_created_on}}'    
