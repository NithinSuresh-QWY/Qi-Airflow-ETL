
SELECT
    c.id,
    c.name AS customer_name,
    c.phone AS customer_phone,
    c.email AS customer_email,
    c.type,
    c.user_status,
    c.customer_key,
    c.payment_mode,
    c.created_on,
    c.client_id,
    c.city,
    c.attempt AS no_of_attempt,
    cdt.name AS delivery_type
FROM public.customer_customer c
LEFT JOIN public.settings_customerdeliverytype cdt ON c.delivery_type_id = cdt.id
WHERE c.created_on > '{{last_created_on}}';
