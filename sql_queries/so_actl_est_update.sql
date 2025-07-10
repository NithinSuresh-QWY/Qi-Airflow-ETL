UPDATE public.sale_orders so
SET
    actual_order_cost = roc.actual_order_cost,
    estimated_order_cost = roc.estimate_order_cost
FROM public.rider_order_costs roc
WHERE roc.order_id = so.id
  AND roc.rider_id = so.driver_uid
  AND so.create_date> '{{so_date_before_pull}}';