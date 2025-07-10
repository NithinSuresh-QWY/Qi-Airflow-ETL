
DELETE FROM rider_avg_cost;


INSERT INTO rider_avg_cost (rider_id, avg_cost_per_distance, avg_cost_per_hr)
SELECT 
    rider_id,
    ROUND(SUM(total_payout) / NULLIF(SUM(total_distance), 0), 2) AS avg_cost_per_distance,
    ROUND(SUM(total_payout) / NULLIF(SUM(total_worked_hours), 0), 2) AS avg_cost_per_hr
FROM driver_batch_payout_details
WHERE total_payout IS NOT NULL 
  AND total_distance IS NOT NULL 
  AND total_worked_hours IS NOT NULL
GROUP BY rider_id;



UPDATE public.rider_registration rr
SET
    avg_cost_per_distance = rac.avg_cost_per_distance,
    avg_cost_per_hr = rac.avg_cost_per_hr
FROM public.rider_avg_cost rac
WHERE rac.rider_id = rr.rider_id;