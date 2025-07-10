
SELECT
	dbpl.id,
	dbpl.region_id,
	r.name AS region_name,
	dbpl.payable_journal_id,
	dbpl.payment_journal_id,  
	dbpl.batch_payout_id,
	dbp.name AS batch_name,
	dbpl.tds_tax_id,
	dbpl.order_qty AS no_of_orders,
	dbpl.driver_uid AS driver_id_erp,   -- vendor id when there is vendor, otherwise rider id
	dbpl.company_id,
	c.name AS company_name,
	dbpl.create_uid AS created_by_uid,  -- Renamed from create_uid
	dbpl.payment_state,
	dbpl.driver_uid AS rider_id,    	-- Renamed from rider_id_dms
	dbp.from_date AS payout_from_date, -- Renamed from from_date
	dbp.to_date AS payout_to_date, 	-- Renamed from to_date
	dpt_sum.worked_hours AS total_worked_hours,	-- Summed worked hours for the date range
	dpt_sum.total_distance AS total_distance,  	-- Summed total distance for the date range
	dbpl.daily_payout_amount,
	dbpl.incentive_amount,
	dbpl.deduction_amount,
	dbpl.tds_amount,
	dbpl.total_payout,
	dbpl.avg_order_cost,
	dbpl.total_revenue,
	dbpl.payment_vendor_acc,
	dbpl.transaction_date,
	dbpl.processed_date,
	dbpl.create_date,
	dbpl.write_date,
	dbp.total_amount AS total_batch_payout,
	dbp.state,
	dbp.create_uid AS batch_create_uid,
	dbp.write_uid AS batch_write_uid,
	dbp.transaction_date AS batch_transaction_date,
	dbp.processed_date AS batch_processed_date,
	dbp.create_date AS batch_create_date,
	dbp.write_date AS batch_write_date,
	dbp.line_count AS batch_riders_count,  -- Renamed from batch_line_count
	dbp.is_vendor_payout,
	dbp.is_reject AS batch_rejected,   	-- Renamed from batch_is_reject
	dbp.payment_mode,
	rp.name as created_by_name
FROM driver_batch_payout_lines dbpl
LEFT JOIN driver_batch_payout dbp
	ON dbpl.batch_payout_id = dbp.id
LEFT JOIN sales_region r
	ON dbpl.region_id = r.id
LEFT JOIN res_company c
	ON dbpl.company_id = c.id
left join res_users as ru on dbp.create_uid = ru.id
left join res_partner rp on ru.partner_id = rp.id
LEFT JOIN (
	SELECT
    	driver_uid,
    	batch_payout_id,
    	SUM(worked_hours) AS worked_hours,
    	SUM(total_distance) AS total_distance
	FROM driver_payout
	GROUP BY batch_payout_id, driver_uid
) dpt_sum
	ON dbp.id = dpt_sum.batch_payout_id AND dbpl.driver_uid = dpt_sum.driver_uid
WHERE dbp.from_date BETWEEN '2025-02-01' AND '{{fetch_until_date}}' AND
dbp.create_date > '{{last_created_on}}';