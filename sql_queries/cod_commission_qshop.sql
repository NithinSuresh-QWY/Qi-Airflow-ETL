  
SELECT qmpl.id AS id,
       qmp.id as qshop_merchant_payout_id,
       qmp.create_date as created_on,
	   qmp.description,
	   qmp.rec_name as name,
	   qmp.line_count as payout_count,
	   qmp.state as status,
       qmp.from_date,
	   qmp.to_date,
	   qmp.total_amount,
	   qmpl.service_charge,
	   rp.name as customer_name
	  FROM  public.qshop_merchant_payout_lines qmpl
	  LEFT JOIN public.qshop_merchant_payout qmp ON
	  qmpl.payout_id = qmp.id
	  LEFT JOIN res_partner rp ON
	  rp.id = qmpl.customer_id
	  WHERE qmp.id IS NOT NULL 
      AND qmp.from_date >= '{{ last_created_on }}'
      AND qmp.from_date <= '{{ fetch_until_date }}'
