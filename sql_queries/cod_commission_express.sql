
SELECT dmpl.id AS id, 
       dmp.id as delivery_merchant_payout_id,
       dmp.create_date as created_on,
	   dmp.description,
	   dmp.rec_name as name,
	   dmp.line_count as payout_count,
	   dmp.state as status,
	   dmp.from_date,
	   dmp.to_date,
	   dmp.total_amount,
	   dmpl.service_charge,
	   rp.name as customer_name
	  FROM  public.delivery_merchant_payout_lines dmpl
	  LEFT JOIN public.delivery_merchant_payout dmp ON
	  dmpl.payout_id = dmp.id
	  LEFT JOIN res_partner rp ON
	  rp.id = dmpl.customer_id
	  WHERE dmp.id IS NOT NULL and
	  dmp.from_date >= '{{ last_created_on }}' and
      dmp.from_date <= '{{ fetch_until_date }}'

	  