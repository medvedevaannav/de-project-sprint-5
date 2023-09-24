from repositories.pg_connect import PgConnect


class CourierLedgerRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_courierledger(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
;with
dm_courier_ledger_group as (
	select c.courier_id                 as courier_id,
       c.courier_name                   as courier_name,
       date_part('year',d.delivery_ts)  as settlement_year,
       date_part('month',d.delivery_ts) as  settlement_month,
       count(d.delivery_id)			    as orders_count,
       sum(d.sum)                       as orders_total_sum,
       avg(rate)                        as rate_avg,
       sum(d.sum*0.25)                  as order_processing_fee,
       sum(tip_sum) 					as courier_tips_sum
	 from dds.dm_deliveries d 
     join dds.dm_couriers c on d.courier_id=c.courier_id
	where date_part('month',d.delivery_ts) >= date_part('month',now())
	   group by c.courier_id,c.courier_name,date_part('year',d.delivery_ts), date_part('month',d.delivery_ts)
)
INSERT INTO cdm.dm_courier_ledger
(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, 
rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
select  courier_id,
		courier_name ,
	    settlement_year,
	    settlement_month,
	    orders_count,
	    orders_total_sum,
	    rate_avg,
	    order_processing_fee,
	    case when rate_avg < 4 then greatest(orders_total_sum * 0.05, 100)
			when rate_avg >= 4 and rate_avg < 4.5 then greatest(orders_total_sum * 0.07, 150)
			when rate_avg >= 4.5 and rate_avg < 4.9 then greatest(orders_total_sum * 0.08, 175)
			when rate_avg >= 4.9 then greatest(orders_total_sum * 0.1, 200)
		end courier_order_sum, 
		courier_tips_sum,
		  (case when rate_avg < 4 then greatest(orders_total_sum * 0.05, 100)
			when rate_avg >= 4 and rate_avg < 4.5 then greatest(orders_total_sum * 0.07, 150)
			when rate_avg >= 4.5 and rate_avg < 4.9 then greatest(orders_total_sum * 0.08, 175)
			when rate_avg >= 4.9 then greatest(orders_total_sum * 0.1, 200)
		end + courier_tips_sum) * 0.95 as courier_reward_sum
  from  dm_courier_ledger_group 
  on conflict (courier_id, settlement_year, settlement_month)  do update  
  set orders_count=EXCLUDED.orders_count,
	  orders_total_sum=EXCLUDED.orders_total_sum,
	    rate_avg=EXCLUDED.rate_avg,
	    order_processing_fee=EXCLUDED.order_processing_fee,
	    courier_order_sum=EXCLUDED.courier_order_sum, 
	    courier_tips_sum=EXCLUDED.courier_tips_sum, 
	    courier_reward_sum=EXCLUDED.courier_reward_sum;
                    """
                )
                conn.commit()


class CourierLedgerReportLoader:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = CourierLedgerRepository(pg)

    def load_CourierLedgerRepor(self):
        self.repository.load_courierledger()
