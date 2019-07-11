set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set enable_nestloop to off; 
set enable_mergejoin to off;

select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date '1993-07-01'
	and o_orderdate < date '1993-07-01' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
		)
group by
	o_orderpriority
order by
	o_orderpriority;

