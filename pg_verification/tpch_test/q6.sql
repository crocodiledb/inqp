set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set enable_nestloop to on; 
set enable_mergejoin to on;

select
	sum(l_extendedprice*l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1' year
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;

