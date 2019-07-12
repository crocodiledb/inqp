set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set enable_nestloop to on; 
set enable_mergejoin to on;

--select
--	l_suppkey,
--	sum(l_extendedprice * (1 - l_discount)) as total_revenue
--from
--	lineitem
--where
--	l_shipdate >= date '1996-01-01'
--	and l_shipdate < date '1996-01-01' + interval '3' month
--group by
--	l_suppkey
--order by
--    total_revenue desc;


create view revenue1 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date '1996-01-01'
		and l_shipdate < date '1996-01-01' + interval '3' month
	group by
		l_suppkey;

select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue1
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue1
	)
order by
	s_suppkey;

drop view revenue1;

