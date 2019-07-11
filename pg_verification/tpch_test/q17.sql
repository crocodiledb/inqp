set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set enable_nestloop to on; 
set enable_mergejoin to on;


select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem as l,
	part as p
where
	p.p_partkey = l.l_partkey
	and p.p_brand = 'Brand#23'
	and p.p_container = 'MED BOX'
	and l_quantity < (
        select avg_quantity from 
            (select l_partkey, 0.2 * avg(l_quantity) as avg_quantity
            from lineitem
            group by l_partkey) as aggl
        where p_partkey = l_partkey
    );

--     (
--		select
--			0.2 * avg(l_quantity)
--		from
--			lineitem
--		where
--			l_partkey = p_partkey
--            and l_partkey = -1
--	)

