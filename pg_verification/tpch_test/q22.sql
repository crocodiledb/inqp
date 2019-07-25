set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set enable_nestloop to on; 
set enable_mergejoin to on;

select c_custkey, c_acctbal
from customer 
where 
substring(c_phone from 1 for 2) in 
    ('13', '31', '23', '29', '30', '18', '17')
and
c_acctbal > (
	select
		avg(c_acctbal)
	from
		customer
	where
		c_acctbal > 0.00
		and substring(c_phone from 1 for 2) in
			('13', '31', '23', '29', '30', '18', '17')
)


--select
--	cntrycode,
--	count(*) as numcust,
--	sum(c_acctbal) as totacctbal
--from
--	(
--		select
--			substring(c_phone from 1 for 2) as cntrycode,
--			c_acctbal
--		from
--			customer
--		where
--			substring(c_phone from 1 for 2) in
--				('13', '31', '23', '29', '30', '18', '17')
--			and c_acctbal > (
--				select
--					avg(c_acctbal)
--				from
--					customer
--				where
--					c_acctbal > 0.00
--					and substring(c_phone from 1 for 2) in
--						('13', '31', '23', '29', '30', '18', '17')
--			)
--			and not exists (
--				select
--					*
--				from
--					orders
--				where
--					o_custkey = c_custkey
--			)
--	) as custsale
--group by
--	cntrycode
--order by
--	cntrycode;
