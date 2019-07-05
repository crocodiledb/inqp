set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

select 
    p_partkey,
    p_retailprice
from
    part
where 
    p_brand = 'Brand#14'
    and p_type = 'SMALL ANODIZED STEEL'
    and p_retailprice < (select 
                            avg(p_retailprice)
                         from
                            part);


