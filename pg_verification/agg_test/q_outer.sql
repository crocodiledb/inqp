
explain select *
from nation as n
full outer join region as r 
on n.n_regionkey = r.r_regionkey 
where n.n_name > r.r_name;
