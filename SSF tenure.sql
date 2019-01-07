with data as (select distinct user_id, count(distinct thread_name) as SSFs, min(thread_name) as First_SSF, max(thread_name) as Latest_SSF
from biffers b --join biffers b2 on b1.user_id = b2.user_id and b1.username = b2.username --and b1.thread_name = b2.thread_name
group by 1)
select distinct max(u.username) as username, d.user_id, SSFs, First_SSF, Latest_SSF
from data d
join users u on d.user_id = u.id
group by 2,3,4,5
order by SSFs desc, username
;