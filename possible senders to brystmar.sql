select u.username, u.location, max(p.timestamp) as last_post, count(distinct p.id) as post_count
from "user" u
join biffer b on u.id = b.user_id
join post p on u.id = p.user_id and p.thread_name = b.thread_name
where my_sender is null and p.thread_name = 'SSF14'
group by 1,2
order by 1