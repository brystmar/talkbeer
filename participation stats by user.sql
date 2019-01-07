with bif as (select max(thread_name) as bif_name from biffers),
	likes_r as (select distinct p.thread_name, p.user_id, count(*) as likes_received from posts p join likes l on p.id = l.post_id group by 1,2),
	likes_g as (select distinct p.thread_name, l.user_id, count(*) as likes_given from likes l join posts p on l.post_id = p.id group by 1,2),
	likes_gifs as (select distinct p.thread_name, p.user_id, count(*) as likes_from_gifs from posts p join likes l on p.id = l.post_id where gifs > 0 group by 1,2),
	likes_posts as (select p.thread_name, count(distinct l.post_id) as posts_with_likes from likes l join posts p on l.post_id = p.id group by 1),
	final as (select distinct u.username, p.thread_name as thread, case when u.id in (select user_id from biffers where thread_name = (select distinct bif_name from bif)) then 'Biffer' else 'Wanker' end as participant,
			coalesce(likes_from_gifs * 100 / likes_received,0) as gif_cm, coalesce(likes_given,0) as likes_given, coalesce(likes_received,0) as likes_rec, coalesce(likes_from_gifs,0) as likes_from_gifs,
			coalesce(likes_given * 100 / (posts_with_likes - count(distinct p.id)),0) as sluttiness, coalesce(count(distinct p.id),0) as post_count,
			coalesce(sum(gifs),0) as gifs, coalesce(sum(pics),0) as pics, coalesce(sum(other_media),0) as media,
			coalesce(count(distinct thread_page),0) as pages, coalesce(max(substr(timestamp,1,16)),0) as last_post
		from users u
		left join posts p on u.id = p.user_id
		left join likes_r lr on lr.user_id = p.user_id and lr.thread_name = p.thread_name
		left join likes_g lg on lg.user_id = p.user_id and lg.thread_name = p.thread_name
		left join likes_gifs lfg on lfg.user_id = p.user_id and lfg.thread_name = p.thread_name
		left join likes_posts lp on lp.thread_name = p.thread_name
		group by 1,2,3,4,5,6,7)

select username, gif_cm, sluttiness, likes_given, likes_rec, likes_from_gifs, post_count, gifs, pics, media, pages, last_post, participant, thread
from final
where post_count > 0 and thread = (select distinct bif_name from bif)
order by thread, sluttiness desc, gif_cm, username
;
