-- Create daily trajectories from tweets

drop table if exists twitter.brasil_trajs_from_tweets;
create table twitter.brasil_trajs_from_tweets as
select user_id_str, dia, count(*) points, st_makeline(st_setsrid(st_makepoint(longitude, latitude), 4326)) 
from 
(
	select *, extract(year from created_at)
	        || '-' || extract(month from created_at)
		        || '-' || extract(day from created_at)  as dia
			from twitter.brasil_tweets_estado 
			order by user_id_str, created_at
) a
group by user_id_str, dia
having count(*) > 1

