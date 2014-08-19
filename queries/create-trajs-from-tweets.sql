-- Create daily trajectories from tweets

drop table if exists twitter.brasil_trajs_from_tweets;
create table twitter.brasil_trajs_count_from_tweets as
select user_id_str, created_at_str, count(*) points, st_makeline(tweet_geom)
group by user_id_str, created_at_str
having count(*) > 1

-- Criar trajetorias
create table twitter.brasil_trajs_from_tweets as
select user_id_str, agg_str(estado, ','), count(*) points_count
group by user_id_str
having count(*) > 1



