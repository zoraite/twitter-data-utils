
-- Tweets por dia
create table twitter.tweets_per_day as
select created_at_str, count(*)
from twitter.brasil_world_cup_geo_ref_tweets_csv
group by created_at_str
order by created_at_str

-- Usuarios por dia
create table twitter.users_per_day as
select created_at_str, count(distinct user_id_str)
from twitter.brasil_world_cup_geo_ref_tweets_csv
group by created_at_str
order by created_at_str

-- Trajs por dia - de local para local
-- Criar trajetorias
create table twitter.brasil_trajs_from_tweets as
select user_id_str, agg_str(place_name, ','), count(*) points_count
group by user_id_str
having count(*) > 1
