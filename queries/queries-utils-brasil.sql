
create table twitter.brasil_geo_ref_tweets_estados as
select *, st_setsrid( st_makepoint(longitude, latitude), 4326 ) tweet_geom
from twitter.brasil_geo_ref_tweets t, brasil.estados b
where st_contains(b.the_geom, st_setsrid( st_makepoint(longitude, latitude), 4326 ) )

update twitter.brasil_geo_ref_tweets_estados
set date = extract( day from created_at ) || '-' || extract( month from created_at )


-------------- Tweets por dia
select date, count(distinct id_tweet_str)
from twitter.brasil_geo_ref_tweets_estados
group by date
order by date


--------------- Users por dia

select date, 
count(distinct user_id_str)
from twitter.brasil_geo_ref_tweets_estados
group by date


-- Intersection
select b.d, count(distinct id_tweet_str)
from
(
select extract( day from created_at ) || '-' || extract( month from created_at )  as d,*
from twitter.brasil_geo_ref_tweets_estados
) b,
(
select extract( day from created_at ) || '-' || extract( month from created_at )  as d,*
from twitter.hpc_brasil_geo_ref_tweets_estados
) h
where h.d = b.d and b.id_tweet_str = h.id_tweet_str
group by b.d
order by b.d