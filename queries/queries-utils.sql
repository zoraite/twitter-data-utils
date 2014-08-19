
create table twitter.hpc_brasil_geo_ref_tweets_estados as
select *, st_setsrid( st_makepoint(longitude, latitude), 4326 ) tweet_geom
from twitter.hpc_brasil_geo_ref_tweets t, brasil.estados b
where st_contains(b.the_geom, st_setsrid( st_makepoint(longitude, latitude), 4326 ) )

-------------- Tweets por dia
select extract( day from created_at ) || '-' || extract( month from created_at ) as day, 
count(distinct id_tweet_str)
from twitter.hpc_brasil_geo_ref_tweets_estados t
group by extract( day from created_at ) || '-' || extract( month from created_at ) 
order by day


--------------- Users por dia

select extract( day from created_at ) || '-' || extract( month from created_at ) as day, 
count(distinct user_id_str)
from twitter.hpc_brasil_geo_ref_tweets_estados
group by extract( day from created_at ) || '-' || extract( month from created_at ) 
order by day

--------------- Tweets com check-ins

select extract( day from created_at ) || '-' || extract( month from created_at ) as day, 
count(distinct id_tweet_str)
from twitter.hpc_brasil_geo_ref_tweets_estados
where text ilike 'I''m at % http%'
group by extract( day from created_at ) || '-' || extract( month from created_at ) 
order by day