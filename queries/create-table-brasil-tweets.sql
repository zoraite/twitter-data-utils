create table twitter.brasil_tweets_estado as
select 
  t.id as id ,
  text,
  latitude,
  longitude,
  st_setsrid(st_makepoint(longitude, latitude), 4326) as tweet_geom
  retweeted,
  id_tweet_str,
  created_at,
  in_reply_to_user_id,
  lang,
  place_name,
  place_id,
  user_id_str,
  e.id as id_estado,
  estado,
  uf
from brasil.estados e, twitter.brasil_geo_ref_tweets_all t
where st_contains(e.the_geom, st_setsrid(st_makepoint(longitude, latitude), 4326)) = True
