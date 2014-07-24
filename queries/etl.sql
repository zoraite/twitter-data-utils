
-- Criar tabela
drop table if exists twitter.brasil_world_cup_geo_ref_tweets_csv;
CREATE TABLE twitter.brasil_world_cup_geo_ref_tweets_csv
(
  id bigserial NOT NULL,
  text text,
  latitude double precision,
  longitude double precision,
  retweeted boolean,
  id_tweet bigint,
  id_tweet_str text,
  created_at timestamp without time zone,
  created_at_str text,
  in_reply_to_user_id text,
  lang text,
  place_name text,
  place_id text,
  place_bounding_box_00 double precision,
  place_bounding_box_01 double precision,
  place_bounding_box_10 double precision,
  place_bounding_box_11 double precision,
  place_bounding_box_20 double precision,
  place_bounding_box_21 double precision,
  place_bounding_box_30 double precision,
  place_bounding_box_31 double precision,
  place_type text,
  place_country_code text,
  place_country text,
  place_full_name text,
  user_name text,
  user_id bigint,
  user_id_str text,
  user_friends_count integer,
  user_created_at timestamp without time zone,
  user_screen_name text
)

-- copiar dados para a tabela
\COPY twitter.brasil_world_cup_geo_ref_tweets_csv FROM '/Users/igobrilhante/Documents/workspace/research/twitter/twitter-data-utils/brasil_world_cup_geo_ref_tweets.csv' WITH CSV;

-- criar atributo geometrico
ALTER TABLE twitter.brasil_world_cup_geo_ref_tweets_csv  ADD COLUMN geom geometry;

-- remover duplicatas
CREATE TABLE twitter.tmp as select distinct * from twitter.brasil_world_cup_geo_ref_tweets_csv;
DROP TABLE twitter.brasil_world_cup_geo_ref_tweets_csv;
ALTER TABLE twitter.tmp RENAME TO brasil_world_cup_geo_ref_tweets_csv;

-- atualizar atributo geometrico
UPDATE twitter.brasil_world_cup_geo_ref_tweets_csv
SET geom = st_setsrid(st_makepoint(longitude, latitude), 4326)

-- criar indice espacial
CREATE INDEX georef_tweets_gix ON twitter.brasil_world_cup_geo_ref_tweets_csv USING GIST (geom);

-- criar outros indices
CREATE UNIQUE INDEX ON twitter.brasil_world_cup_geo_ref_tweets_csv (id);
CREATE INDEX ON twitter.brasil_world_cup_geo_ref_tweets_csv (created_at_str);
CREATE INDEX ON twitter.brasil_world_cup_geo_ref_tweets_csv (user_id_str);
CREATE INDEX ON twitter.brasil_world_cup_geo_ref_tweets_csv (place_name);

-- criar tabela com estados brasileiros
drop table if exists twitter.brasil_tweets_estado;
create table twitter.brasil_tweets_estado as
select
  t.id as id ,
  text,
  latitude,
  longitude,
  geom as tweet_geom,
  the_geom as estado_geom,
  retweeted,
  id_tweet_str,
  created_at,
  created_at_str,
  in_reply_to_user_id,
  lang,
  place_name,
  place_id,
  user_id_str,
  e.estado_id as id_estado,
  estado,
  uf
from brasil.estados e, twitter.brasil_world_cup_geo_ref_tweets_csv t
where st_contains(e.the_geom, t.geom) = True;

-- criar outros indices
CREATE UNIQUE INDEX ON twitter.brasil_tweets_estado(id);
CREATE INDEX ON twitter.brasil_tweets_estado(created_at_str);
CREATE INDEX ON twitter.brasil_tweets_estado(user_id_str);
CREATE INDEX ON twitter.brasil_tweets_estado(uf);
CREATE INDEX ON twitter.brasil_tweets_estado(place_name);

