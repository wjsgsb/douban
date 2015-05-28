-- mysql
create database douban;

use douban;

-- user_url table save all user watched movies
create table user
(
    user_id           varchar(50),
)

create table user_url
(
    user_id            varchar(50),
    user_movie_url     varchar(200),
    curr_url           varchar(200),
    is_finish          varchar(1)
);

create table user_movie_rating
(
    user_id        varchar(50),
    movie_id       varchar(50),
    rating         varchar(50)
);