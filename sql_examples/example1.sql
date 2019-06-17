-- some simple example using small amount of data
drop table if exists movies;

create table if not exists movies (title varchar, year int, duration int, score float);

load data infile "movies2.csv" into table movies;

select year, avg(duration) from movies group by year order by year desc;

select year, avg(duration) as avg_d from movies group by year order by avg_d;

select * from movies where year is NULL;

