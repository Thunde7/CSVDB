-- some example with larger data files
drop table if exists ratings;

create table if not exists ratings (userId int ,movieId int ,rating float, ts timestamp);

load data infile "ratings.csv" into table ratings;

select movieId, avg(rating) from ratings group by movieId order by avg(rating) desc;
