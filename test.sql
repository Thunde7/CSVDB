drop table test;
create table test (name varchar, age int);
load data infile "input.csv" into table test;
select name,age into outfile "outfile.csv" from test where age is NULL;
