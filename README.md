MiniSQL
================
It is an SQL engine supporting set of queries including join of two tables.
You can run queries as python dbms.py "query".
You need to have metadata stored in file named metadata.txt in format as in sample file.
tables are .csv files made of only integers.

It supports queries of following format:
----------------------------------------------
1. select * from table1;
2. select max(A) from table1;
3. select A,D from table1,table2;
4. select table1.A,table2.D from table1,table2 where table1.B=table2.D
5. select distict(A) from table1;
6. select table1.A from table1,table2 where table1.B=20 OR/AND table2.B=50
