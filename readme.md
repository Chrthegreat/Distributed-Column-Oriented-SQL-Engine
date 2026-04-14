MiniDist: Distributed Column-Oriented SQL Engine

***Overview***
MiniDist is a simplified distributed analytical SQL engine implemented in Python. It includes a custom columnar storage layer, a distributed coordinator–worker architecture, and query optimizations such as Zone Map pruning.

***How to Use***
Unzip and run the code. There are no external packages, all includes are from basic python. I used Python 3.10.2 so I can't guarantee backwards compatibility.

***Initialize a Table***
First you need to run the minidist.py. This provides 2 commands, init and load. 
For each table you need to provide a schema file where you list every column and its type of data. Check the examples. 
Init takes 2 arguments. The table directory (where the table will be saved) and the ssf file for the table.

Example init: 
    -> python minidist.py init data/people people.ssf

A new directory will be created with the schema file copied and a table.txt file with useful metadata. 
In table.txt you will find segment_target_rows controlling number of rows per segment. The default is 100000 per segment. 
You can also use a parameter in load to determine the number of segments

Then you need to run load command. You must provide the path to the table directory (the one you just created using init) and the csv file with the data. Optionally you decide
how many partitions you will have for the data by passing an optional argument. If not, the default will be used defined in the metadata _table.txt

Example load: 
    -> python minidist.py load data/people --csv people.csv (--segments n)

You will now see some seg-000001, seg-000002 ... folders created. Each one of these will be assigned to 1 worker.

***Starting the Cluster***
In order to run the workers you have 2 choices. Run each worker seperately (you will need mupltiple terminals),
or run them all at once using cluster.py. 

Cluster.py takes 2 arguments, the table path (example: data/people) and the port for the first worker.

Example cluster: 
    -> python cluster.py --table data/people --port 5050

***Running queries***
To run queries, you need to run the coordinator that accepts queries and assignes them to the workers.

This takes 2 argumnets as well, the table path and the port of the first workers (so it knows where to look)

Example coordinator:  
    -> python coordinator.py --table data/people --port 5050

A promt will show up DIST-SQL>... 

***Commands Supported***
-SELECT 
-FROM
-WHERE (=,>,<,>=,<=, BETWEEN,AND)
-GROUB BY with COUNT, SUM, AVG, MIN, MAX

***Query Examples***
Using the example dataset called people. Adjust to your dataset.

-> SELECT * FROM people;
-> SELECT name, country FROM people;
-> SELECT * FROM people WHERE age > 50;
-> SELECT name, age FROM people WHERE country = 'JP';
-> SELECT * FROM people WHERE height BETWEEN 180 AND 190;
-> SELECT COUNT(id), MIN(age), MAX(height) FROM people;
-> SELECT AVG(height) FROM people;
-> SELECT country, COUNT(id) FROM people GROUP BY country;
-> SELECT country, AVG(age), MAX(height) FROM people GROUP BY country;
-> SELECT * FROM people WHERE id < 5;
-> SELECT * FROM people WHERE id > 1000;
-> SELECT * FROM people WHERE age = 24;
-> SELECT country, height, age, name, id FROM people;

***Exit coordinator***

To exit type exit or quit. To stop cluster do Ctrl+c.