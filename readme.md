# MiniDist: Distributed Column-Oriented SQL Engine

## Overview

MiniDist is a simplified distributed analytical SQL engine implemented in Python. It includes:

- A custom columnar storage layer  
- A distributed coordinator–worker architecture  
- Query optimizations such as Zone Map pruning  

---

## How to Use

Unzip and run the code. No external packages are required — all dependencies are from standard Python.

Tested with Python 3.10.2 (backwards compatibility is not guaranteed).

---

## Initialize a Table

Run `minidist.py` to initialize and load tables. It provides two commands:

- `init`
- `load`

Each table requires a schema file (`.ssf`) defining column names and data types (see examples).

### Init Command

Arguments:
1. Table directory (where the table will be stored)  
2. Schema file  

Example:

```bash
python minidist.py init data/people people.ssf
```

This creates:
- A new table directory  
- A copy of the schema file  
- A `table.txt` file with metadata  

### Metadata

Inside `table.txt`:

- `segment_target_rows`: controls rows per segment (default: 100000)

---

## Load Data

Load data into the initialized table.

Arguments:
1. Path to table directory  
2. CSV file  
3. (Optional) Number of segments  

Example:

```bash
python minidist.py load data/people --csv people.csv --segments n
```

If `--segments` is not provided, the default from metadata is used.

After loading, directories like:

```
seg-000001
seg-000002
...
```

will be created. Each segment is assigned to one worker.

---

## Starting the Cluster

You can:

- Run workers manually (multiple terminals), or  
- Start all workers using `cluster.py`  

### Cluster Command

Arguments:
1. Table path  
2. Port for the first worker  

Example:

```bash
python cluster.py --table data/people --port 5050
```

---

## Running Queries

Start the coordinator to accept and distribute queries.

### Coordinator Command

Arguments:
1. Table path  
2. Port of the first worker  

Example:

```bash
python coordinator.py --table data/people --port 5050
```

You will see a prompt:

```
DIST-SQL>
```

---

## Commands Supported

- SELECT  
- FROM  
- WHERE (`=`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `AND`)  
- GROUP BY with:
  - COUNT  
  - SUM  
  - AVG  
  - MIN  
  - MAX  

---

## Query Examples

Using the example dataset `people`:

```sql
SELECT * FROM people;
SELECT name, country FROM people;
SELECT * FROM people WHERE age > 50;
SELECT name, age FROM people WHERE country = 'JP';
SELECT * FROM people WHERE height BETWEEN 180 AND 190;
SELECT COUNT(id), MIN(age), MAX(height) FROM people;
SELECT AVG(height) FROM people;
SELECT country, COUNT(id) FROM people GROUP BY country;
SELECT country, AVG(age), MAX(height) FROM people GROUP BY country;
SELECT * FROM people WHERE id < 5;
SELECT * FROM people WHERE id > 1000;
SELECT * FROM people WHERE age = 24;
SELECT country, height, age, name, id FROM people;
```

---

## Exit

- To exit the coordinator: type `exit` or `quit`  
- To stop the cluster: press `Ctrl + C`  
