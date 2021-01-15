# udacity-de-cloud-data-warehouses

## How-to
* create redshift cluster (if it doesn't exist already you
can run create_redshift_cluster.py)
* create tables by running create_tables.py
* run etl.py to copy data to stg tables and insert into
fact and dimension tables
* delete cluster by running delete_redshift_cluster.py

## Explanation

TABLE DESIGN

4 dimesnsion tables
* users
* songs
* artists
* time

1 fact table
* songplays

FIELD MAPPING

* see '# FINAL TABLES' in sql_queries.py

ETL PROCESSING

* Data in dimension tables is updated as defined in sql_queries.py
  * user_table_insert
  * song_table_insert
  * artist_table_insert
  * time_table_insert 
* only insert of new data, no updates
* dimension tables do not contain null values for PKs

FILTERING/MODIFICATION

* only logs with the action 'Next Song' are processed in the ETL
* all fields are inserted as in source files, only time dimensions have calculated date columns (hour, day, month, etc.)

## Questions:
- [ ] data type for user_agent?
- [ ] jsonpath for songdata?
- [ ] special characters in songs/artist ("artist":"Emil Gilels\\/Orchestre de la Soci\\u00c3\\u0083\\u00c2\\u00a9t\\u00c3\\u0083\\u00c2\\u00a9 des Concerts du Conservatoire\\/Andr\\u00c3\\u0083\\u00c2\\u00a9 Cluytens)

## Dev Setup
```
source venv/bin/activate
python -m pip install -r requirements.txt
```

