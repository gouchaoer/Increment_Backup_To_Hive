USE test_database;
CREATE TABLE test_table1 (
`id`  STRING,
`created_date`  STRING,
`updated_date`  STRING,
`is_delete`  STRING,
`msg`  STRING
)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS RCFILE;

			
CREATE TABLE test_table1__tmp (
`id`  STRING,
`created_date`  STRING,
`updated_date`  STRING,
`is_delete`  STRING,
`msg`  STRING
)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

