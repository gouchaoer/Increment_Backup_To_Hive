# csv2mysql for PHP#

Creates an importable MySQL file from a CSV file.

## Usage ##

```
php csv2mysql.php [input.csv] [mysql_database_name] [mysql_table_name]
```

The script doesn't actually do anything to your database, but the importable file will need to know the destination database and table(all fields type is TEXT).

this script is forked from `https://github.com/swt83/php-csv2mysql`, I did some changes to fit my task.