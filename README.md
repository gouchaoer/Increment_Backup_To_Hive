# Increment_Backup_To_Hive
A php script to incremental backup rational database(MySQL, PostgreSQL, SQL Server, SQLite, Oracle..) to hive.

[中文文档](README_cn.md)
## Environment

1. As this script directly call hive shell, so it should run on the host which your hive is installed. You should install php5.4+ or php7.x(php7.x is recommended).
2. As `PDO` extension is used to query your database, You should make sure your php had `PDO` extension and corresponding databse adaptor. MySQL database need `PDO`+`pdo_mysql`+`mysqlnd`. PostgreSQL database need `PDO`+`pdo_pgsql`+`pgsql`. SQL Server database need `PDO`+`pdo_dblib`. use `php -m` to check installed extensions. 

## Usage

- `workers_num`, the numbers of the proxy process 
- `proxy_addr`, the address of the proxy
- `redis_addr`, the real redis address
- `white_or_black`, white list or black list strategy, if "white" chosen then redis commands will be invalid except for `white_list_commands`, if "black" chosen the redis commands will be valid except for `black_list_commands`
- `database_0_and`，due to the default database is 0, here you can allow client to use other databases, of course you should allow `SELECT` command.

