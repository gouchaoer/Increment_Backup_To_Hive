# Increment_Backup_To_Hive
一个增量备份关系数据库(MySQL, PostgreSQL, SQL Server, SQLite, Oracle等)到hive的php脚本工具

## 原理
由于sqoop可定制性太差，本工具针对增量备份场景，备份某张表时只需要用户填写几个关键参数，就能自动化生成hive表，把脚本加入cron就能实现每天增量备份了。增量备份时，脚本主要根据表的自增主键来查询新增数据，然后把新增的数据按照hive表的格式导出成文本，最后调用hive命令直接把文本导入hive内部。

## 环境

1. 脚本内部会调用hive命令，所以必须运行在安装hive的Linux主机上。你需要安装PHP5.4以上的版本，推荐安装PHP7.x
2. 使用`PDO`扩展来查询关系数据库，你需要确认你的PHP安装了`PDO`扩展以及对应数据库适配器。MySQL需要`PDO`+`pdo_mysql`+`mysqlnd`扩展，PostgreSQL需要`PDO`+`pdo_pgsql`+`pgsql`扩展， SQL Server需要`PDO`+`pdo_dblib`等。用`php -m`来查看你是否安装了对应扩展

## Usage

- `workers_num`, the numbers of the proxy process 