# Increment_Backup_To_Hive
一个增量备份关系数据库(MySQL, PostgreSQL, SQL Server, SQLite, Oracle等)到hive的php脚本工具

## 原理
由于sqoop可定制性太差，本工具针对增量备份场景，备份某张表时只需要用户填写几个关键参数，就能自动化生成hive表，把脚本加入cron就能实现每天增量备份了。增量备份时，脚本主要根据表的自增主键来查询新增数据，然后把新增的数据按照hive表的格式导出成文本，最后调用hive命令直接把文本导入hive内部。

## 环境

1. 脚本内部会调用hive命令，所以必须运行在安装hive的Linux主机上。你需要安装PHP5.4以上的版本，推荐安装PHP7.x
2. 使用`PDO`扩展来查询关系数据库，你需要确认你的PHP安装了`PDO`扩展以及对应数据库适配器。MySQL需要`PDO`+`pdo_mysql`+`mysqlnd`扩展，PostgreSQL需要`PDO`+`pdo_pgsql`+`pgsql`扩展， SQL Server需要`PDO`+`pdo_dblib`等。用`php -m`来查看你是否安装了对应扩展

## 用法

- 下载本repo到安装hive的linux主机上，进入databases目录，可以看到有一个test_database的样例，里面有几张表的备份例子。假如你要备份一个名叫"my_database"数据库中的"my_table1"表，那么就在databases目录下新建一个名为"my_database"的目录，复制databases/test_database/config.ini过来到`databases/my_database`目录下并且修改PDO数据源参数。然后复制`/databases/test_database/test_table1.php`到`databases/my_database`目录下并且改名为`my_table1.php`，打开文件`my_table1.php`并且按照你的需要修改关键参数，这些关键参数的意义如下：

- $TABLE：要备份的表名
- $TABLE_AUTO_INCREMENT_ID ：表中用来进行增量备份的自增INT列，由于会使用类似`SELECT * FROM `table` WHERE `id`>=M AND `id`<M+1000`这种遍历方式，所以自增INT列必须加上索引。如果该表没有自增INT列，设置`$TABLE_AUTO_INCREMENT_COLUMN = null;`即可，此时会使用`SELECT * FROM `table` LIMIT M,1000这种遍历方式，如果记录数太大性能



## 注意

