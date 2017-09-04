
# Increment_Backup_To_Hive
一个增量备份关系数据库(MySQL, PostgreSQL, SQL Server, SQLite, Oracle等)到hive的php脚本工具

## 原理
由于sqoop可定制性太差，本工具针对增量备份场景，备份某张表时只需要用户填写几个关键参数，就能自动化生成hive表，把脚本加入cron就能实现每天增量备份了。增量备份时，脚本主要根据表的自增主键来查询新增数据，然后把新增的数据按照hive表的格式导出成文本，最后调用hive命令直接把文本导入hive内部。

## 环境

1. 脚本内部会调用hive命令，所以必须运行在安装hive的Linux主机上。你需要安装PHP5.4以上的版本，推荐安装PHP7.x
2. 使用`PDO`扩展来查询关系数据库，你需要确认你的PHP安装了`PDO`扩展以及对应数据库适配器。MySQL需要`PDO`+`pdo_mysql`+`mysqlnd`扩展，PostgreSQL需要`PDO`+`pdo_pgsql`+`pgsql`扩展， SQL Server需要`PDO`+`pdo_dblib`等。用`php -m`来查看你是否安装了对应扩展

## 用法

- 下载本repo到安装hive的linux主机上，进入databases目录，可以看到有一个test_database的样例，里面有几张表的备份例子。假如你要备份一个名叫"my_database"数据库中的"my_table1"表，那么就在databases目录下新建一个名为"my_database"的目录，复制databases/test_database/config.ini过来到`databases/my_database`目录下并且修改PDO数据源参数。然后复制`/databases/test_database/test_table1.php`到`databases/my_database`目录下并且改名为`my_table1.php`，打开文件`my_table1.php`并且按照你的需要修改关键参数，这些关键参数的意义如下：

- $TABLE:要备份的表名
- $TABLE_AUTO_INCREMENT_ID :表中用来进行增量备份的自增INT列，由于会使用类似`SELECT * FROM `table` WHERE `id`>=M AND `id`<M+1000`这种遍历方式，所以自增INT列必须加上索引。如果该表没有自增INT列，设置`$TABLE_AUTO_INCREMENT_COLUMN = null;`即可，此时会使用`SELECT * FROM `table` LIMIT M,1000`这种遍历方式，如果记录数太大性能会急剧下降，而且数据只能插入不能删除
- $TABLE_BATCH:每次从数据源读多少行数据
- $HIVE_DB:导入hive数据库名，没有则自动创建
- $HIVE_TABLE:导入hive表名
- $HIVE_FORMAT:创建hive表的格式，如果本身表体积就不大可以直接使用默认的TEXTFILE纯文本格式，此时设置`$HIVE_FORMAT = null`；对于占用磁盘太大的表使用ORCFILE格式压缩，此时设置`$HIVE_FORMAT = "ORCFILE";`即可；使用ORCFILE格式时，脚本在创建了名为`table`的ORCFILE格式的hive表之后会再创建一个名为`table__tmp`的TEXTFILE的临时hive表，从数据源把数据导入了`table__tmp`表之后再转存到`table`表，最后清空`table__tmp`表
- $ROW_CALLBACK_PARTITIONS:*hive表的分区策略，有如下2种情况：
 第一：不要分区，此时设置`$ROW_CALLBACK_PARTITIONS = null;`即可
 第二：根据数据源读到的每行字段来确定分区，此时自己设置一个以表的行数据为参数的回调函数的数组即可，数组键为分区名(分区类型只能为STRING)，比如：
 
```
 (a),假如created_date字段代表插入时间，类型为TIMESTAMP，按照天分区
 $ROW_CALLBACK_PARTITIONS = [
 'partition_day' => function(Array $row)
 {
	 $created_date = empty($row['created_date'])?'0000-00-00 00:00:00':$row['created_date'];
	 $partition = substr($created_date, 0, 10);
	 return $partition;
 }
 ];
 
 (b),多分区情况下，假如created_date字段代表插入时间，类型为INT，按照月分区；假如province字段代表省，按照省分区
 $ROW_CALLBACK_PARTITIONS = [
 
 'partition_month' => function(Array $row)
 {
	 $created_date = empty($row['created_date'])? 0:$row['created_date'];
	 $created_date_str = date('Y-m-d H:i:s', $created_date)
	 $partition = substr($created_date, 0, 7);
	 return $partition;
 },
 'partition_province' => function(Arrar $row)
 {
    $province = empty($row['province'])? "default":$row['province'];
	return $province;
 }
 ];
 
 (c),表中没有分区字段，现在按照备份时间进行按天分区
 $ROW_CALLBACK_PARTITIONS = [
 'partition_day' => function(Array $row)
 {
	 $created_date = time()
	 $created_date_str = date('Y-m-d H:i:s', $created_date)
	 $partition = substr($created_date, 0, 7);
	 return $partition;
 }
 ];
```



## 注意

