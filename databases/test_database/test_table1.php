<?php
//php -d xdebug.remote_autostart=On test_table1.php

//要备份的表名
$TABLE = "test_table1";

/**
 *表中用来进行增量备份的自增INT列，由于会使用类似`SELECT * FROM `table` WHERE `id`>=M AND `id`<M+1000`这种遍历方式，所以自增INT列必须加上索引
 *如果该表没有自增INT列，设置`$TABLE_AUTO_INCREMENT_COLUMN = null;`即可，此时会使用`SELECT * FROM `table` LIMIT M,1000这种遍历方式，如果记录数太大性能会急剧下降，而且数据只能插入不能删除
 */
$TABLE_AUTO_INCREMENT_ID = "id";

//每次从数据源读多少行数据
$TABLE_BATCH=1000;

//导入hive数据库名，没有则自动创建
$HIVE_DB = "test_database";

//要创建的hive表名
$HIVE_TABLE = "test_table1";

/**
 *创建hive表的格式，如果本身表体积就不大可以直接使用默认的TEXTFILE纯文本格式，此时设置`$HIVE_FORMAT = null`；对于占用磁盘太大的表使用RCFILE格式压缩，此时设置`$HIVE_FORMAT = "RCFILE";`即可；
 *使用RCFILE格式时，脚本在创建了名为`table`的RCFILE格式的hive表之后会再创建一个名为`table__tmp`的TEXTFILE的临时hive表，从数据源把数据导入了`table__tmp`表之后再转存到`table`表，最后清空`table__tmp`表
 */
$HIVE_FORMAT = "RCFILE";

/**
 *hive表的分区策略，有如下2种情况：
 *第一：不要分区，此时设置`$ROW_CALLBACK_PARTITIONS = null;`即可
 *第二：根据数据源读到的每行字段来确定分区，此时自己设置一个以表的行数据为参数的回调函数的数组即可，数组键为分区名(分区类型只能为STRING)，比如：
 ```
 //假如created_date字段代表插入时间，类型为TIMESTAMP，按照天分区
 $ROW_CALLBACK_PARTITIONS = [
 'partition_day' => function(Array $row)
 {
	 $created_date = empty($row['created_date'])?'0000-00-00 00:00:00':$row['created_date'];
	 $partition = substr($created_date, 0, 10);
	 return $partition;
 }
 ];
  //多分区情况下，假如created_date字段代表插入时间，类型为INT，按照月分区；假如province字段代表省，按照省分区
 $ROW_CALLBACK_PARTITIONS = [
 
 'partition_month' => function(Array $row)
 {
	 $created_date = empty($row['created_date'])? 0:$row['created_date'];
	 $created_date_str = date('Y-m-d H:i:s', $created_date)
	 $partition = substr($created_date, 0, 7);
	 return $partition;
 },
 'partition_province' => function(Array $row)
 {
    $province = empty($row['province'])? "default":$row['province'];
	return $province;
 }
 ];
 
   //表中没有分区字段，现在按照备份时间进行按天分区
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
 */
$ROW_CALLBACK_PARTITIONS = null;

/**
 * 如果从数据源读到的行数据和hive中不一样，比如你对自动生成的hive表增减了一些字段，此时你需要对每一行的数据进行处理满足hive表的格式，返回的数组$row的字段顺序必须和对应的hive表一致，如果不一致程序会检测到错误并退出
 * 
```
//假如数据源表有`id, tel, birthday`这3个字段，你修改了自动生成的hive建表文件，把`tel`字段进行加密，把birthday改成birth_year字段，你的hive字段为`id, tel, birth_year`
$ROW_CALLBACK_CHANGE=function (Array $row)
 {
    //$row数组为：['id'=>1, 'tel'=>'15888888888', 'birthday'=>'1990-01-01'];
 	$row['tel'] = my_encrypt_fun($row['tel']);
 	$row['birth_year']= substr($row['birthday'], 0, 4);
 	unset($row['birthday'];
 	//$row数组为：['id'=>1, 'tel'=>'encrypted content', 'birth_year'=>'1990'];和hive表结构一致
 	return $row;
 }
 ``` 
 */
$ROW_CALLBACK_CHANGE = null;

//文本文件缓存大小(Byte)，脚本会把数据缓存到本地文件中，最后再统一导入hive，默认的null为8G
$EXPORTED_FILE_BUFFER = null;

//设置工作目录，必须为__DIR__
$WORK_DIR = __DIR__ ;

//包含初始化类
include __DIR__ . "/../../Increment_Backup_To_Hive.php";

class My_Increment_Backup_To_Hive extends Increment_Backup_To_Hive
{
    //你可以重写一些方法满足你的特殊需求
}

My_Increment_Backup_To_Hive::run();
