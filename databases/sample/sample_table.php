<?php

//包含初始化类
include __DIR__ . "/../../Increment_Backup_To_Hive.php";

//设置工作目录
$WORK_DIR = __DIR__ ;

//sample数据库中需要备份到hive的表
$TABLE = "sample_table";

/*
 *表中用来进行增量备份的自增INT列，由于会使用`SELECT * FROM `sample_table` WHERE `id`>=M AND `id`<M+1000`这种遍历方式，所以自增INT列必须加上索引
 *如果该表没有自增INT列，设置`$TABLE_AUTO_INCREMENT_COLUMN = null;`即可，此时会使用`SELECT * FROM `sample_table` LIMIT M,1000这种遍历方式，如果记录数太大性能会急剧下降
 */
$TABLE_AUTO_INCREMENT_COLUMN = "id";

//导入hive数据库名，没有则自动创建
$HIVE_DB = "sample";

//导入hive表名
$HIVE_TABLE = "sample_table";

/*
 *是否创建ORCFILE格式的hive表，对于占用磁盘太大的表使用ORCFILE格式压缩可以节约HDFS磁盘空间，此时设置`$HIVE_ORCFILE = true;`即可；如果本身表体积就不大就没必要使用ORCFILE，直接使用默认的TEXTFILE纯文本格式即可，此时设置`$HIVE_ORCFILE = false`
 *使用ORCFILE格式时，脚本在创建了名为`sample_table`的ORCFILE格式的hive表之后会再创建一个名为`sample_table_tmp`的TEXTFILE的临时hive表，从数据源把数据导入了`sample_table_tmp`表之后再转存到`sample_table`表，最后清空`sample_table_tmp`表
 */
$HIVE_ORCFILE = false;


/*
 *hive表的分区策略有如下2种情况：
 *第一：不要分区，此时设置`$HIVE_PARTITION = null;`即可，此时设置`$HIVE_PARTITION = null;`即可
 *第二：根据表的字段确定分区（hive中分区字段为`partition`不可修改），此时自己设置一个以表的行数据为参数的回调函数即可，比如：
 ```
 //假如created_date行代表插入时间，类型为TIMESTAMP，按照天分区
 $HIVE_PARTITION = function(Array $row)
 {
	 $created_date = empty($row['created_date'])?'0000-00-00 00:00:00':$row['created_date'];
	 $partition = substr($created_date, 0, 10);
	 return $partition;
 }
 
  //假如created_date行代表插入时间，类型为INT，按照月分区
 $HIVE_PARTITION = function(Array $row)
 {
	 $created_date = empty($row['created_date'])? 0:$row['created_date'];
	 $created_date_str = date('Y-m-d H:i:s', $created_date)
	 $partition = substr($created_date, 0, 7);
	 return $partition;
 }
 
   //表中没有分区字段，现在按照备份时间进行分区
 $HIVE_PARTITION = function(Array $row)
 {
	 $created_date = time()
	 $created_date_str = date('Y-m-d H:i:s', $created_date)
	 $partition = substr($created_date, 0, 7);
	 return $partition;
 }
 ```
 */
$HIVE_PARTITION = null;


/*
 * 从数据源读到的行数据不一定和hive中一样（比如你对自动生成的hive表增减了一些字段，此时你需要对每一行的数据进行处理满足hive表的格式），比如这里hive表把`sample_table`表的`created_date`字段删除，然后另外添加一个id_md5字段；
 * 返回的$row的字段顺序必须和hive中一致，如果不一致程序会检测到并且推出
 * $ROW_CALLBACK=function (Array $row)
 {
 	unset($row['created_date']);
 	$row['id_md5']=md5($row['id']);
 	return $row;
 }
 * 
 * 
 * */
$ROW_CALLBACK = null;