<?php

/**
 * A standalone class to convert CSV files into importable MySQL files.
 *
 * @package    csv2mysql
 * @author     Scott Travis <scott.w.travis@gmail.com>
 * @link       http://github.com/swt83/php-csv2mysql
 * @license    MIT License
 */


/*
 |--------------------------------------------------------------------------
| Set variables for conversion.
|--------------------------------------------------------------------------
*/

$import_file = $argv[1];
$database = $argv[2];
$table= $argv[3];
$export_file = $table . '.sql';

if(!function_exists('mysql_real_escape_string'))
{
	function mysql_real_escape_string($str)
	{
		static $search = array("\\",  "\x00", "\n",  "\r",  "'",  '"', "\x1a");
		static $replace = array("\\\\","\\0","\\n", "\\r", "\'", '\"', "\\Z");

		return str_replace($search, $replace, $str);
	}
}


/*
 |--------------------------------------------------------------------------
| report progress.
|--------------------------------------------------------------------------
*/
function progress($incr)
{
	global $import_file;
	static $LINE_BUF_SZ = 64;
	static $import_file_size=null;
	static $accum_size=0;
	static $startTime;
	static $line_buf_old;
	if($import_file_size===null)
	{
		$import_file_size = filesize($import_file);
	}
	if($startTime==null)
	{
		$tz = date_default_timezone_get();
		if ($tz === "UTC")
		{
			date_default_timezone_set('Asia/Shanghai');
		}
		$startTime=time();
	}
	$accum_size += $incr;
	$percent = intval(($accum_size/$import_file_size)*100);
	$gt = intval(($percent/2));
	$elapsedTime = time() - $startTime;
	$line_buf = implode([
			'percent0'=>"[{$accum_size}/{$import_file_size}]",
			'percent'=>"[{$percent}%]",
			'elapsedTime'=>"[{$elapsedTime}s]",
			'gt'=>str_repeat('>', $gt),
			]);
	$line_buf_sz = strlen($line_buf);
	if($line_buf_sz>$LINE_BUF_SZ)
	{
		$line_buf = substr($line_buf, 0, $LINE_BUF_SZ);
	}
	if($line_buf_old !== $line_buf)
	{
		$line_buf_old = $line_buf;
		$buf = str_pad($line_buf_old, $LINE_BUF_SZ, ' ');
		echo "\r" . $buf . "\r";
	}
}

/*
 |--------------------------------------------------------------------------
| Run initial scan of CSV file.
|--------------------------------------------------------------------------
*/
$headers=[];
if (($input = @fopen($import_file, 'r')) != false)
{
	if(($fields = fgetcsv($input, 0, ',')) != false)
	{
		foreach ($fields as $field)
		{
			if(empty($field))
			{
				die('csv header format error!');
			}
			$headers[] = str_ireplace(' ', '_', $field);
		}

	}else
	{
		die('csv file format error!');
	}
	fclose($input);
}
else
{
	die('Unable to open file "'.$import_file.'".'."\n");
}
$msg="csv header fileds: " . implode(', ', $headers) . PHP_EOL;
echo $msg;
/*
 |--------------------------------------------------------------------------
| Build new importable SQL file.
|--------------------------------------------------------------------------
*/

$output = fopen($export_file, 'w');
fwrite($output, "CREATE DATABASE IF NOT EXISTS `{$database}`;\n");
fwrite($output, 'CREATE TABLE `'.$database.'`.`'.$table.'` ('."\n");
fwrite($output, '`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,'."\n");
foreach ($headers as $key=>$header)
{
	fwrite($output, '`'.$header.'` TEXT,'."\n");//所有字段都用TEXT类型，字段不能超过64KB
}
fwrite($output, 'PRIMARY KEY (`id`)'."\n".') DEFAULT CHARACTER SET \'utf8mb4\';'."\n"."\n");
if (($input = @fopen($import_file, 'r')) != false)
{
	$row = 1;
	while (($fields = fgetcsv($input, 1000, ',')) != false)
	{
		if (sizeof($fields) != sizeof($headers))
		{
			$fields_sz = sizeof($fields);
			$headers_sz = sizeof($headers);
			echo "ROW:{$row}, fields_size:{$fields_sz}, headers_size:{$headers_sz}, ".' NCORRECT NUMBER OF FIELDS :';
			echo print_r($fields, true);
			die();
		}

		if ($row != 1)
		{
			$sql = 'INSERT INTO `'.$database.'`.`'.$table.'` VALUES(null, ';

			foreach ($fields as $field)
			{
				$sql .= '\''.mysql_real_escape_string($field).'\', ';
			}
			$sql = rtrim($sql, ', ');
			$sql .= ');';

			fwrite($output, $sql."\n");

			static $sql_insert_sz = null;
			if($sql_insert_sz===null)
			{
				$sql_insert_sz = strlen("INSERT INTO `{$database}'`.`'{$table}` VALUES(null,");
			}
			progress(strlen($sql)-strlen($sql_insert_sz));
		}
		$row++;
	}
	fclose($input);
}
else
{
	echo 'Unable to open file "'.$import_file.'".'."\n";
}
fclose($output);
