<?php
class Increment_Backup_To_Hive
{
	private static $config_arr;
	private static $cache_dir;
	private static $log_dir;
	protected static $dbh;
	static protected function init()
	{
		global $TABLE;
		global $WORK_DIR;
		
		ini_set ( 'memory_limit', - 1 );
		set_time_limit ( 0 );
		
		Log::setting ( $TABLE );
		
		$config_path = $WORK_DIR . "/config.ini";
		self::$config_arr = parse_ini_file ( $config_path );
		if (empty ( self::$config_arr ))
		{
			$msg = "read config error:{$config_path}, exit 1";
			Log::log_step ( $msg, 'init', true );
			exit ( 1 );
		}
		
		self::$cache_dir = $WORK_DIR . "/cache/";
		if (! file_exists ( self::$cache_dir ))
		{
			if (! mkdir ( self::$cache_dir, 0777, true ))
			{
				$msg = "failed to create folder:" . self::$cache_dir;
				Log::log_step ( $msg, 'init', true );
				exit ( 1 );
			}
		}
		
		self::$log_dir = $WORK_DIR . "/log/";
		if (! file_exists ( self::$log_dir ))
		{
			if (! mkdir ( self::$log_dir, 0777, true ))
			{
				$msg = "failed to create folder:" . self::$log_dir;
				Log::log_step ( $msg, 'init', true );
				exit ( 1 );
			}
		}
		
		$running_lock = self::$cache_dir . "{$TABLE}-running.pid";
		$running_lock_content = @file_get_contents ( $running_lock );
		if (! empty ( $running_lock_content ))
		{
			$pieces = explode ( "|", $running_lock_content );
			$pid_old = empty($pieces [1])?-1:$pieces [1];
			if (file_exists ( "/proc/{$pid_old}" ))
			{
				$msg = "running_lock:{$running_lock} exist, running_lock_content:{$running_lock_content}, another program is running, exit 1";
				Log::log_step ( $msg, 'init', true );
				exit ( 1 );
			} else
			{
				$msg = "running_lock:{$running_lock} exist, running_lock_content:{$running_lock_content}, another program unproperly exited, go on";
				Log::log_step ( $msg, 'init', true );
			}
		}
		$pid=getmypid();
		$date_formated = date("Y-m-d H:i:s");
		file_put_contents($running_lock, "{$date_formated}|{$pid}");
		register_shutdown_function ( function () use ($running_lock)
		{
			@unlink ( $running_lock );
			Log::log_step ( "unlink {$running_lock}", 'init' );
		} );
		
		try
		{
			self::$dbh = new PDO ( self::$config_arr ['DB_DSN'], self::$config_arr ['DB_USER'], self::$config_arr ['DB_PASSWD'] );
			self::$dbh->setAttribute ( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
		} catch ( PDOException $e )
		{
			$msg = "PDO Connection failed, exit 1... " . $e->getMessage ();
			Log::log_step ( $msg );
			exit ( 1 );
		}
	}
	
	// 检测从数据源读到的行数据是否和hive建表语句一致
	protected static $hive_cols;
	static protected function check_row(Array $row)
	{
		if (empty ( static::$hive_cols ))
		{
			$hive_schema_fn = self::$cache_dir . "/{$TABLE}-schema.sql";
			$hive_schema = file_get_contents ( $hive_schema_fn );
			// extract column names
			preg_match ( "/\((\b*\,?\b*\`?\w\`\b*\w)\)/", $subject );
		}
		
		$ct_0 = count ( static::$hive_cols );
		$ct_1 = count ( $row );
		if ($ct_0 !== $ct_1)
		{
			$msg = "row column num:{$ct_0} not match hive table schema column num:{$ct_1}, exit 1, row columns:" . var_export ( $row, true ) . ", hive_cols:" . var_export ( static::$hive_cols, true );
			Log::log_step ( $msg, 'check_row', true );
			exit ( 1 );
		}
		$idx = 0;
		foreach ( $row as $k => $v )
		{
			if ($k !== static::$hive_cols [$idx])
			{
				$msg = "row column:{$k} not match hive table schema column:{static::$hive_cols[$idx]}, exit 1, row columns:" . var_export ( $row, true ) . ", hive_cols:" . var_export ( static::$hive_cols, true );
				Log::log_step ( $msg, 'check_row', true );
				exit ( 1 );
			}
			$idx ++;
		}
	}
	
	static protected function id_end()
	{
		global $TABLE_AUTO_INCREMENT_ID;
		global $TABLE;
		
		$ID_END = null;
		try
		{
			if (empty ( $TABLE_AUTO_INCREMENT_ID ))
			{
				$sql = "SELECT COUNT(*) FROM `{$TABLE}`";
				
				$rs = static::$dbh->query($sql);
				$ID_END = $rs->fetchColumn();
				
				$msg = "TABLE_AUTO_INCREMENT_ID is null, ID_END:{$ID_END}";
				Log::log_step ( $msg, 'id_end' );
			} else
			{
				$sql = "SELECT MAX(`{$TABLE_AUTO_INCREMENT_ID}`) FROM `{$TABLE}`";
				
				$rs = static::$dbh->query ( $sql );
				$ID_END = $rs->fetchColumn ();
				if ($ID_END === null)
				{
					$ID_END = 0;
					$msg = "empty table:{$TABLE}, set ID_END=0, sql:{$sql}";
					Log::log_step ( $msg, 'id_end' );
				}
				
				$msg = "ID_START:{$ID_START} is selected, sql:{$sql}";
				Log::log_step ( $msg, 'id_end' );
			}
		} catch ( \Exception $e )
		{
			$msg = "failed to query ID_END, exit 1, sql:{$sql}..." . $e->getMessage ();
			Log::log_step ( $msg, 'id_end', true );
			exit ( 1 );
		}
		
		return $ID_END;
	}
	
	static protected function id_start()
	{
		global $TABLE;
		global $TABLE_AUTO_INCREMENT_ID;
		
		$exportedId_fn = static::$log_dir . $TABLE . '-exportedId.log';
		$file_str = @file_get_contents ( $exportedId_fn );
		$lines = explode ( "\n", $file_str );
		$lines_ct = count ( $lines );
		//parse last 5 lines
		for($i = $lines_ct - 1; $i >= $lines_ct - 5 && $i >= 0; $i --)
		{
			$line = $lines [$i];
			preg_match ( '/.+id<(\d+)/', $line, $matches );
			if (isset ( $matches [1] ) && $matches [1] > $ID_START)
			{
				$ID_START = $matches [1];
				$msg = "ID_START:{$ID_START} is parsed in line:{$line}";
				Log::log_step ( $msg, 'id_start' );
				return $ID_START;
			}
		}

		//first time backup
		if(empty($TABLE_AUTO_INCREMENT_ID))
		{
			$ID_START=0;
			$msg = 'TABLE_AUTO_INCREMENT_ID is null, set ID_START=0';
			Log::log_step ( $msg, 'id_start' );
			return $ID_START;
		}else
		{
			$sql="SELECT MIN(`{$TABLE_AUTO_INCREMENT_ID}`) FROM `{$TABLE}`";
			try 
			{
				$rs = static::$dbh->query($sql);
				$ID_START = $rs->fetchColumn();
				if($ID_START===null)
				{
					$ID_START=0;
					$msg="empty table:{$TABLE}, set ID_START=0, sql:{$sql}";
					Log::log_step($msg, 'id_start');
				}
				
				$msg = "ID_START:{$ID_START} is selected, sql:{$sql}";
				Log::log_step ( $msg, 'id_start' );
				return $ID_START;
			}catch (\Exception $e)
			{
				$msg="failed to select min id, sql:{$sql}..." . $e->getMessage();
				Log::log_step($msg, 'id_start', true);
				exit(1);
			}
		}
	}
	static protected function flushToHive($create_date_partition_lt = null)
	{
		$TABLE = TABLE;
		$text_files = glob ( __DIR__ . "/data/{$TABLE}_*" );
		if (! empty ( $create_date_partition_lt )) // 姣旀寚瀹氭棩鏈熷皬鐨勬枃浠舵墠瀵煎叆hive
		{
			$text_files_new = [ ];
			foreach ( $text_files as $k => $v )
			{
				$v_base = basename ( $v );
				$create_date_partition = substr ( $v_base, strlen ( $TABLE . '_' ) );
				if (strlen ( $create_date_partition ) > 0 && $create_date_partition < $create_date_partition_lt)
				{
					$text_files_new [] = $v;
				}
			}
			$text_files = $text_files_new;
		}
		
		foreach ( $text_files as $fn )
		{
			// 瀵煎叆hive鐨則b_hlf_order_text琛�
			$o = null;
			$r = null;
			$v_base = basename ( $fn );
			$create_date_partition = substr ( $v_base, strlen ( $TABLE . '_' ) );
			$HIVE_TABLE = HIVE_TABLE;
			$sql = <<<EOL
load data local inpath '{$fn}' into table {$HIVE_TABLE} partition ( create_date_partition='{$create_date_partition}');
EOL;
			file_put_contents ( __DIR__ . "/data/sql_{$TABLE}", $sql );
			$exec_str = "hive -f " . __DIR__ . "/data/sql_{$TABLE}";
			
			Log::log_step ( "fn:{$fn}", "flushToHive" );
			
			exec ( $exec_str, $o, $r );
			if ($r !== 0)
			{
				$msg = var_export ( $o, true );
				Log::log_step ( $msg, 'flushToHive_error', true );
				exit ( 'flushToHive_error' );
			}
			unlink ( $fn );
		}
	}
	static protected function exportToText(Array $rows_new)
	{
		if (count ( $rows_new ) === 0)
		{
			return;
		}

		$buffer = '';
		$__PARTITIONS=null;
		foreach ( $rows_new as $k => $row )
		{
			$__PARTITIONS_STR_tmp='';
			if(!empty($row['__PARTITIONS']))
			{
				
			}
			$kk_tmp = 0;
			foreach ( $row as $kk => $vv )
			{
				if ($kk_tmp !== 0)
				{
					$buffer .= "\001";
				} else
				{
					$kk_tmp ++;
				}
				if (is_null ( $vv ))
				{
					$buffer .= "\N"; 
				} else
				{
					$vv_tmp = str_replace ( [ 
							"\n",
							"'",
							"\001" 
					], [ 
							"",
							"\'",
							"" 
					], $vv );
					$$buffertext .= $vv_tmp;
				}
			}
			$buffer .= "\n";
		}
		$text_sz = strlen ( $text );
		$rows_new_ct = count ( $rows_new );
		Log::log_step ( "rows_new_ct:{$rows_new_ct}, create_date_partition:{$create_date_partition}, text_sz:{$text_sz}", 'exportToText' );
		
		$fn = __DIR__ . "/data/" . TABLE . "_{$create_date_partition}";
		file_put_contents ( $fn, $text, FILE_APPEND );
	}
	static protected function controller_create()
	{
		global $TABLE;
		global $HIVE_DB;
		global $HIVE_TABLE;
		global $argv;
		global $HIVE_FORMAT;
		global $HIVE_PARTITION;
		
		$msg = "create hive table:{$HIVE_TABLE}, this will drop old hive table:{$HIVE_TABLE} and  delete all old {$TABLE}'s cache & log files.\ntype (Y/y) for yes, others for no.";
		Log::log_step($msg, 'controller_create');
		$type = fgets ( STDIN );
		if (substr($type, 0, 1) === 'Y' || substr($type, 0, 1) === 'y')
		{
			// delete cache&log
			$hive_table_log = self::$log_dir . "{$TABLE}-*";
			$hive_table_log_files = glob ( $hive_table_log );
			$hive_table_cache = self::$cache_dir . "{$TABLE}-*";
			$hive_table_cache_files = glob ( $hive_table_cache );
			$files = array_merge ( $hive_table_log_files, $hive_table_cache_files );
			$files_text = implode ( "\n", $files );
			foreach ( $files as $file )
			{
				@unlink ( $file );
			}
			$msg="cache & log files deleted:{$files_text}";
			Log::log_step($msg, 'controller_create' );
			
			// DROP hive table
			$o = null;
			$r = null;
			$exec_str = "hive -e 'USE `{$HIVE_DB}`; DROP TABLE IF EXISTS `{$HIVE_TABLE}`; DROP TABLE IF EXISTS `{$HIVE_TABLE}__tmp`' 2>&1";
			exec ( $exec_str, $o, $r );
			if ($r !== 0)
			{
				$o_text = implode ( "\n", $o );
				$msg = "unknow error, exit 1, exec_str:{$exec_str}, exec output:{$o_text}";
				Log::log_step ( $msg, 'controller_delete', true );
				exit ( 1 );
			}

			$msg = "hive table:{$HIVE_TABLE} dropped...";
			Log::log_step ( $msg, 'controller_create');

		} else
		{
			$msg = "typed:{$type} for no, exit..";
			Log::log_step ( $msg );
			exit ( 0 );
		}

		// prepare hive table schema file
		$msg = "generating hive table schema of {$TABLE}...";
		Log::log_step ( $msg );
		// https://stackoverflow.com/questions/5428262/php-pdo-get-the-columns-name-of-a-table
		$sql = "SELECT * from {$TABLE} LIMIT 1";
		$columns_name = [ ];
		$colmuns_pdo_type = [ ];
		try
		{
			$rs = static::$dbh->query ( $sql );
			for($i = 0; $i < $rs->columnCount (); $i ++)
			{
				$col = $rs->getColumnMeta ( $i );
				$columns_name [] = $col ['name'];
				$colmuns_pdo_type [] = $col ['pdo_type'];
			}
		} catch ( \Exception $e )
		{
			$msg = "pdo error, sql:{$sql}, exit 1..." . $e->getMessage ();
			Log::log_step ( $msg, 'controller_create', true );
			exit ( 1 );
		}
		
		if (empty ( $columns_name ))
		{
			$msg = "empty column returned, sql:{$sql}, exit 1";
			Log::log_step ( $msg, 'controller_create', true );
			exit ( 1 );
		}
		
		$columns_str = '';
		foreach ( $columns_name as $k => $name )
		{
			if ($k !== 0)
			{
				$columns_str .= ",\n";
			}
			// map pdo_type to hive type
			// PDO has only 3 data types, sett http://php.net/manual/en/pdo.constants.php
			$pdo_type = $colmuns_pdo_type [$k];
			$hive_type = '';
			if ($pdo_type === PDO::PARAM_BOOL)
			{
				$hive_type = ' BOOLEAN';
			} else if ($pdo_type === PDO::PARAM_INT)
			{
				$hive_type = ' INT';
			} else // NO FLOAT, DECIMAL, TIMESTAMP, BINARY
			{
				$hive_type = ' STRING';
			}
			
			$columns_str .= "`{$name}` {$hive_type}";
		}
		
		$hive_format_str = empty ( $HIVE_FORMAT ) ? 'TEXTFILE' : strtoupper ( $HIVE_FORMAT );
		$partition_str = $HIVE_PARTITION === null ? '' : 'PARTITIONED BY (`partition` string)';
		
		$hive_schema_template = <<<EOL
USE {$HIVE_DB};
CREATE TABLE {$HIVE_TABLE} (
{$columns_str}
)
{$partition_str}
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS {$hive_format_str};
EOL;
		if ($hive_format_str !== 'TEXTFILE') // 如果不是TEXTFILE的话就需要创建一个TEXTFILE的tmp表
		{
			$hive_schema_template .= <<<EOL
			
CREATE TABLE {$HIVE_TABLE}__tmp (
{$columns_str}
)
{$partition_str}
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
EOL;
		}
		
		$hive_schema_fn = self::$cache_dir . "/{$TABLE}-schema.sql";
		file_put_contents ( $hive_schema_fn, $hive_schema_template );
		
		$msg = "hive table schema generated:{$hive_schema_fn}, change it if you need.\nuse {$hive_schema_fn} to create hive table?\ntype (Y/y) for yes, others for no.";
		Log::log_step ( $msg );
		
		$type = fgets ( STDIN );
		if (substr($type, 0, 1) === 'Y' || substr($type, 0, 1) === 'y')
		{
			$o = null;
			$r = null;
			$exec_str = "hive -f {$hive_schema_fn} 2>&1";
			exec ( $exec_str, $o, $r );
			if ($r !== 0)
			{
				$o_text = implode ( "\n", $o );
				$msg = "HIVE_TABLE:{$HIVE_TABLE} create failed, exit 1";
				Log::log_step ( $msg, 'controller_create', true );
				$msg = "exec_str:{$exec_str}, exec output:{$o_text}";
				Log::log_step ( $msg, 'controller_create', true );
				exit(1);
			} else
			{
				$msg = "HIVE_TABLE:{$HIVE_TABLE} created";
				Log::log_step ( $msg );
			}
		} else
		{
			$msg = "typed:{$type} for no, exit 0";
			Log::log_step ( $msg );
			exit ( 0 );
		}
		
		$msg = "create done, use `php {$argv[0]} backup` to backup to hive, you can add it to cron.sh for daily backup";
		Log::log_step ( $msg );
	}
	static protected function check_enter_pressed()
	{
		$read = [ 
				STDIN 
		];
		$write = [ ];
		$except = [ ];
		$result = stream_select ( $read, $write, $except, 0 );
		if ($result === false)
			throw new Exception ( 'stream_select failed' );
		if ($result === 0)
			return false;
		$data = stream_get_line ( $fd, 1 );
		if (strpos ( $data, "\n" ) !== false)
		{
			return true;
		}
	}
	static protected function controller_backup()
	{
		global $TABLE;
		global $TABLE_AUTO_INCREMENT_COLUMN;
		global $HIVE_PARTITION;
		global $ROW_CALLBACK;
		global $TABLE_BATCH;
		
		$ID_START = static::id_start ();
		$ID_END = static::id_end ();
		$ID=$ID_START;
		try 
		{
		while ( true )
		{
			// if ENTER is pressed, stop backup
			$enter_pressed = static::check_enter_pressed ();
			if ($enter_pressed)
			{
				$msg = "enter is pressed, stopping backup...";
				Log::log_step ( $msg, 'enter_pressed' );
				break;
			}
			
			if($ID>=$ID_END)
			{
				$msg = "ID:{$ID} >= ID_END:{$ID_END}, complete";
				Log::log_step ( $msg, 'complete' );
			}
			
			$mem_sz = memory_get_usage ();
			$mem_sz_pk = memory_get_peak_usage ();
			$BATCH=empty($TABLE_BATCH)?1000:$TABLE_BATCH;
			$msg = "ID:{$ID}, BATCH:{$BATCH}, mem_sz:{$mem_sz}, mem_sz_pk:{$mem_sz_pk}";
			Log::log_step ( $msg );
			
			
			$sql=null;
			if(empty($TABLE_AUTO_INCREMENT_COLUMN))
			{
				$sql = "SELECT * FROM `{$TABLE}` LIMIT {$ID}, {$BATCH}";
			}
			else
			{
				$bound = $ID + $BATCH;
				$sql = "SELECT * FROM `{$TABLE}` WHERE `{$TABLE_AUTO_INCREMENT_COLUMN}`>={$ID} AND `{$TABLE_AUTO_INCREMENT_COLUMN}`<{$bound}";
			}
			$rs = static::$dbh->query($sql);
			$rows = $rs->fetchAll(PDO::FETCH_ASSOC);
			
			if (count ( $rows > 0 ))
			{
				$rows_new = [ ];
				//分区
				$__PARTITIONS = '';
				foreach ( $rows as $row )
				{
					if(!empty($ROW_CALLBACK_PARTITION))
					{
						$__PARTITIONS='';
						$idx=0;
						foreach ($ROW_CALLBACK_PARTITION as $partition_name => $callback)
						{

							if($idx!==0)
							{
								$__PARTITIONS .= ",";
							}
							$idx++;
							if($callback instanceof \Closure)
							{
								$__PARTITIONS = "{$partition_name}=" . $callback($row);
							}else
							{
								$__PARTITIONS = "{$partition_name}=" . $callback;
							}
						}
					}
					
					//处理行使之和hive格式一致
					if(!empty($ROW_CALLBACK_CHANGE))
					{
						$row=$ROW_CALLBACK_CHANGE($row);
					}
					
					if(!empty($__PARTITIONS))
					{
						$row['__PARTITIONS']=$__PARTITIONS;
					}
					
					$same_as_hive = static::check_row($row);
					if(!$same_as_hive)
					{
						$msg = "check_row failed, row format is different from hive table, exit 1, row:" . var_export($row, true);
						Log::log_step ( $msg, 'check_row', true );
						exit(1);
					}
					
					$rows_new[]=$row;
				}
				static::exportToText ( $rows_new);
			}
			
			$ID += $BATCH;
			$rs = null;
			$rows = null;
		}
		}catch(\Exception $e)
		{
			$msg = "PDO Exception:" . $e->getMessage();
			Log::log_step ( $msg, 'pdo', true );
			exit(1);
		}finally 
		{
			static::flushToHive();
		}
	}
	static public function run()
	{
		static::init ();
		
		global $argv;
		$supported_arguments = [ 
				'create',
				'backup'
		];
		$arg = empty ( $argv [1] ) ? 'empty' : $argv [1];
		if (! in_array ( $arg,  $supported_arguments))
		{
			$msg = <<<EOL
{$arg} is not supported argument:
create: generate hive table schema and create it
backup: backup to hive
EOL;
			Log::log_step ( $msg, 'run', true );
			exit ( 1 );
		}
		
		if ($arg === $supported_arguments [0])
		{
			static::controller_create ();
		} else if ($arg === $supported_arguments [1])
		{
			static::controller_backup ();
		} else
		{
			$msg = "{$arg} not supported, exit 1";
			Log::log_step ( $msg );
			exit ( 1 );
		}
	}
}

// 简单的Log类
class Log
{
	const LOG_MAX = 8 * 1204 * 1024; // 8M
	protected static $start = null;
	protected static $log = null;
	private static $log_dir = null;
	private static $app = null;
	static public function setting($app = 'table')
	{
		global $WORK_DIR;
		
		self::$start = time ();
		
		self::$log_dir = $WORK_DIR . "/log/";
		if (! file_exists ( self::$log_dir ))
		{
			if (! mkdir ( self::$log_dir, 0777, true ))
			{
				$msg = "Failed to create folder:" . self::$log_dir;
				$fh = fopen ( 'php://stderr', 'a' );
				fwrite ( $fh, $msg );
				fclose ( $fh );
				exit ( 1 );
			}
		}
		
		$tz = date_default_timezone_get ();
		if ($tz === "UTC") // 为设置时区
		{
			date_default_timezone_set ( 'Asia/Shanghai' );
		}
		
		self::$app = $app;
	}
	static protected function log_file($str, $cate = null)
	{
		$fn = null;
		if (empty ( $cate ))
		{
			$fn = self::$log_dir . self::$app . "-all.log";
		} else
		{
			$fn = self::$log_dir . self::$app . "-{$cate}.log";
		}
		
		file_put_contents ( $fn, $str, FILE_APPEND );
		
		clearstatcache ();
		$filesize = filesize ( $fn );
		if ($filesize > self::LOG_MAX)
		{
			
			$old_fn = str_replace ( '.log', ".old.log", $fn );
			@unlink ( $old_fn );
			rename ( $fn, $old_fn );
			$now = time ();
			$msg = date ( 'Y-m-d H:i:s', $now ) . " [], rotate log file, filesize:{$filesize}\r\n";
			file_put_contents ( $fn, $msg, FILE_APPEND );
		}
	}
	static public function log_step($message, $cate = null, $stderr = false)
	{
		if (empty ( self::$start ))
		{
			self::setting ();
		}
		$now = time ();
		$str = date ( 'Y-m-d H:i:s', $now ) . " [$cate] {$message}\r\n";
		if ($stderr === false)
		{
			echo $str;
		} else
		{
			$fh = fopen ( 'php://stderr', 'a' );
			fwrite ( $fh, $str );
			fclose ( $fh );
		}
		self::log_file ( $str );
		if (! empty ( $cate ))
		{
			self::log_file ( $str, $cate );
		}
	}
}

