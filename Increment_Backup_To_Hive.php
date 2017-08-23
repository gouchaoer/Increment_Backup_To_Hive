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
		
		$CONFIG_PATH = $WORK_DIR . "config.ini";
		self::$config_arr = parse_ini_file ( $CONFIG_PATH );
		if (empty ( self::$config_arr ))
		{
			$msg = "read config error:{$CONFIG_PATH}, exit 1";
			Log::log_step ( $msg, 'init', true );
			exit ( 1 );
		}
		
		self::$cache_dir = $WORK_DIR . "/cache/";
		if (! file_exists ( self::$cache_dir ))
		{
			if (! mkdir ( self::$cache_dir, 0777, true ))
			{
				$msg = "Failed to create folder:{self::$cache_dir}, exit 1";
				Log::log_step ( $msg, 'init', true );
				exit ( 1 );
			}
		}
		
		self::$log_dir = $WORK_DIR . "/log/";
		if (! file_exists ( self::$log_dir ))
		{
			if (! mkdir ( self::$log_dir, 0777, true ))
			{
				$msg = "Failed to create folder:{self::$log_dir}, exit 1";
				Log::log_step ( $msg, 'init', true );
				exit ( 1 );
			}
		}
		
		$running_lock = self::$cache_dir . "{$TABLE}_running.pid";
		$running_lock_content = @file_get_contents ( $running_lock );
		if (! empty ( $running_lock_content ))
		{
			$pieces = explode ( "|", $running_lock_content );
			$pid_old = $pieces [1];
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
	
	// 检查$row的格式和hive建表语句是否一致
	static protected function check_row(Array $row)
	{
	}
	
	// 灏濊瘯鑾峰彇涓�鏄熸湡閽辩殑ID
	static protected function id_end($id_max)
	{
		$ID_END = null;
		
		$now = time ();
		$week_ago = $now - 7 * 24 * 3600;
		$now_date = date ( "Ymd", $now );
		$week_ago_date = date ( "Ymd", $week_ago );
		// log浠婂ぉ鐨刬d_max鍊�
		$msg = "{$now_date}:{$id_max}";
		Log::log_step ( $msg, 'id_max' );
		
		$id_max_fp = __DIR__ . '/log/' . TABLE . '-id_max.log'; // 鍏堜粠log鏂囦欢涓璸arse
		$file_str = @file_get_contents ( $id_max_fp );
		$lines = explode ( "\n", $file_str );
		$lines_ct = count ( $lines );
		for($i = 0; $i < $lines_ct; $i ++)
		{
			$line = $lines [$i];
			preg_match ( "/{$week_ago_date}:(\d+)/", $line, $matches );
			if (isset ( $matches [1] ))
			{
				$ID_END = $matches [1];
				Log::log_step ( "ID_END:{$ID_END} of {$week_ago_date} parsed in {$id_max_fp}", 'id_end' );
				return $ID_END;
			}
		}
		
		$msg = "ID_END of {$week_ago_date} not found in {$id_max_fp}, set it to id_max:{$id_max}";
		Log::log_step ( $msg, 'id_end' );
		$ID_END = $id_max;
		return $ID_END;
	}
	
	// 灏濊瘯鑾峰彇琛↖D_START鐨勫嚱鏁�
	static protected function id_start()
	{
		$exportToText_id_fp = __DIR__ . '/log/' . TABLE . '-exportToText_id.log'; // 鍏堜粠log鏂囦欢涓璸arse
		
		$ID_START = null;
		$file_str = @file_get_contents ( $exportToText_id_fp );
		$lines = explode ( "\n", $file_str );
		$lines_ct = count ( $lines );
		for($i = $lines_ct - 1; $i >= $lines_ct - 5 && $i >= 0; $i --) // 鍙鍊掓暟5琛�
		{
			$line = $lines [$i];
			preg_match ( '/.+id<(\d+)/', $line, $matches );
			if (isset ( $matches [1] ) && $matches [1] > $ID_START)
			{
				$ID_START = $matches [1];
			}
		}
		if ($ID_START !== null)
		{
			$msg = "ID_START:{$ID_START} is parsed in last line of {$exportToText_id_fp}";
			Log::log_step ( $msg, 'id_start' );
			return $ID_START;
		} else // 娌arse鍒板垯闇�瑕佹墜鍔ㄨ緭鍏�
		{
			$msg = "No id found in last line of {$exportToText_id_fp}.\nThis must be the first execution of this script, create hive table if not exits:" . HIVE_TABLE . "\n\n";
			Log::log_step ( $msg, "hiveTableCreate" );
			// 鍒涘缓hive琛�
			$sql_table_fn = __DIR__ . "/" . TABLE . ".sql";
			if (! file_exists ( $sql_table_fn ))
			{
				$msg = "sql_table_fn:{$sql_table_fn} not found, exit!";
				Log::log_step ( $msg, 'sql_table_fn_error', true );
				exit ();
			}
			$o = null;
			$r = null;
			$exec_str = "hive -e 'use {$sql_table_fn}'";
			exec ( $exec_str, $o, $r );
			if ($r !== 0)
			{
				$msg = var_export ( $o, true );
				Log::log_step ( $msg, 'sql_table_fn_error', true );
				exit ();
			}
			Log::log_step ( "create hive table if not exits done...sql_table_fn:{$sql_table_fn}", "hiveTableCreate" );
			
			$TIME_OUT = 60;
			$msg = "input ID_START(timeout:{$TIME_OUT}s):\n";
			Log::log_step ( $msg, 'id_start' );
			
			$read = array (
					STDIN 
			);
			$write = NULL;
			$except = NULL;
			$num_changed_streams = stream_select ( $read, $write, $except, $TIME_OUT ); // 300s瓒呮椂
			if (false === $num_changed_streams)
			{
				Log::log_step ( "stream_select error", 'id_start', true );
				exit ();
			} elseif ($num_changed_streams > 0)
			{
				$arg = trim ( fgets ( STDIN ) );
				$ID_START = intval ( $arg );
				
				Log::log_step ( "ID_START is set:{$ID_START}\nplease make sure the hive table:" . HIVE_TABLE . " is empty and files with prefix:" . TABLE . " in data/* is deleted...\ntype 'yes' if you are sure(initialization done, add this script to cron for everyday update), otherwise exit if you are not sure(abandon initialization)", 'start' );
				$arg = trim ( fgets ( STDIN ) );
				if ($arg !== 'yes')
				{
					Log::log_step ( "typed {$arg}, exit", 'id_start' );
					exit ();
				} else
				{
					Log::log_step ( "typed {$arg}, continue", 'id_start' );
				}
			} else
			{
				Log::log_step ( "stream_select timeout:{$TIME_OUT},exit", 'id_start', true );
				exit ();
			}
		}
		return $ID_START;
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
	static protected function exportToText(Array $rows_new, string $create_date_partition)
	{
		if (count ( $rows_new ) === 0)
		{
			return;
		}
		// 鏋勯�爃ive鏍煎紡琛�
		$text = '';
		$text_sz = null;
		foreach ( $rows_new as $k => $v )
		{
			$kk_tmp = 0;
			
			foreach ( $v as $kk => $vv )
			{
				if ($kk_tmp !== 0)
				{
					$text .= "\001"; // hive琛ㄥ垎闅旂
				} else
				{
					$kk_tmp ++;
				}
				
				// 杩囨护鎺夌壒娈婂瓧绗�
				if (is_null ( $vv ))
				{
					$text .= "\N"; // hive鐨刵ull
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
					$text .= $vv_tmp;
				}
			}
			$text .= "\n"; // hive鍙敮鎸乗n鎹㈣
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
		
		// check if hive table exists
		$o = null;
		$r = null;
		$exec_str = "hive -e 'CREATE DATABASE IF NOT EXISTS `{$HIVE_DB}`; USE `{$HIVE_DB}`; create table `{$HIVE_TABLE}`(`test` string); drop table `{$HIVE_TABLE}`' 2>&1";
		exec ( $exec_str, $o, $r );
		if ($r !== 0)
		{
			$o_text = implode ( "\n", $o );
			if (stripos ( $o_text, 'already exists' ) !== false)
			{
				$msg = "HIVE_TABLE:{$HIVE_TABLE} already exists in HIVE_DB:{$HIVE_DB}, you must delete it first, use `php {$argv[0]} delete`, exit 1";
				Log::log_step ( $msg, 'controller_create', true );
				$msg = "exec_str:{$exec_str}, exec output:{$o_text}";
				Log::log_step ( $msg, 'controller_create', true );
				exit ( 1 );
			} else
			{
				$msg = "unknow error, exit 1, exec_str:{$exec_str}, exec output:{$o_text}";
				Log::log_step ( $msg, 'controller_create', true );
				exit ( 1 );
			}
		}
		// check if hive table log & cache exists
		$hive_table_log = self::$log_dir . "{$TABLE}-*";
		$hive_table_log_files = glob ( $hive_table_log );
		$hive_table_cache = self::$cache_dir . "{$TABLE}-*";
		$hive_table_cache_files = glob ( $hive_table_cache );
		$files = array_merge ( $hive_table_log_files, $hive_table_cache_files );
		if (! empty ( $files ))
		{
			$msg = "TABLE:{$TABLE} log & cache exists, you must delete it first, use `php {$argv[0]} delete`, exit 1";
			Log::log_step ( $msg, 'controller_create', true );
			$files_text = implode ( "\n", $files );
			$msg = "log & cache files:{$files_text}";
			Log::log_step ( $msg, 'controller_create', true );
			exit ( 1 );
		}
		
		// prepare hive table schema file
		$msg = "generating hive table schema from data source...";
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
				$hive_type = 'BOOLEAN';
			} else if ($pdo_type === PDO::PARAM_INT)
			{
				$hive_type = 'INT';
			} else // NO FLOAT, DECIMAL, TIMESTAMP, BINARY
			{
				$hive_type = 'STRING';
			}
			
			$columns_str .= "`{$name}` {$hive_type}";
		}
		
		$hive_format_str = empty ( $HIVE_FORMAT ) ? 'TEXTFILE' :  strtoupper($HIVE_FORMAT);
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
		if($hive_format_str!=='TEXTFILE')//如果不是TEXTFILE的话就需要创建一个TEXTFILE的tmp表
		{
			$hive_schema_template .=<<<EOL
			
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

		$hive_schema_fn = "{self::$cache_dir}{$TABLE}-schema.sql";
		file_put_contents ( $hive_schema_fn, $hive_schema_template );
		
		$msg = "hive table schema generated:{$hive_schema_fn}, change it if you need.\nuse {$hive_schema_fn} to create hive table?\ntype (Y/y) for yes, others for no.";
		Log::log_step ( $msg );
		
		$type = fgets ( STDIN );
		if ($type === 'Y' || $type === 'y')
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
				$msg = "\n\nexec output:\n{$o_text}";
				Log::log_step ( $msg, 'controller_create', true );
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
		
		// Backup DB_BATCH data to hive
		$DB_BATCH = empty ( self::$config_arr ['DB_BATCH'] ) ? 1000 : self::$config_arr ['DB_BATCH'];
		$msg = <<<EOL
		create done, use `php {$argv[0]} backup` to backup to hive, you can add it to cron.sh for daily backup
EOL;
		Log::log_step ( $msg );
	}
	
	static protected function check_enter_pressed()
	{
		$read = [STDIN];
		$write = [];
		$except = [];
		$result = stream_select($read, $write, $except, 0);
		if($result === false) 
			throw new Exception('stream_select failed');
		if($result === 0) 
			return false;
		$data = stream_get_line($fd, 1);
		if(strpos($data, "\n")!==false)
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
		
		$ID_START = static::id_start ();
		$ID_END = static::id_end ();
		
		while ( true )
		{
			//if ENTER is pressed, stop backup
			$enter_pressed = static::check_enter_pressed();
			if($enter_pressed)
			{
				$msg = "enter is pressed, stopping backup...";
				Log::log_step ( $msg, 'controller_backup' );
				
				$msg = 'finally, flush data into hive';
				Log::log_step ( $msg );
				Util::flushToHive ();
			}
			
			$mem_sz = memory_get_usage ();
			$mem_sz_pk = memory_get_peak_usage ();
			$msg = "ID:{$ID},ID_BATCH:{$CONFIG_ARR['ID_BATCH']}, mem_sz:{$mem_sz}, mem_sz_pk:{$mem_sz_pk}";
			Log::log_step ( $msg );
			
			$ID2 = $ID + $CONFIG_ARR ['ID_BATCH'];
			if ($ID2 > $ID_END)
				$ID2 = $ID_END + 1;
			if ($ID > $ID_END)
			{
				$msg = "ID:{$ID} > ID_END:{$ID_END}, complete!";
				Log::log_step ( $msg, 'complete' );
				
				$msg = 'finally, flushToHive() for the newest day';
				Log::log_step ( $msg );
				Util::flushToHive ();
				
				break;
			}
			
			$result = $mysqli->query ( "SELECT * from " . TABLE . " where ID>={$ID} and ID<{$ID2} order by CREATE_TIME" );
			if (! $result)
			{
				$msg = $mysqli->error;
				Log::log_step ( $msg, 'mysqli_error' );
				exit ();
			}
			$rows = $result->fetch_all ( MYSQLI_ASSOC );
			
			if (count ( $rows > 0 ))
			{
				$rows_new = [ ];
				
				$create_date_partition = '0000-00-00';
				
				foreach ( $rows as $row )
				{
					$row_new = $row;
					
					$create_date = $row ['CREATE_TIME'];
					$create_date_partition_tmp = substr ( $create_date, 0, 7 ); // 只取月份
					if ($create_date_partition === '0000-00-00' && ! empty ( $create_date_partition_tmp ))
					{
						$create_date_partition = $create_date_partition_tmp;
					}
					
					if (! empty ( $create_date_partition_tmp ) && $create_date_partition != $create_date_partition_tmp) // 跳月份了
					{
						Log::log_step ( "jump_date:{$create_date_partition_tmp}" );
						Util::exportToText ( $rows_new, $create_date_partition );
						$rows_new = [ ];
						$create_date_partition = $create_date_partition_tmp;
					}
					$rows_new [] = $row_new;
				}
				Util::exportToText ( $rows_new, $create_date_partition );
				$rows_new = [ ];
				// 处理完毕，记录ID
				$msg = "exportToText_id:id>={$ID} and id<{$ID2}";
				Log::log_step ( $msg, 'exportToText_id' );
				
				// Util::flushToHive ( $create_date_partition ); // 把$create_date_partition日期之前的都导入hive，这样就尽可能保证所有日期的数据只导入一次
			}
			
			$ID += $CONFIG_ARR ['ID_BATCH'];
			$rs = null;
			$rows = null;
		}
	}
	static protected function controller_delete()
	{
		global $HIVE_DB;
		global $HIVE_TABLE;
		
		// delete cache&log
		$hive_table_log = self::$log_dir . "{$TABLE}-*";
		$hive_table_log_files = glob ( $hive_table_log );
		$hive_table_cache = self::$cache_dir . "{$TABLE}-*";
		$hive_table_cache_files = glob ( $hive_table_cache );
		$files = array_merge ( $hive_table_log_files, $hive_table_cache_files );
		$files_text = implode ( "\n", $files );
		$msg = "do you want to drop hive table:{$HIVE_TABLE}, and delete all cache & log files:{$files_text}?\ntype (Y/y) for yes, others for no.";
		$type = fgets ( STDIN );
		if ($type === 'Y' || $type === 'y')
		{
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
			// unlink files
			foreach ( $files as $file )
			{
				@unlink ( $file );
			}
			
			$msg = "delete done, exit 0...";
			Log::log_step ( $msg );
			exit ( 0 );
		} else
		{
			$msg = "typed:{$type} for no, exit..";
			Log::log_step ( $msg );
			exit ( 0 );
		}
	}
	static protected function run()
	{
		static::init ();
		
		global $argv;
		$supported_arguments = [ 
				'create',
				'backup',
				'delete' 
		];
		$arg = empty ( $argv [1] ) ? 'empty' : $argv [1];
		if (! in_array ( $argv [1], array_keys ( $supported_arguments ) ))
		{
			$msg = <<<EOL
			{$arg} is not supported argument, exit 1:
			create: generate hive table schema and create it
			backup: backup to hive
			delete: drop hive table, delete log, delete cache
EOL;
			Log::log_step ( $msg, 'run', true );
			exit ( 1 );
		}
		
		if ($arg === $supported_arguments [0])
		{
			static::controller_create ();
		} else if ($arg === $supported_arguments [1])
		{
			
		} else if ($arg === $supported_arguments [2])
		{
			
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
				$msg = "Failed to create folder:{self::$log_dir}, exit 1";
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

