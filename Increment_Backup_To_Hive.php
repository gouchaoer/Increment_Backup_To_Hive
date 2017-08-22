<?php

class Increment_Backup_To_Hive
{
	static function init()
	{
		global $TABLE;
		ini_set ( 'memory_limit', - 1 );
		set_time_limit ( 0 );
		Log::setting ( $TABLE . '.log' );
		
		$running_lock = __DIR__ . "/" . TABLE . "_running.pid";
		$running_lock_content = @file_get_contents ( $running_lock );
		if (! empty ( $running_lock_content ))
		{
			$pieces = explode("|", $running_lock_content);
			$pid_old = $pieces[1];
			if(file_exists("/proc/{$pid_old}"))
			{
				$msg = "running_lock:{$running_lock} exist, running_lock_content:{$running_lock_content}, another program is running, exit";
				Log::log_step ( $msg, 'init', true );
				exit (1);
			}
			else
			{
				$msg = "running_lock:{$running_lock} exist, running_lock_content:{$running_lock_content}, another program unproperly exited, go on";
				Log::log_step($msg, 'init', true);
			}
		}
		
		$pid = getmypid();
		$running_lock_msg = date ( 'Y-m-d H:i:s', time () ) . "|{$pid}";
		file_put_contents ( $running_lock, $running_lock_msg );
		function shutdown()
		{
			global $running_lock;
			@unlink ( $running_lock );
			Log::log_step ( "unlink {$running_lock}", 'init' );
		}
		register_shutdown_function ( 'shutdown' );
	}
	
	// 灏濊瘯鑾峰彇涓�鏄熸湡閽辩殑ID
	static function id_end($id_max)
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
	static function id_start()
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
			$exec_str = "hive -f {$sql_table_fn}";
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
	static function flushToHive($create_date_partition_lt = null)
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
	static function exportToText(Array $rows_new, string $create_date_partition)
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
	
	static function run()
	{
		// 获取配置信息
		$CONFIG_PATH =  __DIR__ . "/ddd.ini";
		$CONFIG_ARR = parse_ini_file ($CONFIG_PATH );
		
		// 确定从哪个id开始
		$ID =Util::id_start();
		
		// 连接mysql
		$mysqli = new mysqli ( $CONFIG_ARR ['MYSQL_ADDR'], $CONFIG_ARR ['MYSQL_USER'], $CONFIG_ARR ['MYSQL_PASSWD'], $CONFIG_ARR ['MYSQL_DB'] );
		if ($mysqli->connect_errno)
		{
			$msg = "Connect failed:" . $mysqli->connect_error;
			Log::log_step ( $msg, 'mysqli_error' );
			exit ();
		}
		$result = $mysqli->query ( 'set names utf8' );
		if (! $result)
		{
			echo $mysqli->error;
			exit ();
		}
		
		// 确定从id最大值
		$result = $mysqli->query ( "select max(ID) from " . TABLE );
		if (! $result)
		{
			$msg = $mysqli->error;
			Log::log_step ( $msg, 'mysqli_error' );
			exit ();
		}
		$value = $result->fetch_array ( MYSQLI_NUM );
		$ID_MAX = is_array ( $value ) ? $value [0] : null;
		$ID_END = Util::id_end($ID_MAX);
		if (empty ( $ID_END ))
		{
			Log::log_step ( "ID_END is empty, error and exit!", 'id_end' );
			exit ();
		} else
		{
			Log::log_step ( "ID_END:{$ID_END}", 'id_end' );
		}
		
		// 开始批处理从mysql导出到hive
		while ( true )
		{
			static $CONFIG_STR = null;
			if (empty ( $CONFIG_STR ))
			{
				$CONFIG_STR = file_get_contents ( $CONFIG_PATH );
			}
			$CONFIG_STR_tmp = file_get_contents ( $CONFIG_PATH  );
			if ($CONFIG_STR_tmp !== $CONFIG_STR)
			{
				$msg =  "{$CONFIG_PATH} changed, exit!";
				Log::log_step ( $msg );
				exit ();
			}
		
			$mem_sz = memory_get_usage ();
			$mem_sz_pk = memory_get_peak_usage ();
			$msg = "ID:{$ID},ID_BATCH:{$CONFIG_ARR['ID_BATCH']}, mem_sz:{$mem_sz}, mem_sz_pk:{$mem_sz_pk}";
			Log::log_step ( $msg );
		
			$ID2 = $ID + $CONFIG_ARR ['ID_BATCH'];
			if($ID2>$ID_END)
				$ID2=$ID_END+1;
				if ($ID > $ID_END)
				{
					$msg = "ID:{$ID} > ID_END:{$ID_END}, complete!";
					Log::log_step ( $msg, 'complete' );
		
					$msg = 'finally, flushToHive() for the newest day';
					Log::log_step ( $msg );
					Util::flushToHive ();
		
					break;
				}
		
				$result = $mysqli->query ( "SELECT * from " . TABLE ." where ID>={$ID} and ID<{$ID2} order by CREATE_TIME" );
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
						$create_date_partition_tmp = substr ( $create_date, 0, 7 );//只取月份
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
		
					//Util::flushToHive ( $create_date_partition ); // 把$create_date_partition日期之前的都导入hive，这样就尽可能保证所有日期的数据只导入一次
				}
		
				$ID += $CONFIG_ARR ['ID_BATCH'];
				$rs = null;
				$rows = null;
		}
	}
}



// 简单的Log类
class Log
{
	const LOG_MAX = 8 * 1204 * 1024; // 8M
	protected static $start = null;
	protected static $log = null;
	static public function setting($fn = 'table.log')
	{
		global $WORK_DIR;
		self::$start = time ();
		$log_dir = $WORK_DIR . "/log/";
		if(!file_exists($log_dir))
		{
			if (!mkdir($structure, 0777, true)) 
			{
				$msg = "Failed to create folder:{$log_dir}, exit";
				$fh = fopen ( 'php://stderr', 'a' );
				fwrite ( $fh, $msg );
				fclose ( $fh );
				exit(1);
			}
		}
		self::$log = $log_dir. "{$fn}";
		$tz = date_default_timezone_get ();
		if ($tz === "UTC") //为设置时区
		{
			date_default_timezone_set ( 'Asia/Shanghai' );
		}
	}
	static protected function log_file($str, $cate = null)
	{
		$fn = null;
		if (empty ( $cate ))
		{
			$fn = self::$log;
		} else
		{
			$fn = str_replace ( '.log', "-{$cate}.log", self::$log );
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
		}
		else
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

