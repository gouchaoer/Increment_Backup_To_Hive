<?php

class Increment_Backup_To_Hive
{

    protected static $config_arr;

    protected static $data_dir;

    protected static $log_dir;

    protected static $dbh;

    static protected function init()
    {
        global $TABLE;
        global $WORK_DIR;
        
        ini_set('memory_limit', - 1);
        set_time_limit(0);
        
        Log::setting($TABLE);
        
        $config_path = $WORK_DIR . "/config.ini";
        static::$config_arr = parse_ini_file($config_path);
        if (empty(static::$config_arr)) {
            $msg = "read config error:{$config_path}, exit 1";
            Log::log_step($msg, 'init', true);
            exit(1);
        }
        
        static::$data_dir = $WORK_DIR . "/data/";
        if (! file_exists(static::$data_dir)) {
            if (! mkdir(static::$data_dir, 0777, true)) {
                $msg = "failed to create folder:" . static::$data_dir;
                Log::log_step($msg, 'init', true);
                exit(1);
            }
        }
        
        static::$log_dir = $WORK_DIR . "/log/";
        if (! file_exists(static::$log_dir)) {
            if (! mkdir(static::$log_dir, 0777, true)) {
                $msg = "failed to create folder:" . static::$log_dir;
                Log::log_step($msg, 'init', true);
                exit(1);
            }
        }
        
        $running_lock = static::$data_dir . "{$TABLE}-running.pid";
        $running_lock_content = @file_get_contents($running_lock);
        if (! empty($running_lock_content)) {
            $pieces = explode("|", $running_lock_content);
            $pid_old = empty($pieces[1]) ? - 1 : $pieces[1];
            if (file_exists("/proc/{$pid_old}")) {
                $msg = "running_lock:{$running_lock} exist, running_lock_content:{$running_lock_content}, another program is running, exit 1";
                Log::log_step($msg, 'init', true);
                exit(1);
            } else {
                $msg = "running_lock:{$running_lock} exist, running_lock_content:{$running_lock_content}, another program unproperly exited, go on";
                Log::log_step($msg, 'init', true);
            }
        }
        $pid = getmypid();
        $date_formated = date("Y-m-d H:i:s");
        file_put_contents($running_lock, "{$date_formated}|{$pid}");
        register_shutdown_function(function () use ($running_lock) {
            @unlink($running_lock);
            //Log::log_step("unlink {$running_lock}", 'init');
        });
        
        try {
            self::$dbh = new PDO(static::$config_arr['DB_DSN'], static::$config_arr['DB_USER'], static::$config_arr['DB_PASSWD']);
            self::$dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e) {
            $msg = "PDO Connection failed, exit 1... " . $e->getMessage();
            Log::log_step($msg);
            exit(1);
        }
    }

    // 读取建表语句，parse出列字段和分区字段
    protected static $hive_cols;
    protected static $hive_partitions;
    static protected function parse_hive_table_schema()
    {
        global $TABLE;
        global $ROW_CALLBACK_PARTITIONS;
        self::$hive_cols =[];
        self::$hive_partitions=[];
        
        $hive_schema_fn = static::$data_dir . "/{$TABLE}-schema.sql";
        $hive_schema = file_get_contents($hive_schema_fn);
        // extract $hive_cols
        preg_match("/CREATE TABLE\W+\w+\W*\(([^\)]+)\)/i", $hive_schema, $matches);
        if(empty($matches[1]))
        {
            $msg="failed to preg_match hive_cols in :{$hive_schema_fn}";
            Log::log_step($msg, 'parse_hive_table_schema', true);
            exit(1);
        }
        $cols_arr = explode(",", $matches[1]);
        foreach ($cols_arr as $col)
        {
            preg_match("/\W*(\w+)\W+/i", $col, $matches);
            if(empty($matches[1]))
            {
                $msg="failed to preg_match column name in col:{$col}";
                Log::log_step($msg, 'parse_hive_table_schema', true);
                exit(1);
            }
            self::$hive_cols[]=$matches[1];
        }
        // extract $hive_partitions
        if(!empty($ROW_CALLBACK_PARTITIONS))
        {
            preg_match("/PARTITIONED BY\W*\(([^\)]+)\)/i", $hive_schema, $matches);
            if(empty($matches[1]))
            {
                $msg="failed to preg_match hive_partitions in :{$hive_schema_fn}";
                Log::log_step($msg, 'parse_hive_table_schema', true);
                exit(1);
            }
            
            $cols_arr = explode(",", $matches[1]);
            foreach ($cols_arr as $col)
            {
                preg_match("/\W*(\w+)\W+/i", $col, $matches);
                if(empty($matches[1]))
                {
                    $msg="failed to preg_match column name in col:{$col}";
                    Log::log_step($msg, 'parse_hive_table_schema', true);
                    exit(1);
                }
                $partition_name = $matches[1];
                if(!isset($ROW_CALLBACK_PARTITIONS[$partition_name]))
                {
                    $msg='$ROW_CALLBACK_PARTITIONS is different from hive table schema, correct it!';
                    Log::log_step($msg, 'parse_hive_table_schema', true);
                    exit(1);
                }
                self::$hive_partitions[]=$partition_name;
            }
        }
    }

    static protected function id_end()
    {
        global $TABLE_AUTO_INCREMENT_ID;
        global $TABLE;
        
        $ID_END = null;
        try {
            if (empty($TABLE_AUTO_INCREMENT_ID)) {
                $sql = "SELECT COUNT(*) FROM `{$TABLE}`";
                
                $rs = static::$dbh->query($sql);
                $ID_END = $rs->fetchColumn();
                
                $msg = "TABLE_AUTO_INCREMENT_ID is null, ID_END:{$ID_END}";
                Log::log_step($msg, 'id_end');
            } else {
                $sql = "SELECT MAX(`{$TABLE_AUTO_INCREMENT_ID}`) FROM `{$TABLE}`";
                
                $rs = static::$dbh->query($sql);
                $ID_END = $rs->fetchColumn();
                if ($ID_END === null) {
                    $ID_END = 0;
                    $msg = "empty table:{$TABLE}, set ID_END=0, sql:{$sql}";
                    Log::log_step($msg, 'id_end');
                }
                
                $msg = "ID_END:{$ID_END} is selected, sql:{$sql}";
                Log::log_step($msg, 'id_end');
            }
        } catch (\Exception $e) {
            $msg = "failed to query ID_END, exit 1, sql:{$sql}..." . $e->getMessage();
            Log::log_step($msg, 'id_end', true);
            exit(1);
        }
        
        return $ID_END;
    }

    static protected function id_start()
    {
        global $TABLE;
        global $TABLE_AUTO_INCREMENT_ID;
        
        $exportedId_fn = static::$data_dir . $TABLE . '-exportedId';
        $file_str = @file_get_contents($exportedId_fn);
        $lines = explode("\n", $file_str);
        $lines_ct = count($lines);
        // parse last 5 lines
        $ID_START=null;
        for ($i = $lines_ct - 1; $i >= $lines_ct - 5 && $i >= 0; $i --) {
            $line = $lines[$i];
            preg_match('/.+ID<(\d+)/', $line, $matches);
            if (isset($matches[1]) && $matches[1] > $ID_START) {
                $ID_START = $matches[1];
            }
        }
        if($ID_START!==null)
        {
            $msg = "ID_START:{$ID_START} is parsed in line:{$line}";
            Log::log_step($msg, 'id_start');
            return $ID_START;
        }
        
        // first time backup
        if (empty($TABLE_AUTO_INCREMENT_ID)) {
            $ID_START = 0;
            $msg = 'TABLE_AUTO_INCREMENT_ID is null, set ID_START=0';
            Log::log_step($msg, 'id_start');
            return $ID_START;
        } else {
            $sql = "SELECT MIN(`{$TABLE_AUTO_INCREMENT_ID}`) FROM `{$TABLE}`";
            try {
                $rs = static::$dbh->query($sql);
                $ID_START = $rs->fetchColumn();
                if ($ID_START === null) {
                    $ID_START = 0;
                    $msg = "empty table:{$TABLE}, set ID_START=0, sql:{$sql}";
                    Log::log_step($msg, 'id_start');
                }
                
                $msg = "ID_START:{$ID_START} is selected, sql:{$sql}";
                Log::log_step($msg, 'id_start');
                return $ID_START;
            } catch (\Exception $e) {
                $msg = "failed to select min id, sql:{$sql}..." . $e->getMessage();
                Log::log_step($msg, 'id_start', true);
                exit(1);
            }
        }
    }

    static protected function file_buf_to_hive($force = false)
    {
        global $EXPORTED_FILE_BUFFER;
        global $TABLE;
        global $HIVE_TABLE;
        global $HIVE_DB;
        global $HIVE_FORMAT;
        global $ROW_CALLBACK_PARTITIONS;
        
        $EXPORTED_FILE_BUFFER_tmp = 8 * 1024 * 1024 * 1024; //default 8G
        if (!empty($EXPORTED_FILE_BUFFER)) {
            $EXPORTED_FILE_BUFFER_tmp = $EXPORTED_FILE_BUFFER;
        }
        if ($force == false && $EXPORTED_FILE_BUFFER_tmp > self::$exported_to_file_size) {
            return;
        }
        $text_files = glob(static::$data_dir . "/{$TABLE}-data-*");
        
        $hive_format_str = empty($HIVE_FORMAT) ? 'TEXTFILE' : strtoupper($HIVE_FORMAT);
        
        foreach ($text_files as $fn) {
            $o = null;
            $r = null;
            $v_base = basename($fn);
            $__PARTITIONS = substr($v_base, strlen("{$TABLE}-data-"));
            $partition_str = "";
            if (!empty($ROW_CALLBACK_PARTITIONS))
            {
                $partition_str="PARTITION ( {$__PARTITIONS})";
            }
            
            $table0=null;
            if($hive_format_str==='TEXTFILE')
            {
                $table0=$HIVE_TABLE;
            }else
            {
                $table0=$HIVE_TABLE . "__tmp";
            }

            $sql = <<<EOL
USE {$HIVE_DB};
LOAD DATA LOCAL INPATH '{$fn}' INTO TABLE `{$table0}` {$partition_str};
EOL;

            $table1=null;
            if($hive_format_str==='TEXTFILE')
            {
                $table1=null;
            }else
            {
                $table1=$HIVE_TABLE;
            }
            if(!empty($table1))
            {
                $hive_cols_str = '';
                foreach(self::$hive_cols as $k=>$v)
                {
                    if($k!==0)
                        $hive_cols_str .=",";
                        $hive_cols_str.=" `{$v}`";
                }
                $sql .= <<<EOL
INSERT INTO TABLE `{$table1}` {$partition_str} SELECT {$hive_cols_str} FROM `{$table0}`;
TRUNCATE TABLE `{$table0}`;
EOL;
            }
            file_put_contents(static::$data_dir . "{$TABLE}-insert.sql", $sql);
            $exec_str = "hive -f " . __DIR__ . "/data/{$TABLE}-insert.sql";
            Log::log_step("fn:{$fn}", "file_buf_to_hive");
            exec($exec_str, $o, $r);
            if ($r !== 0) {
                $msg = var_export($o, true);
                Log::log_step($msg, 'file_buf_to_hive', true);
                exit(1);
            }
            unlink($fn);
        }
        self::$exported_to_file_size=0;
    }

    static private $exported_to_file_size = 0;
    static protected function export_to_file_buf(Array $rows_new)
    {
        global $TABLE;
        if (count($rows_new) === 0) {
            return;
        }
        $buffer_arr = [];
        foreach ($rows_new as $k => $row) {
            $__PARTITIONS = '';
            if (! empty($row['__PARTITIONS'])) {
                $__PARTITIONS = $row['__PARTITIONS'];
            }
            if (! isset($buffer_arr[$__PARTITIONS])) {
                $buffer_arr[$__PARTITIONS] = '';
            }
            unset($row['__PARTITIONS']);
            $kk_tmp = 0;
            foreach ($row as $kk => $vv) {
                if ($kk_tmp !== 0) {
                    $buffer_arr[$__PARTITIONS] .= "\001";
                } else {
                    $kk_tmp ++;
                }
                if (is_null($vv)) {
                    $buffer_arr[$__PARTITIONS] .= "\N";
                } else {
                    $vv_tmp = str_replace([
                        "\n",
                        "'",
                        "\001"
                    ], [
                        "",
                        "\'",
                        ""
                    ], $vv);
                    $buffer_arr[$__PARTITIONS] .= $vv_tmp;
                }
            }
            $buffer_arr[$__PARTITIONS] .= "\n";
        }
        $buffer_arr_sz = 0;
        foreach ($buffer_arr as $__PARTITIONS => $buffer) {
            $buffer_sz = strlen($buffer);
            $buffer_arr_sz += $buffer_sz;
            $fn = static::$data_dir . "/{$TABLE}-data-{$__PARTITIONS}";
            file_put_contents($fn, $buffer, FILE_APPEND);
        }
        self::$exported_to_file_size += $buffer_arr_sz;
        $rows_new_ct = count($rows_new);
        Log::log_step("rows_new_ct:{$rows_new_ct}, buffer_arr_sz:{$buffer_arr_sz}, exported_to_file_size:" . self::$exported_to_file_size, 'export_to_file_buf');
    }

    static protected function controller_create()
    {
        global $TABLE;
        global $HIVE_DB;
        global $HIVE_TABLE;
        global $argv;
        global $HIVE_FORMAT;
        global $ROW_CALLBACK_PARTITIONS;
        
        $msg = "create hive table:{$HIVE_TABLE}?\nthis will drop old hive table:{$HIVE_TABLE} and  delete all old {$TABLE}'s data files.\ntype (Y/y) for yes, others for no.";
        Log::log_step($msg, 'controller_create');
        $type = fgets(STDIN);
        if (substr($type, 0, 1) === 'Y' || substr($type, 0, 1) === 'y') {
            // delete data files
            $hive_table_cache = static::$data_dir . "{$TABLE}-*";
            $hive_table_cache_files = glob($hive_table_cache);
            $files_text = implode("\n", $hive_table_cache_files);
            foreach ($hive_table_cache_files as $file) {
                @unlink($file);
            }
            $msg = "data files deleted:{$files_text}";
            Log::log_step($msg, 'controller_create');
            
            // DROP hive table
            $o = null;
            $r = null;
            $exec_str = "hive -e 'USE `{$HIVE_DB}`; DROP TABLE IF EXISTS `{$HIVE_TABLE}`; DROP TABLE IF EXISTS `{$HIVE_TABLE}__tmp`' 2>&1";
            exec($exec_str, $o, $r);
            if ($r !== 0) {
                $o_text = implode("\n", $o);
                $msg = "unknow error, exit 1, exec_str:{$exec_str}, exec output:{$o_text}";
                Log::log_step($msg, 'controller_delete', true);
                //exit(1);
            }
            
            $msg = "hive table:{$HIVE_TABLE} dropped...";
            Log::log_step($msg, 'controller_create');
        } else {
            $msg = "typed:{$type} for no, exit..";
            Log::log_step($msg);
            exit(0);
        }
        
        // prepare hive table schema file
        $msg = "generating hive table schema of {$TABLE}...";
        Log::log_step($msg);
        // https://stackoverflow.com/questions/5428262/php-pdo-get-the-columns-name-of-a-table
        $sql = "SELECT * from {$TABLE} LIMIT 1";
        $columns_name = [];
        $colmuns_pdo_type = [];
        try {
            $rs = static::$dbh->query($sql);
            for ($i = 0; $i < $rs->columnCount(); $i ++) {
                $col = $rs->getColumnMeta($i);
                $columns_name[] = $col['name'];
                $colmuns_pdo_type[] = $col['pdo_type'];
            }
        } catch (\Exception $e) {
            $msg = "pdo error, sql:{$sql}, exit 1..." . $e->getMessage();
            Log::log_step($msg, 'controller_create', true);
            exit(1);
        }
        
        if (empty($columns_name)) {
            $msg = "empty column returned, sql:{$sql}, exit 1";
            Log::log_step($msg, 'controller_create', true);
            exit(1);
        }
        
        $columns_str = '';
        foreach ($columns_name as $k => $name) {
            if ($k !== 0) {
                $columns_str .= ",\n";
            }
            // map pdo_type to hive type
            // PDO has only 3 data types, sett http://php.net/manual/en/pdo.constants.php
            $pdo_type = $colmuns_pdo_type[$k];
            $hive_type = '';
            if ($pdo_type === PDO::PARAM_BOOL) {
                $hive_type = ' BOOLEAN';
            } else if ($pdo_type === PDO::PARAM_INT) {
                $hive_type = ' INT';
            } else // NO FLOAT, DECIMAL, TIMESTAMP, BINARY
            {
                $hive_type = ' STRING';
            }
            
            $columns_str .= "`{$name}` {$hive_type}";
        }
        
        $hive_format_str = empty($HIVE_FORMAT) ? 'TEXTFILE' : strtoupper($HIVE_FORMAT);
        $partition_str = '';
        if(!empty($ROW_CALLBACK_PARTITIONS))
        {
            $partition_str .= "\nPARTITIONED BY ( ";
            $idx=0;
            foreach($ROW_CALLBACK_PARTITIONS as $k=>$v)
            {
                if($idx!==0)
                    $partition_str .=", ";
                $idx++;
                $partition_str .= "`{$k}` STRING";
            }
            $partition_str .= " )";
        }
        
        $hive_schema_template = <<<EOL
USE `{$HIVE_DB}`;
CREATE TABLE ``{$HIVE_TABLE}` (
{$columns_str}
){$partition_str}
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\001'
LINES TERMINATED BY '\\n'
STORED AS {$hive_format_str};


EOL;
        if ($hive_format_str !== 'TEXTFILE') // 如果不是TEXTFILE的话就需要创建一个TEXTFILE的tmp表
        {
            $hive_schema_template .= <<<EOL
			
CREATE TABLE `{$HIVE_TABLE}__tmp` (
{$columns_str}
){$partition_str}
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\001'
LINES TERMINATED BY '\\n'
STORED AS TEXTFILE;


EOL;
        }
        
        $hive_schema_fn = static::$data_dir . "/{$TABLE}-schema.sql";
        file_put_contents($hive_schema_fn, $hive_schema_template);
        
        $msg = "hive table schema generated:{$hive_schema_fn}, change it if you need.\nuse {$hive_schema_fn} to create hive table?\ntype (Y/y) for yes, others for no.";
        Log::log_step($msg);


        $type = fgets(STDIN);
        if (substr($type, 0, 1) === 'Y' || substr($type, 0, 1) === 'y') {
            $o = null;
            $r = null;
            $exec_str = "hive -f {$hive_schema_fn} 2>&1";
            exec($exec_str, $o, $r);
            if ($r !== 0) {
                $o_text = implode("\n", $o);
                $msg = "HIVE_TABLE:{$HIVE_TABLE} create failed, exit 1";
                Log::log_step($msg, 'controller_create', true);
                $msg = "exec_str:{$exec_str}, exec output:{$o_text}";
                Log::log_step($msg, 'controller_create', true);
                exit(1);
            } else {
                $msg = "HIVE_TABLE:{$HIVE_TABLE} created";
                Log::log_step($msg);
            }
        } else {
            $msg = "typed:{$type} for no, exit 0";
            Log::log_step($msg);
            exit(0);
        }
        
        $msg = "create done, use `php {$argv[0]} backup` to backup to hive, you can add it to cron.sh for daily backup";
        Log::log_step($msg);
    }

    static protected function check_enter_pressed()
    {
        $read = [
            STDIN
        ];
        $write = [];
        $except = [];
        $result = stream_select($read, $write, $except, 0);
        if ($result === false)
            throw new Exception('stream_select failed');
        if ($result === 0)
            return false;
        $data = stream_get_line(STDIN, 1);
        if (strpos($data, "\n") !== false) {
            return true;
        }
        //TODO:
        //https://stackoverflow.com/questions/21464457/why-stream-select-on-stdin-becomes-blocking-when-cmd-exe-loses-focus
        return false;
    }

    static protected function controller_backup()
    {
        global $TABLE;
        global $TABLE_AUTO_INCREMENT_ID;
        global $ROW_CALLBACK_PARTITIONS;
        global $ROW_CALLBACK_CHANGE;
        global $TABLE_BATCH;
        
        static::parse_hive_table_schema();
        $ID_START = static::id_start();
        $ID_END = static::id_end();
        $ID = $ID_START;
        try {
            while (true) {
                // if ENTER is pressed, stop backup
                $enter_pressed = static::check_enter_pressed();
                if ($enter_pressed) {
                    $msg = "enter is pressed, stopping backup...";
                    Log::log_step($msg, 'enter_pressed');
                    break;
                }
                
                if ($ID >= $ID_END) {
                    $msg = "ID:{$ID} >= ID_END:{$ID_END}, complete";
                    Log::log_step($msg, 'complete');
                    break;
                }
                
                $mem_sz = memory_get_usage();
                $mem_sz_pk = memory_get_peak_usage();
                $BATCH = empty($TABLE_BATCH) ? 1000 : $TABLE_BATCH;
                $msg = "ID:{$ID}, BATCH:{$BATCH}, mem_sz:{$mem_sz}, mem_sz_pk:{$mem_sz_pk}";
                Log::log_step($msg);
                
                $sql = null;
                $ID2 = $ID + $BATCH;
                if($ID2>$ID_END)
                {
                    $ID2=$ID_END;
                }
                if (empty($TABLE_AUTO_INCREMENT_ID)) {
                    $limit_n = $ID2 - $ID;
                    $sql = "SELECT * FROM `{$TABLE}` LIMIT {$ID}, {$limit_n}";
                } else {
                    $sql = "SELECT * FROM `{$TABLE}` WHERE `{$TABLE_AUTO_INCREMENT_ID}`>={$ID} AND `{$TABLE_AUTO_INCREMENT_ID}`<{$ID2}";
                }
                $rs = static::$dbh->query($sql);
                $rows = $rs->fetchAll(PDO::FETCH_ASSOC);
                
                if (count($rows > 0)) {
                    $rows_new = [];
                    // 分区
                    $__PARTITIONS = '';
                    foreach ($rows as $row) {
                        if (! empty($ROW_CALLBACK_PARTITIONS)) {
                            $__PARTITIONS = '';
                            $idx = 0;
                            foreach ($ROW_CALLBACK_PARTITIONS as $partition_name => $callback) {
                                
                                if ($idx !== 0) {
                                    $__PARTITIONS .= ",";
                                }
                                $idx ++;
                                if ($callback instanceof \Closure) {
                                    $__PARTITIONS .= "{$partition_name}='" . $callback($row) . "'";
                                } else {
                                    $__PARTITIONS .= "{$partition_name}='{$callback}'";
                                }
                            }
                        }
                        
                        // 处理行使之和hive格式一致
                        if (! empty($ROW_CALLBACK_CHANGE)) {
                            $row = $ROW_CALLBACK_CHANGE($row);
                        }
                        $row_keys =  array_keys($row);
                        if(count($row_keys)!==count(self::$hive_cols))
                        {
                            $msg="check_row failed, row format is different from hive table, use ROW_CALLBACK_CHANGE to change row, row:" . var_export($row, true) .", hive_cols:" . var_export(self::$hive_cols, true);
                            Log::log_step($msg, 'check_row', true);
                            exit(1);
                        }
                        foreach($row_keys as $k=>$v)
                        {
                            if(self::$hive_cols[$k]!==$v)
                            {
                                $msg="check_row failed, row format is different from hive table, use ROW_CALLBACK_CHANGE to change row, row:" . var_export($row, true) .", hive_cols:" . var_export(self::$hive_cols, true);
                                Log::log_step($msg, 'check_row', true);
                                exit(1);
                            }
                        }
                        
                        if (! empty($__PARTITIONS)) {
                            $row['__PARTITIONS'] = $__PARTITIONS;
                        }
                        
                        $rows_new[] = $row;
                    }
                    static::export_to_file_buf($rows_new);

                    $msg = date('Y-m-d H:i:s') . " ID>={$ID} AND ID<{$ID2}";
                    $exportedId_fn = static::$data_dir . $TABLE . '-exportedId';
                    file_put_contents($exportedId_fn, $msg, FILE_APPEND);
                }
                
                $ID += $BATCH;
                $rs = null;
                $rows = null;
                static::file_buf_to_hive();
            }
        } catch (\Exception $e) {
            $msg = "PDO Exception:" . $e->getMessage();
            Log::log_step($msg, 'PDO', true);
            exit(1);
        } finally 
		{
		    static::file_buf_to_hive(true);
        }
    }

    static public function run()
    {
        static::init();
        
        global $argv;
        $supported_arguments = [
            'create',
            'backup'
        ];
        $arg = empty($argv[1]) ? 'empty' : $argv[1];
        if (! in_array($arg, $supported_arguments)) {
            $msg = <<<EOL
{$arg} is not supported argument。
        
create: generate hive table schema and create it
backup: increment backup to hive
EOL;
            Log::log_step($msg, 'run', true);
            exit(1);
        }
        
        if ($arg === $supported_arguments[0]) {
            static::controller_create();
        } else if ($arg === $supported_arguments[1]) {
            static::controller_backup();
        } else {
            $msg = "{$arg} not supported, exit 1";
            Log::log_step($msg);
            exit(1);
        }
    }
}

// 简单的Log类
class Log
{

    const LOG_MAX = 8 * 1204 * 1024;
 // 8M
    protected static $start = null;

    protected static $log = null;

    private static $log_dir = null;

    private static $app = null;

    static public function setting($app = 'table')
    {
        global $WORK_DIR;
        
        self::$start = time();
        
        static::$log_dir = $WORK_DIR . "/log/";
        if (! file_exists(static::$log_dir)) {
            if (! mkdir(static::$log_dir, 0777, true)) {
                $msg = "Failed to create folder:" . static::$log_dir;
                $fh = fopen('php://stderr', 'a');
                fwrite($fh, $msg);
                fclose($fh);
                exit(1);
            }
        }
        
        $tz = date_default_timezone_get();
        if ($tz === "UTC") // 为设置时区
{
            date_default_timezone_set('Asia/Shanghai');
        }
        
        self::$app = $app;
    }

    static protected function log_file($str, $cate = null)
    {
        $fn = null;
        if (empty($cate)) {
            $fn = static::$log_dir . self::$app . "-all.log";
        } else {
            $fn = static::$log_dir . self::$app . "-{$cate}.log";
        }
        
        file_put_contents($fn, $str, FILE_APPEND);
        
        clearstatcache();
        $filesize = filesize($fn);
        if ($filesize > self::LOG_MAX) {
            
            $old_fn = str_replace('.log', ".old.log", $fn);
            @unlink($old_fn);
            rename($fn, $old_fn);
            $now = time();
            $msg = date('Y-m-d H:i:s', $now) . " [], rotate log file, filesize:{$filesize}\r\n";
            file_put_contents($fn, $msg, FILE_APPEND);
        }
    }

    static public function log_step($message, $cate = null, $stderr = false)
    {
        if (empty(self::$start)) {
            self::setting();
        }
        $now = time();
        $str = date('Y-m-d H:i:s', $now) . " [$cate] {$message}\r\n";
        if ($stderr === false) {
            echo $str;
        } else {
            $fh = fopen('php://stderr', 'a');
            fwrite($fh, $str);
            fclose($fh);
        }
        self::log_file($str);
        if (! empty($cate)) {
            self::log_file($str, $cate);
        }
    }
}

