<?php

class Increment_Backup_To_Hive
{

    protected static $config_arr;

    protected static $data_dir;

    protected static $log_dir;

    protected static $dbh;

    static protected function init()
    {
        global $HIVE_TABLE;
        global $WORK_DIR;

        ini_set('memory_limit', - 1);
        set_time_limit(0);

        Log::setting($HIVE_TABLE);

        $config_path = $WORK_DIR . "/config.ini";
        self::$config_arr = parse_ini_file($config_path);
        if (empty(self::$config_arr)) {
            $msg = "read config error:{$config_path}, exit 1";
            Log::log_step($msg, 'init', true);
            exit(1);
        }

        self::$data_dir = $WORK_DIR . "/data/";
        if (! file_exists(self::$data_dir)) {
            if (! mkdir(self::$data_dir, 0777, true)) {
                $msg = "failed to create folder:" . self::$data_dir;
                Log::log_step($msg, 'init', true);
                exit(1);
            }
        }

        self::$log_dir = $WORK_DIR . "/log/";
        if (! file_exists(self::$log_dir)) {
            if (! mkdir(self::$log_dir, 0777, true)) {
                $msg = "failed to create folder:" . self::$log_dir;
                Log::log_step($msg, 'init', true);
                exit(1);
            }
        }

        $running_lock = self::$data_dir . "{$HIVE_TABLE}-running.pid";
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
            //https://stackoverflow.com/questions/29493197
            ini_set("default_socket_timeout", 2);
            self::$dbh = new PDO(
                self::$config_arr['DB_DSN'],
                self::$config_arr['DB_USER'],
                self::$config_arr['DB_PASSWD'],
                [
                    PDO::ATTR_TIMEOUT => 300,
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                ]
            );
            self::$dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e) {
            $msg = "PDO Connection failed, exit 1... " . $e->getMessage();
            Log::log_step($msg, 'init', true);
            exit(1);
        }
    }

    // 读取建表语句，parse出列字段和分区字段
    protected static $hive_cols;
    protected static $hive_partitions;
    static protected function parse_hive_table_schema()
    {
        global $HIVE_TABLE;
        global $ROW_CALLBACK_PARTITIONS;
        self::$hive_cols =[];
        self::$hive_partitions=[];

        $hive_schema_fn = self::$data_dir . "/{$HIVE_TABLE}-schema.sql";
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

    //decide where to stop backup
    static protected function id_end()
    {
        global $TABLE_AUTO_INCREMENT_ID;
        global $TABLE;

        $ID_END = null;
        try {
            if (empty($TABLE_AUTO_INCREMENT_ID)) {
                $sql = "SELECT COUNT(*) FROM `{$TABLE}`";

                $rs = self::$dbh->query($sql);
                $ID_END = $rs->fetchColumn();

                $msg = "TABLE_AUTO_INCREMENT_ID is null, ID_END:{$ID_END}";
                Log::log_step($msg, 'id_end');
            } else {
                $sql = "SELECT MAX(`{$TABLE_AUTO_INCREMENT_ID}`) FROM `{$TABLE}`";

                $rs = self::$dbh->query($sql);
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

    //decide where to start backup
    static protected function id_start()
    {
        global $HIVE_DB;
        global $HIVE_TABLE;
        global $TABLE;
        global $TABLE_AUTO_INCREMENT_ID;

        $exportedId_fn = self::$data_dir . $HIVE_TABLE . '-exportedId';
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

        //ID_START not parsed in {$exportedId_fn}. Backup for the first time, let's check if there's data in the hive
        $sql = "'USE `{$HIVE_DB}`;SELECT * FROM {$HIVE_TABLE} LIMIT 1;'";
        //重定向stderr到/dev/null，因为要根据stdout有无来判断表是否为空
        $exec_str = "hive -e {$sql} 2>/dev/null";
        $o = null;
        $r = null;
        exec($exec_str, $o, $r);
        if ($r !== 0) {
            $msg = var_export($o, true);
            Log::log_step("exec_str:{$exec_str}, {$msg}", 'id_start', true);
            exit(1);
        }
        if(!empty($o))
        {
            $msg="ID_START not parsed in {$exportedId_fn} means that this is the first time to backup. But you already have data in {$HIVE_DB}.{$HIVE_TABLE}. Maybe the {$exportedId_fn} has last, try to create again to delete old hive table...exit";
            Log::log_step($msg, 'id_start', true);
            exit(1);
        }

        if (empty($TABLE_AUTO_INCREMENT_ID)) {
            $ID_START = 0;
            $msg = 'TABLE_AUTO_INCREMENT_ID is null, set ID_START=0';
            Log::log_step($msg, 'id_start');
            return $ID_START;
        } else {
            $sql = "SELECT MIN(`{$TABLE_AUTO_INCREMENT_ID}`) FROM `{$TABLE}`";
            try {
                $rs = self::$dbh->query($sql);
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

    //把缓存在文件中的数据导入hive
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
        $text_files = glob(self::$data_dir . "/{$HIVE_TABLE}-data-*");
        if(empty($text_files))
            return;
        $hive_format_str = empty($HIVE_FORMAT) ? 'TEXTFILE' : strtoupper($HIVE_FORMAT);
        //if enter is pressed, only import 1 file into hive to save time
        $text_files_batch=[];
        if(static::check_enter_pressed())
        {
            $text_files_batch=[$text_files[0]];
            $msg="enter pressed, only import one file to hive to save time";
            Log::log_step($msg, "file_buf_to_hive");
        }else
        {
            $text_files_batch=$text_files;
        }
        $sql = "";
        if($HIVE_FORMAT)//有RCFile的压缩格式
        {
            $sql .= "SET hive.exec.compress.output=true;" . PHP_EOL;
        }
        $sql .= "USE `{$HIVE_DB}`;" . PHP_EOL;
        foreach ($text_files_batch as $fn) {
            $v_base = basename($fn);
            $fn2 = addslashes($fn);
            $__PARTITIONS = substr($v_base, strlen("{$HIVE_TABLE}-data-"));
            $partition_str = "";
            if (!empty($ROW_CALLBACK_PARTITIONS))
            {
                $partition_str="PARTITION ( {$__PARTITIONS})";
            }

            if($hive_format_str==='TEXTFILE')
            {
                $sql .= <<<EOL
LOAD DATA LOCAL INPATH '{$fn2}' INTO TABLE `{$HIVE_TABLE}` {$partition_str};

EOL;
            }else
            {
                $table_tmp=$HIVE_TABLE . "__tmp";
                $sql .= <<<EOL
TRUNCATE TABLE `{$table_tmp}`;
LOAD DATA LOCAL INPATH '{$fn2}' INTO TABLE `{$table_tmp}` {$partition_str};

EOL;
            }

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
INSERT INTO TABLE `{$table1}` {$partition_str} SELECT {$hive_cols_str} FROM `{$table_tmp}`;
TRUNCATE TABLE `{$table_tmp}`;

EOL;
            }
        }
        file_put_contents(self::$data_dir . "{$HIVE_TABLE}-insert.sql", $sql);
        //这里把hive的stderr重定向到stdout，因为hive会输出很多奇怪的stderr信息影响判断
        $exec_str = "hive -f " . self::$data_dir . "{$HIVE_TABLE}-insert.sql 2>&1";
        $text_files_batch_ct = count($text_files_batch);
        Log::log_step("text_files_batch_ct:{$text_files_batch_ct}, exec_str:{$exec_str}", "file_buf_to_hive");
        $o = null;
        $r = null;
        exec($exec_str, $o, $r);
        if ($r !== 0) {
            $msg = var_export($o, true);
            Log::log_step($msg, 'file_buf_to_hive', true);
            exit(1);
        }
        foreach ($text_files_batch as $fn)
        {
            $fn_sz = filesize($fn);
            unlink($fn);
            $msg="size:{$fn_sz}, unlink {$fn}";
            Log::log_step($msg);
        }
        $exec_output = var_export($o,true);
        Log::log_step("import to hive done, force:{$force}, unlink all {$text_files_batch_ct} text_files_batch files. exec_output:{$exec_output}", "exec_output");
        self::$exported_to_file_size=0;
    }

    static protected $exported_to_file_size = 0;
    //把处理后的行数据按分区存入本地文件缓存
    static protected function export_to_file_buf(Array $rows_new)
    {
        global $HIVE_TABLE;
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
            $fn = self::$data_dir . "/{$HIVE_TABLE}-data-{$__PARTITIONS}";
            //$fn2 = addslashes($fn);
            $res = file_put_contents($fn, $buffer, FILE_APPEND);
            if($res===false)
            {
                Log::log_step("file_put_contents return false, this may be the disk is full, exit...", 'export_to_file_buf', true);
                exit(1);
            }
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

        $msg = "create hive table:{$HIVE_TABLE}?\nthis will drop old hive table:{$HIVE_DB}.{$HIVE_TABLE} and  delete all old {$HIVE_TABLE}'s data files.\ntype (Y/y) for yes, others for no.";
        Log::log_step($msg, 'controller_create');
        $type = fgets(STDIN);
        if (substr($type, 0, 1) === 'Y' || substr($type, 0, 1) === 'y') {
            // delete data files
            $hive_table_cache = self::$data_dir . "{$HIVE_TABLE}-*";
            $hive_table_cache_files = glob($hive_table_cache);
            $files_text = implode("\n", $hive_table_cache_files);
            foreach ($hive_table_cache_files as $file) {
                @unlink($file);
            }
            $msg = "data files deleted:" . PHP_EOL . "{$files_text}" ;
            Log::log_step($msg, 'controller_create');

            // DROP hive table
            $o = null;
            $r = null;
            $exec_str = "hive -e 'CREATE DATABASE IF NOT EXISTS `{$HIVE_DB}`; USE `{$HIVE_DB}`; DROP TABLE IF EXISTS `{$HIVE_TABLE}`; DROP TABLE IF EXISTS `{$HIVE_TABLE}__tmp`' 2>&1";
            exec($exec_str, $o, $r);
            if ($r !== 0) {
                $o_text = implode("\n", $o);
                $msg = "unknow error, exit 1, exec_str:{$exec_str}, exec output:{$o_text}";
                Log::log_step($msg, 'controller_delete', true);
                exit(1);
            }

            $msg = "hive table:{$HIVE_TABLE} dropped...";
            Log::log_step($msg, 'controller_create');
        } else {
            $msg = "typed:{$type} for no, exit..";
            Log::log_step($msg);
            exit(0);
        }

        // prepare hive table schema file
        $msg = "generating hive table schema of {$HIVE_TABLE}...";
        Log::log_step($msg);
        // https://stackoverflow.com/questions/5428262/php-pdo-get-the-columns-name-of-a-table
        $sql = "SELECT * from {$TABLE} LIMIT 1";
        $columns_name = [];
        $colmuns_pdo_type = [];
        try {
            $rs = self::$dbh->query($sql);
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
        $index=0;
        foreach ($columns_name as $k => $name) {
            // map pdo_type to hive type
            // PDO has only 3 data types, sett http://php.net/manual/en/pdo.constants.php
            $pdo_type = $colmuns_pdo_type[$k];
            $hive_type = '';
            if ($pdo_type === PDO::PARAM_BOOL) {
                $hive_type = 'BOOLEAN';
            } else if ($pdo_type === PDO::PARAM_INT) {
                $hive_type = 'INT';
            } else // NO FLOAT, DECIMAL, TIMESTAMP, BINARY
            {
                $hive_type = 'STRING';
            }
            //`__ID`为https://github.com/gouchaoer/Increment_Backup_To_Hive/blob/master/tools/csv2mysql.php为csv文件自动生成自增int主键
            if($name!=='__ID')
            {
                if ($index !== 0) {
                    $columns_str .= ",\n";
                }
                $index++;
                $columns_str .= "`{$name}` {$hive_type}";
            }
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
CREATE TABLE `{$HIVE_TABLE}` (
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

        $hive_schema_fn = self::$data_dir . "/{$HIVE_TABLE}-schema.sql";
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

    //检测回车键是否按下
    static protected  $enter_pressed=false;
    static protected function check_enter_pressed()
    {
        if(self::$enter_pressed===true)
            return true;
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
            self::$enter_pressed=true;
            return true;
        }
        //TODO:
        //https://stackoverflow.com/questions/21464457/why-stream-select-on-stdin-becomes-blocking-when-cmd-exe-loses-focus
        return false;
    }

    static protected function controller_backup()
    {
        global $TABLE;
        global $HIVE_TABLE;
        global $TABLE_AUTO_INCREMENT_ID;
        global $ROW_CALLBACK_PARTITIONS;
        global $ROW_CALLBACK_CHANGE;
        global $TABLE_BATCH;

        static::parse_hive_table_schema();
        $ID_START = static::id_start();
        $ID_END = static::id_end();
        $BATCH = empty($TABLE_BATCH) ? 1000 : $TABLE_BATCH;
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
                $rs = self::$dbh->query($sql);
                if($rs===false)
                {
                    throw new Exception("PDO query return false, sql:{$sql}");
                }
                $rows = $rs->fetchAll(PDO::FETCH_ASSOC);
                $rows_ct = count($rows);
                if ($rows_ct > 0) {
                    $rows_new = [];
                    // 分区
                    $__PARTITIONS = '';
                    foreach ($rows as $index => $row) {
                        if (! empty($ROW_CALLBACK_PARTITIONS)) {
                            $__PARTITIONS = '';
                            $idx = 0;
                            foreach ($ROW_CALLBACK_PARTITIONS as $partition_name => $callback) {

                                if ($idx !== 0) {
                                    $__PARTITIONS .= ",";
                                }
                                $idx ++;
                                $callback_v = $callback instanceof \Closure ? $callback($row) : $callback;
                                if($callback_v===false){
                                    if($BATCH===1)
                                    {
                                        $msg="rows_ct:{$rows_ct}, BATCH:{$BATCH}, For {$index} row:" . substr(var_export($row, true), 0, 256) .
                                            ', $ROW_CALLBACK_PARTITIONS return false, which means that backup should stop here.';
                                        Log::log_step($msg, 'controller_backup');
                                        $msg="break 3 and exit...";
                                        Log::log_step($msg);
                                        break 3;
                                    }else
                                    {
                                        $msg="rows_ct:{$rows_ct}, BATCH:{$BATCH}, For {$index} row:" . substr(var_export($row, true), 0, 256) .
                                            ', $ROW_CALLBACK_PARTITIONS return false, let\'s set BATCH=1 and backup available rows in this batch.';
                                        Log::log_step($msg);
                                        $BATCH =1;
                                        $msg="continue 3 and enter while loop again...";
                                        Log::log_step($msg);
                                        continue 3;
                                    }
                                }
                                if ( empty($callback_v) && $callback !== '0' ) {
                                    $__PARTITIONS .= "{$partition_name}='empty'";//hive partition can't be a empty string
                                } else {
                                    $__PARTITIONS .= "{$partition_name}='{$callback_v}'";
                                }
                            }
                        }

                        // 处理行使之和hive格式一致
                        if (! empty($ROW_CALLBACK_CHANGE)) {
                            $row = $ROW_CALLBACK_CHANGE($row);
                        }
                        //https://github.com/gouchaoer/Increment_Backup_To_Hive/blob/master/tools/csv2mysql.md
                        unset($row['__ID']);
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
                }

                //记录到exportedId文件中，下次备份会从该文件读出上次备份位置
                $msg = date('Y-m-d H:i:s') . " rows_ct:{$rows_ct}, ID>={$ID} AND ID<{$ID2}\n";
                $exportedId_fn = self::$data_dir . $HIVE_TABLE . '-exportedId';
                clearstatcache();
                if(@filesize($exportedId_fn) > Log::LOG_MAX)
                {
                    $old_fn = $exportedId_fn . ".old";
                    @unlink($old_fn);
                    rename($exportedId_fn, $old_fn);
                }
                $res = file_put_contents($exportedId_fn, $msg, FILE_APPEND);
                if($res===false)
                {
                    Log::log_step("file_put_contents return false, this may be the disk is full, exit...", 'export_to_file_buf', true);
                    exit(1);
                }

                $mem_sz = memory_get_usage();
                $msg = "ID:{$ID}, BATCH:{$BATCH}, mem_sz:{$mem_sz}, rows_ct:{$rows_ct}";
                Log::log_step($msg);

                $ID += $BATCH;
                $rs = null;
                $rows = null;
                static::file_buf_to_hive();
            }
        } catch (\Exception $e) {
            $msg = "PDO Exception:" . $e->getMessage();
            Log::log_step($msg, 'PDO', true);
            exit(1);
        }

        static::file_buf_to_hive(true);
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
        }
        Log::log_step("complete, exit...");
    }
}

// 简单的Log类
class Log
{

    const LOG_MAX = 32 * 1204 * 1024;// 32M
    protected static $start = null;

    protected static $log = null;

    private static $log_dir = null;

    private static $app = null;

    static public function setting($app = 'table')
    {
        global $WORK_DIR;

        self::$start = time();

        self::$log_dir = $WORK_DIR . "/log/";
        if (! file_exists(self::$log_dir)) {
            if (! mkdir(self::$log_dir, 0777, true)) {
                $msg = "Failed to create folder:" . self::$log_dir;
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
            $fn = self::$log_dir . self::$app . "-all.log";
        } else {
            $fn = self::$log_dir . self::$app . "-{$cate}.log";
        }

        file_put_contents($fn, $str, FILE_APPEND);

        clearstatcache();
        $filesize = filesize($fn);
        if ($filesize > self::LOG_MAX) {

            $old_fn = str_replace('.log', ".old.log", $fn);
            @unlink($old_fn);
            rename($fn, $old_fn);
            $now = time();
            $msg = date('Y-m-d H:i:s', $now) . " [], rotate log file, filesize:{$filesize}".PHP_EOL;
            file_put_contents($fn, $msg, FILE_APPEND);
        }
    }

    static public function log_step($message, $cate = null, $stderr = false)
    {
        global $ALARM;
        if (empty(self::$start)) {
            self::setting();
        }
        $app = self::$app;
        $now = time();
        $str = date('Y-m-d H:i:s', $now) . " [{$app}][{$cate}] {$message}".PHP_EOL;
        if ($stderr === false)
        {
            echo $str;
        } else
        {
            $fh = fopen('php://stderr', 'a');
            fwrite($fh, $str);
            fclose($fh);
            // an error happend, let's alarmForward
            if(!empty($ALARM))
            {
                $ALARM($str);
            }
        }
        self::log_file($str);
        if (! empty($cate))
        {
            self::log_file($str, $cate);
        }
    }
}

