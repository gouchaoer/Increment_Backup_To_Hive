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
$export_file = $argv[2];
$database = $argv[3];
$table = basename($export_file, '.sql');


/*
|--------------------------------------------------------------------------
| Run initial scan of CSV file.
|--------------------------------------------------------------------------
*/

if (($input = @fopen($import_file, 'r')) != false)
{
    $row = 1;
    while (($fields = fgetcsv($input, 1000, ',')) != false)
    {
        if ($row == 1)
        {
            foreach ($fields as $field)
            {
                $headers[] = strtolower(str_ireplace(' ', '_', $field));
            }
        }
        else
        {
            foreach ($fields as $key=>$value)
            {
                if (!isset($max_field_lengths[$key]))
                {
                    $max_field_lengths[$key] = 0;
                }

                if (strlen($value) > $max_field_lengths[$key])
                {
                    $max_field_lengths[$key] = strlen($value);
                }
                $field++;
            }
        }
        $row++;
    }
    fclose($input);
}
else
{
    echo 'Unable to open file "'.$import_file.'".'."\n";
}


/*
|--------------------------------------------------------------------------
| Build new importable SQL file.
|--------------------------------------------------------------------------
*/

$output = fopen($export_file, 'w');
fwrite($output, "CREATE DATABASE IF NOT EXISTS {$database};\n");
fwrite($output, 'CREATE TABLE `'.$database.'`.`'.$table.'` ('."\n");
fwrite($output, '`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,'."\n");
foreach ($headers as $key=>$header)
{
    fwrite($output, '`'.$header.'` VARCHAR('.$max_field_lengths[$key].') NOT NULL,'."\n");
}
fwrite($output, 'PRIMARY KEY (`id`)'."\n".') DEFAULT CHARACTER SET \'utf8mb4\';'."\n"."\n");
if (($input = @fopen($import_file, 'r')) != false)
{
    $row = 1;
    while (($fields = fgetcsv($input, 1000, ',')) != false)
    {
        if (sizeof($fields) != sizeof($headers))
        {
            echo 'INCORRECT NUMBER OF FIELDS  (search your file for \'\"\' string):';
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