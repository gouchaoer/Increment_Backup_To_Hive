#!/bin/sh

#cron执行时注意更新环境变量，使hive和php命令可用
source /etc/profile

#进入本目录
cd `dirname $0`

#添加要备份的表
php test_table1.php backup  >>cron.log 2>>cron_error.log
