#!/bin/sh
source /etc/profile
cd `dirname $0`

d
/pocketmine/bin/php7/bin/php dd.php  >cron.log 2>>cron_error.log
