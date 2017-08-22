#!/bin/bash
/pocketmine/bin/php7/bin/php -v > /dev/null 2>&1
#如果没安装绿色版的pocketmine-php70，安装php到/pocketmine目录
if [ $? -ne 0 ];then
    if [ ! -f "/pocketmine/bin/php7/bin/php" ]; then  
        echo "install pocketmine-php70"
        sudo mkdir /pocketmine
        wget https://coding.net/u/qsalg/p/pocketmine-php70/git/raw/master/pocketmine-php70-64.tar.gz -P /pocketmine
        tar -zxvf /pocketmine/pocketmine-php70-64.tar.gz -C /pocketmine/
    fi
	#检测是否少了libltdl库
    libltdl=`/pocketmine/bin/php7/bin/php -v 2>&1 |grep "libltdl"`
    if [ -n "$libltdl" ];then
	    echo "libltdl not found"
        lsb=`lsb_release -i |grep "CentOS"`
		#centos可以自动安装，别的发行版手动安装
        if [ -n "$lsb" ];then
             echo "install libltdl"
             yum -y install libtool-ltdl
         else
             echo "please install libltdl, exit"
             exit
         fi
    fi

    /pocketmine/bin/php7/bin/php -v > /dev/null 2>&1
    if [ $? -ne 0 ];then
        echo "install pocketmine-php70 failed, exit"
        exit
    fi
fi

#php script
/pocketmine/bin/php7/bin/php<<'PHPSCRIPT'
<?php
echo "pocketmine php version:" . phpversion() . PHP_EOL;

PHPSCRIPT
