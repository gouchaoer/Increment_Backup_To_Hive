#!/bin/bash

yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
yum -y install http://rpms.remirepo.net/enterprise/remi-release-7.rpm
yum --enablerepo=remi -y install php70 php70-php-pdo php70-php-mysqlnd php70-php-mbstring php70-php-xml php70-php-mcrypt
