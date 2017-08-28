CREATE DATABASE IF NOT EXISTS `test_database`;
USE `test_database`;

DROP TABLE IF EXISTS `test_table1`;
CREATE TABLE `test_table1` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_date` int(11) DEFAULT NULL,
  `is_delete` tinyint(4) DEFAULT NULL,
  `msg` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


INSERT INTO `test_table1` (`id`, `created_date`, `updated_date`, `is_delete`, `msg`) VALUES
(NULL, NULL, NULL, NULL, 'msg 1'),
(NULL, NULL, NULL, NULL, 'msg 2'),
(NULL, NULL, NULL, NULL, 'msg 3'),
(NULL, NULL, NULL, NULL, 'msg 4'),
(NULL, NULL, NULL, NULL, 'msg 5'),
(NULL, NULL, NULL, NULL, 'msg 6'),
(NULL, NULL, NULL, NULL, 'msg 7'),
(NULL, NULL, NULL, NULL, 'msg 8'),
(NULL, NULL, NULL, NULL, 'msg 9'),
(NULL, NULL, NULL, NULL, 'msg 10');
