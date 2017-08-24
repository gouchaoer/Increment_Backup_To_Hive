DROP TABLE IF EXISTS `sample_table`;
CREATE TABLE `sample_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_date` int(11) DEFAULT NULL,
  `is_delete` tinyint(4) DEFAULT NULL,
  `msg` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- 转存表中的数据 `sample_table`
--

INSERT INTO `sample_table` (`id`, `created_date`, `updated_date`, `is_delete`, `msg`) VALUES
(NULL, NULL, NULL, NULL, 'msg 1'),
(NULL, NULL, NULL, NULL, 'msg 2'),
(NULL, NULL, NULL, NULL, 'msg 3'),
(NULL, NULL, NULL, NULL, 'msg 4'),
(NULL, NULL, NULL, NULL, 'msg 5'),
(NULL, NULL, NULL, NULL, 'msg 6'),
(NULL, NULL, NULL, NULL, 'msg 7'),
(NULL, NULL, NULL, NULL, 'msg 8'),
(NULL, NULL, NULL, NULL, 'msg 9'),
(NULL, NULL, NULL, NULL, 'msg 10'),
(NULL, NULL, NULL, NULL, 'msg 11'),
(NULL, NULL, NULL, NULL, 'msg 12'),
(NULL, NULL, NULL, NULL, 'msg 13'),
(NULL, NULL, NULL, NULL, 'msg 14'),
(NULL, NULL, NULL, NULL, 'msg 15'),
(NULL, NULL, NULL, NULL, 'msg 16'),
(NULL, NULL, NULL, NULL, 'msg 17'),
(NULL, NULL, NULL, NULL, 'msg 18'),
(NULL, NULL, NULL, NULL, 'msg 19'),
(NULL, NULL, NULL, NULL, 'msg 20'),
(NULL, NULL, NULL, NULL, 'msg 21'),
(NULL, NULL, NULL, NULL, 'msg 22'),
(NULL, NULL, NULL, NULL, 'msg 23'),
(NULL, NULL, NULL, NULL, 'msg 24'),
(NULL, NULL, NULL, NULL, 'msg 25'),
(NULL, NULL, NULL, NULL, 'msg 25'),
(NULL, NULL, NULL, NULL, 'msg 27'),
(NULL, NULL, NULL, NULL, 'msg 28'),
(NULL, NULL, NULL, NULL, 'msg 29'),
(NULL, NULL, NULL, NULL, 'msg 30'),
(NULL, NULL, NULL, NULL, 'msg 31'),
(NULL, NULL, NULL, NULL, 'msg 32');
