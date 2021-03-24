CREATE TABLE `user` (
  `name` varchar(255) DEFAULT NULL,
  `sex` varchar(255) DEFAULT NULL,
  `age` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `bigdata`.`user`(`name`, `sex`, `age`) VALUES ('11', '123', '23');
INSERT INTO `bigdata`.`user`(`name`, `sex`, `age`) VALUES ('12312', '2433', '345');
INSERT INTO `bigdata`.`user`(`name`, `sex`, `age`) VALUES ('234256', '34r', '12323');
INSERT INTO `bigdata`.`user`(`name`, `sex`, `age`) VALUES ('2342', '454', '544');
INSERT INTO `bigdata`.`user`(`name`, `sex`, `age`) VALUES ('werwer', 'erewrw', '45');

CREATE TABLE `nginx_log` (
  `remoteAddr` varchar(255) DEFAULT NULL,
  `httpXForwardedFor` varchar(255) DEFAULT NULL,
  `timeLocal` varchar(255) DEFAULT NULL,
  `status` longtext,
  `bodyBytesSent` longtext,
  `httpUserAgent` varchar(255) DEFAULT NULL,
  `httpReferer` varchar(255) DEFAULT NULL,
  `requestMethod` varchar(255) DEFAULT NULL,
  `requestTime` varchar(255) DEFAULT NULL,
  `requestUri` varchar(255) DEFAULT NULL,
  `serverProtocol` varchar(255) DEFAULT NULL,
  `requestBody` varchar(255) DEFAULT NULL,
  `httpToken` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `log_xml` (
  `_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `author` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `genre` varchar(255) DEFAULT NULL,
  `price` double(10,2) DEFAULT NULL,
  `publish_date` date DEFAULT NULL,
  `title` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;