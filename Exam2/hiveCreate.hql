CREATE DATABASE IF NOT EXISTS ${hivevar:databaseName};
USE ${hivevar:databaseName};
CREATE TABLE IF NOT EXISTS ${hivevar:tempTableName}(name string,cno int,cname string, grade int) PARTITIONED BY (username String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
CREATE TABLE IF NOT EXISTS ${hivevar:tableName}(name string,cno int,cname string, grade int) PARTITIONED BY (username String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS ORC;